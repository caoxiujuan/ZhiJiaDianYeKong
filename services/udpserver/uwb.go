package udpserver

import (
	"context"
	"fmt"
	"log"
	"sort"

	"gocode/utils"
	"net"
	"time"

	"github.com/tbrandon/mbserver"
)

type UWBCard struct {
	Distance    uint32
	Signal      uint16
	Lightning   uint16
	Alarm       uint16
	MotionState uint16
}

func GetUwb(ctx context.Context) {

	var Station1Pos uint32 = 38
	var Station2Pos uint32 = 104

	UDPAddr, err := net.ResolveUDPAddr("udp", ":2602")
	if err != nil {
		log.Println("ResolveUDP err,err=", err)
		return
	}

	UDPConn, err := net.ListenUDP("udp", UDPAddr)
	if err != nil {
		log.Println("ListenUDP err,err=", err)
		return
	}
	defer UDPConn.Close()
	//log.Println("人员定位UDP开始监听")
	var buffer [512]byte
	var CRC_JY uint16
	var UWBData [2][10]UWBCard
	var UWBDataTime [2][10]int64
	var thisBsStatus []int
	var lastOpenBstime []int64
	var idOpenBs []int64
	var lastDistancelist [2][10]uint32
	var lastCardSupport [10]int

	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		lastOpenBstime = append(lastOpenBstime, 0)
	}
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		idOpenBs = append(idOpenBs, 0)
	}
	tickerTime := time.NewTicker(time.Second * 1) //检验数据过期
	defer tickerTime.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-Ruanbisuo:
			if (time.Now().Unix() - lastOpenBstime[id-1]) > 5 {
				log.Println("控制台", id, "号支架解除软件闭锁")
				BSClose(byte(id))
				BSClose(byte(id))
				BSClose(byte(id))
			}

		case <-tickerTime.C:
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				if (time.Now().Unix()-lastOpenBstime[i]) > 5 && idOpenBs[i] == 1 {
					log.Println("控制台", i+1, "号支架解除软件闭锁")
					BSClose(byte(i + 1))
					BSClose(byte(i + 1))
					BSClose(byte(i + 1))
					idOpenBs[i] = 0
				}
			}

		default:
			//log.Println("人员定位开始读数据")

			err = UDPConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if err != nil {
				log.Println("SetReadDeadline err:", err)
				break
			}
			n, addr, err := UDPConn.ReadFromUDP(buffer[0:])
			if err != nil {

				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					break
				}
				log.Println("ReadFromUDP err,err=", err)
				time.Sleep(100 * time.Millisecond)
				break
			}

			thisBsStatus = []int{}
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				thisBsStatus = append(thisBsStatus, 0)
			}

			fmt.Printf("接收到来自%v的数据,数据为%v\n", addr, buffer[0:n])
			CRC_JY = 0
			for i := 2; i < 21; i++ {
				CRC_JY += uint16(buffer[i])
			}
			CRC_JY = ^CRC_JY
			//log.Println("CRC_JY=", CRC_JY)
			Low8 := byte(CRC_JY & 0xFF)
			High8 := byte((CRC_JY & 0xFF00) >> 8)
			//CRC校验
			if (Low8 == buffer[21]) && (High8 == buffer[22]) {
				//log.Println("开始进行CRC校验：", buffer[0], buffer[1], buffer[2], buffer[3], "对比：", 0xdd, 0x66, 0x17, 0x11)
				//log.Printf("开始进行CRC校验: [%02x %02x %02x %02x] 对比: [%02x %02x %02x %02x]\n", buffer[0], buffer[1], buffer[2], buffer[3], 0xdd, 0x66, 0x17, 0x11)
				//包头+长度+类型判断
				if buffer[0] == 0xdd && buffer[1] == 0x66 && buffer[2] == 0x17 && buffer[3] == 0x11 {
					//1号基站mac地址判断

					log.Printf("IP: %s, 读取到的标签卡ID: [%02x %02x %02x %02x]\n", addr.IP.String(), buffer[8], buffer[9], buffer[10], buffer[11])

					if addr.IP.String() == "172.16.0.99" {
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //1号基站1号标签卡

							lastDistancelist[0][0] = UWBData[0][0].Distance

							UWBData[0][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][0].Lightning = uint16(buffer[18])
							UWBData[0][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //1号基站2号标签卡

							lastDistancelist[0][1] = UWBData[0][1].Distance

							UWBData[0][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][1].Lightning = uint16(buffer[18])
							UWBData[0][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][1] = time.Now().Unix()

						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //1号基站3号标签卡

							lastDistancelist[0][2] = UWBData[0][2].Distance

							UWBData[0][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][2].Lightning = uint16(buffer[18])
							UWBData[0][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][2] = time.Now().Unix()

						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //1号基站4号标签卡

							lastDistancelist[0][3] = UWBData[0][3].Distance

							UWBData[0][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][3].Lightning = uint16(buffer[18])
							UWBData[0][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][3] = time.Now().Unix()

						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //1号基站5号标签卡

							lastDistancelist[0][4] = UWBData[0][4].Distance

							UWBData[0][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][4].Lightning = uint16(buffer[18])
							UWBData[0][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][4] = time.Now().Unix()

						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //1号基站6号标签卡

							lastDistancelist[0][5] = UWBData[0][5].Distance

							UWBData[0][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][5].Lightning = uint16(buffer[18])
							UWBData[0][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //1号基站7号标签卡

							lastDistancelist[0][6] = UWBData[0][6].Distance

							UWBData[0][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][6].Lightning = uint16(buffer[18])
							UWBData[0][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //1号基站8号标签卡

							lastDistancelist[0][7] = UWBData[0][7].Distance

							UWBData[0][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][7].Lightning = uint16(buffer[18])
							UWBData[0][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //1号基站9号标签卡

							lastDistancelist[0][8] = UWBData[0][8].Distance

							UWBData[0][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][8].Lightning = uint16(buffer[18])
							UWBData[0][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //1号基站10号标签卡

							lastDistancelist[0][9] = UWBData[0][9].Distance

							UWBData[0][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][9].Lightning = uint16(buffer[18])
							UWBData[0][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][9] = time.Now().Unix()

						}
						//2号基站mac地址判断
					} else if addr.IP.String() == "172.16.0.100" {
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //2号基站1号标签卡

							lastDistancelist[1][0] = UWBData[1][0].Distance

							UWBData[1][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][0].Lightning = uint16(buffer[18])
							UWBData[1][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //2号基站2号标签卡

							lastDistancelist[1][1] = UWBData[1][1].Distance

							UWBData[1][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][1].Lightning = uint16(buffer[18])
							UWBData[1][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][1] = time.Now().Unix()
						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //2号基站3号标签卡

							lastDistancelist[1][2] = UWBData[1][2].Distance

							UWBData[1][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][2].Lightning = uint16(buffer[18])
							UWBData[1][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][2] = time.Now().Unix()
						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //2号基站4号标签卡

							lastDistancelist[1][3] = UWBData[1][3].Distance

							UWBData[1][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][3].Lightning = uint16(buffer[18])
							UWBData[1][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][3] = time.Now().Unix()
						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //2号基站5号标签卡

							lastDistancelist[1][4] = UWBData[1][4].Distance

							UWBData[1][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][4].Lightning = uint16(buffer[18])
							UWBData[1][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][4] = time.Now().Unix()
						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //2号基站6号标签卡

							lastDistancelist[1][5] = UWBData[1][5].Distance

							UWBData[1][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][5].Lightning = uint16(buffer[18])
							UWBData[1][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //2号基站7号标签卡

							lastDistancelist[1][6] = UWBData[1][6].Distance

							UWBData[1][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][6].Lightning = uint16(buffer[18])
							UWBData[1][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //2号基站8号标签卡

							lastDistancelist[1][7] = UWBData[1][7].Distance

							UWBData[1][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][7].Lightning = uint16(buffer[18])
							UWBData[1][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //2号基站9号标签卡

							lastDistancelist[1][8] = UWBData[1][8].Distance

							UWBData[1][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][8].Lightning = uint16(buffer[18])
							UWBData[1][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //2号基站10号标签卡

							lastDistancelist[1][9] = UWBData[1][9].Distance

							UWBData[1][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][9].Lightning = uint16(buffer[18])
							UWBData[1][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][9] = time.Now().Unix()
						}
					}

				}
			}

			log.Printf("1号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][0].Distance, lastDistancelist[0][0], UWBData[0][0].Signal, UWBData[0][0].Lightning)
			log.Printf("1号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][1].Distance, lastDistancelist[0][1], UWBData[0][1].Signal, UWBData[0][1].Lightning)
			log.Printf("1号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][2].Distance, lastDistancelist[0][2], UWBData[0][2].Signal, UWBData[0][2].Lightning)
			log.Printf("1号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][3].Distance, lastDistancelist[0][3], UWBData[0][3].Signal, UWBData[0][3].Lightning)
			log.Printf("1号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][4].Distance, lastDistancelist[0][4], UWBData[0][4].Signal, UWBData[0][4].Lightning)
			log.Printf("1号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][5].Distance, lastDistancelist[0][5], UWBData[0][5].Signal, UWBData[0][5].Lightning)
			log.Printf("1号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][6].Distance, lastDistancelist[0][6], UWBData[0][6].Signal, UWBData[0][6].Lightning)
			log.Printf("1号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][7].Distance, lastDistancelist[0][7], UWBData[0][7].Signal, UWBData[0][7].Lightning)
			log.Printf("1号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][8].Distance, lastDistancelist[0][8], UWBData[0][8].Signal, UWBData[0][8].Lightning)
			log.Printf("1号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][9].Distance, lastDistancelist[0][9], UWBData[0][9].Signal, UWBData[0][9].Lightning)

			log.Printf("2号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][0].Distance, lastDistancelist[1][0], UWBData[1][0].Signal, UWBData[1][0].Lightning)
			log.Printf("2号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][1].Distance, lastDistancelist[1][1], UWBData[1][1].Signal, UWBData[1][1].Lightning)
			log.Printf("2号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][2].Distance, lastDistancelist[1][2], UWBData[1][2].Signal, UWBData[1][2].Lightning)
			log.Printf("2号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][3].Distance, lastDistancelist[1][3], UWBData[1][3].Signal, UWBData[1][3].Lightning)
			log.Printf("2号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][4].Distance, lastDistancelist[1][4], UWBData[1][4].Signal, UWBData[1][4].Lightning)
			log.Printf("2号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][5].Distance, lastDistancelist[1][5], UWBData[1][5].Signal, UWBData[1][5].Lightning)
			log.Printf("2号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][6].Distance, lastDistancelist[1][6], UWBData[1][6].Signal, UWBData[1][6].Lightning)
			log.Printf("2号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][7].Distance, lastDistancelist[1][7], UWBData[1][7].Signal, UWBData[1][7].Lightning)
			log.Printf("2号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][8].Distance, lastDistancelist[1][8], UWBData[1][8].Signal, UWBData[1][8].Lightning)
			log.Printf("2号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][9].Distance, lastDistancelist[1][9], UWBData[1][9].Signal, UWBData[1][9].Lightning)

			var CardSupport [10]int

			for i := 0; i < 10; i++ {
				//log.Println("开始计算每个标签卡所处位置")
				now := time.Now().Unix()
				if (now-UWBDataTime[0][i]) < 5 && (now-UWBDataTime[1][i]) < 5 {
					log.Println(i+1, "号标签卡数据有效，两个基站数据都在5秒内进行逻辑处理")
					if UWBData[0][i].Distance > (Station2Pos-Station1Pos)*175 || UWBData[1][i].Distance > (Station2Pos-Station1Pos)*175 {
						if UWBData[1][i].Distance > UWBData[0][i].Distance {
							CardSupport[i] = int((Station1Pos*175 - UWBData[0][i].Distance) / 175)
						} else if UWBData[0][i].Distance > UWBData[1][i].Distance {
							CardSupport[i] = int((UWBData[1][i].Distance + (Station2Pos * 175)) / 175)
						}
					} else if UWBData[0][i].Distance < (Station2Pos-Station1Pos)*175 && UWBData[1][i].Distance < (Station2Pos-Station1Pos)*175 {
						if UWBData[1][i].Distance > UWBData[0][i].Distance {
							CardSupport[i] = int((UWBData[0][i].Distance + (Station1Pos * 175)) / 175)
						} else if UWBData[0][i].Distance > UWBData[1][i].Distance {
							CardSupport[i] = int((Station2Pos*175 - UWBData[1][i].Distance) / 175)
						} else if UWBData[0][i].Distance == UWBData[1][i].Distance {
							CardSupport[i] = int((Station1Pos + Station2Pos) / 2)
						}
					}
					log.Println(i+1, "号标签卡在", CardSupport[i], "架，两个基站数据都有效")
				} else if (now-UWBDataTime[0][i]) < 5 && (now-UWBDataTime[1][i]) > 5 {
					//基站1的数据有效，基站2的数据失效

					currentDistance := UWBData[0][i].Distance
					if currentDistance < 250 && lastCardSupport[i] > int(Station1Pos-2) && lastCardSupport[i] < int(Station1Pos+2) {
						CardSupport[i] = int(Station1Pos)
					} else if lastCardSupport[i] > int(Station1Pos) || (lastCardSupport[i] == int(Station1Pos) && lastDistancelist[0][i] < currentDistance) {
						CardSupport[i] = int((currentDistance + (Station1Pos * 175)) / 175)
					} else {
						CardSupport[i] = int((Station1Pos*175 - currentDistance) / 175)
					}

					log.Println(i+1, "号标签卡在", CardSupport[i], "架，一号基站数据有效，二号失效,上一次标签卡在：", lastCardSupport)

				} else if (now-UWBDataTime[0][i]) > 5 && (now-UWBDataTime[1][i]) < 5 {

					currentDistance := UWBData[1][i].Distance
					if currentDistance < 250 && lastCardSupport[i] > int(Station2Pos-2) && lastCardSupport[i] < int(Station2Pos+2) {
						CardSupport[i] = int(Station2Pos)
					} else if lastCardSupport[i] > int(Station2Pos) || (lastCardSupport[i] == int(Station2Pos) && lastDistancelist[1][i] < currentDistance) {
						CardSupport[i] = int((currentDistance + (Station2Pos * 175)) / 175)
					} else {
						CardSupport[i] = int((Station2Pos*175 - currentDistance) / 175)
					}

					log.Println(i+1, "号标签卡在", CardSupport[i], "架，二号基站数据有效，一号失效,上一次标签卡在：", lastCardSupport)
				}
			}

			for i := 0; i < 10; i++ { //过滤超出工作面范围的无效数据

				if CardSupport[i] > utils.Conf.SYSTEM.SupportNum || CardSupport[i] < 0 {
					CardSupport[i] = 0
					lastDistancelist[0][i] = 0
					lastDistancelist[1][i] = 0
				}
				lastCardSupport[i] = CardSupport[i]
			}
			//维护闭锁列表
			for i := 0; i < 10; i++ {
				//log.Println("进入维护闭锁列表方法")
				if CardSupport[i] != 0 {
					log.Println(i+1, "闭锁,根据标签卡位置更新闭锁状态与时间，用于后期逻辑判断")
					pos := CardSupport[i]
					for supportID := pos - 2; supportID <= pos+2; supportID++ {
						if supportID < 1 || supportID > utils.Conf.SYSTEM.SupportNum {
							continue
						}
						idx := supportID - 1
						thisBsStatus[idx] = 1
						lastOpenBstime[idx] = time.Now().Unix()
					}
				}
			}
			log.Println("uwb基站给出一次数据后", thisBsStatus, "")
			for i := 0; i < len(thisBsStatus); i++ {
				if thisBsStatus[i] == 1 { //按下软件闭锁
					idOpenBs[i] = 1
					log.Println(i+1, "号支架按下软件闭锁")
					BSOpen(byte(i + 1))
					BSOpen(byte(i + 1))
					BSOpen(byte(i + 1))
				}
			}

		}
	}
}

func GetUwb_Three(ctx context.Context, mb *mbserver.Server) {
	fmt.Println("人员定位UDP开始")
	var Station1Pos uint32 = 38
	var Station2Pos uint32 = 104
	var Station3Pos uint32 = 170
	UDPAddr, err := net.ResolveUDPAddr("udp", ":2602")
	if err != nil {
		log.Println("ResolveUDP err,err=", err)
		return
	}

	UDPConn, err := net.ListenUDP("udp", UDPAddr)
	if err != nil {
		log.Println("ListenUDP err,err=", err)
		return
	}
	defer UDPConn.Close()
	log.Println("人员定位UDP开始监听")
	var buffer [512]byte
	var CRC_JY uint16
	var UWBData [3][10]UWBCard
	var UWBDataTime [3][10]int64
	var thisBsStatus []int
	var lastOpenBstime []int64
	var idOpenBs []int64
	var lastDistancelist [3][10]uint32
	var lastCardSupport [10]int
	var uwbpos []uint16

	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		lastOpenBstime = append(lastOpenBstime, 0)
	}
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		idOpenBs = append(idOpenBs, 0)
	}
	tickerTime := time.NewTicker(time.Second * 1) //检验数据过期
	defer tickerTime.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-Ruanbisuo:
			if id < 1 || id > utils.Conf.SYSTEM.SupportNum {
				log.Println("控制台", id, "号支架不在有效范围内")
				continue
			}
			if (time.Now().Unix() - lastOpenBstime[id-1]) > 5 {
				log.Println("控制台", id, "号支架解除软件闭锁")
				BSClose(byte(id))
				BSClose(byte(id))
				BSClose(byte(id))
			}

		case <-tickerTime.C:
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				if (time.Now().Unix()-lastOpenBstime[i]) > 5 && idOpenBs[i] == 1 {
					log.Println("控制台", i+1, "号支架解除软件闭锁")
					BSClose(byte(i + 1))
					BSClose(byte(i + 1))
					BSClose(byte(i + 1))
					idOpenBs[i] = 0
				}
			}

		default:
			//log.Println("人员定位开始读数据")

			err = UDPConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if err != nil {
				log.Println("SetReadDeadline err:", err)
				break
			}
			n, addr, err := UDPConn.ReadFromUDP(buffer[0:])
			if err != nil {

				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					break
				}
				log.Println("ReadFromUDP err,err=", err)
				time.Sleep(100 * time.Millisecond)
				break
			}

			thisBsStatus = []int{}
			uwbpos = []uint16{}
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				thisBsStatus = append(thisBsStatus, 0)
				uwbpos = append(uwbpos, 0)
			}

			log.Printf("接收到来自%v的数据,数据为%v\n", addr, buffer[0:n])
			if n < 23 {
				log.Printf("UWB数据长度异常: n=%d, addr=%v, data=%v", n, addr, buffer[:n])
				continue
			}
			CRC_JY = 0
			for i := 2; i < 21; i++ {
				CRC_JY += uint16(buffer[i])
			}
			CRC_JY = ^CRC_JY
			//log.Println("CRC_JY=", CRC_JY)
			Low8 := byte(CRC_JY & 0xFF)
			High8 := byte((CRC_JY & 0xFF00) >> 8)
			//CRC校验
			if (Low8 == buffer[21]) && (High8 == buffer[22]) {
				//fmt.Println("开始进行CRC校验：", buffer[0], buffer[1], buffer[2], buffer[3], "对比：", 0xdd, 0x66, 0x17, 0x11)
				//log.Printf("开始进行CRC校验: [%02x %02x %02x %02x] 对比: [%02x %02x %02x %02x]\n", buffer[0], buffer[1], buffer[2], buffer[3], 0xdd, 0x66, 0x17, 0x11)
				//包头+长度+类型判断
				if buffer[0] == 0xdd && buffer[1] == 0x66 && buffer[2] == 0x17 && buffer[3] == 0x11 {
					//1号基站mac地址判断

					log.Printf("IP: %s, 读取到的标签卡ID: [%02x %02x %02x %02x]\n", addr.IP.String(), buffer[8], buffer[9], buffer[10], buffer[11])

					if addr.IP.String() == "192.168.12.187" {
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //1号基站1号标签卡

							lastDistancelist[0][0] = UWBData[0][0].Distance

							UWBData[0][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][0].Lightning = uint16(buffer[18])
							UWBData[0][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //1号基站2号标签卡

							lastDistancelist[0][1] = UWBData[0][1].Distance

							UWBData[0][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][1].Lightning = uint16(buffer[18])
							UWBData[0][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][1] = time.Now().Unix()

						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //1号基站3号标签卡

							lastDistancelist[0][2] = UWBData[0][2].Distance

							UWBData[0][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][2].Lightning = uint16(buffer[18])
							UWBData[0][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][2] = time.Now().Unix()

						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //1号基站4号标签卡

							lastDistancelist[0][3] = UWBData[0][3].Distance

							UWBData[0][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][3].Lightning = uint16(buffer[18])
							UWBData[0][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][3] = time.Now().Unix()

						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //1号基站5号标签卡

							lastDistancelist[0][4] = UWBData[0][4].Distance

							UWBData[0][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][4].Lightning = uint16(buffer[18])
							UWBData[0][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][4] = time.Now().Unix()

						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //1号基站6号标签卡

							lastDistancelist[0][5] = UWBData[0][5].Distance

							UWBData[0][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][5].Lightning = uint16(buffer[18])
							UWBData[0][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //1号基站7号标签卡

							lastDistancelist[0][6] = UWBData[0][6].Distance

							UWBData[0][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][6].Lightning = uint16(buffer[18])
							UWBData[0][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //1号基站8号标签卡

							lastDistancelist[0][7] = UWBData[0][7].Distance

							UWBData[0][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][7].Lightning = uint16(buffer[18])
							UWBData[0][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //1号基站9号标签卡

							lastDistancelist[0][8] = UWBData[0][8].Distance

							UWBData[0][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][8].Lightning = uint16(buffer[18])
							UWBData[0][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //1号基站10号标签卡

							lastDistancelist[0][9] = UWBData[0][9].Distance

							UWBData[0][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][9].Lightning = uint16(buffer[18])
							UWBData[0][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][9] = time.Now().Unix()

						}
						//2号基站mac地址判断
					} else if addr.IP.String() == "172.16.0.100" {
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //2号基站1号标签卡

							lastDistancelist[1][0] = UWBData[1][0].Distance

							UWBData[1][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][0].Lightning = uint16(buffer[18])
							UWBData[1][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //2号基站2号标签卡

							lastDistancelist[1][1] = UWBData[1][1].Distance

							UWBData[1][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][1].Lightning = uint16(buffer[18])
							UWBData[1][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][1] = time.Now().Unix()
						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //2号基站3号标签卡

							lastDistancelist[1][2] = UWBData[1][2].Distance

							UWBData[1][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][2].Lightning = uint16(buffer[18])
							UWBData[1][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][2] = time.Now().Unix()
						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //2号基站4号标签卡

							lastDistancelist[1][3] = UWBData[1][3].Distance

							UWBData[1][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][3].Lightning = uint16(buffer[18])
							UWBData[1][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][3] = time.Now().Unix()
						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //2号基站5号标签卡

							lastDistancelist[1][4] = UWBData[1][4].Distance

							UWBData[1][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][4].Lightning = uint16(buffer[18])
							UWBData[1][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][4] = time.Now().Unix()
						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //2号基站6号标签卡

							lastDistancelist[1][5] = UWBData[1][5].Distance

							UWBData[1][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][5].Lightning = uint16(buffer[18])
							UWBData[1][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //2号基站7号标签卡

							lastDistancelist[1][6] = UWBData[1][6].Distance

							UWBData[1][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][6].Lightning = uint16(buffer[18])
							UWBData[1][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //2号基站8号标签卡

							lastDistancelist[1][7] = UWBData[1][7].Distance

							UWBData[1][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][7].Lightning = uint16(buffer[18])
							UWBData[1][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //2号基站9号标签卡

							lastDistancelist[1][8] = UWBData[1][8].Distance

							UWBData[1][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][8].Lightning = uint16(buffer[18])
							UWBData[1][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //2号基站10号标签卡

							lastDistancelist[1][9] = UWBData[1][9].Distance

							UWBData[1][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][9].Lightning = uint16(buffer[18])
							UWBData[1][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][9] = time.Now().Unix()
						}
					} else if addr.IP.String() == "172.16.0.101" { //3号基站mac地址判断
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //3号基站1号标签卡

							lastDistancelist[2][0] = UWBData[2][0].Distance

							UWBData[2][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][0].Lightning = uint16(buffer[18])
							UWBData[2][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //3号基站2号标签卡

							lastDistancelist[2][1] = UWBData[2][1].Distance

							UWBData[2][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][1].Lightning = uint16(buffer[18])
							UWBData[2][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][1] = time.Now().Unix()
						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //3号基站3号标签卡

							lastDistancelist[2][2] = UWBData[2][2].Distance

							UWBData[2][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][2].Lightning = uint16(buffer[18])
							UWBData[2][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][2] = time.Now().Unix()
						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //3号基站4号标签卡

							lastDistancelist[2][3] = UWBData[2][3].Distance

							UWBData[2][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][3].Lightning = uint16(buffer[18])
							UWBData[2][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][3] = time.Now().Unix()
						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //3号基站5号标签卡

							lastDistancelist[2][4] = UWBData[2][4].Distance

							UWBData[2][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][4].Lightning = uint16(buffer[18])
							UWBData[2][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][4] = time.Now().Unix()
						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //3号基站6号标签卡

							lastDistancelist[2][5] = UWBData[2][5].Distance

							UWBData[2][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][5].Lightning = uint16(buffer[18])
							UWBData[2][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //3号基站7号标签卡

							lastDistancelist[2][6] = UWBData[2][6].Distance

							UWBData[2][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][6].Lightning = uint16(buffer[18])
							UWBData[2][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //3号基站8号标签卡

							lastDistancelist[2][7] = UWBData[2][7].Distance

							UWBData[2][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][7].Lightning = uint16(buffer[18])
							UWBData[2][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //3号基站9号标签卡

							lastDistancelist[2][8] = UWBData[2][8].Distance

							UWBData[2][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][8].Lightning = uint16(buffer[18])
							UWBData[2][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //3号基站10号标签卡

							lastDistancelist[2][9] = UWBData[2][9].Distance

							UWBData[2][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][9].Lightning = uint16(buffer[18])
							UWBData[2][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][9] = time.Now().Unix()
						}
					}

				}
			}

			log.Printf("1号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][0].Distance, lastDistancelist[0][0], UWBData[0][0].Signal, UWBData[0][0].Lightning)
			log.Printf("1号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][1].Distance, lastDistancelist[0][1], UWBData[0][1].Signal, UWBData[0][1].Lightning)
			log.Printf("1号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][2].Distance, lastDistancelist[0][2], UWBData[0][2].Signal, UWBData[0][2].Lightning)
			log.Printf("1号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][3].Distance, lastDistancelist[0][3], UWBData[0][3].Signal, UWBData[0][3].Lightning)
			log.Printf("1号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][4].Distance, lastDistancelist[0][4], UWBData[0][4].Signal, UWBData[0][4].Lightning)
			log.Printf("1号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][5].Distance, lastDistancelist[0][5], UWBData[0][5].Signal, UWBData[0][5].Lightning)
			log.Printf("1号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][6].Distance, lastDistancelist[0][6], UWBData[0][6].Signal, UWBData[0][6].Lightning)
			log.Printf("1号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][7].Distance, lastDistancelist[0][7], UWBData[0][7].Signal, UWBData[0][7].Lightning)
			log.Printf("1号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][8].Distance, lastDistancelist[0][8], UWBData[0][8].Signal, UWBData[0][8].Lightning)
			log.Printf("1号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][9].Distance, lastDistancelist[0][9], UWBData[0][9].Signal, UWBData[0][9].Lightning)

			log.Printf("2号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][0].Distance, lastDistancelist[1][0], UWBData[1][0].Signal, UWBData[1][0].Lightning)
			log.Printf("2号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][1].Distance, lastDistancelist[1][1], UWBData[1][1].Signal, UWBData[1][1].Lightning)
			log.Printf("2号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][2].Distance, lastDistancelist[1][2], UWBData[1][2].Signal, UWBData[1][2].Lightning)
			log.Printf("2号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][3].Distance, lastDistancelist[1][3], UWBData[1][3].Signal, UWBData[1][3].Lightning)
			log.Printf("2号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][4].Distance, lastDistancelist[1][4], UWBData[1][4].Signal, UWBData[1][4].Lightning)
			log.Printf("2号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][5].Distance, lastDistancelist[1][5], UWBData[1][5].Signal, UWBData[1][5].Lightning)
			log.Printf("2号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][6].Distance, lastDistancelist[1][6], UWBData[1][6].Signal, UWBData[1][6].Lightning)
			log.Printf("2号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][7].Distance, lastDistancelist[1][7], UWBData[1][7].Signal, UWBData[1][7].Lightning)
			log.Printf("2号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][8].Distance, lastDistancelist[1][8], UWBData[1][8].Signal, UWBData[1][8].Lightning)
			log.Printf("2号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][9].Distance, lastDistancelist[1][9], UWBData[1][9].Signal, UWBData[1][9].Lightning)

			log.Printf("3号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][0].Distance, lastDistancelist[2][0], UWBData[2][0].Signal, UWBData[2][0].Lightning)
			log.Printf("3号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][1].Distance, lastDistancelist[2][1], UWBData[2][1].Signal, UWBData[2][1].Lightning)
			log.Printf("3号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][2].Distance, lastDistancelist[2][2], UWBData[2][2].Signal, UWBData[2][2].Lightning)
			log.Printf("3号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][3].Distance, lastDistancelist[2][3], UWBData[2][3].Signal, UWBData[2][3].Lightning)
			log.Printf("3号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][4].Distance, lastDistancelist[2][4], UWBData[2][4].Signal, UWBData[2][4].Lightning)
			log.Printf("3号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][5].Distance, lastDistancelist[2][5], UWBData[2][5].Signal, UWBData[2][5].Lightning)
			log.Printf("3号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][6].Distance, lastDistancelist[2][6], UWBData[2][6].Signal, UWBData[2][6].Lightning)
			log.Printf("3号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][7].Distance, lastDistancelist[2][7], UWBData[2][7].Signal, UWBData[2][7].Lightning)
			log.Printf("3号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][8].Distance, lastDistancelist[2][8], UWBData[2][8].Signal, UWBData[2][8].Lightning)
			log.Printf("3号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][9].Distance, lastDistancelist[2][9], UWBData[2][9].Signal, UWBData[2][9].Lightning)

			var CardSupport [10]int

			for i := 0; i < 10; i++ {
				//log.Println("开始计算每个标签卡所处位置")
				now := time.Now().Unix()

				validStations := []int{}
				for bs := 0; bs < 3; bs++ {
					if now-UWBDataTime[bs][i] < 5 {
						validStations = append(validStations, bs) //填充基站id
					}
				}

				switch len(validStations) {
				case 0:
					// 无有效数据，保持原位置或置0
					CardSupport[i] = 0
					log.Println(i+1, "号标签卡无有效基站数据")

				case 1:
					// 单基站有效，使用单基站估计算法
					bs := validStations[0]
					var stationPos uint32
					switch bs {
					case 0:
						stationPos = Station1Pos
					case 1:
						stationPos = Station2Pos
					case 2:
						stationPos = Station3Pos
					}
					currentDistance := UWBData[bs][i].Distance
					lastPos := lastCardSupport[i]
					lastDist := lastDistancelist[bs][i]
					// 单基站定位：若距离很小且上次位置靠近该基站，则直接置为基站位置
					if currentDistance < 250 && lastPos > int(stationPos-2) && lastPos < int(stationPos+2) {
						CardSupport[i] = int(stationPos)
					} else if lastPos > int(stationPos) || (lastPos == int(stationPos) && lastDist < currentDistance) {
						// 标签卡在基站右侧（更远处）
						CardSupport[i] = int((currentDistance + stationPos*175) / 175)
					} else {
						// 标签卡在基站左侧
						CardSupport[i] = int((stationPos*175 - currentDistance) / 175)
					}
					log.Printf("%d号标签卡在 %d 架，仅基站%d有效，上一次位置：%d", i+1, CardSupport[i], bs+1, lastPos)

				case 2:
					// 双基站有效，复用原有双基站逻辑，动态传入两个基站的位置和距离
					bs1, bs2 := validStations[0], validStations[1]
					var pos1, pos2 uint32
					switch bs1 {
					case 0:
						pos1 = Station1Pos
					case 1:
						pos1 = Station2Pos
					case 2:
						pos1 = Station3Pos
					}
					switch bs2 {
					case 0:
						pos2 = Station1Pos
					case 1:
						pos2 = Station2Pos
					case 2:
						pos2 = Station3Pos
					}
					// 确保 pos1 < pos2，便于计算
					if pos1 > pos2 {
						pos1, pos2 = pos2, pos1
						bs1, bs2 = bs2, bs1
					}
					d1 := UWBData[bs1][i].Distance
					d2 := UWBData[bs2][i].Distance
					deltaPos := int(pos2 - pos1)
					threshold := deltaPos * 175

					if d1 > uint32(threshold) || d2 > uint32(threshold) {
						if d2 > d1 {
							CardSupport[i] = int((int(pos1)*175 - int(d1)) / 175)
						} else {
							CardSupport[i] = int((int(d2) + int(pos2)*175) / 175)
						}
					} else if d1 <= uint32(threshold) && d2 <= uint32(threshold) {
						if d2 > d1 {
							CardSupport[i] = int((int(d1) + int(pos1)*175) / 175)
						} else if d1 > d2 {
							CardSupport[i] = int((int(pos2)*175 - int(d2)) / 175)
						} else {
							CardSupport[i] = int((pos1 + pos2) / 2)
						}
					}
					log.Printf("%d号标签卡在 %d 架，基站%d和%d数据有效", i+1, CardSupport[i], bs1+1, bs2+1)

				case 3:
					// 三个基站都有效：选择距离最小的两个基站进行双基站定位，提高稳定性
					type distIdx struct {
						dist uint32
						idx  int
						pos  uint32
					}
					stations := []distIdx{
						{UWBData[0][i].Distance, 0, Station1Pos},
						{UWBData[1][i].Distance, 1, Station2Pos},
						{UWBData[2][i].Distance, 2, Station3Pos},
					}
					// 按距离升序排序
					sort.Slice(stations, func(a, b int) bool {
						return stations[a].dist < stations[b].dist
					})
					// 取最近的两个基站进行双基站定位
					bsA, bsB := stations[0].idx, stations[1].idx
					var posA, posB uint32
					switch bsA {
					case 0:
						posA = Station1Pos
					case 1:
						posA = Station2Pos
					case 2:
						posA = Station3Pos
					}
					switch bsB {
					case 0:
						posB = Station1Pos
					case 1:
						posB = Station2Pos
					case 2:
						posB = Station3Pos
					}
					if posA > posB {
						posA, posB = posB, posA
						bsA, bsB = bsB, bsA
					}
					dA := UWBData[bsA][i].Distance
					dB := UWBData[bsB][i].Distance
					deltaPos := int(posB - posA)
					threshold := deltaPos * 175

					if dA > uint32(threshold) || dB > uint32(threshold) {
						if dB > dA {
							CardSupport[i] = int((int(posA)*175 - int(dA)) / 175)
						} else {
							CardSupport[i] = int((int(dB) + int(posB)*175) / 175)
						}
					} else if dA <= uint32(threshold) && dB <= uint32(threshold) {
						if dB > dA {
							CardSupport[i] = int((int(dA) + int(posA)*175) / 175)
						} else if dA > dB {
							CardSupport[i] = int((int(posB)*175 - int(dB)) / 175)
						} else {
							CardSupport[i] = int((posA + posB) / 2)
						}
					}
					log.Printf("%d号标签卡在 %d 架，三个基站均有效，使用最近两个基站（%d和%d）定位", i+1, CardSupport[i], bsA+1, bsB+1)
				}
			}

			// 过滤超出工作面范围的无效数据
			for i := 0; i < 10; i++ {
				if CardSupport[i] > utils.Conf.SYSTEM.SupportNum || CardSupport[i] < 0 {
					CardSupport[i] = 0
					lastDistancelist[0][i] = 0
					lastDistancelist[1][i] = 0
					lastDistancelist[2][i] = 0
				}
				lastCardSupport[i] = CardSupport[i]
			}

			// 维护闭锁列表
			for i := 0; i < 10; i++ {
				if CardSupport[i] != 0 {
					log.Println(i+1, "闭锁,根据标签卡位置更新闭锁状态与时间")
					pos := CardSupport[i]
					for supportID := pos - 2; supportID <= pos+2; supportID++ {
						if supportID < 1 || supportID > utils.Conf.SYSTEM.SupportNum {
							continue
						}
						idx := supportID - 1
						thisBsStatus[idx] = 1
						lastOpenBstime[idx] = time.Now().Unix()
					}
					uwbpos[pos-1] = 1

				}
			}
			log.Println("uwb基站给出一次数据后", thisBsStatus, "")
			for i := 0; i < len(thisBsStatus); i++ {
				if thisBsStatus[i] == 1 {
					idOpenBs[i] = 1
					log.Println(i+1, "号支架按下软件闭锁")
					BSOpen(byte(i + 1))
					BSOpen(byte(i + 1))
					BSOpen(byte(i + 1))
				}
			}
			//维护人员定位位置，给外部系统
			for i := 0; i < len(uwbpos); i++ {

				mb.HoldingRegisters[6400+i] = uwbpos[i]
			}
			log.Println("uwb基站-写人员位置信息给到外部系统使用", uwbpos, "")
		}
	}
}

func GetUwb_four(ctx context.Context) {

	var Station1Pos uint32 = 38
	var Station2Pos uint32 = 104
	var Station3Pos uint32 = 170 // 第三基站位置
	var Station4Pos uint32 = 236 // 新增第四基站位置 (示例值，请根据实际部署调整)
	UDPAddr, err := net.ResolveUDPAddr("udp", ":2602")
	if err != nil {
		log.Println("ResolveUDP err,err=", err)
		return
	}

	UDPConn, err := net.ListenUDP("udp", UDPAddr)
	if err != nil {
		log.Println("ListenUDP err,err=", err)
		return
	}
	defer UDPConn.Close()
	//log.Println("人员定位UDP开始监听")
	var buffer [512]byte
	var CRC_JY uint16            // CRC校验值
	var UWBData [4][10]UWBCard   // 扩展为4个基站的数据存储
	var UWBDataTime [4][10]int64 // 扩展为4个基站的数据时间戳
	var thisBsStatus []int
	var lastOpenBstime []int64
	var idOpenBs []int64
	var lastDistancelist [4][10]uint32
	var lastCardSupport [10]int

	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		lastOpenBstime = append(lastOpenBstime, 0)
	}
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		idOpenBs = append(idOpenBs, 0)
	}
	tickerTime := time.NewTicker(time.Second * 1) //检验数据过期

	for {
		select {
		case <-ctx.Done():
			return
		case id := <-Ruanbisuo:
			if (time.Now().Unix() - lastOpenBstime[id-1]) > 5 {
				log.Println("控制台", id, "号支架解除软件闭锁")
				BSClose(byte(id))
				BSClose(byte(id))
				BSClose(byte(id))
			}

		case <-tickerTime.C:
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				if (time.Now().Unix()-lastOpenBstime[i]) > 5 && idOpenBs[i] == 1 {
					log.Println("控制台", i+1, "号支架解除软件闭锁")
					BSClose(byte(i + 1))
					BSClose(byte(i + 1))
					BSClose(byte(i + 1))
					idOpenBs[i] = 0
				}
			}

		default:
			//log.Println("人员定位开始读数据")

			err = UDPConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if err != nil {
				log.Println("SetReadDeadline err:", err)
				break
			}
			n, addr, err := UDPConn.ReadFromUDP(buffer[0:])
			if err != nil {

				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					break
				}
				log.Println("ReadFromUDP err,err=", err)
				time.Sleep(100 * time.Millisecond)
				break
			}

			thisBsStatus = []int{}
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				thisBsStatus = append(thisBsStatus, 0)
			}

			log.Printf("接收到来自%v的数据,数据为%v\n", addr, buffer[0:n])
			CRC_JY = 0
			for i := 2; i < 21; i++ {
				CRC_JY += uint16(buffer[i])
			}
			CRC_JY = ^CRC_JY
			//log.Println("CRC_JY=", CRC_JY)
			Low8 := byte(CRC_JY & 0xFF)
			High8 := byte((CRC_JY & 0xFF00) >> 8)
			//CRC校验
			if (Low8 == buffer[21]) && (High8 == buffer[22]) {
				//log.Println("开始进行CRC校验：", buffer[0], buffer[1], buffer[2], buffer[3], "对比：", 0xdd, 0x66, 0x17, 0x11)
				//log.Printf("开始进行CRC校验: [%02x %02x %02x %02x] 对比: [%02x %02x %02x %02x]\n", buffer[0], buffer[1], buffer[2], buffer[3], 0xdd, 0x66, 0x17, 0x11)
				//包头+长度+类型判断
				if buffer[0] == 0xdd && buffer[1] == 0x66 && buffer[2] == 0x17 && buffer[3] == 0x11 {
					//1号基站mac地址判断

					log.Printf("IP: %s, 读取到的标签卡ID: [%02x %02x %02x %02x]\n", addr.IP.String(), buffer[8], buffer[9], buffer[10], buffer[11])

					if addr.IP.String() == "172.16.0.99" {
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //1号基站1号标签卡

							lastDistancelist[0][0] = UWBData[0][0].Distance

							UWBData[0][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][0].Lightning = uint16(buffer[18])
							UWBData[0][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //1号基站2号标签卡

							lastDistancelist[0][1] = UWBData[0][1].Distance

							UWBData[0][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][1].Lightning = uint16(buffer[18])
							UWBData[0][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][1] = time.Now().Unix()

						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //1号基站3号标签卡

							lastDistancelist[0][2] = UWBData[0][2].Distance

							UWBData[0][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][2].Lightning = uint16(buffer[18])
							UWBData[0][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][2] = time.Now().Unix()

						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //1号基站4号标签卡

							lastDistancelist[0][3] = UWBData[0][3].Distance

							UWBData[0][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][3].Lightning = uint16(buffer[18])
							UWBData[0][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][3] = time.Now().Unix()

						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //1号基站5号标签卡

							lastDistancelist[0][4] = UWBData[0][4].Distance

							UWBData[0][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][4].Lightning = uint16(buffer[18])
							UWBData[0][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][4] = time.Now().Unix()

						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //1号基站6号标签卡

							lastDistancelist[0][5] = UWBData[0][5].Distance

							UWBData[0][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][5].Lightning = uint16(buffer[18])
							UWBData[0][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //1号基站7号标签卡

							lastDistancelist[0][6] = UWBData[0][6].Distance

							UWBData[0][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][6].Lightning = uint16(buffer[18])
							UWBData[0][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //1号基站8号标签卡

							lastDistancelist[0][7] = UWBData[0][7].Distance

							UWBData[0][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][7].Lightning = uint16(buffer[18])
							UWBData[0][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //1号基站9号标签卡

							lastDistancelist[0][8] = UWBData[0][8].Distance

							UWBData[0][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][8].Lightning = uint16(buffer[18])
							UWBData[0][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //1号基站10号标签卡

							lastDistancelist[0][9] = UWBData[0][9].Distance

							UWBData[0][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[0][9].Lightning = uint16(buffer[18])
							UWBData[0][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[0][9] = time.Now().Unix()

						}
						//2号基站mac地址判断
					} else if addr.IP.String() == "172.16.0.100" {
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //2号基站1号标签卡

							lastDistancelist[1][0] = UWBData[1][0].Distance

							UWBData[1][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][0].Lightning = uint16(buffer[18])
							UWBData[1][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //2号基站2号标签卡

							lastDistancelist[1][1] = UWBData[1][1].Distance

							UWBData[1][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][1].Lightning = uint16(buffer[18])
							UWBData[1][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][1] = time.Now().Unix()
						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //2号基站3号标签卡

							lastDistancelist[1][2] = UWBData[1][2].Distance

							UWBData[1][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][2].Lightning = uint16(buffer[18])
							UWBData[1][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][2] = time.Now().Unix()
						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //2号基站4号标签卡

							lastDistancelist[1][3] = UWBData[1][3].Distance

							UWBData[1][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][3].Lightning = uint16(buffer[18])
							UWBData[1][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][3] = time.Now().Unix()
						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //2号基站5号标签卡

							lastDistancelist[1][4] = UWBData[1][4].Distance

							UWBData[1][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][4].Lightning = uint16(buffer[18])
							UWBData[1][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][4] = time.Now().Unix()
						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //2号基站6号标签卡

							lastDistancelist[1][5] = UWBData[1][5].Distance

							UWBData[1][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][5].Lightning = uint16(buffer[18])
							UWBData[1][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //2号基站7号标签卡

							lastDistancelist[1][6] = UWBData[1][6].Distance

							UWBData[1][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][6].Lightning = uint16(buffer[18])
							UWBData[1][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //2号基站8号标签卡

							lastDistancelist[1][7] = UWBData[1][7].Distance

							UWBData[1][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][7].Lightning = uint16(buffer[18])
							UWBData[1][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //2号基站9号标签卡

							lastDistancelist[1][8] = UWBData[1][8].Distance

							UWBData[1][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][8].Lightning = uint16(buffer[18])
							UWBData[1][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //2号基站10号标签卡

							lastDistancelist[1][9] = UWBData[1][9].Distance

							UWBData[1][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[1][9].Lightning = uint16(buffer[18])
							UWBData[1][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[1][9] = time.Now().Unix()
						}
					} else if addr.IP.String() == "172.16.0.101" { //3号基站mac地址判断
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //3号基站1号标签卡

							lastDistancelist[2][0] = UWBData[2][0].Distance

							UWBData[2][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][0].Lightning = uint16(buffer[18])
							UWBData[2][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //3号基站2号标签卡

							lastDistancelist[2][1] = UWBData[2][1].Distance

							UWBData[2][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][1].Lightning = uint16(buffer[18])
							UWBData[2][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][1] = time.Now().Unix()
						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //3号基站3号标签卡

							lastDistancelist[2][2] = UWBData[2][2].Distance

							UWBData[2][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][2].Lightning = uint16(buffer[18])
							UWBData[2][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][2] = time.Now().Unix()
						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //3号基站4号标签卡

							lastDistancelist[2][3] = UWBData[2][3].Distance

							UWBData[2][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][3].Lightning = uint16(buffer[18])
							UWBData[2][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][3] = time.Now().Unix()
						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //3号基站5号标签卡

							lastDistancelist[2][4] = UWBData[2][4].Distance

							UWBData[2][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][4].Lightning = uint16(buffer[18])
							UWBData[2][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][4] = time.Now().Unix()
						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //3号基站6号标签卡

							lastDistancelist[2][5] = UWBData[2][5].Distance

							UWBData[2][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][5].Lightning = uint16(buffer[18])
							UWBData[2][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //3号基站7号标签卡

							lastDistancelist[2][6] = UWBData[2][6].Distance

							UWBData[2][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][6].Lightning = uint16(buffer[18])
							UWBData[2][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //3号基站8号标签卡

							lastDistancelist[2][7] = UWBData[2][7].Distance

							UWBData[2][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][7].Lightning = uint16(buffer[18])
							UWBData[2][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //3号基站9号标签卡

							lastDistancelist[2][8] = UWBData[2][8].Distance

							UWBData[2][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][8].Lightning = uint16(buffer[18])
							UWBData[2][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //3号基站10号标签卡

							lastDistancelist[2][9] = UWBData[2][9].Distance

							UWBData[2][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[2][9].Lightning = uint16(buffer[18])
							UWBData[2][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[2][9] = time.Now().Unix()
						}
					} else if addr.IP.String() == "172.16.0.102" { // 新增：4号基站mac地址判断 (假设IP为172.16.0.102)
						if buffer[8] == 0x4A && buffer[9] == 0x26 && buffer[10] == 0xEA && buffer[11] == 0x61 { //4号基站1号标签卡

							lastDistancelist[3][0] = UWBData[3][0].Distance

							UWBData[3][0].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][0].Lightning = uint16(buffer[18])
							UWBData[3][0].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][0] = time.Now().Unix()

						} else if buffer[8] == 0x29 && buffer[9] == 0x61 && buffer[10] == 0x3B && buffer[11] == 0x88 { //4号基站2号标签卡

							lastDistancelist[3][1] = UWBData[3][1].Distance

							UWBData[3][1].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][1].Lightning = uint16(buffer[18])
							UWBData[3][1].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][1] = time.Now().Unix()
						} else if buffer[8] == 0x16 && buffer[9] == 0xA7 && buffer[10] == 0x0B && buffer[11] == 0x21 { //4号基站3号标签卡

							lastDistancelist[3][2] = UWBData[3][2].Distance

							UWBData[3][2].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][2].Lightning = uint16(buffer[18])
							UWBData[3][2].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][2] = time.Now().Unix()
						} else if buffer[8] == 0x99 && buffer[9] == 0xF1 && buffer[10] == 0xE7 && buffer[11] == 0x13 { //4号基站4号标签卡

							lastDistancelist[3][3] = UWBData[3][3].Distance

							UWBData[3][3].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][3].Lightning = uint16(buffer[18])
							UWBData[3][3].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][3] = time.Now().Unix()
						} else if buffer[8] == 0x88 && buffer[9] == 0xF0 && buffer[10] == 0x30 && buffer[11] == 0xFA { //4号基站5号标签卡

							lastDistancelist[3][4] = UWBData[3][4].Distance

							UWBData[3][4].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][4].Lightning = uint16(buffer[18])
							UWBData[3][4].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][4] = time.Now().Unix()
						} else if buffer[8] == 0x14 && buffer[9] == 0xF2 && buffer[10] == 0x2D && buffer[11] == 0x4D { //4号基站6号标签卡

							lastDistancelist[3][5] = UWBData[3][5].Distance

							UWBData[3][5].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][5].Lightning = uint16(buffer[18])
							UWBData[3][5].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][5] = time.Now().Unix()
						} else if buffer[8] == 0xAB && buffer[9] == 0x96 && buffer[10] == 0xF5 && buffer[11] == 0xC9 { //4号基站7号标签卡

							lastDistancelist[3][6] = UWBData[3][6].Distance

							UWBData[3][6].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][6].Lightning = uint16(buffer[18])
							UWBData[3][6].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][6] = time.Now().Unix()
						} else if buffer[8] == 0x53 && buffer[9] == 0xCD && buffer[10] == 0x7E && buffer[11] == 0x3F { //4号基站8号标签卡

							lastDistancelist[3][7] = UWBData[3][7].Distance

							UWBData[3][7].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][7].Lightning = uint16(buffer[18])
							UWBData[3][7].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][7] = time.Now().Unix()
						} else if buffer[8] == 0x73 && buffer[9] == 0x51 && buffer[10] == 0x1F && buffer[11] == 0xD8 { //4号基站9号标签卡

							lastDistancelist[3][8] = UWBData[3][8].Distance

							UWBData[3][8].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][8].Lightning = uint16(buffer[18])
							UWBData[3][8].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][8] = time.Now().Unix()
						} else if buffer[8] == 0x1C && buffer[9] == 0x4B && buffer[10] == 0x70 && buffer[11] == 0x25 { //4号基站10号标签卡

							lastDistancelist[3][9] = UWBData[3][9].Distance

							UWBData[3][9].Distance = uint32(buffer[12]) | uint32(buffer[13])<<8 | uint32(buffer[14])<<16 | uint32(buffer[15])<<24
							UWBData[3][9].Lightning = uint16(buffer[18])
							UWBData[3][9].Signal = uint16(buffer[16]) + 256*uint16(buffer[17])
							UWBDataTime[3][9] = time.Now().Unix()
						}
					}

				}
			}

			log.Printf("1号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][0].Distance, lastDistancelist[0][0], UWBData[0][0].Signal, UWBData[0][0].Lightning)
			log.Printf("1号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][1].Distance, lastDistancelist[0][1], UWBData[0][1].Signal, UWBData[0][1].Lightning)
			log.Printf("1号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][2].Distance, lastDistancelist[0][2], UWBData[0][2].Signal, UWBData[0][2].Lightning)
			log.Printf("1号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][3].Distance, lastDistancelist[0][3], UWBData[0][3].Signal, UWBData[0][3].Lightning)
			log.Printf("1号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][4].Distance, lastDistancelist[0][4], UWBData[0][4].Signal, UWBData[0][4].Lightning)
			log.Printf("1号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][5].Distance, lastDistancelist[0][5], UWBData[0][5].Signal, UWBData[0][5].Lightning)
			log.Printf("1号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][6].Distance, lastDistancelist[0][6], UWBData[0][6].Signal, UWBData[0][6].Lightning)
			log.Printf("1号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][7].Distance, lastDistancelist[0][7], UWBData[0][7].Signal, UWBData[0][7].Lightning)
			log.Printf("1号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][8].Distance, lastDistancelist[0][8], UWBData[0][8].Signal, UWBData[0][8].Lightning)
			log.Printf("1号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[0][9].Distance, lastDistancelist[0][9], UWBData[0][9].Signal, UWBData[0][9].Lightning)

			log.Printf("2号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][0].Distance, lastDistancelist[1][0], UWBData[1][0].Signal, UWBData[1][0].Lightning)
			log.Printf("2号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][1].Distance, lastDistancelist[1][1], UWBData[1][1].Signal, UWBData[1][1].Lightning)
			log.Printf("2号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][2].Distance, lastDistancelist[1][2], UWBData[1][2].Signal, UWBData[1][2].Lightning)
			log.Printf("2号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][3].Distance, lastDistancelist[1][3], UWBData[1][3].Signal, UWBData[1][3].Lightning)
			log.Printf("2号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][4].Distance, lastDistancelist[1][4], UWBData[1][4].Signal, UWBData[1][4].Lightning)
			log.Printf("2号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][5].Distance, lastDistancelist[1][5], UWBData[1][5].Signal, UWBData[1][5].Lightning)
			log.Printf("2号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][6].Distance, lastDistancelist[1][6], UWBData[1][6].Signal, UWBData[1][6].Lightning)
			log.Printf("2号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][7].Distance, lastDistancelist[1][7], UWBData[1][7].Signal, UWBData[1][7].Lightning)
			log.Printf("2号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][8].Distance, lastDistancelist[1][8], UWBData[1][8].Signal, UWBData[1][8].Lightning)
			log.Printf("2号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[1][9].Distance, lastDistancelist[1][9], UWBData[1][9].Signal, UWBData[1][9].Lightning)

			log.Printf("3号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][0].Distance, lastDistancelist[2][0], UWBData[2][0].Signal, UWBData[2][0].Lightning)
			log.Printf("3号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][1].Distance, lastDistancelist[2][1], UWBData[2][1].Signal, UWBData[2][1].Lightning)
			log.Printf("3号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][2].Distance, lastDistancelist[2][2], UWBData[2][2].Signal, UWBData[2][2].Lightning)
			log.Printf("3号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][3].Distance, lastDistancelist[2][3], UWBData[2][3].Signal, UWBData[2][3].Lightning)
			log.Printf("3号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][4].Distance, lastDistancelist[2][4], UWBData[2][4].Signal, UWBData[2][4].Lightning)
			log.Printf("3号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][5].Distance, lastDistancelist[2][5], UWBData[2][5].Signal, UWBData[2][5].Lightning)
			log.Printf("3号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][6].Distance, lastDistancelist[2][6], UWBData[2][6].Signal, UWBData[2][6].Lightning)
			log.Printf("3号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][7].Distance, lastDistancelist[2][7], UWBData[2][7].Signal, UWBData[2][7].Lightning)
			log.Printf("3号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][8].Distance, lastDistancelist[2][8], UWBData[2][8].Signal, UWBData[2][8].Lightning)
			log.Printf("3号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[2][9].Distance, lastDistancelist[2][9], UWBData[2][9].Signal, UWBData[2][9].Lightning)

			// 新增：4号基站的日志输出
			log.Printf("4号基站的1号标签卡61EA264A距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][0].Distance, lastDistancelist[3][0], UWBData[3][0].Signal, UWBData[3][0].Lightning)
			log.Printf("4号基站的2号标签卡883B6129距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][1].Distance, lastDistancelist[3][1], UWBData[3][1].Signal, UWBData[3][1].Lightning)
			log.Printf("4号基站的3号标签卡210BA716距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][2].Distance, lastDistancelist[3][2], UWBData[3][2].Signal, UWBData[3][2].Lightning)
			log.Printf("4号基站的4号标签卡99F1E713距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][3].Distance, lastDistancelist[3][3], UWBData[3][3].Signal, UWBData[3][3].Lightning)
			log.Printf("4号基站的5号标签卡FA30F088距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][4].Distance, lastDistancelist[3][4], UWBData[3][4].Signal, UWBData[3][4].Lightning)
			log.Printf("4号基站的6号标签卡4D2DF214距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][5].Distance, lastDistancelist[3][5], UWBData[3][5].Signal, UWBData[3][5].Lightning)
			log.Printf("4号基站的7号标签卡C9F596AB距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][6].Distance, lastDistancelist[3][6], UWBData[3][6].Signal, UWBData[3][6].Lightning)
			log.Printf("4号基站的8号标签卡3F7ECD53距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][7].Distance, lastDistancelist[3][7], UWBData[3][7].Signal, UWBData[3][7].Lightning)
			log.Printf("4号基站的9号标签卡D81F5173距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][8].Distance, lastDistancelist[3][8], UWBData[3][8].Signal, UWBData[3][8].Lightning)
			log.Printf("4号基站的10号标签卡25704B1C距离值为%v厘米,上次距离值为%v厘米，信号强度为%v,电量为%v\n", UWBData[3][9].Distance, lastDistancelist[3][9], UWBData[3][9].Signal, UWBData[3][9].Lightning)

			var CardSupport [10]int

			for i := 0; i < 10; i++ {
				//log.Println("开始计算每个标签卡所处位置")
				now := time.Now().Unix()
				type distIdx struct {
					dist uint32
					idx  int
					pos  uint32
				}
				allStations := []distIdx{}
				if now-UWBDataTime[0][i] < 5 {
					allStations = append(allStations, distIdx{UWBData[0][i].Distance, 0, Station1Pos})
				}
				if now-UWBDataTime[1][i] < 5 {
					allStations = append(allStations, distIdx{UWBData[1][i].Distance, 1, Station2Pos})
				}
				if now-UWBDataTime[2][i] < 5 {
					allStations = append(allStations, distIdx{UWBData[2][i].Distance, 2, Station3Pos})
				}
				if now-UWBDataTime[3][i] < 5 {
					allStations = append(allStations, distIdx{UWBData[3][i].Distance, 3, Station4Pos})
				}

				switch len(allStations) {
				case 0:
					CardSupport[i] = 0
					log.Println(i+1, "号标签卡无有效基站数据")
				case 1:
					bs := allStations[0].idx
					stationPos := allStations[0].pos
					currentDistance := UWBData[bs][i].Distance
					lastPos := lastCardSupport[i]
					lastDist := lastDistancelist[bs][i]
					if currentDistance < 250 && lastPos > int(stationPos-2) && lastPos < int(stationPos+2) {
						CardSupport[i] = int(stationPos)
					} else if lastPos > int(stationPos) || (lastPos == int(stationPos) && lastDist < currentDistance) {
						CardSupport[i] = int((float64(currentDistance) + float64(stationPos)*175.0 + 87.5) / 175.0)
					} else {
						CardSupport[i] = int((float64(stationPos)*175.0 - float64(currentDistance) + 87.5) / 175.0)
					}
					log.Printf("%d号标签卡在 %d 架，仅基站%d有效，上一次位置：%d", i+1, CardSupport[i], bs+1, lastPos)
				case 2:
					bs1, bs2 := allStations[0].idx, allStations[1].idx
					pos1, pos2 := allStations[0].pos, allStations[1].pos
					if pos1 > pos2 {
						pos1, pos2 = pos2, pos1
						bs1, bs2 = bs2, bs1
					}
					d1 := UWBData[bs1][i].Distance
					d2 := UWBData[bs2][i].Distance
					threshold := float64(pos2-pos1) * 175.0
					if float64(d1) > threshold || float64(d2) > threshold {
						if d2 > d1 {
							CardSupport[i] = int((float64(pos1)*175.0 - float64(d1) + 87.5) / 175.0)
						} else {
							CardSupport[i] = int((float64(d2) + float64(pos2)*175.0 + 87.5) / 175.0)
						}
					} else {
						if d2 > d1 {
							CardSupport[i] = int((float64(d1) + float64(pos1)*175.0 + 87.5) / 175.0)
						} else if d1 > d2 {
							CardSupport[i] = int((float64(pos2)*175.0 - float64(d2) + 87.5) / 175.0)
						} else {
							CardSupport[i] = int((float64(pos1+pos2) / 2.0) + 0.5)
						}
					}
					log.Printf("%d号标签卡在 %d 架，基站%d和%d数据有效", i+1, CardSupport[i], bs1+1, bs2+1)
				case 3, 4:
					sort.Slice(allStations, func(a, b int) bool {
						return allStations[a].dist < allStations[b].dist
					})
					bsA, bsB := allStations[0].idx, allStations[1].idx
					posA, posB := allStations[0].pos, allStations[1].pos
					if posA > posB {
						posA, posB = posB, posA
						bsA, bsB = bsB, bsA
					}
					dA := UWBData[bsA][i].Distance
					dB := UWBData[bsB][i].Distance
					threshold := float64(posB-posA) * 175.0
					if float64(dA) > threshold || float64(dB) > threshold {
						if dB > dA {
							CardSupport[i] = int((float64(posA)*175.0 - float64(dA) + 87.5) / 175.0)
						} else {
							CardSupport[i] = int((float64(dB) + float64(posB)*175.0 + 87.5) / 175.0)
						}
					} else {
						if dB > dA {
							CardSupport[i] = int((float64(dA) + float64(posA)*175.0 + 87.5) / 175.0)
						} else if dA > dB {
							CardSupport[i] = int((float64(posB)*175.0 - float64(dB) + 87.5) / 175.0)
						} else {
							CardSupport[i] = int((float64(posA+posB) / 2.0) + 0.5)
						}
					}
					log.Printf("%d号标签卡在 %d 架，%d个基站有效，选最近基站%d和%d定位", i+1, CardSupport[i], len(allStations), bsA+1, bsB+1)
				}
			}

			// 过滤超出工作面范围的无效数据
			for i := 0; i < 10; i++ {
				if CardSupport[i] > utils.Conf.SYSTEM.SupportNum || CardSupport[i] < 0 {
					CardSupport[i] = 0
					lastDistancelist[0][i] = 0
					lastDistancelist[1][i] = 0
					lastDistancelist[2][i] = 0
					lastDistancelist[3][i] = 0
				}
				lastCardSupport[i] = CardSupport[i]
			}

			// 维护闭锁列表（与原逻辑相同）
			for i := 0; i < 10; i++ {
				if CardSupport[i] != 0 {
					log.Println(i+1, "闭锁,根据标签卡位置更新闭锁状态与时间")
					pos := CardSupport[i]
					for supportID := pos - 2; supportID <= pos+2; supportID++ {
						if supportID < 1 || supportID > utils.Conf.SYSTEM.SupportNum {
							continue
						}
						idx := supportID - 1
						thisBsStatus[idx] = 1
						lastOpenBstime[idx] = time.Now().Unix()
					}
				}
			}
			log.Println("uwb基站给出一次数据后", thisBsStatus, "")
			for i := 0; i < len(thisBsStatus); i++ {
				if thisBsStatus[i] == 1 {
					idOpenBs[i] = 1
					log.Println(i+1, "号支架按下软件闭锁")
					BSOpen(byte(i + 1))
					BSOpen(byte(i + 1))
					BSOpen(byte(i + 1))
				}
			}
		}
	}
}
