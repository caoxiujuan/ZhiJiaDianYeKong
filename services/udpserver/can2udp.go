package udpserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gocode/model"
	service "gocode/services"
	"gocode/services/modbus"
	"gocode/services/mysql"
	tcpService "gocode/services/tcpserver"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	// "gocode/services/upload"
	"log"
	"math"

	//"gocode/services/mysql"

	"gocode/utils"
	"math/rand"
	"net"
	"time"

	mbmaster "github.com/goburrow/modbus"
	"github.com/tbrandon/mbserver"
)

var (
	buff            *utils.Buffer
	Mb              *mbserver.Server
	Controlcommand  chan uint16
	stopGroupNum    int = -1
	HeartString     string
	randomAutoArr   [10]int
	data_type_can   string
	sub_type_can    string
	dest_id_can     string
	source_id_can   string
	target_id_can   string
	interveneTimes  []int
	AutoRate        float64
	CanChannel      chan []byte
	CanSendChannel1 chan []byte
	CanSendChannel2 chan []byte
	CanSendChannel3 chan []byte
	Ruanbisuo       chan int

	SendTestData       [][]byte
	ReceiveTestData    chan []byte
	SendTestDataNum    int64
	ReceiveTestDataNum int64

	IsQF bool

	ReceiveTestDataRightNum    int64
	ReceiveTestDataErrorNum    int64
	ReceiveTestDataRightBitNum int64
	ReceiveTestDataErrorBitNum int64

	SendTestDataText        string
	ReceiveTestDataText     string
	command                 map[int]string
	lastTableName           string
	SensorCache             map[int]int
	sensorMutex             sync.RWMutex
	DataLocker              sync.Mutex
	DBRSChan                = make(chan model.WjwRecord, 1000)
	DBSChan                 = make(chan model.WjwSendRecord, 2000)
	FaultRecordChan         = make(chan model.FaultRecord, 2000) // New channel for fault records
	faultMap                map[int]string                       // Map to store fault descriptions
	RecordCommandChan       = make(chan model.RecordCommand, 2000)
	recordCommandTableCache sync.Map
)

const timeFormat1 = "2006-01-02 15:04:05"

// CanFrame can数据帧结构
type CanFrame struct {
	Length  byte
	FrameID uint32
	Data    [8]byte
}

// ToByte CanFrame to byte
func (c CanFrame) ToByte() []byte {
	buf := new(bytes.Buffer)
	var data = []interface{}{
		c.Length | 0x80,
		c.FrameID,
		c.Data,
	}
	for _, v := range data {
		err := binary.Write(buf, binary.BigEndian, v)
		if err != nil {
			fmt.Println("binary.Write failed:", err)
		}
	}
	//fmt.Println("转换之后",buf.Bytes())
	return buf.Bytes()

}

// parseCanID 解析CanID头
func parseCanID(data []byte) (data_type, sub_type, dest_id, source_id byte) {
	data_type = data[0] & 0x07
	sub_type = data[1]
	dest_id = data[2]
	source_id = data[3]
	return
}

// assembleCanID 封装CanID头
func assembleCanID(data_type, sub_type, dest_id, source_id byte) (data uint32) {
	data = (uint32(data_type&0x07) << 24) + uint32(uint(sub_type)<<16) + uint32(uint(dest_id)<<8) + uint32(source_id)
	return
}

// type MCanData struct {
// 	data      []byte
// 	Can2udpIp string
// }

// sendUDPdata 向udp设备发送数据
func sendUDPdata(data []byte, Can2udpIp string) {
	//fmt.Println("收到的", data, Can2udpIp)
	if Can2udpIp == utils.Conf.CAN.Can2udpIp1 || Can2udpIp == utils.Conf.CAN.Can2udpIp2 || Can2udpIp == utils.Conf.CAN.Can2udpIp3 {
		CanSendChannel1 <- data //工作面
	} else if Can2udpIp == utils.Conf.CAN.Can2udpIpExtra {
		CanSendChannel2 <- data
	} else if Can2udpIp == utils.Conf.CAN.Can2udpIpExtra1 {
		CanSendChannel3 <- data
	}

}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func setSupportSort(value uint16) int {
	utils.SupportSort = int(value & 0x0001)
	return utils.SupportSort
}

func currentSupportSort() int {
	if Mb != nil {
		return setSupportSort(Mb.HoldingRegisters[18])
	}
	return utils.SupportSort
}

func isCqljWarningSupport(supportIndex int, supportSort int) bool {
	if Mb.HoldingRegisters[181] == 0 {
		return false
	}
	if Mb.HoldingRegisters[1522+supportIndex*9] >= (960-uint16(utils.Conf.GLOBAL.Cqlj)) || Mb.HoldingRegisters[5000+supportIndex] != 0 {
		return false
	}
	supportNo := supportIndex + 1
	shearerSupportNo := int(Mb.HoldingRegisters[180] / 10)

	switch supportSort {
	case 0: //支架右升序
		if Mb.HoldingRegisters[182] > 0 {
			return supportNo > shearerSupportNo+4
		} else {
			return supportNo < shearerSupportNo-4
		}

	case 1: //支架左升序
		if Mb.HoldingRegisters[182] > 0 {
			return supportNo < shearerSupportNo-4
		} else {
			return supportNo > shearerSupportNo+4
		}
	}
	return false
}

func isInArray(arr [][]byte, target []byte) bool {

	for _, num := range arr {
		if reflect.DeepEqual(num, target) {
			return true
		}
	}
	return false
}

func isInArrayQuFan(arr [][]byte, target []byte) (bool, int) {

	for index, num := range arr {
		flag := true
		for i := 0; i < len(num); i++ {
			if num[i]+target[i] != 255 {
				flag = false
			}
		}
		if flag {
			return true, index
		}
	}
	return false, -1
}

func delExist(arr [][]byte, index int) [][]byte {
	if index == len(arr)-1 {
		arr = arr[:index]
	} else {
		// 使用copy将i之后的元素向前移动一位
		copy(arr[index:], arr[index+1:])
		// 截取切片以删除最后一个元素
		arr = arr[:len(arr)-1]
	}
	return arr
}

func TestWml(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-ReceiveTestData:
			//SendTestDataText = ""
			//ReceiveTestDataText = ""
			ReceiveTestDataNum += 1
			DataLocker.Lock()
			fmt.Println("回帧", ReceiveTestDataNum, r)
			fmt.Println("发送帧", len(SendTestData), SendTestData)
			//判断回帧是否存在池子里，返回下标
			isExist, index := isInArrayQuFan(SendTestData, r)

			if isExist {

				temps := SendTestData[index]
				for _, temp := range temps {
					SendTestDataText += fmt.Sprintf("%02x", temp) + " "
				}
				for _, temp := range r {
					ReceiveTestDataText += fmt.Sprintf("%02x", temp) + " "
				}
				sendDB := model.WjwRecord{Time: time.Now(), SendData: hex.EncodeToString(temps), ReceiveData: hex.EncodeToString(r)}
				select {
				case DBRSChan <- sendDB:
					// 发送成功
				default:
					// 通道满了，丢弃记录或打印警告
					fmt.Println("Warning: DB Log Channel full, dropping record")
				}
				//fmt.Println("Hexadecimal:", SendTestDataText)
				SendTestData = delExist(SendTestData, index)
				ReceiveTestDataRightNum += 1

				// var WjwRData model.WjwRecord
				// WjwRData.Time = time.Now()
				// WjwRData.SendData = SendTestDataText
				// WjwRData.ReceiveData = ReceiveTestDataText

				// fmt.Println("记录", WjwRData.SendData, WjwRData.ReceiveData)
				// //user := model.WjwRecord{Time: WjwRData.Time, SendData: WjwRData.SendData, ReceiveData: WjwRData.ReceiveData}
				// mysql.Mysqlclient.Select("Time", "SendData", "ReceiveData").Create(&WjwRData)
				//SendTestDataText =
				//删除池子里的相同元素
			}
			DataLocker.Unlock()
			ReceiveTestDataErrorNum = ReceiveTestDataNum - ReceiveTestDataRightNum

			// isD := true
			// if len(r) != len(s) {
			// 	isD = false
			// 	ReceiveTestDataRightBitNum += 64

			// } else {
			// 	for i := 0; i < len(r); i++ {
			// 		if (r[i] + s[i]) != 255 {
			// 			isD = false
			// 			ReceiveTestDataErrorBitNum += 8
			// 		} else {
			// 			ReceiveTestDataRightBitNum += 8
			// 		}
			// 	}

			// }
			// if isD {
			// 	ReceiveTestDataRightNum += 1
			// } else {
			// 	ReceiveTestDataErrorNum += 1
			// }

			//fmt.Println("误码测试校验", r, len(SendTestData), ReceiveTestDataRightNum, ReceiveTestDataErrorNum, ReceiveTestDataNum)

		}
	}
}

// Run 启动UDP监听服务   can
func Run(ctx context.Context, mbServer *mbserver.Server, supportNum int, CanHeart []int64, PressInterval []int, PressLastTime []time.Time, WorkMode []int, WorkModeTime []int64, LastMode2Time []int64, IsAuto []int, SimulationHeart []int64, isTableExist []string, Param1 []int, Param2 []int, Param3 []int, Param4 []int, RandomAutoReceive []int) {

	command = CommandMap()
	Ruanbisuo = make(chan int, 50)
	listener, err := net.ListenUDP("udp", &net.UDPAddr{Port: utils.Conf.CAN.Can2udpPort})
	if err != nil {
		log.Println("UDP监听(", utils.Conf.CAN.Can2udpPort, ")失败", net.IPv4zero, utils.Conf.CAN.Can2udpPort, err)
		return
	}
	data := make([]byte, 1024)
	fmt.Println("UDP监听(", utils.Conf.CAN.Can2udpPort, ")正常启动", net.IPv4zero, utils.Conf.CAN.Can2udpPort)
	buff = utils.NewPointer()

	if SensorCache == nil {
		SensorCache = make(map[int]int)
	}
	faultMap = map[int]string{
		0:  "左邻架CAN故障",
		1:  "右邻架CAN故障",
		2:  "Wifi连接故障",
		3:  "IS1回路故障",
		4:  "参数配置故障",
		5:  "IS2回路故障",
		6:  "本机闭锁故障",
		7:  "本机急停故障",
		8:  "键盘按键粘连",
		9:  "跳架/漏架",
		10: "软件闭锁故障",
		11: "掉电重启",
		12: "软件重启",
		13: "推移行程传感器故障",
		14: "电磁阀故障",
		15: "ADS芯片故障",
	}

	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		interveneTimes = append(interveneTimes, 0)
		SensorCache[i+1] = 0
	}
	//fmt.Println("初始化SensorCache：", SensorCache)
	//log.Println("初始化SensorCache：", SensorCache)

	Mb = mbServer
	CanChannel = make(chan []byte, 1024) //监听通道
	CanSendChannel1 = make(chan []byte, 1024)
	CanSendChannel2 = make(chan []byte, 1024)
	CanSendChannel3 = make(chan []byte, 1024)
	//ReceiveTestData = make(chan []byte, 65535) //误码率

	//go SimulationCompensate(ctx, PressInterval)
	go WriteFrame(ctx)
	go ParseFrame(ctx, CanHeart, PressInterval, PressLastTime, WorkMode, WorkModeTime, LastMode2Time, IsAuto, SimulationHeart, isTableExist, Param1, Param2, Param3, Param4, RandomAutoReceive, command)
	go StartFaultRecordWorker() // Start the new worker
	//go QueryLoopWifi(ctx, Mb)

	//go TestWml(ctx) //误码率测试

	for {
		n, _, err := listener.ReadFrom(data)
		if err != nil {
			log.Println("error during read:", err)
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
			//fmt.Println("can转udp通道", n)
			for i := 0; i < n/13; i++ {
				udpcan := data[i*13 : i*13+13]
				//fmt.Println("进这里了", i, udpcan)
				// if runtime.NumGoroutine() > 300 {
				// 	fmt.Print("CPU:", runtime.NumCPU(), "GoRoutine:", runtime.NumGoroutine())
				// }

				CanChannel <- udpcan
			}
			//buff.Write(data[0:n])
			//fmt.Println("buff", buff)
		}
	}
}

func CommandMap() (n map[int]string) {
	command = make(map[int]string)
	command[0] = "空闲"
	command[1] = "升柱"
	command[2] = "降柱"
	command[3] = "移架"
	command[4] = "推溜"
	command[5] = "伸一级护帮板"
	command[6] = "收一级护帮板"
	command[7] = "伸前梁"
	command[8] = "收前梁"
	command[9] = "起底"
	command[10] = "伸平衡"
	command[11] = "收平衡"
	command[12] = "开喷雾"
	command[13] = "抬底移架"
	command[14] = "伸侧护板"
	command[15] = "收侧护板"
	command[16] = "伸底调"
	command[17] = "收底调"
	command[18] = "降柱抬底"
	command[19] = "反冲洗"
	command[20] = "伸二级护帮板"
	command[21] = "收二级护帮板"
	command[22] = "伸三级护帮板"
	command[23] = "收三级护帮板"
	command[24] = "升后柱"
	command[25] = "降后柱"
	command[26] = "打开插板"
	command[27] = "关闭插板"
	command[28] = "推后溜"
	command[29] = "拉后溜"
	command[30] = "单支架自动移架"
	command[31] = "编组自动移架"
	command[32] = "编组推溜"
	command[33] = "编组拉溜"
	command[34] = "编组自动收护帮板"
	command[35] = "编组自动伸护帮板"
	command[36] = "编组自动喷雾"
	command[37] = "选中被控支架号"
	command[38] = "自动跟机"
	command[39] = "停止自动跟机"
	command[40] = "自动配置控制器ID"
	command[41] = "同步传感器设置"
	command[42] = "同步公共参数"
	command[43] = "选择被控支架"
	command[44] = "停止"
	command[45] = "补偿推溜，推出设定的距离，修正AFC的直线度"
	command[46] = "自动补压"
	command[47] = "降柱抬底收平衡"
	command[48] = "降柱收平衡"
	command[49] = "升柱伸平衡"
	command[50] = "抬底移架降柱"
	command[51] = "移架降柱"
	command[52] = "不可编组"
	command[53] = "软件闭锁"
	command[54] = "自动调直"
	return command
}

func CommandMap1() (n map[int]string) {
	command = make(map[int]string)
	command[0] = "空闲"
	command[1] = "停止升柱"
	command[2] = "停止降柱"
	command[3] = "停止移架"
	command[4] = "停止推溜"
	command[5] = "停止伸一级护帮板"
	command[6] = "停止收一级护帮板"
	command[7] = "停止伸前梁"
	command[8] = "停止收前梁"
	command[9] = "停止起底"
	command[10] = "停止伸平衡"
	command[11] = "停止收平衡"
	command[12] = "停止开喷雾"
	command[13] = "停止抬底移架"
	command[14] = "停止伸侧护板"
	command[15] = "停止收侧护板"
	command[16] = "停止伸底调"
	command[17] = "停止收底调"
	command[18] = "停止降柱抬底"
	command[19] = "停止反冲洗"
	command[20] = "停止伸二级护帮板"
	command[21] = "停止收二级护帮板"
	command[22] = "停止伸三级护帮板"
	command[23] = "停止收三级护帮板"
	command[24] = "停止升后柱"
	command[25] = "停止降后柱"
	command[26] = "停止打开插板"
	command[27] = "停止关闭插板"
	command[28] = "停止推后溜"
	command[29] = "停止拉后溜"
	command[30] = "单支架自动移架"
	command[31] = "编组自动移架"
	command[32] = "编组推溜"
	command[33] = "编组拉溜"
	command[34] = "编组自动收护帮板"
	command[35] = "编组自动伸护帮板"
	command[36] = "编组自动喷雾"
	command[37] = "选中被控支架号"
	command[38] = "自动跟机"
	command[39] = "停止自动跟机"
	command[40] = "自动配置控制器ID"
	command[41] = "同步传感器设置"
	command[42] = "同步公共参数"
	command[43] = "选择被控支架"
	command[44] = "停止"
	command[45] = "补偿推溜，推出设定的距离，修正AFC的直线度"
	command[46] = "自动补压"
	command[47] = "降柱抬底收平衡"
	command[48] = "降柱收平衡"
	command[49] = "升柱伸平衡"
	command[50] = "抬底移架降柱"
	command[51] = "移架降柱"
	command[52] = "不可编组"
	command[53] = "软件闭锁"
	command[54] = "自动调直"
	command[129] = "启动升柱"
	command[130] = "启动降柱"
	command[131] = "启动移架"
	command[132] = "启动推溜"
	command[133] = "启动伸一级护帮板"
	command[134] = "启动收一级护帮板"
	command[135] = "启动伸前梁"
	command[136] = "启动收前梁"
	command[137] = "启动起底"
	command[138] = "启动伸平衡"
	command[139] = "启动收平衡"
	command[140] = "启动开喷雾"
	command[141] = "启动抬底移架"
	command[142] = "启动伸侧护板"
	command[143] = "启动收侧护板"
	command[144] = "启动伸底调"
	command[145] = "启动收底调"
	command[146] = "启动降柱抬底"
	command[147] = "启动反冲洗"
	command[148] = "启动伸二级护帮板"
	command[149] = "启动收二级护帮板"
	command[150] = "启动伸三级护帮板"
	command[151] = "启动收三级护帮板"
	command[152] = "启动升后柱"
	command[153] = "启动降后柱"
	command[154] = "启动打开插板"
	command[155] = "启动关闭插板"
	command[156] = "启动推后溜"
	command[157] = "启动拉后溜"
	return command
}
func WriteFrame(ctx context.Context) {
	add1 := fmt.Sprintf("%v:%v", utils.Conf.CAN.Can2udpIp1, utils.Conf.CAN.Can2udpPort)
	conn1, err := net.Dial("udp", add1)
	if err != nil {
		fmt.Println("udp建立链接错误:", err)
	}
	defer conn1.Close()
	add2 := fmt.Sprintf("%v:%v", utils.Conf.CAN.Can2udpIpExtra, utils.Conf.CAN.Can2udpPort)
	conn2, _ := net.Dial("udp", add2)
	defer conn2.Close()
	add3 := fmt.Sprintf("%v:%v", utils.Conf.CAN.Can2udpIpExtra1, utils.Conf.CAN.Can2udpPort)
	conn3, _ := net.Dial("udp", add3)
	defer conn3.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-CanSendChannel1:
			//fmt.Println("给1号连接写值", s)
			_, err = conn1.Write(s)
			if err != nil {
				fmt.Println("写值错误s:", err)
				conn1.Close()
				add1 := fmt.Sprintf("%v:%v", utils.Conf.CAN.Can2udpIp1, utils.Conf.CAN.Can2udpPort)
				conn1, err = net.Dial("udp", add1)
				if err != nil {
					fmt.Println("重连udp建立链接错误:", err)
				}
			}
		case d := <-CanSendChannel2:
			_, err = conn2.Write(d)
			if err != nil {
				fmt.Println("写值错误d:", err)
				conn1.Close()
				add1 := fmt.Sprintf("%v:%v", utils.Conf.CAN.Can2udpIpExtra, utils.Conf.CAN.Can2udpPort)
				conn1, err = net.Dial("udp", add1)
				if err != nil {
					fmt.Println("重连udp建立链接错误:", err)
				}
			}
		case f := <-CanSendChannel3:
			_, err = conn3.Write(f)
			if err != nil {
				fmt.Println("写值错误f:", err)
				conn1.Close()
				add1 := fmt.Sprintf("%v:%v", utils.Conf.CAN.Can2udpIpExtra1, utils.Conf.CAN.Can2udpPort)
				conn1, err = net.Dial("udp", add1)
				if err != nil {
					fmt.Println("重连udp建立链接错误:", err)
				}
			}
		}

	}
}

type RealData1 struct {
	Xh        int       `json:"xh"`
	Dd        string    `json:"dd"`
	Sbmc      string    `json:"sbmc"`
	Zxzt      string    `json:"zxzt"`
	Sfsbdmj   string    `json:"sfsbdmj"`
	Zjdz      string    `json:"zjdz"`
	Lzyl      int       `json:"lzyl"`
	Mjwz      int       `json:"mjwz"`
	Tyxc      int       `json:"tyxc"`
	Dingbxzqj string    `json:"dingbxzqj"`
	Dingbyzqj string    `json:"dingbyzqj"`
	Dibxzqj   string    `json:"dibxzqj"`
	Dibyzqj   string    `json:"dibyzqj"`
	Sjgxsj    time.Time `json:"sjgxsj"`
	Bjzt      string    `json:"bjzt"`
	Bjyy      string    `json:"bjyy"`
}

func ParseFrame(ctx context.Context, CanHeart []int64, PressInterval []int, PressLastTime []time.Time, WorkMode []int, WorkModeTime []int64, LastMode2Time []int64, IsAuto []int, SimulationHeart []int64, isTableExist []string, Param1 []int, Param2 []int, Param3 []int, Param4 []int, RandomAutoReceive []int, m map[int]string) {

	for {
		select {
		case <-ctx.Done():
			return
		case s := <-CanChannel:
			go func() {
				t1 := time.Now().UnixNano()
				// if buff.Len() >= 13 {
				// 	s := buff.Next(buff.Len())
				// loop:
				var state uint16
				length := int(s[0] & 0x0F)
				data_type, sub_type, target_id, source_id := parseCanID(s[1:5])
				information_data := s[5 : 5+length]
				//fmt.Println("解析携程", time.Now().Format(timeFormat1), len(s))

				//查询参数
				if data_type == 0x01 && sub_type == 0x89 {
					//fmt.Println(source_id)
					CanHeart[source_id-1] = time.Now().Unix()
					//fmt.Println("收到模拟值上传了")
					if information_data[0] == 13 {

						Mb.HoldingRegisters[202+int(source_id-1)*6] = (uint16(information_data[2]) << 8) | uint16(information_data[3])
						Mb.HoldingRegisters[203+int(source_id-1)*6] = (uint16(information_data[4]) << 8) | uint16(information_data[5])

						var privateParam model.PrivateParam
						privateParam.BackwashValve = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 13 & 0x0001)
						privateParam.RearPlateCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 12 & 0x0001)
						privateParam.RearPillarCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 11 & 0x0001)
						privateParam.BottomAdjustmentCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 10 & 0x0001)
						privateParam.SideGuardCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 9 & 0x0001)
						privateParam.SprayValve = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 8 & 0x0001)
						privateParam.BottomingCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 7 & 0x0001)
						privateParam.PushCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 6 & 0x0001)
						privateParam.FrontPillarCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 5 & 0x0001)
						privateParam.BalanceCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 4 & 0x0001)
						privateParam.FrontBeamCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 3 & 0x0001)
						privateParam.ThreeStageGuardPlateCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 2 & 0x0001)
						privateParam.SecondaryGuardPlateCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 1 & 0x0001)
						privateParam.FirstClassGuardPlateCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] & 0x0001)
						privateParam.AutoStraightenSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 7 & 0x0001)
						privateParam.ShearerPositionSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 6 & 0x0001)
						privateParam.GuardPlateLimitSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 5 & 0x0001)
						privateParam.TopBeamInclinationSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 4 & 0x0001)
						privateParam.TopPlateHeightSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 3 & 0x0001)
						privateParam.PushDisplacementSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 2 & 0x0001)
						privateParam.RPillarPressureSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 1 & 0x0001)
						privateParam.LPillarPressureSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] & 0x0001)
						privateParam.HeightOfAltimeterCase = int(Mb.HoldingRegisters[200+int(source_id-1)*6])
						privateParam.RX = int(Mb.HoldingRegisters[1524+int(source_id-1)*9])
						privateParam.RY = int(Mb.HoldingRegisters[1525+int(source_id-1)*9])
						privateParam.FX = int(Mb.HoldingRegisters[1526+int(source_id-1)*9])
						privateParam.FY = int(Mb.HoldingRegisters[1527+int(source_id-1)*9])
						privateParam.Version = int(Mb.HoldingRegisters[201+int(source_id-1)*6] >> 8)
						WebsocketMessage := model.WebsocketMessage{
							Type:    "privateparam",
							Source:  int(source_id),
							Message: privateParam,
						}
						strings, _ := json.Marshal(WebsocketMessage)

						fmt.Println("私参", source_id, (uint16(information_data[2])<<8)|uint16(information_data[3]), int(Mb.HoldingRegisters[202+int(source_id-1)*6]>>6&0x0001))

						service.WebsocketManager.SendAll(strings)
					} else if information_data[0] == 11 {
						fmt.Println("私参回帧", source_id, information_data[0], (uint16(information_data[2])<<8)|uint16(information_data[3]), (uint16(information_data[4])<<8)|uint16(information_data[5]))
						Mb.HoldingRegisters[200+int(source_id-1)*6] = (uint16(information_data[2]) << 8) | uint16(information_data[3])
						Mb.HoldingRegisters[201+int(source_id-1)*6] = (uint16(information_data[4]) << 8) | uint16(information_data[5])
						var privateParam model.PrivateParam
						privateParam.BackwashValve = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 13 & 0x0001)
						privateParam.RearPlateCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 12 & 0x0001)
						privateParam.RearPillarCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 11 & 0x0001)
						privateParam.BottomAdjustmentCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 10 & 0x0001)
						privateParam.SideGuardCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 9 & 0x0001)
						privateParam.SprayValve = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 8 & 0x0001)
						privateParam.BottomingCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 7 & 0x0001)
						privateParam.PushCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 6 & 0x0001)
						privateParam.FrontPillarCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 5 & 0x0001)
						privateParam.BalanceCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 4 & 0x0001)
						privateParam.FrontBeamCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 3 & 0x0001)
						privateParam.ThreeStageGuardPlateCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 2 & 0x0001)
						privateParam.SecondaryGuardPlateCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] >> 1 & 0x0001)
						privateParam.FirstClassGuardPlateCylinder = int(Mb.HoldingRegisters[202+int(source_id-1)*6] & 0x0001)
						privateParam.AutoStraightenSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 7 & 0x0001)
						privateParam.ShearerPositionSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 6 & 0x0001)
						privateParam.GuardPlateLimitSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 5 & 0x0001)
						privateParam.TopBeamInclinationSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 4 & 0x0001)
						privateParam.TopPlateHeightSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 3 & 0x0001)
						privateParam.PushDisplacementSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 2 & 0x0001)
						privateParam.RPillarPressureSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] >> 1 & 0x0001)
						privateParam.LPillarPressureSensorEnable = int(Mb.HoldingRegisters[203+int(source_id-1)*6] & 0x0001)
						privateParam.HeightOfAltimeterCase = int(Mb.HoldingRegisters[200+int(source_id-1)*6])
						privateParam.RX = int(Mb.HoldingRegisters[1524+int(source_id-1)*9])
						privateParam.RY = int(Mb.HoldingRegisters[1525+int(source_id-1)*9])
						privateParam.FX = int(Mb.HoldingRegisters[1526+int(source_id-1)*9])
						privateParam.FY = int(Mb.HoldingRegisters[1527+int(source_id-1)*9])
						privateParam.Version = int(Mb.HoldingRegisters[201+int(source_id-1)*6] >> 8)
						WebsocketMessage := model.WebsocketMessage{
							Type:    "privateparam",
							Source:  int(source_id),
							Message: privateParam,
						}
						strings, _ := json.Marshal(WebsocketMessage)
						//fmt.Println("私参", len(strings))
						service.WebsocketManager.SendAll(strings)

					} else if (information_data[0] > 14 && information_data[0] < 73) || (information_data[0] == 11) {
						fmt.Println("公参回帧", information_data[0])
						if information_data[0] == 11 {
							fmt.Println("公参顶板", uint16(information_data[2])<<8|uint16(information_data[3]))
							Mb.HoldingRegisters[11] = uint16(information_data[2])<<8 | uint16(information_data[3])
						} else {
							Mb.HoldingRegisters[information_data[0]] = uint16(information_data[2])<<8 | uint16(information_data[3])
							Mb.HoldingRegisters[information_data[0]+1] = uint16(information_data[4])<<8 | uint16(information_data[5])
							Mb.HoldingRegisters[information_data[0]+2] = uint16(information_data[6])<<8 | uint16(information_data[7])
						}

						var publicParam model.PublicParam
						publicParam.BracingMethod = int(Mb.HoldingRegisters[15] >> 12 & 0x0001)
						publicParam.ShearerDataEnable = int(Mb.HoldingRegisters[15] >> 11 & 0x0001)
						publicParam.RemoteControlEnable = int(Mb.HoldingRegisters[15] >> 10 & 0x0001)
						publicParam.TelecontrolEnable = int(Mb.HoldingRegisters[15] >> 9 & 0x0001)
						publicParam.WifiEnable = int(Mb.HoldingRegisters[15] >> 8 & 0x0001)
						publicParam.AutomaticPushEndEnable = int(Mb.HoldingRegisters[15] >> 7 & 0x0001)
						publicParam.AutoFollowMachineEnable = int(Mb.HoldingRegisters[15] >> 6 & 0x0001)
						publicParam.AutomaticBackwashEnable = int(Mb.HoldingRegisters[15] >> 5 & 0x0001)
						publicParam.AutomaticSprayEnable = int(Mb.HoldingRegisters[15] >> 4 & 0x0001)
						publicParam.AutomaticGuardBoardEnable = int(Mb.HoldingRegisters[15] >> 3 & 0x0001)
						publicParam.AutomaticPushAndSlideEnable = int(Mb.HoldingRegisters[15] >> 2 & 0x0001)
						publicParam.AutomaticRackTransferEnable = int(Mb.HoldingRegisters[15] >> 1 & 0x0001)
						publicParam.AutoCompensationEnable = int(Mb.HoldingRegisters[15] & 0x0001)
						publicParam.LoweringColumnLiftingBottom = int(Mb.HoldingRegisters[16] >> 9 & 0x0001)
						publicParam.SimultaneousAutomaticRackTransfer = int(Mb.HoldingRegisters[16] >> 8 & 0x0001)
						publicParam.GuardPlateControl = int(Mb.HoldingRegisters[16] >> 7 & 0x0001)
						publicParam.SidePanelControls = int(Mb.HoldingRegisters[16] >> 6 & 0x0001)
						publicParam.BalanceControlEnable = int(Mb.HoldingRegisters[16] >> 5 & 0x0001)
						publicParam.BottomLifterEnable = int(Mb.HoldingRegisters[16] >> 4 & 0x0001)
						publicParam.FrontBeamControlEnable = int(Mb.HoldingRegisters[16] >> 3 & 0x0001)
						publicParam.PressureTransferFrameEnable = int(Mb.HoldingRegisters[16] >> 2 & 0x0001)
						publicParam.AdjacentFrameAssistEnable = int(Mb.HoldingRegisters[16] >> 1 & 0x0001)
						publicParam.AdjacentRackPressureCorrelationEnable = int(Mb.HoldingRegisters[16] & 0x0001)
						publicParam.SSIDBYTE2 = int(Mb.HoldingRegisters[17] >> 8)
						publicParam.SSIDBYTE1 = int(Mb.HoldingRegisters[17] & 0x00ff)
						publicParam.SupportSorting = int(Mb.HoldingRegisters[18] & 0x0001)
						utils.SupportSort = publicParam.SupportSorting
						publicParam.TailSupportID = int(Mb.HoldingRegisters[19] >> 8)
						publicParam.FirstSupportID = int(Mb.HoldingRegisters[19] & 0x00ff)
						publicParam.AutoTailSupportID = int(Mb.HoldingRegisters[20] >> 8)
						publicParam.AutoFirstSupportID = int(Mb.HoldingRegisters[20] & 0x00ff)
						publicParam.TailTurningPoint = int(Mb.HoldingRegisters[21] >> 8)
						publicParam.MachineHeadTurningPoint = int(Mb.HoldingRegisters[21] & 0x00ff)
						publicParam.TailCutThroughPoint = int(Mb.HoldingRegisters[24] >> 8)
						publicParam.MachineHeadCutThroughPoint = int(Mb.HoldingRegisters[24] & 0x00ff)
						publicParam.SuspensionStartThreshold = int(Mb.HoldingRegisters[25])
						publicParam.SuspensionStopThreshold = int(Mb.HoldingRegisters[26])
						publicParam.RackTransferPressureSetting = int(Mb.HoldingRegisters[27])
						publicParam.TransitionPressureSetting = int(Mb.HoldingRegisters[28])
						publicParam.InitialPressureSetting = int(Mb.HoldingRegisters[29])
						publicParam.PushSlipAllowablePressure = int(Mb.HoldingRegisters[31])
						publicParam.MoveDistanceSettingValue = int(Mb.HoldingRegisters[32])
						publicParam.PinHysteresisCompensation = int(Mb.HoldingRegisters[33] & 0x00ff)

						publicParam.ShiftDistanceZeroOffset = int(Mb.HoldingRegisters[35])
						publicParam.JumpProtectionDistance = int(Mb.HoldingRegisters[36] >> 8)
						publicParam.FarthestControlDistance = int(Mb.HoldingRegisters[36] & 0x00ff)
						publicParam.GuardPlateInterval = int(Mb.HoldingRegisters[38] >> 12 & 0x000f)
						publicParam.GuardPlateDelay = int(Mb.HoldingRegisters[38] >> 8 & 0x000f)
						publicParam.GuardPlateGrouping = int(Mb.HoldingRegisters[38] & 0x00ff)
						publicParam.ColumnInterval = int(Mb.HoldingRegisters[39] >> 12 & 0x000f)
						publicParam.ColumnDelay = int(Mb.HoldingRegisters[39] >> 8 & 0x000f)
						publicParam.ColumnGrouping = int(Mb.HoldingRegisters[39] & 0x00ff)
						publicParam.TransferRackInterval = int(Mb.HoldingRegisters[40] >> 12 & 0x000f)
						publicParam.TransferRackDelay = int(Mb.HoldingRegisters[40] >> 8 & 0x000f)
						publicParam.TransferRackGrouping = int(Mb.HoldingRegisters[40] & 0x00ff)
						publicParam.ShoveInterval = int(Mb.HoldingRegisters[41] >> 12 & 0x000f)
						publicParam.ShoveDelay = int(Mb.HoldingRegisters[41] >> 8 & 0x000f)
						publicParam.ShoveGrouping = int(Mb.HoldingRegisters[41] & 0x00ff)
						publicParam.SprayDurationGrouping = int(Mb.HoldingRegisters[42] >> 8)
						publicParam.SprayGrouping = int(Mb.HoldingRegisters[42] & 0x00ff)
						publicParam.StopLevel1Duration = int(Mb.HoldingRegisters[43] >> 8)
						publicParam.StartLevel1Duration = int(Mb.HoldingRegisters[43] & 0x00ff)
						publicParam.StopLevel2Duration = int(Mb.HoldingRegisters[44] >> 8)
						publicParam.StartLevel2Duration = int(Mb.HoldingRegisters[44] & 0x00ff)
						publicParam.StopLevel3Duration = int(Mb.HoldingRegisters[45] >> 8)
						publicParam.StartLevel3Duration = int(Mb.HoldingRegisters[45] & 0x00ff)
						publicParam.StopFrontBeamDuration = int(Mb.HoldingRegisters[46] >> 8)
						publicParam.StartFrontBeamDuration = int(Mb.HoldingRegisters[46] & 0x00ff)
						publicParam.ColumnRiseTime = int(Mb.HoldingRegisters[47] >> 8)
						publicParam.ColumnDropTime = int(Mb.HoldingRegisters[47] & 0x00ff)
						publicParam.PushTime = int(Mb.HoldingRegisters[48] >> 8)
						publicParam.RackTransferTime = int(Mb.HoldingRegisters[48] & 0x00ff)
						publicParam.BottomLiftingDelayTime = int(Mb.HoldingRegisters[49] & 0x00ff)
						publicParam.AutomaticBackwashCycle = int(Mb.HoldingRegisters[50])
						publicParam.AutomaticRefillTimes = int(Mb.HoldingRegisters[51] >> 13 & 0x0007)
						publicParam.AutomaticRefillInterval = int(Mb.HoldingRegisters[51] >> 8 & 0x001f)
						publicParam.AutomaticRefillCycle = int(Mb.HoldingRegisters[51] & 0x00ff)
						publicParam.SprayDuration = int(Mb.HoldingRegisters[52] >> 8)
						publicParam.BottomLiftDuration = int(Mb.HoldingRegisters[52] & 0x00ff)
						publicParam.LowColumnStopBlanceDuration = int(Mb.HoldingRegisters[53] >> 12 & 0x000f)
						publicParam.LowColumnStopBlanceStartTime = int(Mb.HoldingRegisters[53] >> 8 & 0x000f)
						publicParam.RiseColumnStartBlanceDuration = int(Mb.HoldingRegisters[53] >> 4 & 0x000f)
						publicParam.RiseColumnStartBlanceStartTime = int(Mb.HoldingRegisters[53] & 0x000f)
						publicParam.AutomaticRackTransferEarlyWarningTime = int(Mb.HoldingRegisters[54] >> 8 & 0x000f)
						publicParam.SpacingDistanceBetweenStretchGuards = int(Mb.HoldingRegisters[55] >> 8)
						publicParam.SpacingDistanceBetweenStopGuards = int(Mb.HoldingRegisters[55] & 0x00ff)
						publicParam.PushingIntervalDistance = int(Mb.HoldingRegisters[57] >> 8)
						publicParam.MovingIntervalDistance = int(Mb.HoldingRegisters[57] & 0x00ff)
						publicParam.SprayIntervalDistance = int(Mb.HoldingRegisters[58] & 0x00ff)
						publicParam.AdjacentBracketCenterDistance = int(Mb.HoldingRegisters[59] >> 8)
						publicParam.ShearerLength = int(Mb.HoldingRegisters[59] & 0x00ff)
						publicParam.PullBackDistance = int(Mb.HoldingRegisters[60] & 0x00ff)
						publicParam.PostFallColumnTime = int(Mb.HoldingRegisters[61] & 0x00ff)
						publicParam.ColumnTimeAfterRise = int(Mb.HoldingRegisters[62] & 0x00ff)
						publicParam.CloseBoardTime = int(Mb.HoldingRegisters[63] >> 8)
						publicParam.OpenBoardTime = int(Mb.HoldingRegisters[63] & 0x00ff)
						publicParam.BackSlipTime = int(Mb.HoldingRegisters[64] & 0x00ff)
						publicParam.DataSheetCRC = int(Mb.HoldingRegisters[69])
						publicParam.AutoBackwashRemainingH = int(Mb.HoldingRegisters[72] >> 8)
						publicParam.AutoBackwashRemainingM = int(Mb.HoldingRegisters[72] & 0x00ff)
						publicParam.AutoRefillRemainingH = int(Mb.HoldingRegisters[73] >> 8)
						publicParam.AutoRefillRemainingM = int(Mb.HoldingRegisters[73] & 0x00ff)
						WebsocketMessage := model.WebsocketMessage{
							Type:    "publicparam",
							Source:  0,
							Message: publicParam,
						}
						strings, _ := json.Marshal(WebsocketMessage)
						fmt.Println("公参", len(strings))
						service.WebsocketManager.SendAll(strings)
					} else if information_data[0] > 102 && information_data[0] < 112 {
						// if (time.Now().Unix() - CanHeart[179]) > 180 {

						// 	Mb.HoldingRegisters[1520+179*9] = uint16(253 + rand.Intn(10))

						// 	CanHeart[179] = time.Now().Unix()
						// 	// user := model.Pressure{}
						// 	// if mysql.Mysqlclient != nil {
						// 	// 	mysql.Mysqlclient.Model(&user).Where("support=?", 180).Update("LeftValue1", int(Mb.HoldingRegisters[1520+179*9]))
						// 	// 	mysql.Mysqlclient.Model(&user).Where("support=?", 180).Update("RightValue1", int(Mb.HoldingRegisters[152+179*9]))
						// 	// 	mysql.Mysqlclient.Model(&user).Where("support=?", 180).Update("Time1", time.Now())
						// 	// }
						// }

						rand.Seed(time.Now().UnixNano())

						SimulationHeart[source_id-1] = time.Now().Unix()

						if utils.Conf.PRESSUREPARAM.Enable == 1 {
							// / (int(source_id) < 7 ||
							if information_data[0] == 103 {
								// if (int(source_id) > (utils.Conf.SYSTEM.SupportNum - 11)) && (uint16(information_data[2])<<8)|uint16(information_data[3]) < 252 {
								// 	Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] = uint16(253 + rand.Intn(10))
								// 	if source_id > 163 {
								// 		fmt.Println("超前架数据补足", source_id, information_data, Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9])
								// 	}
								// } else {
								// 	Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] = (uint16(information_data[2]) << 8) | uint16(information_data[3])
								// }
								//fmt.Println("矿压数据上传")
								if (uint16(information_data[2])<<8)|uint16(information_data[3]) > (uint16(information_data[4])<<8)|uint16(information_data[5]) {
									Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] = (uint16(information_data[2]) << 8) | uint16(information_data[3])
								} else {
									Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] = (uint16(information_data[4]) << 8) | uint16(information_data[5])
								}

								// if Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] < 252 { //压力不足
								// 	var count int64
								// 	//mysql.Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), source_id).Count(&count)
								// 	if count == 0 {
								// 		//user := model.PressureFaultDiagnosis{Support: int(source_id), Date: time.Now(), LowAlarmTimes: 1}
								// 		//mysql.Mysqlclient.Select("Support", "Date", "AlarmTimes").Create(&user)
								// 	} else {
								// 		//var pressureFaultDiagnosis model.PressureFaultDiagnosis
								// 		//mysql.Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), int(source_id)).Find(&pressureFaultDiagnosis)
								// 		//mysql.Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), int(source_id)).Update("low_alarm_times", pressureFaultDiagnosis.LowAlarmTimes+1)
								// 	}
								// } else if Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] > 400 {
								// 	var count int64
								// 	//mysql.Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), source_id).Count(&count)
								// 	if count == 0 {
								// 		//user := model.PressureFaultDiagnosis{Support: int(source_id), Date: time.Now(), HighAlarmTimes: 1}
								// 		//mysql.Mysqlclient.Select("Support", "Date", "AlarmTimes").Create(&user)
								// 	} else {
								// 		//var pressureFaultDiagnosis model.PressureFaultDiagnosis
								// 		//mysql.Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), int(source_id)).Find(&pressureFaultDiagnosis)
								// 		//mysql.Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), int(source_id)).Update("high_alarm_times", pressureFaultDiagnosis.HighAlarmTimes+1)
								// 	}
								// }
							}

							if information_data[0] == 106 {
								if (int(source_id) % 10) == 0 {
									Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] = (uint16(information_data[2]) << 8) | uint16(information_data[3])
								} else {
									Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] = 0
								}

							}

							Mb.HoldingRegisters[int(information_data[0])+1418+int(source_id-1)*9] = (uint16(information_data[4]) << 8) | uint16(information_data[5])

							//推移距离
							Mb.HoldingRegisters[int(information_data[0])+1419+int(source_id-1)*9] = (uint16(information_data[6]) << 8) | uint16(information_data[7])
						} else {
							Mb.HoldingRegisters[int(information_data[0])+1417+int(source_id-1)*9] = (uint16(information_data[2]) << 8) | uint16(information_data[3])
							Mb.HoldingRegisters[int(information_data[0])+1418+int(source_id-1)*9] = (uint16(information_data[4]) << 8) | uint16(information_data[5])
							Mb.HoldingRegisters[int(information_data[0])+1419+int(source_id-1)*9] = (uint16(information_data[6]) << 8) | uint16(information_data[7])
						}

						if information_data[0] == 103 {
							//fmt.Println(source_id, "号支架上传压力")
							// if source_id == 196 {

							// 	log.Println(source_id, "号支架左压力:", (uint16(information_data[2])<<8)|uint16(information_data[3]), "右压力:", (uint16(information_data[4])<<8)|uint16(information_data[5]))
							// }
							//user := model.Pressure{}
							//if mysql.Mysqlclient != nil {
							// mysql.Mysqlclient.Model(&user).Where("support=?", source_id).Update("LeftValue1", (uint16(information_data[2])<<8)|uint16(information_data[3]))
							// mysql.Mysqlclient.Model(&user).Where("support=?", source_id).Update("RightValue1", (uint16(information_data[4])<<8)|uint16(information_data[5]))
							// mysql.Mysqlclient.Model(&user).Where("support=?", source_id).Update("Time1", time.Now())
							//mysql.Mysqlclient.Model(&user).Where("support=?", source_id).Update("Interval1", int(time.Now().Unix())-PressInterval[source_id-1])
							// for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
							// 	PressInterval[i] = int(PressLastTime[i].Unix())
							// }
							//PressInterval[source_id-1] = int(time.Now().Unix() - PressLastTime[source_id-1].Unix())
							PressLastTime[source_id-1] = time.Now()
							//上传异常数据库记录

							//}
						}

					} else if information_data[0] == 126 {
						// Param1[source_id-1] = int(information_data[2] >> 4)
						// Param3[source_id-1] = int(time.Now().Unix() - LastMode2Time[source_id-1])
						// Param4[source_id-1] = int(math.Abs(float64(int(Mb.HoldingRegisters[180]) - int(source_id)*10)))
						//Mb.HoldingRegisters[5400+int(source_id)-1] = (uint16(information_data[4]) << 8) | uint16(information_data[5])

					} else if information_data[0] == 129 {
						//Param2[int(source_id)-1] = int(information_data[2] & 0x7f)
						if int(information_data[2]) == 0 || int(information_data[2]) == 38 { //空闲状态
							WorkMode[int(source_id)-1] = 0
						} else {
							if int(information_data[2]) == 3 || int(information_data[2]) == 4 || int(information_data[2]) == 5 || int(information_data[2]) == 6 {
								WorkMode[int(source_id)-1] = 2
							} else {
								WorkMode[int(source_id)-1] = 1
							}
							//fmt.Println("wifi有动作上传", int(v[6]>>4), int(v[12]&0x7f), (int(time.Now().Unix() - LastMode2Time[int(v[3])-1])), (int(math.Abs(float64(int(mb.HoldingRegisters[180]) - int(v[3])*10)))))
							// if Param1[source_id-1] > utils.Conf.MODEPARAM.Intervention &&
							// 	(int(information_data[2]&0x7f) == utils.Conf.MODEPARAM.ActionCode1 ||
							// 		int(information_data[2]&0x7f) == utils.Conf.MODEPARAM.ActionCode2 ||
							// 		int(information_data[2]&0x7f) == utils.Conf.MODEPARAM.ActionCode3 ||
							// 		int(information_data[2]&0x7f) == utils.Conf.MODEPARAM.ActionCode4 ||
							// 		int(information_data[2]&0x7f) == utils.Conf.MODEPARAM.ActionCode5 ||
							// 		int(information_data[2]&0x7f) == utils.Conf.MODEPARAM.ActionCode6 ||
							// 		int(information_data[2]&0x7f) == utils.Conf.MODEPARAM.ActionCode7 ||
							// 		int(information_data[2]&0x7f) == utils.Conf.MODEPARAM.ActionCode8) &&
							// 	(int(time.Now().Unix()-LastMode2Time[int(source_id)-1]) > utils.Conf.MODEPARAM.TimeInterval) &&
							// 	(int(math.Abs(float64(int(Mb.HoldingRegisters[180])-int(source_id)*10))) < utils.Conf.MODEPARAM.PositionLimit) {
							// 	if rand.Intn(100) >= utils.Conf.MODEPARAM.ProbabilityHand {
							// 		WorkMode[int(source_id)-1] = 2
							// 	} else {
							// 		WorkMode[int(source_id)-1] = 1
							// 	}

							// 	LastMode2Time[int(source_id)-1] = time.Now().Unix()
							// } else {
							// 	if rand.Intn(100) >= utils.Conf.MODEPARAM.ProbabilityAuto {
							// 		WorkMode[int(source_id)-1] = 1
							// 	} else {
							// 		WorkMode[int(source_id)-1] = 2
							// 	}

							// }

						}
						// if information_data[3] == 0 || information_data[3] == 13 || information_data[3] == 14 {
						// 	Mb.HoldingRegisters[4000+int(source_id-1)] = 0
						// } else if information_data[3] == 12 {
						// 	Mb.HoldingRegisters[4000+int(source_id-1)] = 1
						// }
						// Mb.HoldingRegisters[4220+int(source_id-1)] = uint16(information_data[3])

						// var lockStatus model.LockStatus
						// lockStatus.KeyStatus = int(Mb.HoldingRegisters[4000+int(source_id-1)])
						// lockStatus.Status = int(Mb.HoldingRegisters[4220+int(source_id-1)])
						// WebsocketMessage := model.WebsocketMessage{
						// 	Type:    "lockStatus",
						// 	Source:  int(source_id),
						// 	Message: lockStatus,
						// }
						// strings, _ := json.Marshal(WebsocketMessage)
						// service.WebsocketManager.SendAll(strings)
					} else if information_data[0] == 138 {
						//fmt.Println("can回复自动化状态", uint16(information_data[2]), uint16(information_data[3]), uint16(information_data[4]), uint16(information_data[5]), uint16(information_data[6]), uint16(information_data[7]))
						RandomAutoReceive[source_id-1] = int(information_data[2] >> 7)
						var autoFollowStatus model.AutoFollowStatus
						for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
							//Mb.HoldingRegisters[3500+i] = Mb.HoldingRegisters[3500+i] & 0xfff0
							autoFollowStatus.IsAutoFollow = RandomAutoReceive
							autoFollowStatus.CompleteAutomaticPush = append(autoFollowStatus.CompleteAutomaticPush, int(Mb.HoldingRegisters[3500+i]>>3&0x0001))
							autoFollowStatus.CompleteAutomaticRackTransfer = append(autoFollowStatus.CompleteAutomaticRackTransfer, int(Mb.HoldingRegisters[3500+i]>>2&0x0001))
							autoFollowStatus.CompleteAutomaticCare = append(autoFollowStatus.CompleteAutomaticCare, int(Mb.HoldingRegisters[3500+i]>>1&0x0001))
							autoFollowStatus.CompleteAutomaticExtension = append(autoFollowStatus.CompleteAutomaticExtension, int(Mb.HoldingRegisters[3500+i]&0x0001))
						}
						WebsocketMessage := model.WebsocketMessage{
							Type:    "autoFollowStatus",
							Source:  0,
							Message: autoFollowStatus,
						}
						strings, _ := json.Marshal(WebsocketMessage)
						service.WebsocketManager.SendAll(strings)

						if Mb.HoldingRegisters[187] == 0 {
							exist := contains(RandomAutoReceive, 1)
							if exist {
								Mb.HoldingRegisters[177] = 1
							} else {
								Mb.HoldingRegisters[177] = 0
							}

						} else {
							Mb.HoldingRegisters[177] = 1
						}

					} else if information_data[0] == 144 {
						//fmt.Println("收到负载率", (uint16(information_data[2])<<8)|uint16(information_data[3]), (uint16(information_data[4])<<8)|uint16(information_data[5]))
						Mb.HoldingRegisters[185] = (uint16(information_data[2]) << 8) | uint16(information_data[3])
						Mb.HoldingRegisters[186] = (uint16(information_data[4]) << 8) | uint16(information_data[5])
					} else if information_data[0] == 101 {
						fmt.Println("收到需要记录支架运行数据")

						// now := time.Now()
						// record := model.RecordCommand{TableTime: now}
						// tableName := record.TableName()
						// if lastTableName != tableName {
						// 	lastTableName = tableName
						// 	fmt.Println("检查表存在 - 时间:", now.Format("2006-01-02 15:04:05"), "表名:", tableName)
						// 	exit := mysql.Mysqlclient.Migrator().HasTable(tableName)
						// 	if !exit {
						// 		fmt.Println("正在创建表: ", tableName)
						// 		if err := mysql.Mysqlclient.Table(tableName).AutoMigrate(&model.RecordCommand{}); err != nil {
						// 			log.Println("创建指令记录表异常: ", err)

						// 		} else {
						// 			// 检查索引是否存在，如果不存在则创建
						// 			indexName := "idx_time"
						// 			migrator := mysql.Mysqlclient.Migrator()
						// 			if !migrator.HasIndex(tableName, indexName) {
						// 				// 如果不存在，创建索引
						// 				sql := fmt.Sprintf("CREATE INDEX %s ON %s (time)", indexName, tableName)

						// 				if err := mysql.Mysqlclient.Exec(sql).Error; err != nil {
						// 					log.Printf("创建索引 %s 失败: %v\n", indexName, err)
						// 				} else {
						// 					fmt.Printf("成功创建索引: %s\n", indexName)
						// 				}
						// 			}
						// 		}

						// 	}
						// }

						temp1 := model.RecordCommand{}
						temp1.SourceId = int(source_id)
						temp1.Time = time.Now()
						temp1.ControlCommandDeviceId = int(information_data[2])
						//fmt.Println("收到记录支架运行数据-当前命令源", int(information_data[3]))
						temp1.CurrentCommandSource = strconv.Itoa(int(information_data[3]))
						//fmt.Println("转化后：", strconv.Itoa(int(information_data[3])))
						//fmt.Println("m", m, int(information_data[5]), m[int(information_data[5])])
						commandDiscribe := m[int(information_data[5])]
						commandCode := int(information_data[5]) & 0x7f
						if commandCode < 37 && commandCode > 0 {
							commandDiscribe = m[commandCode]
							isRun := int(information_data[5] >> 7)
							if isRun == 1 {
								commandDiscribe = "启动" + commandDiscribe
							} else {
								commandDiscribe = "停止" + commandDiscribe
							}

						} else {

							if commandDiscribe == "" {
								commandDiscribe = strconv.Itoa(int(information_data[5]))
							}
						}

						temp1.CommandType = commandDiscribe
						temp1.TableTime = time.Now()
						fmt.Println("收到记录支架运行数据", temp1)
						//mysql.Mysqlclient.Table(tableName).Select("Time", "CurrentCommandSource", "ControlCommandDeviceId", "CommandType", "SourceId").Create(&temp1)

						select {
						case RecordCommandChan <- temp1:
						default:
						}

					}

					//fmt.Println("0189")
				} else if data_type == 0x01 && sub_type == 0x84 {
					CanHeart[source_id-1] = time.Now().Unix()
					state = Mb.HoldingRegisters[3500+int(source_id-1)] & 0x000f
					//Mb.HoldingRegisters[3500+int(source_id-1)] = uint16(information_data[6])<<8 | (uint16(information_data[5]) << 4) | (Mb.HoldingRegisters[3500+int(source_id-1)])
					//if uint16(information_data[6]) > 60 {
					//fmt.Println("0184当前执行命令", "target_id", target_id, "source_id", source_id, "数据", information_data, "煤机位置", Mb.HoldingRegisters[180])
					log.Println("0184当前执行命令", "target_id", target_id, "source_id", source_id, "数据", information_data, "煤机位置", Mb.HoldingRegisters[180])
					//}
					//柳塔红绿灯
					// if source_id >= 1 && source_id <= 5 {
					// 	if (uint16(information_data[2])<<8|(uint16(information_data[3])) == 4) || (uint16(information_data[2])<<8|(uint16(information_data[3])) == 8) {
					// 		Mb.HoldingRegisters[187+source_id] = 1
					// 	} else {
					// 		Mb.HoldingRegisters[187+source_id] = 0
					// 	}
					// }
					// var data upload.RealData
					// data.Year = time.Now().Year()
					// data.Month = int(time.Now().Month())
					// data.Date = time.Now().Day()
					// data.Hour = time.Now().Hour()
					// data.Minute = time.Now().Minute()
					// data.Second = time.Now().Second()
					// data.SupportNum = int(source_id)
					// if uint16(information_data[6]) == 30 || uint16(information_data[6]) == 31 || uint16(information_data[6]) == 32 || uint16(information_data[6]) == 34 || uint16(information_data[6]) == 35 {
					// 	data.ActionType = 0
					// } else if uint16(information_data[6]) >= 1 && uint16(information_data[6]) <= 29 {
					// 	data.ActionType = 1
					// }
					// data.ActionCode = int(information_data[6])
					// upload.RealAciton <- data

					if uint16(information_data[6]) == 30 {
						log.Println("自动化动作", "单支架自动移架", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 31 {
						state = state | 0x0004

						log.Println("自动化动作", "编组自动移架", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 32 {
						state = state | 0x0008
						log.Println("自动化动作", "编组推溜", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 34 {
						state = state & 0xfffe
						state = state | 0x0002
						log.Println("自动化动作", "编组自动收护帮板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 35 {
						state = state & 0xfffd
						state = state | 0x0001
						log.Println("自动化动作", "编组自动伸护帮板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 1 {
						log.Println("手动动作", "升柱", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])

					} else if uint16(information_data[6]) == 2 {
						log.Println("手动动作", "降柱", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 3 {
						log.Println("手动动作", "移架", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 4 {
						log.Println("手动动作", "推溜", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 5 {
						log.Println("手动动作", "伸一级护帮板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 6 {
						log.Println("手动动作", "收一级护帮板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])

						// } else if uint16(information_data[6]) == 7 {
						// 	log.Println("手动动作", "伸前梁", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 8 {
						// 	log.Println("手动动作", "收前梁", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
					} else if uint16(information_data[6]) == 9 {
						log.Println("手动动作", "起底", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 10 {
						log.Println("手动动作", "伸平衡", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 11 {
						log.Println("手动动作", "收平衡", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])
					} else if uint16(information_data[6]) == 12 {
						log.Println("手动动作", "开喷雾", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9], Mb.HoldingRegisters[179], RandomAutoReceive[source_id-1])

						// } else if uint16(information_data[6]) == 13 {
						// 	log.Println("手动动作", "抬低移架", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 14 {
						// 	log.Println("手动动作", "伸侧护板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 15 {
						// 	log.Println("手动动作", "收侧护板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 16 {
						// 	log.Println("手动动作", "伸底调", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 17 {
						// 	log.Println("手动动作", "收底调", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 18 {
						// 	log.Println("手动动作", "降柱抬底", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 19 {
						// 	log.Println("手动动作", "反冲洗", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 20 {
						// 	log.Println("手动动作", "伸二级护帮板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 21 {
						// 	log.Println("手动动作", "收二级护帮板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 22 {
						// 	log.Println("手动动作", "伸三级护帮板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 23 {
						// 	log.Println("手动动作", "收三级护帮板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 24 {
						// 	log.Println("手动动作", "升后柱", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 25 {
						// 	log.Println("手动动作", "降后柱", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 26 {
						// 	log.Println("手动动作", "打开插板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 27 {
						// 	log.Println("手动动作", "关闭插板", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 28 {
						// 	log.Println("手动动作", "推后溜", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
						// } else if uint16(information_data[6]) == 29 {
						// 	log.Println("手动动作", "拉后溜", source_id, Mb.HoldingRegisters[180], Mb.HoldingRegisters[1522+(int(source_id)-1)*9],Mb.HoldingRegisters[179])
					}

					Mb.HoldingRegisters[3500+int(source_id-1)] = uint16(information_data[6])<<8 | (uint16(information_data[5]) << 4) | state
					fmt.Println("寄存器", 3500+int(source_id-1), Mb.HoldingRegisters[3500+int(source_id-1)])
					// fmt.Println("收到了命令码", source_id, information_data[6]&0x7f)
					// CommandCode := information_data[6] & 0x7f
					// if CommandCode > 0 && CommandCode < 30 { //手动命令
					// 	if int(math.Abs(float64(int(Mb.HoldingRegisters[180])-int(source_id)*10))) < utils.Conf.MODEPARAM.PositionLimit { //位置限制和手动频率限制
					// 		fmt.Println("收到了手动命令码", source_id, information_data[6]&0x7f)
					// 		if int(time.Now().Unix()-LastMode2Time[int(source_id)-1]) > utils.Conf.MODEPARAM.TimeInterval {
					// 			WorkMode[source_id-1] = 2
					// 			WorkModeTime[source_id-1] = time.Now().Unix()
					// 			LastMode2Time[int(source_id)-1] = time.Now().Unix()
					// 		}

					// 	} else {
					// 		fmt.Println("过滤手动命令", source_id, information_data[6]&0x7f, (int(math.Abs(float64(int(Mb.HoldingRegisters[180]) - int(source_id)*10)))))
					// 		WorkMode[source_id-1] = 0
					// 	}

					// } else if CommandCode > 30 && CommandCode < 37 { //自动命令
					// 	fmt.Println("收到了自动化命令码", source_id, information_data[6]&0x7f)
					// 	WorkMode[source_id-1] = 1
					// 	WorkModeTime[source_id-1] = time.Now().Unix()
					// }

					// if information_data[6] == 38 {
					// 	IsAuto[int(source_id-1)] = 1
					// }
					// if IsAuto[int(source_id-1)] < 2 {

					// } else {
					// 	Mb.HoldingRegisters[3500+int(source_id-1)] = uint16(0)<<8 | (uint16(information_data[5]) << 4) | uint16(information_data[1]&0x0f)
					// 	var autoFollowStatus model.AutoFollowStatus
					// 	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
					// 		autoFollowStatus.IsAutoFollow = append(autoFollowStatus.IsAutoFollow, int(Mb.HoldingRegisters[3500+i]>>8&0x00ff))
					// 		autoFollowStatus.CompleteAutomaticPush = append(autoFollowStatus.CompleteAutomaticPush, int(Mb.HoldingRegisters[3500+i]>>3&0x0001))
					// 		autoFollowStatus.CompleteAutomaticRackTransfer = append(autoFollowStatus.CompleteAutomaticRackTransfer, int(Mb.HoldingRegisters[3500+i]>>2&0x0001))
					// 		autoFollowStatus.CompleteAutomaticCare = append(autoFollowStatus.CompleteAutomaticCare, int(Mb.HoldingRegisters[3500+i]>>1&0x0001))
					// 		autoFollowStatus.CompleteAutomaticExtension = append(autoFollowStatus.CompleteAutomaticExtension, int(Mb.HoldingRegisters[3500+i]&0x0001))
					// 	}
					// 	WebsocketMessage := model.WebsocketMessage{
					// 		Type:    "autoFollowStatus",
					// 		Source:  0,
					// 		Message: autoFollowStatus,
					// 	}
					// 	strings, _ := json.Marshal(WebsocketMessage)
					// 	service.WebsocketManager.SendAll(strings)
					// }
					// if information_data[6] == 39 {
					// 	IsAuto[int(source_id-1)] = 2
					// }
					// if mysql.Mysqlclient != nil {
					// 	user := model.CanActionData{Time: time.Now(), CanId: canid, Len: len(information_data), Information: information_data_string}
					// 	mysql.Mysqlclient.Select("Time", "Type", "CanId", "Len", "Information").Create(&user)
					// }
					//fmt.Println("0184")
				} else if data_type == 0x00 && sub_type == 0x01 { //紧急故障广播
					// if source_id != 180 {
					// 	CanHeart[source_id-1] = time.Now().Unix()
					// }
					//fmt.Println("0181")
					faultCode := uint16(information_data[0])<<8 | uint16(information_data[1])
					Mb.HoldingRegisters[8000+int(source_id-1)] = faultCode
					if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 7 & 0x0001) == 1 {
						Mb.HoldingRegisters[4000+int(source_id-1)] = 0
						Mb.HoldingRegisters[4220+int(source_id-1)] = 14

					} else if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 7 & 0x0001) == 0 {
						if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 6 & 0x0001) == 1 {
							Mb.HoldingRegisters[4000+int(source_id-1)] = 0
							Mb.HoldingRegisters[4220+int(source_id-1)] = 13
						}
					}

					// if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 5 & 0x0001) == 1 {
					// 	// temp := model.PowerData{}
					// 	// temp.Time = time.Now()
					// 	// temp.Error = "电池电压不足"
					// 	// temp.SourceId = int(source_id)
					// 	// temp.TargetId = int(target_id)
					// 	// mysql.Mysqlclient.Select("Time", "Error", "SourceId", "TargetId").Create(&temp)
					// 	//log.Println("电池电压不足", "source_id", source_id, "target_id", target_id)
					// }

					// 故障解析组装给到通道进行存储记录
					for bit, desc := range faultMap {
						if (faultCode >> bit & 0x0001) == 1 {
							fault := model.FaultRecord{
								Time:      time.Now(),
								SourceId:  int(source_id),
								FaultType: desc,
								TableTime: time.Now().Format("200601"), // Table name format
							}
							select {
							case FaultRecordChan <- fault: // Send to the new fault channel
							default:
								log.Println("Warning: FaultRecordChan full, dropping fault record:", fault)
							}
						}
					}
					//巡检软件闭锁-uwb
					if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 10 & 0x0001) == 1 { //软件闭锁
						select {
						case Ruanbisuo <- int(source_id):
						default:
							log.Println("Ruanbisuo channel full, drop", source_id)
						}
					}
					// if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 9 & 0x0001) == 1 {
					// 	//log.Println("触发跳架保护")
					// }
					sensorMutex.Lock()
					if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 13 & 0x0001) == 1 {
						Mb.HoldingRegisters[5000+int(source_id-1)] = 1
						SensorCache[int(source_id)] = 1
						//fmt.Println(source_id, "推移行程传感器故障", SensorCache)
					} else if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 13 & 0x0001) == 0 {
						Mb.HoldingRegisters[5000+int(source_id-1)] = 0
						SensorCache[int(source_id)] = 0
						//fmt.Println(source_id, "推移行程传感器故障解除", SensorCache)
					}
					sensorMutex.Unlock()
					if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 14 & 0x0001) == 1 {
						Mb.HoldingRegisters[5200+int(source_id-1)] = 1
						//fmt.Println(source_id, "lin故障")
					} else if (Mb.HoldingRegisters[8000+int(source_id-1)] >> 14 & 0x0001) == 0 {
						Mb.HoldingRegisters[5200+int(source_id-1)] = 0
						//fmt.Println(source_id, "lin故障解除")
					}

					var lockStatus model.LockStatus
					lockStatus.KeyStatus = int(Mb.HoldingRegisters[4000+int(source_id-1)]) //是否软件闭锁
					lockStatus.Status = int(Mb.HoldingRegisters[4220+int(source_id-1)])    // 急停 or 闭锁
					WebsocketMessage := model.WebsocketMessage{
						Type:    "lockStatus",
						Source:  int(source_id),
						Message: lockStatus,
					}
					strings, _ := json.Marshal(WebsocketMessage)
					//fmt.Println("故障", len(strings))
					service.WebsocketManager.SendAll(strings)
					//fmt.Println("0181")
				} else if data_type == 0x00 && sub_type == 0x0b {
					log.Println("000b帧头", "target_id", target_id, "source_id", source_id, "数据", information_data, "煤机位置", Mb.HoldingRegisters[180])
				} else if data_type == 0x07 && sub_type == 0x00 { //王总
					//fmt.Println("收到误码率测试数据回帧")
					//ReceiveTestData <- information_data
				}
				// } else if data_type == 0x01 && sub_type == 0x83 { //查询状态
				// 	//fmt.Println(source_id, "号支架查询状态回复")
				// 	CanHeart[source_id-1] = time.Now().Unix()
				// }
				// 	if len(s) > 13 {
				// 		s = s[13:]
				// 		goto loop
				// 	}
				// } else {
				// 	time.Sleep(time.Millisecond * 20)
				// }//ixNano()-t1, s)
				if (time.Now().UnixNano() - t1) > 1 {
					//fmt.Println("耗时", time.Now().UnixNano()-t1, s)
				}

			}()
		}
	}
}

func assembleWifiIDQuery(TargetID byte, address, number int) (data bytes.Buffer) {
	var b bytes.Buffer
	b.Write([]byte{0xcd, 0xef})
	b.WriteByte(TargetID)
	b.WriteByte(0)
	b.WriteByte(0x03)
	b.WriteByte(byte(address >> 8))     //起始地址高八位
	b.WriteByte(byte(address & 0x00ff)) //起始地址低八位
	b.WriteByte(byte(number >> 8))      //寄存器数量高八位
	b.WriteByte(byte(number & 0x00ff))  //寄存器数量低八位
	data = b
	return
}

// 读取私有动态参数（只读）
func CANRequestPrivateData(TargetID int) {
	cf := CanFrame{}
	cf.Length = 2
	cf.FrameID = assembleCanID(0x00, 0x09, byte(TargetID), 0xff)
	cf.Data[0] = byte(100)
	cf.Data[1] = byte(9)
	if TargetID < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if TargetID >= utils.Conf.CAN.Point1 && TargetID < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if TargetID >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}

}

func CANRequestStatus(TargetID int) {
	cf := CanFrame{}
	cf.Length = 0
	cf.FrameID = assembleCanID(0x00, 0x03, byte(TargetID), 0xff)
	if TargetID < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if TargetID >= utils.Conf.CAN.Point1 && TargetID < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if TargetID >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
}

// 读取私有静态参数
func CANRequestPrivateStaticData(TargetID int) {
	fmt.Println("通用读取私有参数-", "支架号：", TargetID)
	// if time.Now().Unix()-tcpService.WifiHeart[TargetID-1] < 5 {
	// 	fmt.Println("wifi查询")
	// 	msg := assembleWifiIDQuery(byte(TargetID), 13, 2)
	// 	tcpService.SendMsgTo(byte(TargetID), utils.AddModbusCRC(msg.Bytes()))
	// } else {
	fmt.Println("can查询")
	cf := CanFrame{}
	cf.Length = 2
	cf.FrameID = assembleCanID(0x00, 0x09, byte(TargetID), 0xff)
	cf.Data[0] = byte(13)
	cf.Data[1] = byte(2)
	if TargetID < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if TargetID >= utils.Conf.CAN.Point1 && TargetID < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if TargetID >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
	cf.Data[0] = byte(11)
	cf.Data[1] = byte(1)
	if TargetID < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if TargetID >= utils.Conf.CAN.Point1 && TargetID < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if TargetID >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
	//}
}

// 读取公共参数
func CANRequestPublicData() {
	fmt.Println("通用读取公共参数")
	// for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
	// 	if time.Now().Unix()-tcpService.WifiHeart[i] < 5 {
	// 		msg := assembleWifiIDQuery(byte(131), 15, 59)
	// 		tcpService.SendMsgTo(byte(1), utils.AddModbusCRC(msg.Bytes()))
	// 		return
	// 	}
	// }
	cf := CanFrame{}
	cf.Length = 2
	cf.FrameID = assembleCanID(0x00, 0x09, byte(1), 0xff)
	for i := 15; i < 74; i += 3 {
		cf.Data[0] = byte(i)
		cf.Data[1] = byte(3)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
		time.Sleep(10 * time.Millisecond)
	}

}

func CANConfigurationPrivateParameterWeb(targetid int, param13 uint16, param14 uint16) {
	fmt.Println("web配置私有参数-", "支架号", targetid)
	cf := CanFrame{}
	cf.Length = 2
	cf.FrameID = assembleCanID(0x00, 0x0e, byte(targetid), byte(13))
	cf.Data[0] = byte(param13 >> 8)
	cf.Data[1] = byte(param13 & 0x00ff)
	if targetid < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if targetid >= utils.Conf.CAN.Point1 && targetid < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if targetid >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	cf.FrameID = assembleCanID(0x00, 0x0e, byte(targetid), byte(14))
	cf.Data[0] = byte(param14 >> 8)
	cf.Data[1] = byte(param14 & 0x00ff)
	if targetid < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if targetid >= utils.Conf.CAN.Point1 && targetid < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if targetid >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}

}

func CANConfigurationPrivateParameter(support int, remainder int) {
	fmt.Println("力控配置私有参数-", "支架号", support)

	cf := CanFrame{}
	cf.Length = 2
	if remainder == 2 {
		cf.FrameID = assembleCanID(0x00, 0x0e, byte(support), byte(13))
		cf.Data[0] = byte(Mb.HoldingRegisters[202+(support-1)*6] >> 8)
		cf.Data[1] = byte(Mb.HoldingRegisters[202+(support-1)*6] & 0x00ff)
	} else if remainder == 3 {
		cf.FrameID = assembleCanID(0x00, 0x0e, byte(support), byte(14))
		cf.Data[0] = byte(Mb.HoldingRegisters[203+(support-1)*6] >> 8)
		cf.Data[1] = byte(Mb.HoldingRegisters[203+(support-1)*6] & 0x00ff)
	}
	if support < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if support >= utils.Conf.CAN.Point1 && support < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if support >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}

}

// 配置公共参数
func CANConfigurationPublicParameter(mbAddr int64, value uint16) {
	fmt.Println("通用配置公共参数", mbAddr, value)
	if mbAddr == 18 {
		setSupportSort(value)
	}
	cf := CanFrame{}
	cf.Length = 2
	cf.FrameID = assembleCanID(0x00, 0x0e, 0x00, byte(mbAddr))
	cf.Data[0] = byte(value >> 8)
	cf.Data[1] = byte(value & 0x00ff)
	sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
}

func ConfigureAddress(targetid int) {
	cf := CanFrame{}
	cf.Length = 6
	if targetid < 0 { //上位机发送
		fmt.Println("力控地址配置-", "支架号：", Mb.HoldingRegisters[4]&0x00ff)
		cf.FrameID = assembleCanID(0x00, 0x02, byte(Mb.HoldingRegisters[4]&0x00ff), 0xff)
	} else if targetid > 0 {
		fmt.Println("web地址配置-", "支架号：", targetid)
		cf.FrameID = assembleCanID(0x00, 0x02, byte(targetid), 0xff)
	}
	cf.Data[0] = byte(40) //a8和28一起发 确保有一帧成功不知道为什么
	cf.Data[3] = 0xff
	if targetid < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if targetid >= utils.Conf.CAN.Point1 && targetid < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if targetid >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
	cf.Data[0] = byte(168)
	if targetid < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if targetid >= utils.Conf.CAN.Point1 && targetid < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if targetid >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
}

func CANSendAutoTraceShearer(value uint16) {
	if value == 1 {
		cf := CanFrame{}
		cf.Length = 4
		cf.FrameID = assembleCanID(0x00, 0x08, 0x00, 0xff)
		cf.Data[0] = byte((Mb.HoldingRegisters[180] + 5) >> 8)
		cf.Data[1] = byte((Mb.HoldingRegisters[180] + 5) & 0x00ff)

		if Mb.HoldingRegisters[179] == 1 || Mb.HoldingRegisters[179] == 5 || Mb.HoldingRegisters[179] == 9 {
			cf.Data[2] = byte(uint16(2)<<6) | byte(Mb.HoldingRegisters[179])
		} else if Mb.HoldingRegisters[179] == 3 || Mb.HoldingRegisters[179] == 7 || Mb.HoldingRegisters[179] == 11 {
			cf.Data[2] = byte(uint16(1)<<6) | byte(Mb.HoldingRegisters[179])
		}

		for i := 0; i < 6; i++ {
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			time.Sleep(time.Millisecond * 50)
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
			time.Sleep(time.Millisecond * 50)
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
		}
		time.Sleep(time.Millisecond * 100)

		cf = CanFrame{}
		cf.Length = 6
		cf.FrameID = assembleCanID(0x00, 0x02, 0x00, 0xff)
		cf.Data[3] = 0xff
		cf.Data[0] = byte(38)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
		time.Sleep(time.Millisecond * 50)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
		time.Sleep(time.Millisecond * 50)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	} else if value == 0 {
		cf := CanFrame{}
		cf.Length = 6
		cf.FrameID = assembleCanID(0x00, 0x02, 0x00, 0xff)
		cf.Data[3] = 0xff
		cf.Data[0] = byte(39)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
		time.Sleep(time.Millisecond * 50)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
		time.Sleep(time.Millisecond * 50)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}

}

// CANSendCommand 发送指令
func CANSendCommand(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case value := <-Controlcommand:
			{
				fmt.Println("收到控制命令", value)
				if utils.Conf.GLOBAL.ControlEnable == 1 { //远控模式
					buf := value //被控支架号+启停命令
					//判断是启动还是停止
					isRun := int(uint8(value&0x00ff) >> 7)
					Command := byte(buf & 0x00bf)     //控制命令
					TargetID := byte(buf >> 8 & 0xff) //被控支架号
					fmt.Println("支架号", TargetID, "是否启动", isRun, "控制命令", Command)
					cf := CanFrame{}
					cf.Length = 6
					cf.Data[3] = 0xff
					if TargetID != 0 {
						//单支架命令
						if isRun == 1 {
							cf.Data[0] = byte(isRun<<7) | byte(Command)
						} else if isRun == 0 {
							cf.Data[0] = byte(44)
						}
						cf.FrameID = assembleCanID(0x00, 0x02, TargetID, 0xff)
						if int(TargetID) < utils.Conf.CAN.Point1 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
						} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
						} else if int(TargetID) >= utils.Conf.CAN.Point2 && int(TargetID) < utils.Conf.CAN.PointExtra {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
						} else if int(TargetID) >= utils.Conf.CAN.PointExtra && int(TargetID) < utils.Conf.CAN.PointExtra1 {
							fmt.Println("发送超前", utils.Conf.CAN.Can2udpIpExtra)
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra)
						} else if int(TargetID) >= utils.Conf.CAN.PointExtra1 {
							fmt.Println("发送挡矸", cf, cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra1)
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra1)
						}
						// if time.Now().UnixNano()-canorwifi[TargetID-1] > 10000000000 {
						// 	if isRun == 1 {
						// 		msg := assembleWifiID(TargetID, Command)
						// 		tcpService.SendMsgTo(TargetID, utils.AddModbusCRC(msg.Bytes()))
						// 	} else if isRun == 0 {
						// 		msg := assembleWifiID(TargetID, byte(44))
						// 		tcpService.SendMsgTo(TargetID, utils.AddModbusCRC(msg.Bytes()))
						// 	}
						// }
					}
					//time.Sleep(time.Duration(300) * time.Millisecond)

				} else if utils.Conf.GLOBAL.ControlEnable == 2 { //支架调直命令

					buf := value                      //被控支架号+推移距离
					targetID := byte(buf >> 8 & 0xff) //高8位
					distance := (buf & 0x00ff) * 100  //低8位 x表示要动x个100ms
					log.Println("支架调直", targetID, distance)
					cf := CanFrame{}
					cf.Length = 6
					cf.FrameID = assembleCanID(0x00, 0x02, targetID, 0xff)
					cf.Data[0] = byte(1<<7) | byte(54)
					cf.Data[1] = byte(distance >> 8)
					cf.Data[2] = byte(distance & 0x00ff)
					cf.Data[3] = 0xff
					fmt.Println(cf)
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
				}

			}
		}
	}

}
func CANSendCommandNew(ctx context.Context) {
	value := 2056
	enable := utils.Conf.GLOBAL.ControlEnable
	fmt.Println("收到控制命令", value, enable)
	if utils.Conf.GLOBAL.ControlEnable == 1 { //远控模式
		buf := value //被控支架号+启停命令
		//判断是启动还是停止
		isRun := int(uint8(value&0x00ff) >> 7)
		Command := byte(buf & 0x00bf)     //控制命令
		TargetID := byte(buf >> 8 & 0xff) //被控支架号
		fmt.Println("支架号", TargetID, "是否启动", isRun, "控制命令", Command)
		cf := CanFrame{}
		cf.Length = 6
		cf.Data[3] = 0xff
		if TargetID != 0 {
			//单支架命令
			if isRun == 1 {
				cf.Data[0] = byte(isRun<<7) | byte(Command)
			} else if isRun == 0 {
				cf.Data[0] = byte(44)
			}
			cf.FrameID = assembleCanID(0x00, 0x02, TargetID, 0xff)
			fmt.Println("远控模式", cf)
			if int(TargetID) < utils.Conf.CAN.Point1 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
			} else if int(TargetID) >= utils.Conf.CAN.Point2 && int(TargetID) < utils.Conf.CAN.PointExtra {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
			} else if int(TargetID) >= utils.Conf.CAN.PointExtra && int(TargetID) < utils.Conf.CAN.PointExtra1 {
				fmt.Println("发送超前", utils.Conf.CAN.Can2udpIpExtra)
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra)
			} else if int(TargetID) >= utils.Conf.CAN.PointExtra1 {
				fmt.Println("发送挡矸", cf, cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra1)
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra1)
			}
			// if time.Now().UnixNano()-canorwifi[TargetID-1] > 10000000000 {
			// 	if isRun == 1 {
			// 		msg := assembleWifiID(TargetID, Command)
			// 		tcpService.SendMsgTo(TargetID, utils.AddModbusCRC(msg.Bytes()))
			// 	} else if isRun == 0 {
			// 		msg := assembleWifiID(TargetID, byte(44))
			// 		tcpService.SendMsgTo(TargetID, utils.AddModbusCRC(msg.Bytes()))
			// 	}
			// }
		}
		//time.Sleep(time.Duration(300) * time.Millisecond)

	} else if utils.Conf.GLOBAL.ControlEnable == 2 { //支架调直命令

		buf := value                      //被控支架号+推移距离
		targetID := byte(buf >> 8 & 0xff) //高8位
		distance := (buf & 0x00ff) * 100  //低8位 x表示要动x个100ms
		fmt.Println("支架调直", targetID, distance)
		cf := CanFrame{}
		cf.Length = 6
		cf.FrameID = assembleCanID(0x00, 0x02, targetID, 0xff)
		cf.Data[0] = byte(1<<7) | byte(54)
		cf.Data[1] = byte(distance >> 8)
		cf.Data[2] = byte(distance & 0x00ff)
		cf.Data[3] = 0xff
		fmt.Println("调直指令", cf)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	}

}

func CANSendCommandNew1(ctx context.Context) {
	value := 2056
	fmt.Println("收到控制命令", value)
	targetID := byte(value >> 8 & 0xff)
	// 解析低8位
	lowByte := byte(value & 0x00ff)

	// 通过第6位 (掩码 0x40) 区分指令类型
	// 0: 远控模式
	// 1: 支架调直模式 lowByte & 0x0040=64就是调直
	if (lowByte & 0x0040) == 0 {

		// 远控模式
		isRun := int(lowByte >> 7)      // 第7位: 启动(1)或停止(0)
		Command := byte(lowByte & 0x3F) // 取第0-5位作为控制命令 (排除第6、7位)

		fmt.Println("支架号", targetID, "是否启动", isRun, "控制命令", Command)

		cf := CanFrame{}
		cf.Length = 6
		cf.Data[3] = 0xff

		if targetID != 0 {
			// 构造数据帧
			if isRun == 1 {
				cf.Data[0] = byte(isRun<<7) | Command
			} else {
				cf.Data[0] = byte(44) // 停止命令
			}

			cf.FrameID = assembleCanID(0x00, 0x02, targetID, 0xff)
			fmt.Println("远控模式", cf)
			// 根据支架号范围路由UDP数据
			if int(targetID) < utils.Conf.CAN.Point1 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			} else if int(targetID) >= utils.Conf.CAN.Point1 && int(targetID) < utils.Conf.CAN.Point2 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
			} else if int(targetID) >= utils.Conf.CAN.Point2 && int(targetID) < utils.Conf.CAN.PointExtra {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
			} else if int(targetID) >= utils.Conf.CAN.PointExtra && int(targetID) < utils.Conf.CAN.PointExtra1 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra)
			} else if int(targetID) >= utils.Conf.CAN.PointExtra1 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra1)
			}
		}

	} else {

		// 支架调直模式
		if targetID != 0 {
			// 获取推移距离值 (取低6位，去掉标志位)
			distance := uint16(lowByte&0x3F) * 100 //表示要动x个100ms
			fmt.Println("支架调直", targetID, distance)
			cf := CanFrame{}
			cf.Length = 6
			cf.FrameID = assembleCanID(0x00, 0x02, targetID, 0xff)
			cf.Data[0] = byte(1<<7) | byte(54)
			cf.Data[1] = byte(distance >> 8)
			cf.Data[2] = byte(distance & 0x00ff)
			cf.Data[3] = 0xff
			fmt.Println("调直指令", cf)
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)

		}

	}
}
func CANSendCommand1(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case value := <-Controlcommand:
			{
				// 解析高8位作为目标ID (支架号)
				targetID := byte(value >> 8 & 0xff)
				// 解析低8位
				lowByte := byte(value & 0x00ff)

				// 通过第6位 (掩码 0x40) 区分指令类型
				// 0: 远控模式
				// 1: 支架调直模式 lowByte & 0x0040=64就是调直
				if (lowByte & 0x0040) == 0 {

					// 远控模式
					isRun := int(lowByte >> 7)      // 第7位: 启动(1)或停止(0)
					Command := byte(lowByte & 0x3F) // 取第0-5位作为控制命令 (排除第6、7位)

					fmt.Println("支架号", targetID, "是否启动", isRun, "控制命令", Command)

					cf := CanFrame{}
					cf.Length = 6
					cf.Data[3] = 0xff

					if targetID != 0 {
						// 构造数据帧
						if isRun == 1 {
							cf.Data[0] = byte(isRun<<7) | Command
						} else {
							cf.Data[0] = byte(44) // 停止命令
						}

						cf.FrameID = assembleCanID(0x00, 0x02, targetID, 0xff)

						// 根据支架号范围路由UDP数据
						if int(targetID) < utils.Conf.CAN.Point1 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
						} else if int(targetID) >= utils.Conf.CAN.Point1 && int(targetID) < utils.Conf.CAN.Point2 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
						} else if int(targetID) >= utils.Conf.CAN.Point2 && int(targetID) < utils.Conf.CAN.PointExtra {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
						} else if int(targetID) >= utils.Conf.CAN.PointExtra && int(targetID) < utils.Conf.CAN.PointExtra1 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra)
						} else if int(targetID) >= utils.Conf.CAN.PointExtra1 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIpExtra1)
						}
					}

				} else {

					// 支架调直模式
					if targetID != 0 {
						// 获取推移距离值 (取低6位，去掉标志位)
						distance := uint16(lowByte&0x3F) * 100 //表示要动x个100ms
						fmt.Println("支架调直", targetID, distance)
						cf := CanFrame{}
						cf.Length = 6
						cf.FrameID = assembleCanID(0x00, 0x02, targetID, 0xff)
						cf.Data[0] = byte(1<<7) | byte(54)
						cf.Data[1] = byte(distance >> 8)
						cf.Data[2] = byte(distance & 0x00ff)
						cf.Data[3] = 0xff
						sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)

					}

				}
			}
		}
	}
}

func ControlWeb(targetid int, endid int, command int, isrun int) {
	fmt.Println("网络控制命令下发")
	isRun := isrun
	com := command
	TargetID := byte(targetid) //被控支架号
	cf := CanFrame{}
	cf.Length = 6
	cf.Data[3] = 0xff
	fmt.Println(targetid, endid, command, isrun)
	if TargetID != 0 {
		if targetid == endid {
			//单支架命令
			fmt.Println("单支架命令")
			if isRun == 1 {
				cf.Data[0] = byte(isRun<<7) | byte(com)
			} else if isRun == 0 {
				cf.Data[0] = byte(44)
			}
			cf.FrameID = assembleCanID(0x00, 0x02, TargetID, 0xff)
			if int(TargetID) < utils.Conf.CAN.Point1 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
			} else if int(TargetID) >= utils.Conf.CAN.Point2 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
			}
		} else {
			//编组命令
			fmt.Println("编组动作命令")
			var direction int
			if targetid > endid {
				direction = 0 //反向
			} else {
				direction = 1 //正向
			}
			GroupNum := int(math.Abs(float64(targetid-endid)) + 1)
			if GroupNum > 10 {
				GroupNum = 10
			}
			TargetID := byte(targetid)
			for i := 0; i < GroupNum; i++ {
				if isRun == 1 {
					cf.Data[0] = byte(isRun<<7) | byte(com)
				} else if isRun == 0 {
					cf.Data[0] = byte(44) //停止命令
				}
				fmt.Println("指令，支架号：", TargetID)
				cf.FrameID = assembleCanID(0x00, 0x02, TargetID, 0xff)
				if int(TargetID) < utils.Conf.CAN.Point1 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
				} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
				} else if int(TargetID) >= utils.Conf.CAN.Point2 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
				}
				if direction == 0 {
					TargetID--
				} else if direction == 1 {
					TargetID++
				}
				time.Sleep(time.Duration(10) * time.Millisecond)
			}

		}

	}
}

// func Heart(ctx context.Context, mb *mbserver.Server) {
// 	tickerSendTime := time.NewTicker(1 * time.Second)
// 	HeartCount = 1
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case <-tickerSendTime.C:
// 			mb.HoldingRegisters[175] = uint16(HeartCount)
// 			HeartCount += 1
// 			if HeartCount > 1024 {
// 				HeartCount = 1
// 			}
// 		}
// 	}
// }

// 问询can负载率顺便问
func CanLoadRate(ctx context.Context, mb *mbserver.Server) {
	tickerSendTime := time.NewTicker(5 * time.Second)
	defer tickerSendTime.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerSendTime.C:
			//fmt.Println("发送问询负载率")
			cf := CanFrame{}
			cf.Length = 2
			cf.FrameID = assembleCanID(0x00, 0x09, byte(1), 0xff) //向第一架问询发送消息
			cf.Data[0] = byte(144)
			cf.Data[1] = byte(3)
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)

		}
	}
}

// 广播方式问询
func CanAutoState(ctx context.Context, mb *mbserver.Server) {
	fmt.Println("进入发送问询can自动化状态", utils.Conf.SYSTEM.SupportNum)
	tickerSendTime := time.NewTicker(25 * time.Second)
	//<-tickerSendTime.C
	defer tickerSendTime.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerSendTime.C:
			// for i := 1; i <= utils.Conf.SYSTEM.SupportNum; i++ {
			//126/4
			fmt.Println("发送问询can自动化状态", time.Now().Format("2006-01-02 15:04:05"))
			cf := CanFrame{}
			cf.Length = 2
			//byte(i)
			cf.FrameID = assembleCanID(0x00, 0x09, 0x00, 0xff) //向第一架问询发送消息
			// cf.Data[0] = byte(126)
			// cf.Data[1] = byte(3)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// <-tickerSendTime.C
			// cf.Data[0] = byte(129)
			// cf.Data[1] = byte(3)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)

			cf.Data[0] = byte(138)
			cf.Data[1] = byte(3)
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// <-tickerSendTime.C
			// cf.Data[0] = byte(103)
			// cf.Data[1] = byte(3)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// <-tickerSendTime.C
			// cf.Data[0] = byte(106)
			// cf.Data[1] = byte(3)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// <-tickerSendTime.C
			// cf.Data[0] = byte(109)
			// cf.Data[1] = byte(3)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// 	<-tickerSendTime.C
			// }

			// case <-tickerSendTime.C:
			// 	cf := CanFrame{}
			// 	cf.Length = 2
			// 	cf.FrameID = assembleCanID(0x00, 0x09, byte(1), 0xff) //向第一架问询发送消息
			// 	cf.Data[0] = byte(144)
			// 	cf.Data[1] = byte(3)
			// 	sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// 	//fmt.Println("发送问询负载率")
		}
	}
}

// 给支架下发煤机数据  也是广播式下发
func SetShearerParam(ctx context.Context, mb *mbserver.Server) {
	lastStep := 0

	for {
		select {
		case <-ctx.Done():
			return
		case tempCmd := <-modbus.AutoCmd:
			HeartString = time.Now().Format("2006-01-02 15:04:05")
			//fmt.Println("接收到煤机数据", tempCmd)
			//进入1or7工步 清除支架自动状态完成数据
			if (lastStep != 1 && tempCmd.Step == 1) || (lastStep != 7 && tempCmd.Step == 7) {
				//fmt.Println("进入清空")
				for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
					isauto := Mb.HoldingRegisters[3500+int(i)] >> 8
					Mb.HoldingRegisters[3500+int(i)] = isauto<<8 | (tempCmd.Step << 4) | uint16(0)
				}
			}
			//偶数工步的时候 工步下发的是前一工步，也就是奇数工步
			if tempCmd.Step == 21 || tempCmd.Step == 22 || tempCmd.Step == 23 || tempCmd.Step == 24 {
				tempCmd.Step = 1
			} else if tempCmd.Step == 61 || tempCmd.Step == 62 || tempCmd.Step == 63 || tempCmd.Step == 64 {
				tempCmd.Step = 5
			} else if tempCmd.Step == 81 || tempCmd.Step == 82 || tempCmd.Step == 83 || tempCmd.Step == 84 {
				tempCmd.Step = 7
			} else if tempCmd.Step == 121 || tempCmd.Step == 122 || tempCmd.Step == 123 || tempCmd.Step == 124 {
				tempCmd.Step = 11
			} else if tempCmd.Step%2 == 0 {
				tempCmd.Step = tempCmd.Step - 1
			}

			cf := CanFrame{}
			cf.Length = 8
			//int(tempCmd.Position/10)
			//byte(int(tempCmd.Position/10))
			cf.FrameID = assembleCanID(0x00, 0x0d, 0x00, 0xff)
			cf.Data[0] = byte(tempCmd.Speed / 10)
			cf.Data[1] = byte(tempCmd.LeftRollHeight >> 8)
			cf.Data[2] = byte(tempCmd.LeftRollHeight & 0x00ff)
			cf.Data[3] = byte(tempCmd.RightRollHeight >> 8)
			cf.Data[4] = byte(tempCmd.RightRollHeight & 0x00ff)
			cf.Data[5] = byte(tempCmd.Position >> 8)
			cf.Data[6] = byte(tempCmd.Position & 0x00ff)
			cf.Data[7] = byte(tempCmd.Direction<<6 | tempCmd.Step)
			//fmt.Println("煤机位置下发0", utils.Conf.MODBUSSHEARER.SlaveEnable)
			if utils.Conf.MODBUSSHEARER.SlaveEnable == 1 {

				if int(tempCmd.Position/10) < utils.Conf.CAN.Point1 {
					//fmt.Println("煤机位置下发1", int(tempCmd.Speed/10), int(tempCmd.Position/10), utils.Conf.CAN.Point1)
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)

				} else if int(tempCmd.Position/10) >= utils.Conf.CAN.Point1 && int(tempCmd.Position/10) < utils.Conf.CAN.Point2 {
					//fmt.Println("煤机位置下发2", int(tempCmd.Speed/10), int(tempCmd.Position/10), utils.Conf.CAN.Point1)
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
				} else if int(tempCmd.Position/10) >= utils.Conf.CAN.Point2 {
					//fmt.Println("煤机位置下发3", int(tempCmd.Speed/10), int(tempCmd.Position/10), utils.Conf.CAN.Point1)
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
				}
				log.Println("煤机位置下发:", tempCmd.Position, HeartString, tempCmd.Step)
			}
			// cf.FrameID = assembleCanID(0x00, 0x0d, byte(2), 0xff)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// cf.FrameID = assembleCanID(0x00, 0x0d, byte(3), 0xff)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// cf.FrameID = assembleCanID(0x00, 0x0d, byte(4), 0xff)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// cf.FrameID = assembleCanID(0x00, 0x0d, byte(5), 0xff)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			// cf.FrameID = assembleCanID(0x00, 0x0d, byte(6), 0xff)
			// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			mb.HoldingRegisters[179] = tempCmd.Step
			mb.HoldingRegisters[180] = tempCmd.Position
			mb.HoldingRegisters[181] = tempCmd.Speed / 10
			mb.HoldingRegisters[182] = tempCmd.Direction
			mb.HoldingRegisters[183] = tempCmd.LeftRollHeight
			mb.HoldingRegisters[184] = tempCmd.RightRollHeight
			lastStep = int(tempCmd.Step)
		}
	}
}

func FirstPublicQuery() {
	// cf := CanFrame{}
	// cf.Length = 2
	// cf.FrameID = assembleCanID(0x00, 0x09, byte(0x06), 0xff)
	// for i := 15; i < 74; i += 3 {
	// 	cf.Data[0] = byte(i)
	// 	cf.Data[1] = byte(3)

	// 	sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)

	// 	time.Sleep(10 * time.Millisecond)
	// }
	CANRequestPublicData()
	fmt.Println("程序启动首次问询公参")
}

func BSOpen(TargetID byte) {
	if Mb.HoldingRegisters[8000+int(TargetID-1)]>>7&0x0001 == 1 || Mb.HoldingRegisters[8000+int(TargetID-1)]>>6&0x0001 == 1 {
		Mb.HoldingRegisters[4000+int(TargetID-1)] = 0
		Mb.HoldingRegisters[4220+int(TargetID-1)] = 13
		log.Println("下发软件闭锁支架命令：发现有急停或者闭锁了，取消了指令下发")
		return
	}
	cf := CanFrame{}
	cf.Length = 0
	cf.FrameID = assembleCanID(0x00, 0x0a, TargetID, 0xff)
	if int(TargetID) < utils.Conf.CAN.Point1 {
		log.Println("下发软件闭锁支架命令：", cf, cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
		//log.Println("下发软件闭锁支架命令：", cf, cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if int(TargetID) >= utils.Conf.CAN.Point2 {
		//log.Println("下发软件闭锁支架命令：", cf, cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
	//time.Sleep(10 * time.Millisecond)
	//问询本机状态
	cf.Length = 2
	cf.FrameID = assembleCanID(0x00, 0x09, byte(TargetID), 0xff)
	cf.Data[0] = byte(129)
	cf.Data[1] = byte(1)
	if int(TargetID) < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if int(TargetID) >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
}

func BSClose(TargetID byte) {
	cf := CanFrame{}
	cf.Length = 0
	cf.FrameID = assembleCanID(0x00, 0x0c, TargetID, 0xff)
	if int(TargetID) < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if int(TargetID) >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
	//time.Sleep(500 * time.Millisecond)
	//问询本机状态
	cf.Length = 2
	cf.FrameID = assembleCanID(0x00, 0x09, byte(TargetID), 0xff)
	cf.Data[0] = byte(129)
	cf.Data[1] = byte(1)
	if int(TargetID) < utils.Conf.CAN.Point1 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
	} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
	} else if int(TargetID) >= utils.Conf.CAN.Point2 {
		sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
	}
}

func ReadRemoteControl(ctx context.Context, m1 map[int]int, m2 map[int]int, m3 map[int]int) {
	lastCommand := 0
	addr := fmt.Sprintf("%v:%v", utils.Conf.REMOTECONTROL.SlaveIp, utils.Conf.REMOTECONTROL.SlavePort)
	handler := mbmaster.NewTCPClientHandler(addr)
	handler.Timeout = 10 * time.Second
	handler.SlaveId = byte(utils.Conf.REMOTECONTROL.SlaveId)

	if err := handler.Connect(); err != nil {
		fmt.Println("Connect Modbus Slave Failed", addr)
	}
	defer handler.Close()
	client := mbmaster.NewClient(handler)
	tickerTime := time.NewTicker(100 * time.Millisecond)
	defer tickerTime.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:
			if Mb.HoldingRegisters[193] > 0 {
				result, err := client.ReadHoldingRegisters(12288, 10)
				if err == nil {
					cf := CanFrame{}
					cf.Length = 6
					cf.Data[3] = 0xff
					d := utils.ByteToUnint16B(result)

					if d[0] > 0 && d[1] == 0 && d[2] == 0 {
						fmt.Println("第一行动作")
						key, ok := m1[int(d[0])]
						if ok {
							cf.Data[0] = byte(128) | byte(key)
							fmt.Println(key)
						} else {
							fmt.Println("非法动作")
							lastCommand = -1
							continue
						}
					} else if d[1] > 0 && d[0] == 0 && d[2] == 0 {
						fmt.Println("第二行动作")
						key, ok := m2[int(d[1])]
						if ok {
							cf.Data[0] = byte(128) | byte(key)
							fmt.Println(key)
						} else {
							fmt.Println("非法动作")
							lastCommand = -1
							continue
						}
					} else if d[2] > 0 && d[0] == 0 && d[1] == 0 {
						fmt.Println("第三行动作")
						key, ok := m3[int(d[2])]
						if ok {
							cf.Data[0] = byte(128) | byte(key)
							fmt.Println(key)
						} else {
							fmt.Println("非法动作")
							lastCommand = -1
							continue
						}
					} else if lastCommand > 0 && d[0] == 0 && d[1] == 0 && d[2] == 0 {
						fmt.Println("停止动作")
						cf.Data[0] = byte(44)
					} else if lastCommand == 0 {
						//fmt.Println("无动作")
						lastCommand = 0
						continue
					} else {
						fmt.Println("非法动作")
						lastCommand = 0
						continue
					}
					//fmt.Println(d[4]&0xff00>>8, d[4]&0x00ff)
					if d[4]&0xff00>>8 == d[4]&0x00ff {
						fmt.Println("byte(d[4]&0x00ff)", byte(d[4]&0x00ff))
						cf.FrameID = assembleCanID(0x00, 0x02, byte(d[4]&0x00ff), 0xff)

						sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
						fmt.Println("远控平台", "单支架动作")

					} else if d[4]&0xff00>>8 > d[4]&0x00ff {
						for i := int(d[4] & 0xff00 >> 8); i >= int(d[4]&0x00ff); i-- {
							cf.FrameID = assembleCanID(0x00, 0x02, byte(i), 0xff)
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
							fmt.Println("远控平台", "编组逆序", i)
						}

					} else if d[4]&0xff00>>8 < d[4]&0x00ff {
						for i := int(d[4] & 0xff00 >> 8); i <= int(d[4]&0x00ff); i++ {
							cf.FrameID = assembleCanID(0x00, 0x02, byte(i), 0xff)
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
							fmt.Println("远控平台", "编组顺序", i)
						}
					}
					if d[0] > 0 {
						lastCommand = int(d[0])
					} else if d[1] > 0 {
						lastCommand = int(d[1])
					} else if d[2] > 0 {
						lastCommand = int(d[2])
					} else {
						lastCommand = 0
					}
				} else {
					//fmt.Println("ReadHoldingRegisters failed:", err)
					handler.Close()
					err := handler.Connect()
					if err == nil {
						client = mbmaster.NewClient(handler)
					}
					lastCommand = -1
				}
			}

		}
	}
}

// 远控平台
func RemoteControl(register int64, value uint16, m map[int]int) {
	bianzuqi := int(Mb.HoldingRegisters[32772] >> 8)
	bianzuting := int(Mb.HoldingRegisters[32772] & 0xff)
	var direction int
	if bianzuqi > bianzuting {
		direction = 0
	} else {
		direction = 1
	}
	//direction := int(Mb.HoldingRegisters[10] >> 8)
	cf := CanFrame{}
	cf.Length = 6
	cf.Data[3] = 0xff
	var GroupNum int

	GroupNum = int(math.Abs(float64(bianzuqi-bianzuting)) + 1)
	// if m[int(value)] == 1 || m[int(value)] == 2 || m[int(value)] == 10 || m[int(value)] == 11 || m[int(value)] == 13 || m[int(value)] == 14 || m[int(value)] == 15 {
	// 	GroupNum = int(Mb.HoldingRegisters[39] & 0x00ff) //立柱编组数量
	// } else if m[int(value)] == 5 || m[int(value)] == 6 || m[int(value)] == 7 || m[int(value)] == 8 || m[int(value)] == 20 || m[int(value)] == 21 || m[int(value)] == 22 || m[int(value)] == 23 {
	// 	GroupNum = int(Mb.HoldingRegisters[38] & 0x00ff) //护帮板编组数量
	// } else if m[int(value)] == 16 || m[int(value)] == 17 || m[int(value)] == 3 || m[int(value)] == 9 || m[int(value)] == 5 || m[int(value)] == 18 {
	// 	GroupNum = int(Mb.HoldingRegisters[40] & 0x00ff) //移架编组数量
	// } else if m[int(value)] == 4 {
	// 	GroupNum = int(Mb.HoldingRegisters[41] & 0x00ff) //推溜编组数量
	// } else if m[int(value)] == 12 {
	// 	GroupNum = int(Mb.HoldingRegisters[42] & 0x00ff) //喷雾编组数量
	// }
	//fmt.Println("方向", direction, "编组数量", GroupNum, "value", value, m[int(value)])
	if value != 0 {
		if m[int(value)] != 0 {
			if byte(Mb.HoldingRegisters[32772]&0x00ff) == byte(Mb.HoldingRegisters[32772]>>8) { //一样为单支架动作
				fmt.Println("单支架动作", Mb.HoldingRegisters[32772]&0x00ff)
				cf.Data[0] = byte(128) | byte(m[int(value)])
				cf.FrameID = assembleCanID(0x00, 0x02, byte(Mb.HoldingRegisters[32772]&0x00ff), 0xff)
				if int(Mb.HoldingRegisters[32772]&0x00ff) < utils.Conf.CAN.Point1 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
				} else if int(Mb.HoldingRegisters[32772]&0x00ff) >= utils.Conf.CAN.Point1 && int(Mb.HoldingRegisters[32772]&0x00ff) < utils.Conf.CAN.Point2 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
				} else if int(Mb.HoldingRegisters[32772]&0x00ff) >= utils.Conf.CAN.Point2 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
				}
			} else { //不一样为编组动作
				stopGroupNum = GroupNum
				cf.Data[0] = byte(128) | byte(m[int(value)])
				TargetID := byte(Mb.HoldingRegisters[32772] >> 8)
				for i := 0; i < GroupNum; i++ {
					cf.FrameID = assembleCanID(0x00, 0x02, TargetID, 0xff)
					if int(TargetID) < utils.Conf.CAN.Point1 {
						sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
					} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
						sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
					} else if int(TargetID) >= utils.Conf.CAN.Point2 {
						sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
					}
					if direction == 0 {
						TargetID--
					} else if direction == 1 {
						TargetID++
					}
					time.Sleep(time.Duration(10) * time.Millisecond)
				}
			}
		}
	} else {
		if byte(Mb.HoldingRegisters[32772]&0x00ff) == byte(Mb.HoldingRegisters[32772]>>8) { //一样为单支架动作
			cf.Data[0] = byte(44)
			cf.FrameID = assembleCanID(0x00, 0x02, byte(Mb.HoldingRegisters[32772]&0x00ff), 0xff)
			if int(Mb.HoldingRegisters[32772]&0x00ff) < utils.Conf.CAN.Point1 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			} else if int(Mb.HoldingRegisters[32772]&0x00ff) >= utils.Conf.CAN.Point1 && int(Mb.HoldingRegisters[32772]&0x00ff) < utils.Conf.CAN.Point2 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
			} else if int(Mb.HoldingRegisters[32772]&0x00ff) >= utils.Conf.CAN.Point2 {
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
			}
		} else { //不一样为编组动作
			if stopGroupNum > 0 {
				cf.Data[0] = byte(44)
				TargetID := byte(Mb.HoldingRegisters[32772] >> 8)
				for i := 0; i < stopGroupNum; i++ {
					cf.FrameID = assembleCanID(0x00, 0x02, TargetID, 0xff)
					if int(TargetID) < utils.Conf.CAN.Point1 {
						sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
					} else if int(TargetID) >= utils.Conf.CAN.Point1 && int(TargetID) < utils.Conf.CAN.Point2 {
						sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
					} else if int(TargetID) >= utils.Conf.CAN.Point2 {
						sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
					}
					if direction == 0 {
						TargetID--
					} else if direction == 1 {
						TargetID++
					}
					time.Sleep(time.Duration(10) * time.Millisecond)
				}
				stopGroupNum = -1
			}

		}
	}

}

func SimulationCompensate(ctx context.Context, PressInterval []int) {
	tickerSleepTime := time.NewTicker(100 * time.Millisecond)
	tickerSendTime := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerSendTime.C:
			for i := 0; i < len(PressInterval); i++ {
				if PressInterval[i] > 200 { //超时未收到上传触发补发机制
					if (time.Now().Unix()-tcpService.WifiHeart[i]) < 30 && tcpService.WifiHeart[i] > 0 { //优先使用wifi补发
						//fmt.Println(i+1, "架数据上传异常,wifi补发", time.Now().Unix()-tcpService.WifiHeart[i])
						msg := tcpService.AssembleWifiIDQuery(byte(i), 103, 9)
						tcpService.SendMsgTo(byte(i), utils.AddModbusCRC(msg.Bytes()))
						time.Sleep(10 * time.Millisecond)
					} else { //wifi异常改用can补发
						//fmt.Println(i+1, "架数据上传异常,can补发", time.Now().Unix()-tcpService.WifiHeart[i])
						cf := CanFrame{}
						cf.Length = 2
						cf.FrameID = assembleCanID(0x00, 0x09, byte(i+1), 0xff)
						cf.Data[0] = byte(103)
						cf.Data[1] = byte(3)
						if (i + 1) < utils.Conf.CAN.Point1 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
						} else if (i+1) >= utils.Conf.CAN.Point1 && (i+1) < utils.Conf.CAN.Point2 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
						} else if (i + 1) >= utils.Conf.CAN.Point2 {
							sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
						}
						time.Sleep(10 * time.Millisecond)
					}
				}

			}
			<-tickerSleepTime.C
		}
	}
}
func findFirstZeroValueKey(m map[int]int) (int, bool) {
	for k, v := range m {
		if v == 0 {
			return k, true
		}
	}
	return 0, false
}
func findClosestKeyWithValueZero(m map[int]int, targetID int) (int, int) {
	var closestKey int = -1
	var minDistance = math.MaxInt64
	var value int = -1

	for key, val := range m {
		if val == 0 {
			distance := int(math.Abs(float64(key - targetID))) // 计算距离
			if distance < minDistance {
				minDistance = distance
				closestKey = key
				value = val
			}
		}
	}
	fmt.Println("坏的传感器是: ", targetID, "找到最近的好的传感器:", closestKey, value)
	return closestKey, value
}

// 优化后的查找函数，当距离相同时按ID排序
func findClosestKeyWithValueZeroNew(m map[int]int, targetID int) (int, int) {
	type sensor struct {
		key      int
		distance int
	}
	var candidates []sensor
	// 收集所有符合条件的传感器
	//log.Println("坏的传感器是:", targetID, "没有进行处理前缓存传感器状态", m)
	for key, val := range m {
		if val == 0 {
			distance := int(math.Abs(float64(key - targetID)))
			candidates = append(candidates, sensor{key: key, distance: distance})
		}
	}
	if len(candidates) == 0 {
		//log.Println("坏的传感器是:", targetID, "没有找到任何好的传感器")
		return -1, -1
	}
	// 按距离排序，距离相同时按ID排序
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].distance == candidates[j].distance {
			return candidates[i].key < candidates[j].key // 距离相同时选择较小的ID
		}
		return candidates[i].distance < candidates[j].distance
	})
	//log.Println("坏的传感器是:", targetID, "对好的行程传感器与坏的目标传感器按距离差进行排序", candidates)
	closestKey := candidates[0].key
	fmt.Println("坏的传感器是:", targetID, "找到最近的好的传感器:", closestKey, "距离:", candidates[0].distance)

	// 如果有多个相同距离的候选，可以在这里处理
	// if len(candidates) > 1 && candidates[0].distance == candidates[1].distance {
	// 	fmt.Printf("注意: 有多个相同距离(%d)的候选传感器: ", candidates[0].distance)
	// 	for i := 0; i < len(candidates) && candidates[i].distance == candidates[0].distance; i++ {
	// 		fmt.Printf("%d ", candidates[i].key)
	// 	}
	// 	fmt.Println()
	// }

	return closestKey, 0
}

// 给前端页面定时1s发送一次数据
func SendWebsocket(ctx context.Context, PressLastTime []time.Time, RandomAutoReceive []int) {
	tickerSendTime := time.NewTicker(1 * time.Second)
	defer tickerSendTime.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerSendTime.C:
			var simulationStatus model.SimulationStatus
			//fmt.Println("模拟值上传大小", utils.Conf.SYSTEM.SupportNum)

			// 按键值排序
			sensorMutex.RLock()

			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				if int(Mb.HoldingRegisters[1520+i*9]) > 600 {
					simulationStatus.ColumnPressureLeft = append(simulationStatus.ColumnPressureLeft, 600)
				} else {
					simulationStatus.ColumnPressureLeft = append(simulationStatus.ColumnPressureLeft, int(Mb.HoldingRegisters[1520+i*9]))
				}

				simulationStatus.ColumnPressureLeftTime = append(simulationStatus.ColumnPressureLeftTime, PressLastTime[i].Format(timeFormat1))
				simulationStatus.ColumnPressureLeftTimeInterval = append(simulationStatus.ColumnPressureLeftTimeInterval, int(time.Now().Unix()-PressLastTime[i].Unix()))
				simulationStatus.ColumnPressureRight = append(simulationStatus.ColumnPressureRight, int(Mb.HoldingRegisters[1521+i*9]))

				//找离损坏传感器最近好的传感器
				if Mb.HoldingRegisters[5000+i] > 0 {
					closestKey, _ := findClosestKeyWithValueZeroNew(SensorCache, i+1)
					if closestKey == -1 {
						//log.Println("没有找到最近的好的传感器，坏的传感器是: ", i+1)
						simulationStatus.PushItinerary = append(simulationStatus.PushItinerary, int(Mb.HoldingRegisters[1522+i*9]))
					} else {
						var badValue int
						var goodValue int
						badBit := Mb.HoldingRegisters[5800+i] >> 15
						if badBit == 1 {
							badValue = -int(Mb.HoldingRegisters[5800+i] & 0x7fff)
						} else {
							badValue = int(Mb.HoldingRegisters[5800+i] & 0x7fff)
						}
						goodBit := Mb.HoldingRegisters[5800+closestKey-1] >> 15
						if goodBit == 1 {
							goodValue = -int(Mb.HoldingRegisters[5800+closestKey-1] & 0x7fff)
						} else {
							goodValue = int(Mb.HoldingRegisters[5800+closestKey-1] & 0x7fff)
						}
						//value := int(Mb.HoldingRegisters[1522+(closestKey-1)*9]) + int(Mb.HoldingRegisters[5800+i]) - int(Mb.HoldingRegisters[5800+closestKey-1])
						value := int(Mb.HoldingRegisters[1522+(closestKey-1)*9]) + goodValue - badValue
						if value > 960 || value < 0 {
							value = int(Mb.HoldingRegisters[1522+(closestKey-1)*9])
						}
						Mb.HoldingRegisters[1522+i*9] = uint16(value)
						simulationStatus.PushItinerary = append(simulationStatus.PushItinerary, value)
						//log.Println("坏的传感器:", i+1, "找到了最近的好的传感器：", closestKey, "好的行程：", int(Mb.HoldingRegisters[1522+(closestKey-1)*9]), int(Mb.HoldingRegisters[5800+i]), int(Mb.HoldingRegisters[5800+closestKey-1]), "计算的结果：", value)
						//log.Println("坏的传感器:", i+1, "找到了最近的好的传感器：", closestKey, "好的行程：", int(Mb.HoldingRegisters[1522+(closestKey-1)*9]), goodValue, badValue, "计算的结果：", value)

					}

				} else {
					//log.Println("推移行程传感器都是好的，支架号为", i+1)
					simulationStatus.PushItinerary = append(simulationStatus.PushItinerary, int(Mb.HoldingRegisters[1522+i*9]))
				}

				simulationStatus.RoofHeight = append(simulationStatus.RoofHeight, int(Mb.HoldingRegisters[1523+i*9])*10)
				simulationStatus.RoofXAxis = append(simulationStatus.RoofXAxis, int(Mb.HoldingRegisters[1524+i*9]))
				simulationStatus.RoofYAxis = append(simulationStatus.RoofYAxis, int(Mb.HoldingRegisters[1525+i*9]))
				simulationStatus.BaseXAxis = append(simulationStatus.BaseXAxis, int(Mb.HoldingRegisters[1526+i*9]))
				simulationStatus.BaseYAxis = append(simulationStatus.BaseYAxis, int(Mb.HoldingRegisters[1527+i*9]))
				simulationStatus.BatteryVoltage = append(simulationStatus.BatteryVoltage, int(Mb.HoldingRegisters[1527+i*9]>>8&0x00ff))
				//借这个12V电池电压给推移传感器故障用
				simulationStatus.Voltage12V = append(simulationStatus.Voltage12V, int(Mb.HoldingRegisters[5000+i]))
			}
			sensorMutex.RUnlock()
			//fmt.Println("传感器故障", simulationStatus.Voltage12V)
			WebsocketMessage := model.WebsocketMessage{
				Type:    "simulationStatus",
				Source:  0,
				Message: simulationStatus,
			}

			strings, _ := json.Marshal(WebsocketMessage)
			//fmt.Println("模拟值长度", len(strings))
			service.WebsocketManager.SendAll(strings)

			var autoFollowStatus model.AutoFollowStatus
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				autoFollowStatus.IsAutoFollow = RandomAutoReceive
				autoFollowStatus.CompleteAutomaticPush = append(autoFollowStatus.CompleteAutomaticPush, int(Mb.HoldingRegisters[3500+i]>>3&0x0001))
				autoFollowStatus.CompleteAutomaticRackTransfer = append(autoFollowStatus.CompleteAutomaticRackTransfer, int(Mb.HoldingRegisters[3500+i]>>2&0x0001))
				autoFollowStatus.CompleteAutomaticCare = append(autoFollowStatus.CompleteAutomaticCare, int(Mb.HoldingRegisters[3500+i]>>1&0x0001))
				autoFollowStatus.CompleteAutomaticExtension = append(autoFollowStatus.CompleteAutomaticExtension, int(Mb.HoldingRegisters[3500+i]&0x0001))
				if isCqljWarningSupport(i, utils.SupportSort) {
					autoFollowStatus.Cqlj = append(autoFollowStatus.Cqlj, 1)
				} else {
					autoFollowStatus.Cqlj = append(autoFollowStatus.Cqlj, 0)
				}

			}
			WebsocketMessage = model.WebsocketMessage{
				Type:    "autoFollowStatus",
				Source:  0,
				Message: autoFollowStatus,
			}

			strings, _ = json.Marshal(WebsocketMessage)
			//fmt.Println("自动跟机长度", len(strings))
			service.WebsocketManager.SendAll(strings)

			var shearer model.Shearer
			shearer.Step = int(Mb.HoldingRegisters[179])
			shearer.Position = int(Mb.HoldingRegisters[180])
			shearer.Speed = int(Mb.HoldingRegisters[181])
			shearer.Direction = int(Mb.HoldingRegisters[182])
			shearer.LeftRollHeight = int(Mb.HoldingRegisters[183])
			shearer.RightRollHeight = int(Mb.HoldingRegisters[184])
			shearer.Heart = HeartString
			shearer.PersonEnable = int(Mb.HoldingRegisters[176])
			shearer.CanLoadRate1 = int(Mb.HoldingRegisters[185])
			shearer.CanLoadRate2 = int(Mb.HoldingRegisters[186])
			shearer.Sort = utils.Conf.MODBUSSHEARER.Sort
			shearer.AutoStatus = int(Mb.HoldingRegisters[177])
			shearer.AutoEnable = int(Mb.HoldingRegisters[193])

			WebsocketMessage = model.WebsocketMessage{
				Type:    "shearer",
				Source:  0,
				Message: shearer,
			}
			//fmt.Println("上传网页煤机数据", WebsocketMessage)
			strings, _ = json.Marshal(WebsocketMessage)
			service.WebsocketManager.SendAll(strings)
		}
	}
}

func QueryLoopWifi(ctx context.Context, mb *mbserver.Server) {
	tickerSendTime := time.NewTicker(100 * time.Millisecond)
	tickerSendTime1 := time.NewTicker(15 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerSendTime1.C:
			for i := 1; i <= utils.Conf.SYSTEM.SupportNum; i++ {
				//if time.Now().Unix()-wifiHeart[i-1] < 3 {
				// msg := assembleWifiIDQuery(byte(i), 103, 9)
				// SendMsgTo(byte(i), utils.AddModbusCRC(msg.Bytes()))
				// <-tickerSendTime.C
				// msg := tcpService.AssembleWifiIDQuery(byte(i), 126, 4)
				// tcpService.SendMsgTo(byte(i), utils.AddModbusCRC(msg.Bytes()))
				// <-tickerSendTime.C

				//fmt.Println("wifi查询自动化状态", i)
				// msg = tcpService.AssembleWifiIDQuery(byte(i), 138, 1)
				// tcpService.SendMsgTo(byte(i), utils.AddModbusCRC(msg.Bytes()))

				cf := CanFrame{}
				cf.Length = 2
				cf.FrameID = assembleCanID(0x00, 0x09, byte(i), 0xff) //向第一架问询发送消息
				cf.Data[0] = byte(129)
				cf.Data[1] = byte(3)
				sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
				<-tickerSendTime.C
				// 	<-tickerSendTime.C
				// 	cf.Data[0] = byte(129)
				// 	cf.Data[1] = byte(3)
				// 	sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
				// 	<-tickerSendTime.C
				// 	// cf.Data[0] = byte(138)
				// 	// cf.Data[1] = byte(3)
				// 	// sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
				// 	// <-tickerSendTime.C

			}

		}
	}

	// for i := 1; i <= utils.Conf.SYSTEM.SupportNum; i++ {
	// 	go SingleQuery(ctx, i, wifiHeart)
	// }

}

func TrafficLight(ctx context.Context, mb *mbserver.Server) {
	println("进来了")
	tickerSendTime := time.NewTicker(200 * time.Millisecond)
	tickerSendTime1 := time.NewTicker(10 * time.Millisecond)
	defer tickerSendTime.Stop()
	defer tickerSendTime1.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerSendTime1.C:

			for i := 1; i <= 5; i++ {
				cf := CanFrame{}
				cf.Length = 0
				cf.FrameID = assembleCanID(0x00, 0x04, byte(i), 0xff)
				if i < utils.Conf.CAN.Point1 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
				} else if i >= utils.Conf.CAN.Point1 && i < utils.Conf.CAN.Point2 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
				} else if i >= utils.Conf.CAN.Point2 {
					sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
				}
				<-tickerSendTime.C
			}

		}
	}

}

// 配置控制器时间
func CANCalibrationTime(ctx context.Context) {

	tickerSendTime := time.NewTicker(600 * time.Second)
	defer tickerSendTime.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerSendTime.C:

			timeNow := time.Now().Local()
			fmt.Println("校准RTC时间", timeNow)
			h := timeNow.Hour()
			m := timeNow.Minute()
			second := timeNow.Second()
			cf := CanFrame{}
			cf.Length = 4
			cf.FrameID = assembleCanID(0x00, 0x0e, 0x00, byte(140))
			cf.Data[0] = byte(m)
			cf.Data[1] = byte(h)
			cf.Data[2] = 0
			cf.Data[3] = byte(second)
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp1)
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp2)
			sendUDPdata(cf.ToByte(), utils.Conf.CAN.Can2udpIp3)
		}
	}
}

func StartDBWorker() {

	//fmt.Println("【Record DB Worker】数据库写入协程已启动...")

	for {

		entry := <-DBRSChan

		var WjwRData model.WjwRecord
		WjwRData.Time = entry.Time
		WjwRData.SendData = entry.SendData
		WjwRData.ReceiveData = entry.ReceiveData

		fmt.Println("记录", WjwRData.SendData, WjwRData.ReceiveData)
		//user := model.WjwRecord{Time: WjwRData.Time, SendData: WjwRData.SendData, ReceiveData: WjwRData.ReceiveData}
		err := mysql.Mysqlclient.Select("Time", "SendData", "ReceiveData").Create(&WjwRData)
		if err != nil {
			fmt.Println("【Record DB Worker】写入失败:", err)
		}
	}
}

func StartDBSendDataWorker() {

	fmt.Println("【Send Data Worker】数据库写入协程已启动...")

	for {

		entry := <-DBSChan
		var WjwSData model.WjwSendRecord
		WjwSData.Time = entry.Time
		WjwSData.SendData = entry.SendData
		fmt.Println("记录", WjwSData.SendData)

		err := mysql.Mysqlclient.Select("Time", "SendData").Create(&WjwSData)
		if err != nil {
			fmt.Println("【Send Data Worker】写入失败:", err)
		}
	}
}

func StartRecordCommandWorker() {
	fmt.Println("【Record Command Worker】指令记录写入协程已启动...")
	var buffer []model.RecordCommand
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case entry, ok := <-RecordCommandChan:
			if !ok {
				// 通道已关闭，将剩余数据写入后退出
				if len(buffer) > 0 {
					flushRecordCommands1(buffer)
				}
				log.Println("【Record Command Worker】通道关闭，协程退出")
				return
			}
			buffer = append(buffer, entry)
			//fmt.Println("buffer=", buffer)
			if len(buffer) >= 100 {
				flushRecordCommands1(buffer)
				buffer = buffer[:0]
			}
		case <-ticker.C:
			if len(buffer) > 0 {
				flushRecordCommands1(buffer)
				buffer = buffer[:0]
			}
		}
	}
}

func flushRecordCommands(records []model.RecordCommand) {
	now := time.Now()
	record := model.RecordCommand{TableTime: now}
	tableName := record.TableName()
	if lastTableName != tableName {
		lastTableName = tableName
		fmt.Println("检查表存在 - 时间:", now.Format("2006-01-02 15:04:05"), "表名:", tableName)
		if !mysql.Mysqlclient.Migrator().HasTable(tableName) {
			fmt.Println("正在创建表: ", tableName)
			if err := mysql.Mysqlclient.Table(tableName).AutoMigrate(&model.RecordCommand{}); err != nil {
				log.Println("创建指令记录表异常: ", err)
			} else {
				indexName := "idx_time"
				if !mysql.Mysqlclient.Migrator().HasIndex(tableName, indexName) {
					sql := fmt.Sprintf("CREATE INDEX %s ON %s (time)", indexName, tableName)
					if err := mysql.Mysqlclient.Exec(sql).Error; err != nil {
						log.Printf("创建索引 %s 失败: %v\n", indexName, err)
					} else {
						fmt.Printf("成功创建索引: %s\n", indexName)
					}
				}
			}
		}
	}

	retryCount := 3
	for i := 0; i < retryCount; i++ {
		result := mysql.Mysqlclient.Table(tableName).Select("Time", "CurrentCommandSource", "ControlCommandDeviceId", "CommandType", "SourceId").Create(&records)
		if result.Error == nil {
			// 写入成功，退出重试
			return
		}
		log.Printf("【Record Command Worker】写入数据库失败，正在重试 %d/%d: %v\n", i+1, retryCount, result.Error)
		time.Sleep(time.Second * 2) // 等待一段时间后重试
	}

	// 所有重试都失败，记录错误日志
	log.Println("【Record Command Worker】写入数据库失败，记录支架动作数据所有重试都失败，数据丢失:", records)

}

// ensureTableAndIndexes 确保指定表存在且拥有所需索引（幂等）
func ensureRecordCommandTable(tableName string) error {
	if _, ok := recordCommandTableCache.Load(tableName); ok {
		return nil
	}

	db := mysql.Mysqlclient
	migrator := db.Migrator()
	// 1. 自动迁移表结构（表不存在则创建，已存在则补充缺失列，不会删除列）
	if !migrator.HasTable(tableName) {
		if err := db.Table(tableName).AutoMigrate(&model.RecordCommand{}); err != nil {
			return fmt.Errorf("创建表失败: %w", err)
		}
	}
	// 2. 确保索引存在（使用 GORM 迁移器，避免硬编码列名）

	indexName := "idx_time"
	if !migrator.HasIndex(tableName, indexName) {
		sql := fmt.Sprintf("CREATE INDEX %s ON %s (time)", indexName, tableName)
		if err := db.Exec(sql).Error; err != nil {
			if !strings.Contains(err.Error(), "Duplicate key name") {
				return fmt.Errorf("创建索引失败: %w", err)
			}

		}
	}
	recordCommandTableCache.Store(tableName, true)
	return nil
}

// batchInsertWithRetry 对单个分组的记录进行带重试的批量插入
func batchInsertWithRetry(tableName string, records []model.RecordCommand) {
	const maxRetry = 3
	db := mysql.Mysqlclient
	// 指定要插入的字段（根据实际需要，建议加上 TableTime）
	// 注意：如果 TableTime 需要存入库中，请取消注释下一行；若不需要，可保持原样
	// fields := []string{"Time", "CurrentCommandSource", "ControlCommandDeviceId", "CommandType", "SourceId", "TableTime"}
	fields := []string{"Time", "CurrentCommandSource", "ControlCommandDeviceId", "CommandType", "SourceId"}

	for i := 0; i < maxRetry; i++ {
		err := db.Table(tableName).Select(fields).Create(&records).Error
		if err == nil {
			return
		}
		log.Printf("【Record Command Worker】写入表 %s 失败 (尝试 %d/%d): %v", tableName, i+1, maxRetry, err)
		// 指数退避：1s, 2s, 4s
		time.Sleep(time.Duration(1<<i) * time.Second)
	}
	// 最终失败处理：写本地文件（防止数据丢失）
	saveFailedRecords(tableName, records)
}

// saveFailedRecords 将最终失败的记录写入本地文件（可扩展为死信队列）
func saveFailedRecords(tableName string, records []model.RecordCommand) {
	// 以 JSON 格式追加写入文件，文件名包含表名和时间戳
	filename := fmt.Sprintf("failed_records_%s_%s.log", tableName, time.Now().Format("20060102"))
	data, err := json.Marshal(records)
	if err != nil {
		log.Printf("【Record Command Worker】序列化失败数据出错: %v", err)
		return
	}
	// 追加写入
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("【Record Command Worker】打开失败文件出错: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("【Record Command Worker】写入失败文件出错: %v", err)
	} else {
		log.Printf("【Record Command Worker】最终失败的 %d 条记录已保存至 %s", len(records), filename)
	}
}

// flushRecordCommands 按真实 TableTime 分组后写入
func flushRecordCommands1(records []model.RecordCommand) {
	if len(records) == 0 {
		return
	}
	// 1. 按 TableTime 分组
	groups := make(map[string][]model.RecordCommand)
	for _, r := range records {
		tn := r.TableName() // 每条记录根据自己的 TableTime 生成表名
		groups[tn] = append(groups[tn], r)
	}
	// 2. 对每个分组独立处理
	for tn, group := range groups {
		// 确保表及索引存在（幂等）
		if err := ensureRecordCommandTable(tn); err != nil {
			log.Printf("【Record Command Worker】准备表 %s 失败: %v，该组 %d 条数据将被丢弃", tn, err, len(group))
			// 可选：将本组数据保存到失败文件
			saveFailedRecords(tn, group)
			continue
		}
		// 批量插入（带重试）
		batchInsertWithRetry(tn, group)
	}
}

func StartFaultRecordWorker() {
	fmt.Println("【Fault Record Worker】故障记录写入协程已启动...")
	var buffer []model.FaultRecord
	ticker := time.NewTicker(1 * time.Second) // Adjust ticker frequency as needed
	defer ticker.Stop()

	for {
		select {
		case entry, ok := <-FaultRecordChan:
			if !ok {
				// 通道已关闭，将剩余数据写入后退出
				if len(buffer) > 0 {
					flushFaultRecords(buffer)
				}
				log.Println("【Fault Record Worker】通道关闭，协程退出")
				return
			}
			buffer = append(buffer, entry)
			if len(buffer) >= 50 {
				flushFaultRecords(buffer)
				buffer = buffer[:0]
			}
		case <-ticker.C:
			if len(buffer) > 0 {
				flushFaultRecords(buffer)
				buffer = buffer[:0]
			}
		}
	}
}

var faultRecordTableCache sync.Map // Cache for fault record tables

// ensureFaultRecordTable ensures the specified fault record table exists and has the required indexes.
func ensureFaultRecordTable(tableName string) error {
	if _, ok := faultRecordTableCache.Load(tableName); ok {
		return nil
	}

	db := mysql.Mysqlclient
	migrator := db.Migrator()

	// 1. Auto-migrate table structure
	if !migrator.HasTable(tableName) {
		if err := db.Table(tableName).AutoMigrate(&model.FaultRecord{}); err != nil {
			return fmt.Errorf("创建故障记录表失败: %w", err)
		}
	}

	// 2. Ensure index exists
	indexName := "idx_source_time"
	if !migrator.HasIndex(tableName, indexName) {
		sql := fmt.Sprintf("CREATE INDEX %s ON %s (source_id,time)", indexName, tableName)
		if err := db.Exec(sql).Error; err != nil {
			// Ignore "Duplicate key name" error if index already exists from a concurrent operation
			if !strings.Contains(err.Error(), "Duplicate key name") {
				return fmt.Errorf("创建故障记录索引失败: %w", err)
			}
		}
	}
	faultRecordTableCache.Store(tableName, true)
	return nil
}

// batchInsertFaultRecordsWithRetry performs a batch insert for fault records with retries.
func batchInsertFaultRecordsWithRetry(tableName string, records []model.FaultRecord) {
	const maxRetry = 3
	db := mysql.Mysqlclient
	fields := []string{"Time", "SourceId", "FaultType"} // TableTime is used for table name, not inserted as a column

	for i := 0; i < maxRetry; i++ {
		err := db.Table(tableName).Select(fields).Create(&records).Error
		if err == nil {
			return
		}
		log.Printf("【Fault Record Worker】写入故障记录表 %s 失败 (尝试 %d/%d): %v", tableName, i+1, maxRetry, err)
		time.Sleep(time.Duration(1<<i) * time.Second) // Exponential backoff
	}
	// If all retries fail, save to a local file to prevent data loss
	saveFailedFaultRecords(tableName, records)
}

// saveFailedFaultRecords saves fault records that failed to be inserted into the database.
func saveFailedFaultRecords(tableName string, records []model.FaultRecord) {
	filename := fmt.Sprintf("failed_fault_records_%s_%s.log", tableName, time.Now().Format("20060102"))
	data, err := json.Marshal(records)
	if err != nil {
		log.Printf("【Fault Record Worker】序列化失败故障数据出错: %v", err)
		return
	}
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("【Fault Record Worker】打开失败故障文件出错: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		log.Printf("【Fault Record Worker】写入失败故障文件出错: %v", err)
	} else {
		log.Printf("【Fault Record Worker】最终失败的 %d 条故障记录已保存至 %s", len(records), filename)
	}
}

// flushFaultRecords groups fault records by TableTime and inserts them into the database.
func flushFaultRecords(records []model.FaultRecord) {
	if len(records) == 0 {
		return
	}

	groups := make(map[string][]model.FaultRecord)
	for _, r := range records {
		tn := r.TableName()
		groups[tn] = append(groups[tn], r)
	}

	for tn, group := range groups {
		if err := ensureFaultRecordTable(tn); err != nil {
			log.Printf("【Fault Record Worker】准备故障记录表 %s 失败: %v，该组 %d 条数据将被丢弃", tn, err, len(group))
			saveFailedFaultRecords(tn, group) // Save to file if table preparation fails
			continue
		}
		batchInsertFaultRecordsWithRetry(tn, group)
	}
}
