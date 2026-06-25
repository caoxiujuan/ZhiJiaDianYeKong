package modbus

import (
	"context"
	"fmt"
	"gocode/utils"
	"log"
	"math"
	"time"

	mbmaster "github.com/goburrow/modbus"
	"github.com/tbrandon/mbserver"
)

var (
	AutoCmd                    chan AutoShearer
	HeartCount                 int
	LastGetShearerDataTime     time.Time
	LastGetShearerDataPosition uint16
)

type AutoShearer struct {
	Step            uint16
	Position        uint16
	Speed           uint16
	Direction       uint16
	LeftRollHeight  uint16
	RightRollHeight uint16
}

func init() {
	AutoCmd = make(chan AutoShearer, 50)
}

// 读煤机数据
func Run(ctx context.Context, mb *mbserver.Server) {

	addr := fmt.Sprintf("%v:%v", utils.Conf.MODBUSSHEARER.SlaveIp, utils.Conf.MODBUSSHEARER.SlavePort)
	handler := mbmaster.NewTCPClientHandler(addr)
	handler.Timeout = 5 * time.Second
	handler.SlaveId = byte(utils.Conf.MODBUSSHEARER.SlaveId)
	HeartCount = 65530
	if err := handler.Connect(); err != nil {
		fmt.Println("Connect Modbus Slave Failed", addr)
	}

	client := mbmaster.NewClient(handler)
	tickerTime := time.NewTicker(1000 * time.Millisecond)
	defer tickerTime.Stop()
	for {
		select {
		case <-ctx.Done():
			handler.Close()
			return
		case <-tickerTime.C:
			//result, err := client.ReadHoldingRegisters(32768, 10)
			//fmt.Println("开始读煤机数据")
			result, err := client.ReadHoldingRegisters(utils.Conf.MODBUSSHEARER.SlavePoint, 10)
			if err == nil {
				mb.HoldingRegisters[175] = uint16(HeartCount)
				HeartCount += 1
				d := utils.ByteToUnint16B(result) //读出来的数据
				//fmt.Println("煤机数据：", d)
				command := AutoShearer{}
				command.Step = d[0]
				command.Position = d[1]
				command.Speed = d[2]
				command.Direction = d[3]
				command.LeftRollHeight = d[4]
				command.RightRollHeight = d[5]
				fmt.Println("煤机位置：", d[1])
				if command.Position == 0 {
					//log.Println("获取煤机位置为0")
					continue
				}
				if !LastGetShearerDataTime.IsZero() && math.Abs(float64(command.Position)-float64(LastGetShearerDataPosition)) > 50 {
					log.Println("触发跳架保护")
					log.Println("上次下发煤机数据时间位置", LastGetShearerDataTime, LastGetShearerDataPosition)
					log.Println("这次下发煤机数据时间位置", time.Now(), command.Position)
				}

				AutoCmd <- command
				LastGetShearerDataPosition = command.Position
				LastGetShearerDataTime = time.Now()
				//往寄存器中写煤机位置和工步，便于记录
				mb.HoldingRegisters[2825] = d[1]
				mb.HoldingRegisters[2826] = d[0]
			} else {
				//fmt.Println("获取煤机数据异常")
				handler.Close()
				err := handler.Connect()
				if err == nil {
					client = mbmaster.NewClient(handler)
					fmt.Println("获取煤机数据异常重连成功", len(AutoCmd), err)
				} else {
					fmt.Println("获取煤机数据异常重连失败", len(AutoCmd), err)
				}
			}

		}
	}
}
