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
	LastShearerReceiveTime     time.Time
	lastShearerSourceHeart     uint16
	lastShearerSourceHeartTime time.Time
	hasShearerSourceHeart      bool
	shearerReadErrorCount      int
)

const (
	shearerSourceHeartIndex = 6
	shearerHeartTimeout     = 3 * time.Second
	shearerJumpDistance     = 50

	// 2825: 最新煤机位置
	// 2826: 最新煤机工步
	// 2827: 煤机源心跳 d[6]
	// 2828: 诊断状态位
	// 2829: 距离上次成功接收数据的秒数
	// 2830: 连续读取错误次数
	// 2831: 源心跳未变化持续秒数
	shearerDiagPositionReg       = 2825
	shearerDiagStepReg           = 2826
	shearerDiagSourceHeartReg    = 2827
	shearerDiagStatusReg         = 2828
	shearerDiagLastReceiveAgeReg = 2829
	shearerDiagReadErrorCountReg = 2830
	shearerDiagSourceHeartAgeReg = 2831

	// 2828 状态位：
	// bit0 = Modbus读取异常
	// bit1 = 源心跳超过3秒未变化
	// bit2 = 煤机位置为0
	// bit3 = 位置跳变超过50
	// bit4 = AutoCmd队列满
	shearerDiagReadError       uint16 = 1 << 0
	shearerDiagSourceHeartStop uint16 = 1 << 1
	shearerDiagPositionZero    uint16 = 1 << 2
	shearerDiagJumpDistance    uint16 = 1 << 3
	shearerDiagAutoCmdFull     uint16 = 1 << 4
)

type AutoShearer struct {
	Step            uint16
	Position        uint16
	Speed           uint16
	Direction       uint16
	LeftRollHeight  uint16
	RightRollHeight uint16
	SourceHeart     uint16
}

func init() {
	AutoCmd = make(chan AutoShearer, 50)
}

func uint16DurationSeconds(t time.Time) uint16 {
	if t.IsZero() {
		return 65535
	}
	seconds := int(time.Since(t).Seconds())
	if seconds < 0 {
		return 0
	}
	if seconds > 65535 {
		return 65535
	}
	return uint16(seconds)
}

func uint16Count(count int) uint16 {
	if count < 0 {
		return 0
	}
	if count > 65535 {
		return 65535
	}
	return uint16(count)
}

func updateShearerDiagnosis(mb *mbserver.Server, status uint16, sourceHeart uint16) {
	if hasShearerSourceHeart && time.Since(lastShearerSourceHeartTime) > shearerHeartTimeout {
		status |= shearerDiagSourceHeartStop
	}

	mb.HoldingRegisters[shearerDiagSourceHeartReg] = sourceHeart
	mb.HoldingRegisters[shearerDiagStatusReg] = status
	mb.HoldingRegisters[shearerDiagLastReceiveAgeReg] = uint16DurationSeconds(LastShearerReceiveTime)
	mb.HoldingRegisters[shearerDiagReadErrorCountReg] = uint16Count(shearerReadErrorCount)
	mb.HoldingRegisters[shearerDiagSourceHeartAgeReg] = uint16DurationSeconds(lastShearerSourceHeartTime)
}

// Run reads shearer data and records receive reliability diagnostics.
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
			result, err := client.ReadHoldingRegisters(utils.Conf.MODBUSSHEARER.SlavePoint, 10)
			if err == nil {
				now := time.Now()
				status := uint16(0)
				shearerReadErrorCount = 0
				LastShearerReceiveTime = now
				mb.HoldingRegisters[175] = uint16(HeartCount)
				HeartCount += 1

				d := utils.ByteToUnint16B(result)
				if len(d) <= shearerSourceHeartIndex {
					shearerReadErrorCount += 1
					updateShearerDiagnosis(mb, shearerDiagReadError, lastShearerSourceHeart)
					continue
				}

				sourceHeart := d[shearerSourceHeartIndex]
				if !hasShearerSourceHeart || sourceHeart != lastShearerSourceHeart {
					lastShearerSourceHeart = sourceHeart
					lastShearerSourceHeartTime = now
					hasShearerSourceHeart = true
				}

				command := AutoShearer{
					Step:            d[0],
					Position:        d[1],
					Speed:           d[2],
					Direction:       d[3],
					LeftRollHeight:  d[4],
					RightRollHeight: d[5],
					SourceHeart:     sourceHeart,
				}
				fmt.Println("煤机位置：", command.Position)

				mb.HoldingRegisters[shearerDiagPositionReg] = command.Position
				mb.HoldingRegisters[shearerDiagStepReg] = command.Step

				if command.Position == 0 {
					status |= shearerDiagPositionZero
					updateShearerDiagnosis(mb, status, sourceHeart)
					continue
				}

				if !LastGetShearerDataTime.IsZero() && math.Abs(float64(command.Position)-float64(LastGetShearerDataPosition)) > shearerJumpDistance {
					status |= shearerDiagJumpDistance
					log.Println("触发跳架保护")
					log.Println("上次下发煤机数据时间位置", LastGetShearerDataTime, LastGetShearerDataPosition)
					log.Println("这次下发煤机数据时间位置", now, command.Position)
				}

				select {
				case AutoCmd <- command:
					LastGetShearerDataPosition = command.Position
					LastGetShearerDataTime = now
				default:
					status |= shearerDiagAutoCmdFull
					log.Println("煤机数据下发队列已满，丢弃本次煤机数据", command.Position, command.Step, len(AutoCmd))
				}

				updateShearerDiagnosis(mb, status, sourceHeart)
			} else {
				shearerReadErrorCount += 1
				updateShearerDiagnosis(mb, shearerDiagReadError, lastShearerSourceHeart)
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
