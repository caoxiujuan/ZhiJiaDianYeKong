package pressalarm

import (
	"context"
	"gocode/utils"
	"time"

	"github.com/tbrandon/mbserver"
)

func UpPressAlarm(ctx context.Context, mb *mbserver.Server, PressStore []int) {
	tickerWriteTime := time.NewTicker(time.Second * 600)
	tickerWriteTime1 := time.NewTicker(time.Second * 120)
	time.Sleep(180)
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		PressStore[i] = int(mb.HoldingRegisters[1520+i*9])
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerWriteTime.C:
			//if time.Now().Format("2006-01-02 15:04:05")[15:19] == "0:00" {
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				PressStore[i] = int(mb.HoldingRegisters[1520+i*9])
			}
			//}
		case <-tickerWriteTime1.C:
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				if (PressStore[i]-int(mb.HoldingRegisters[1520+i*9])) > 10 && int(mb.HoldingRegisters[1520+i*9]) < 252 {
					mb.HoldingRegisters[6000+i] = 1
				} else {
					mb.HoldingRegisters[6000+i] = 0
				}
			}
			// default:
			// 	if time.Now().Format("2006-01-02 15:04:05")[14:19] == "00:00" {
			// 		for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
			// 			PressStore[i] = int(mb.HoldingRegisters[1520+i*9])
			// 		}
			// 	}

			// 	time.Sleep(100 * time.Millisecond)
		}
	}

}
