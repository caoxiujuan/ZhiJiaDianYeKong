package tcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"gocode/model"
	service "gocode/services"
	"gocode/utils"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tbrandon/mbserver"
)

var (
	// MsgCmd 命令消息channel
	MsgCmd             chan Cmd
	SmController       *SessionMap
	WifiControlcommand chan uint16
	WifiHeart          []int64
	randomAutoReceive  []int
)

func init() {

}

// Cmd 下发给支架控制器的协议struct
type Cmd struct {
	ID     byte
	Des    byte
	Func   byte
	Start  uint16
	RegLen uint16
	Data   []uint16
}

// ToByte 按照协议将下发给支架控制器的数据格式化
func (c Cmd) ToByte() []byte {
	var b bytes.Buffer
	b.Write([]byte{0xcd, 0xef})
	b.WriteByte(c.ID)
	b.WriteByte(c.Des)
	b.WriteByte(c.Func)

	tmp := make([]uint16, 2)
	tmp[0] = c.Start
	tmp[1] = c.RegLen
	b.Write(utils.Uint16ToBytes(tmp, 0))
	b.WriteByte(byte(c.RegLen * 2))
	b.Write(utils.Uint16ToBytes(c.Data, 0))
	return b.Bytes()
}

// NumberFuncCode66Byte 方法码66,数据包总字节数
const NumberFuncCode16Byte = 16
const NumberFuncCode66Byte = 15
const NumberFuncCodeByte = 10
const timeFormat1 = "2006-01-02 15:04:05"

// Session is client struct
type Session struct {
	Key         uint8
	Conn        net.Conn
	ResponseMsg chan []byte
	LeaveMsg    chan uint8
	Buff        *utils.Buffer
}

// Update 更新当前session的key为IP地址
func (s *Session) Update() {
	arr := strings.Split(strings.Split(s.Conn.RemoteAddr().String(), ":")[0], ".")
	id, _ := strconv.Atoi(arr[3])
	s.Key = uint8(id)
}

// SessionMap session map
type SessionMap struct {
	Data map[byte]*Session
	Lock *sync.Mutex
}

// Set set
func (sm *SessionMap) Set(k byte, s *Session) {
	sm.Lock.Lock()
	defer sm.Lock.Unlock()
	sm.Data[k] = s
}

// Delete del
func (sm *SessionMap) Delete(k byte) {
	sm.Lock.Lock()
	defer sm.Lock.Unlock()
	delete(sm.Data, k)
}

// Run 启动转发服务wifi
func Run(ctx context.Context, mb *mbserver.Server, canHeart []int64, SimulationHeart []int64, PressInterval []int, PressLastTime []time.Time, WorkMode []int, LastMode2Time []int64, Param1 []int, Param2 []int, Param3 []int, Param4 []int) {
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		WifiHeart = append(WifiHeart, 0)
	}
	SmController = &SessionMap{Data: make(map[byte]*Session), Lock: &sync.Mutex{}}
	serverAddr, err := net.ResolveTCPAddr("tcp4", "0.0.0.0:9990")
	if err != nil {
		fmt.Println(err, "tcp server (9990) 初始化失败")
		return
	}
	listener, err := net.ListenTCP("tcp4", serverAddr)
	if err != nil {
		fmt.Println(err, "tcp server (9990) 监听失败")
		return
	}
	defer listener.Close()

	fmt.Println("tcp转发服务器 (9990) 正常启动")

	go JBWC(ctx, mb, canHeart, WifiHeart)

	go TimingWrite(ctx, mb) //定时下发数据，时间和煤机自动化数据

	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				log.Println("tcp监听", err)
				continue
			} else {
				session := &Session{Conn: conn,
					ResponseMsg: make(chan []byte),
					LeaveMsg:    make(chan uint8),
					Buff:        utils.NewPointer(),
				}
				session.Update()
				SmController.Set(session.Key, session)
				go handleLink(ctx, session, mb, WifiHeart, SimulationHeart, PressInterval, PressLastTime, WorkMode, LastMode2Time, Param1, Param2, Param3, Param4)

			}
		}
	}
}

// 处理连接支架控制器的TCP连接
func handleLink(ctx context.Context, session *Session, mb *mbserver.Server, wifiHeart []int64, SimulationHeart []int64, PressInterval []int, PressLastTime []time.Time, WorkMode []int, LastMode2Time []int64, Param1 []int, Param2 []int, Param3 []int, Param4 []int) {
	defer func(session *Session) {
		session.Conn.Close()
		SmController.Delete(session.Key)
		fmt.Println("ID:", session.Key, "handleLink Exit.")
	}(session)
	fmt.Println(session.Conn.RemoteAddr().String(), " 已连接！连接总数为:", len(SmController.Data))
	ctx1, cancel := context.WithCancel(ctx)
	go readRequest(ctx1, session, mb, wifiHeart, SimulationHeart, PressInterval, PressLastTime, WorkMode, LastMode2Time, Param1, Param2, Param3, Param4)
	for {
		select {
		case <-ctx1.Done():
			cancel()
			return
		case <-session.LeaveMsg:
			cancel()
		case msg := <-session.ResponseMsg:
			_, err := session.Conn.Write(msg)
			if err != nil {
				fmt.Println("ID:[", session.Key, "] Write Error:", err)
				cancel()
			}
		}
	}
}

// 接收支架控制器的tcp数据
func readRequest(ctx context.Context, session *Session, mb *mbserver.Server, wifiHeart []int64, SimulationHeart []int64, PressInterval []int, PressLastTime []time.Time, WorkMode []int, LastMode2Time []int64, Param1 []int, Param2 []int, Param3 []int, Param4 []int) {
	defer fmt.Println("ID", session.Key, "readRequest exit")
	buffer := make([]byte, 512)
	//lock := sync.Mutex{}
	for {
		select {
		case <-ctx.Done():
			fmt.Println("ID:", session.Key, " TCPRequest() Exit")
			return
		default:
			time.Sleep(10 * time.Millisecond)
			n, err := session.Conn.Read(buffer)

			if err != nil {
				fmt.Println(
					session.Key, "Read Error:", err)
				session.LeaveMsg <- session.Key
				return
			}
			if n > 0 {
				session.Buff.Reset()
				session.Buff.Write(buffer[0:n])
				for {
					if session.Buff.FindHead() && session.Buff.Len() >= NumberFuncCodeByte {
						fucntion := session.Buff.View(4)
						if fucntion == 3 {
							v := session.Buff.Next(8 + int(session.Buff.View(5)))
							crc1 := v[len(v)-2]
							crc2 := v[len(v)-1]
							delcrc := v[:len(v)-2]
							calcrc := utils.CalModbusCRC(delcrc)
							if crc1 == calcrc[0] && crc2 == calcrc[1] {
								if v[5] == 18 && len(v) == 26 { //回答私有参数100-107
									//fmt.Println(time.Now(), "wifi回复查询", v[3])
									wifiHeart[int(v[3]-1)] = time.Now().Unix()
									SimulationHeart[int(v[3]-1)] = time.Now().Unix()
									// if (uint16(v[12])<<8)|uint16(v[13]) > 0 {
									// 	fmt.Println(v[3], "号支架接收到wifi数据:", "顶板高度:", (uint16(v[12])<<8)|uint16(v[13]))
									// 	fmt.Println()
									// }

									mb.HoldingRegisters[1520+int(v[3]-1)*9] = (uint16(v[6]) << 8) | uint16(v[7])
									mb.HoldingRegisters[1521+int(v[3]-1)*9] = (uint16(v[8]) << 8) | uint16(v[9])
									mb.HoldingRegisters[1522+int(v[3]-1)*9] = (uint16(v[10]) << 8) | uint16(v[11])
									if int(v[3])%10 == 0 {
										//fmt.Println(v[3], "号支架接收到wifi数据:", "顶板高度:", (uint16(v[12])<<8)|uint16(v[13]))
										mb.HoldingRegisters[1523+int(v[3]-1)*9] = (uint16(v[12]) << 8) | uint16(v[13])
									} else {
										mb.HoldingRegisters[1523+int(v[3]-1)*9] = 0
									}
									mb.HoldingRegisters[1524+int(v[3]-1)*9] = (uint16(v[14]) << 8) | uint16(v[15])
									mb.HoldingRegisters[1525+int(v[3]-1)*9] = (uint16(v[16]) << 8) | uint16(v[17])
									mb.HoldingRegisters[1526+int(v[3]-1)*9] = (uint16(v[18]) << 8) | uint16(v[19])
									mb.HoldingRegisters[1527+int(v[3]-1)*9] = (uint16(v[20]) << 8) | uint16(v[21])
									mb.HoldingRegisters[1528+int(v[3]-1)*9] = (uint16(v[22]) << 8) | uint16(v[23])
									// user := model.Pressure{}
									// if mysql.Mysqlclient != nil {
									// 	mysql.Mysqlclient.Model(&user).Where("support=?", int(v[3])).Update("LeftValue2", int(mb.HoldingRegisters[1520+int(v[3]-1)*9]))
									// 	mysql.Mysqlclient.Model(&user).Where("support=?", int(v[3])).Update("RightValue2", int(mb.HoldingRegisters[1521+int(v[3]-1)*9]))
									// 	mysql.Mysqlclient.Model(&user).Where("support=?", int(v[3])).Update("Time2", time.Now())
									// }
									for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
										PressInterval[i] = int(time.Now().Unix() - PressLastTime[i].Unix())
									}
									PressLastTime[int(v[3]-1)] = time.Now()

									var simulationStatus model.SimulationStatus
									for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
										if utils.Conf.PRESSUREPARAM.Enable == 1 && (i < 6 || i > (utils.Conf.SYSTEM.SupportNum-10)) && int(mb.HoldingRegisters[1520+i*9]) < utils.Conf.PRESSUREPARAM.PressThreshold {
											simulationStatus.ColumnPressureLeft = append(simulationStatus.ColumnPressureLeft, 253+rand.Intn(10))
										} else {
											simulationStatus.ColumnPressureLeft = append(simulationStatus.ColumnPressureLeft, int(mb.HoldingRegisters[1520+i*9]))
										}
										simulationStatus.ColumnPressureLeftTime = append(simulationStatus.ColumnPressureLeftTime, PressLastTime[i].Format(timeFormat1))
										simulationStatus.ColumnPressureLeftTimeInterval = append(simulationStatus.ColumnPressureLeftTimeInterval, PressInterval[i])
										simulationStatus.ColumnPressureRight = append(simulationStatus.ColumnPressureRight, int(mb.HoldingRegisters[1521+i*9]))
										simulationStatus.PushItinerary = append(simulationStatus.PushItinerary, int(mb.HoldingRegisters[1522+i*9]))
										simulationStatus.RoofHeight = append(simulationStatus.RoofHeight, int(mb.HoldingRegisters[1523+i*9]))
										simulationStatus.RoofXAxis = append(simulationStatus.RoofXAxis, int(mb.HoldingRegisters[1524+i*9]))
										simulationStatus.RoofYAxis = append(simulationStatus.RoofYAxis, int(mb.HoldingRegisters[1525+i*9]))
										simulationStatus.BaseXAxis = append(simulationStatus.BaseXAxis, int(mb.HoldingRegisters[1526+i*9]))
										simulationStatus.BaseYAxis = append(simulationStatus.BaseYAxis, int(mb.HoldingRegisters[1527+i*9]))
										simulationStatus.BatteryVoltage = append(simulationStatus.BatteryVoltage, int(mb.HoldingRegisters[1527+i*9]>>8&0x00ff))
										simulationStatus.Voltage12V = append(simulationStatus.Voltage12V, int(mb.HoldingRegisters[1527+i*9]&0x00ff))
									}
									WebsocketMessage := model.WebsocketMessage{
										Type:    "simulationStatus",
										Source:  0,
										Message: simulationStatus,
									}
									strings, _ := json.Marshal(WebsocketMessage)
									service.WebsocketManager.SendAll(strings)

								} else if v[5] == 4 && len(v) == 12 { //回答私有参数13-14
									wifiHeart[int(v[3]-1)] = time.Now().Unix()
									fmt.Println("wifi私参回帧", "支架号:", int(v[3]))
									mb.HoldingRegisters[202+int(v[3]-1)*6] = (uint16(v[6]) << 8) | uint16(v[7])
									mb.HoldingRegisters[203+int(v[3]-1)*6] = (uint16(v[8]) << 8) | uint16(v[9])
									var privateParam model.PrivateParam
									privateParam.BackwashValve = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 13 & 0x0001)
									privateParam.RearPlateCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 12 & 0x0001)
									privateParam.RearPillarCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 11 & 0x0001)
									privateParam.BottomAdjustmentCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 10 & 0x0001)
									privateParam.SideGuardCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 9 & 0x0001)
									privateParam.SprayValve = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 8 & 0x0001)
									privateParam.BottomingCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 7 & 0x0001)
									privateParam.PushCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 6 & 0x0001)
									privateParam.FrontPillarCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 5 & 0x0001)
									privateParam.BalanceCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 4 & 0x0001)
									privateParam.FrontBeamCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 3 & 0x0001)
									privateParam.ThreeStageGuardPlateCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 2 & 0x0001)
									privateParam.SecondaryGuardPlateCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] >> 1 & 0x0001)
									privateParam.FirstClassGuardPlateCylinder = int(mb.HoldingRegisters[202+int(v[3]-1)*6] & 0x0001)
									privateParam.AutoStraightenSensorEnable = int(mb.HoldingRegisters[203+int(v[3]-1)*6] >> 7 & 0x0001)
									privateParam.ShearerPositionSensorEnable = int(mb.HoldingRegisters[203+int(v[3]-1)*6] >> 6 & 0x0001)
									privateParam.GuardPlateLimitSensorEnable = int(mb.HoldingRegisters[203+int(v[3]-1)*6] >> 5 & 0x0001)
									privateParam.TopBeamInclinationSensorEnable = int(mb.HoldingRegisters[203+int(v[3]-1)*6] >> 4 & 0x0001)
									privateParam.TopPlateHeightSensorEnable = int(mb.HoldingRegisters[203+int(v[3]-1)*6] >> 3 & 0x0001)
									privateParam.PushDisplacementSensorEnable = int(mb.HoldingRegisters[203+int(v[3]-1)*6] >> 2 & 0x0001)
									privateParam.RPillarPressureSensorEnable = int(mb.HoldingRegisters[203+int(v[3]-1)*6] >> 1 & 0x0001)
									privateParam.LPillarPressureSensorEnable = int(mb.HoldingRegisters[203+int(v[3]-1)*6] & 0x0001)
									WebsocketMessage := model.WebsocketMessage{
										Type:    "privateparam",
										Source:  int(v[3]),
										Message: privateParam,
									}
									strings, _ := json.Marshal(WebsocketMessage)
									service.WebsocketManager.SendAll(strings)

								} else if v[5] == 118 && len(v) == 126 { //回答公共参数15-73
									wifiHeart[int(v[3]-1)] = time.Now().Unix()
									for i := 0; i < 118; i += 2 {
										mb.HoldingRegisters[15+int(i/2)] = (uint16(v[6+i]) << 8) | uint16(v[7+i])
									}
									var publicParam model.PublicParam
									publicParam.ShearerDataEnable = int(mb.HoldingRegisters[15] >> 11 & 0x0001)
									publicParam.RemoteControlEnable = int(mb.HoldingRegisters[15] >> 10 & 0x0001)
									publicParam.TelecontrolEnable = int(mb.HoldingRegisters[15] >> 9 & 0x0001)
									publicParam.WifiEnable = int(mb.HoldingRegisters[15] >> 8 & 0x0001)
									publicParam.AutomaticPushEndEnable = int(mb.HoldingRegisters[15] >> 7 & 0x0001)
									publicParam.AutoFollowMachineEnable = int(mb.HoldingRegisters[15] >> 6 & 0x0001)
									publicParam.AutomaticBackwashEnable = int(mb.HoldingRegisters[15] >> 5 & 0x0001)
									publicParam.AutomaticSprayEnable = int(mb.HoldingRegisters[15] >> 4 & 0x0001)
									publicParam.AutomaticGuardBoardEnable = int(mb.HoldingRegisters[15] >> 3 & 0x0001)
									publicParam.AutomaticPushAndSlideEnable = int(mb.HoldingRegisters[15] >> 2 & 0x0001)
									publicParam.AutomaticRackTransferEnable = int(mb.HoldingRegisters[15] >> 1 & 0x0001)
									publicParam.AutoCompensationEnable = int(mb.HoldingRegisters[15] & 0x0001)
									publicParam.LoweringColumnLiftingBottom = int(mb.HoldingRegisters[16] >> 9 & 0x0001)
									publicParam.SimultaneousAutomaticRackTransfer = int(mb.HoldingRegisters[16] >> 8 & 0x0001)
									publicParam.GuardPlateControl = int(mb.HoldingRegisters[16] >> 7 & 0x0001)
									publicParam.SidePanelControls = int(mb.HoldingRegisters[16] >> 6 & 0x0001)
									publicParam.BalanceControlEnable = int(mb.HoldingRegisters[16] >> 5 & 0x0001)
									publicParam.BottomLifterEnable = int(mb.HoldingRegisters[16] >> 4 & 0x0001)
									publicParam.FrontBeamControlEnable = int(mb.HoldingRegisters[16] >> 3 & 0x0001)
									publicParam.PressureTransferFrameEnable = int(mb.HoldingRegisters[16] >> 2 & 0x0001)
									publicParam.AdjacentFrameAssistEnable = int(mb.HoldingRegisters[16] >> 1 & 0x0001)
									publicParam.AdjacentRackPressureCorrelationEnable = int(mb.HoldingRegisters[16] & 0x0001)
									publicParam.SSIDBYTE2 = int(mb.HoldingRegisters[17] >> 8)
									publicParam.SSIDBYTE1 = int(mb.HoldingRegisters[17] & 0x00ff)
									publicParam.SupportSorting = int(mb.HoldingRegisters[18] & 0x0001)
									utils.SupportSort = publicParam.SupportSorting
									publicParam.TailSupportID = int(mb.HoldingRegisters[19] >> 8)
									publicParam.FirstSupportID = int(mb.HoldingRegisters[19] & 0x00ff)
									publicParam.AutoTailSupportID = int(mb.HoldingRegisters[20] >> 8)
									publicParam.AutoFirstSupportID = int(mb.HoldingRegisters[20] & 0x00ff)
									publicParam.TailTurningPoint = int(mb.HoldingRegisters[21] >> 8)
									publicParam.MachineHeadTurningPoint = int(mb.HoldingRegisters[21] & 0x00ff)
									publicParam.TailCutThroughPoint = int(mb.HoldingRegisters[24] >> 8)
									publicParam.MachineHeadCutThroughPoint = int(mb.HoldingRegisters[24] & 0x00ff)
									publicParam.SuspensionStartThreshold = int(mb.HoldingRegisters[25])
									publicParam.SuspensionStopThreshold = int(mb.HoldingRegisters[26])
									publicParam.RackTransferPressureSetting = int(mb.HoldingRegisters[27])
									publicParam.TransitionPressureSetting = int(mb.HoldingRegisters[28])
									publicParam.InitialPressureSetting = int(mb.HoldingRegisters[29])
									publicParam.PushSlipAllowablePressure = int(mb.HoldingRegisters[31])
									publicParam.MoveDistanceSettingValue = int(mb.HoldingRegisters[32])
									publicParam.PinHysteresisCompensation = int(mb.HoldingRegisters[33] & 0x00ff)
									//publicParam.HeightOfAltimeterCase = int(mb.HoldingRegisters[34])
									publicParam.ShiftDistanceZeroOffset = int(mb.HoldingRegisters[35])
									publicParam.JumpProtectionDistance = int(mb.HoldingRegisters[36] >> 8)
									publicParam.FarthestControlDistance = int(mb.HoldingRegisters[36] & 0x00ff)
									publicParam.GuardPlateInterval = int(mb.HoldingRegisters[38] >> 12 & 0x000f)
									publicParam.GuardPlateDelay = int(mb.HoldingRegisters[38] >> 8 & 0x000f)
									publicParam.GuardPlateGrouping = int(mb.HoldingRegisters[38] & 0x00ff)
									publicParam.ColumnInterval = int(mb.HoldingRegisters[39] >> 12 & 0x000f)
									publicParam.ColumnDelay = int(mb.HoldingRegisters[39] >> 8 & 0x000f)
									publicParam.ColumnGrouping = int(mb.HoldingRegisters[39] & 0x00ff)
									publicParam.TransferRackInterval = int(mb.HoldingRegisters[40] >> 12 & 0x000f)
									publicParam.TransferRackDelay = int(mb.HoldingRegisters[40] >> 8 & 0x000f)
									publicParam.TransferRackGrouping = int(mb.HoldingRegisters[40] & 0x00ff)
									publicParam.ShoveInterval = int(mb.HoldingRegisters[41] >> 12 & 0x000f)
									publicParam.ShoveDelay = int(mb.HoldingRegisters[41] >> 8 & 0x000f)
									publicParam.ShoveGrouping = int(mb.HoldingRegisters[41] & 0x00ff)
									publicParam.SprayDurationGrouping = int(mb.HoldingRegisters[42] >> 8)
									publicParam.SprayGrouping = int(mb.HoldingRegisters[42] & 0x00ff)
									publicParam.StopLevel1Duration = int(mb.HoldingRegisters[43] >> 8)
									publicParam.StartLevel1Duration = int(mb.HoldingRegisters[43] & 0x00ff)
									publicParam.StopLevel2Duration = int(mb.HoldingRegisters[44] >> 8)
									publicParam.StartLevel2Duration = int(mb.HoldingRegisters[44] & 0x00ff)
									publicParam.StopLevel3Duration = int(mb.HoldingRegisters[45] >> 8)
									publicParam.StartLevel3Duration = int(mb.HoldingRegisters[45] & 0x00ff)
									publicParam.StopFrontBeamDuration = int(mb.HoldingRegisters[46] >> 8)
									publicParam.StartFrontBeamDuration = int(mb.HoldingRegisters[46] & 0x00ff)
									publicParam.ColumnRiseTime = int(mb.HoldingRegisters[47] >> 8)
									publicParam.ColumnDropTime = int(mb.HoldingRegisters[47] & 0x00ff)
									publicParam.PushTime = int(mb.HoldingRegisters[48] >> 8)
									publicParam.RackTransferTime = int(mb.HoldingRegisters[48] & 0x00ff)
									publicParam.BottomLiftingDelayTime = int(mb.HoldingRegisters[49] >> 8)
									publicParam.AutomaticBackwashCycle = int(mb.HoldingRegisters[50])
									publicParam.AutomaticRefillTimes = int(mb.HoldingRegisters[51] >> 13 & 0x0007)
									publicParam.AutomaticRefillInterval = int(mb.HoldingRegisters[51] >> 8 & 0x001f)
									publicParam.AutomaticRefillCycle = int(mb.HoldingRegisters[51] & 0x00ff)
									publicParam.SprayDuration = int(mb.HoldingRegisters[52] >> 8)
									publicParam.BottomLiftDuration = int(mb.HoldingRegisters[52] & 0x00ff)
									publicParam.LowColumnStopBlanceDuration = int(mb.HoldingRegisters[53] >> 12 & 0x000f)
									publicParam.LowColumnStopBlanceStartTime = int(mb.HoldingRegisters[53] >> 8 & 0x000f)
									publicParam.RiseColumnStartBlanceDuration = int(mb.HoldingRegisters[53] >> 4 & 0x000f)
									publicParam.RiseColumnStartBlanceStartTime = int(mb.HoldingRegisters[53] & 0x000f)
									publicParam.AutomaticRackTransferEarlyWarningTime = int(mb.HoldingRegisters[54] >> 8 & 0x000f)
									publicParam.SpacingDistanceBetweenStretchGuards = int(mb.HoldingRegisters[55] >> 8)
									publicParam.SpacingDistanceBetweenStopGuards = int(mb.HoldingRegisters[55] & 0x00ff)
									publicParam.PushingIntervalDistance = int(mb.HoldingRegisters[57] >> 8)
									publicParam.MovingIntervalDistance = int(mb.HoldingRegisters[57] & 0x00ff)
									publicParam.SprayIntervalDistance = int(mb.HoldingRegisters[58] & 0x00ff)
									publicParam.AdjacentBracketCenterDistance = int(mb.HoldingRegisters[59] >> 8)
									publicParam.ShearerLength = int(mb.HoldingRegisters[59] & 0x00ff)
									publicParam.PullBackDistance = int(mb.HoldingRegisters[60] & 0x00ff)
									publicParam.PostFallColumnTime = int(mb.HoldingRegisters[61] & 0x00ff)
									publicParam.ColumnTimeAfterRise = int(mb.HoldingRegisters[62] & 0x00ff)
									publicParam.CloseBoardTime = int(mb.HoldingRegisters[63] >> 8)
									publicParam.OpenBoardTime = int(mb.HoldingRegisters[63] & 0x00ff)
									publicParam.BackSlipTime = int(mb.HoldingRegisters[64] & 0x00ff)
									publicParam.DataSheetCRC = int(mb.HoldingRegisters[69])
									publicParam.AutoBackwashRemainingH = int(mb.HoldingRegisters[72] >> 8)
									publicParam.AutoBackwashRemainingM = int(mb.HoldingRegisters[72] & 0x00ff)
									publicParam.AutoRefillRemainingH = int(mb.HoldingRegisters[73] >> 8)
									publicParam.AutoRefillRemainingM = int(mb.HoldingRegisters[73] & 0x00ff)

									WebsocketMessage := model.WebsocketMessage{
										Type:    "publicparam",
										Source:  0,
										Message: publicParam,
									}
									strings, _ := json.Marshal(WebsocketMessage)
									service.WebsocketManager.SendAll(strings)
								} else if v[5] == 8 && len(v) == 16 {
									rand.Seed(time.Now().UnixNano())
									wifiHeart[int(v[3]-1)] = time.Now().Unix()
									fmt.Println("更新wifi时间")
									Param1[int(v[3])-1] = int(v[6] >> 4)
									Param2[int(v[3])-1] = int(v[12] & 0x7f)
									Param3[int(v[3])-1] = int(time.Now().Unix() - LastMode2Time[int(v[3])-1])
									Param4[int(v[3])-1] = int(math.Abs(float64(int(mb.HoldingRegisters[180]) - int(v[3])*10)))
									// user := model.ModeData{Time: time.Now(), Intervention: int(v[12] >> 4), ActionCode: int(v[12] & 0x7f)}
									// mysql.Mysqlclient.Select("Time", "Intervention", "ActionCode").Create(&user)
									if int(v[12]) == 0 || int(v[12]) == 38 { //空闲状态
										WorkMode[int(v[3])-1] = 0
										fmt.Println("空闲")
									} else {
										//fmt.Println("wifi有动作上传", int(v[6]>>4), int(v[12]&0x7f), (int(time.Now().Unix() - LastMode2Time[int(v[3])-1])), (int(math.Abs(float64(int(mb.HoldingRegisters[180]) - int(v[3])*10)))))
										// if int(v[6]>>4) > utils.Conf.MODEPARAM.Intervention &&
										// 	(int(v[12]&0x7f) == utils.Conf.MODEPARAM.ActionCode1 ||
										// 		int(v[12]&0x7f) == utils.Conf.MODEPARAM.ActionCode2 ||
										// 		int(v[12]&0x7f) == utils.Conf.MODEPARAM.ActionCode3 ||
										// 		int(v[12]&0x7f) == utils.Conf.MODEPARAM.ActionCode4 ||
										// 		int(v[12]&0x7f) == utils.Conf.MODEPARAM.ActionCode5 ||
										// 		int(v[12]&0x7f) == utils.Conf.MODEPARAM.ActionCode6 ||
										// 		int(v[12]&0x7f) == utils.Conf.MODEPARAM.ActionCode7 ||
										// 		int(v[12]&0x7f) == utils.Conf.MODEPARAM.ActionCode8) &&
										// 	(int(time.Now().Unix()-LastMode2Time[int(v[3])-1]) > utils.Conf.MODEPARAM.TimeInterval) &&
										// 	(int(math.Abs(float64(int(mb.HoldingRegisters[180])-int(v[3])*10))) < utils.Conf.MODEPARAM.PositionLimit) {
										// 	if rand.Intn(100) >= utils.Conf.MODEPARAM.ProbabilityHand {
										// 		WorkMode[int(v[3])-1] = 2
										// 	} else {
										// 		WorkMode[int(v[3])-1] = 1
										// 	}

										// 	LastMode2Time[int(v[3])-1] = time.Now().Unix()
										// } else {
										// 	if rand.Intn(100) >= utils.Conf.MODEPARAM.ProbabilityAuto {
										// 		WorkMode[int(v[3])-1] = 1
										// 	} else {
										// 		WorkMode[int(v[3])-1] = 2
										// 	}

										// }
										//&& rand.Intn(100) <= 50
										if int(v[12]&0x7f) == 9 || (int(v[12]&0x7f) == 4 && rand.Intn(100) <= 2) {
											fmt.Println("人为干预动作")
											WorkMode[int(v[3])-1] = 2
										} else {
											fmt.Println("自动动作")
											WorkMode[int(v[3])-1] = 1
										}
									}

									//电磁阀状态
									mb.HoldingRegisters[5400+int(v[3])-1] = (uint16(v[8]) << 8) | uint16(v[9])
									//当前执行命令
									// if v[12]&0x7f >= 30 && v[12]&0x7f <= 36 {
									// 	WorkMode[int(v[3])-1] = 1 //自动命令
									// } else if v[12]&0x7f > 0 && v[12]&0x7f <= 29 {
									// 	WorkMode[int(v[3])-1] = 2 //手动干预命令
									// } else {
									// 	WorkMode[int(v[3])-1] = 0
									// }
									//fmt.Println(time.Now(), "wifi回复查询支架动作", v[3])
								} else if v[5] == 2 && len(v) == 10 {
									wifiHeart[int(v[3]-1)] = time.Now().Unix()
									//lock.Lock()
									randomAutoReceive = append(randomAutoReceive, int(v[6]>>7))
									fmt.Println("wifi查询自动化状态回复", int(v[3]), len(randomAutoReceive), randomAutoReceive)
									if len(randomAutoReceive) > 10 {
										mb.HoldingRegisters[177] = uint16(randomAutoReceive[0] | randomAutoReceive[1] | randomAutoReceive[2] | randomAutoReceive[3] | randomAutoReceive[4] | randomAutoReceive[5] | randomAutoReceive[6] | randomAutoReceive[7] | randomAutoReceive[8] | randomAutoReceive[9])
										//fmt.Println("写入自动化状态：", mb.HoldingRegisters[177], randomAutoReceive)
										randomAutoReceive = []int{}
										if mb.HoldingRegisters[177] == 0 {
											var autoFollowStatus model.AutoFollowStatus
											for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
												mb.HoldingRegisters[3500+i] = mb.HoldingRegisters[3500+i] & 0xfff0
												autoFollowStatus.IsAutoFollow = append(autoFollowStatus.IsAutoFollow, int(mb.HoldingRegisters[177]))
												autoFollowStatus.CompleteAutomaticPush = append(autoFollowStatus.CompleteAutomaticPush, 0)
												autoFollowStatus.CompleteAutomaticRackTransfer = append(autoFollowStatus.CompleteAutomaticRackTransfer, 0)
												autoFollowStatus.CompleteAutomaticCare = append(autoFollowStatus.CompleteAutomaticCare, 0)
												autoFollowStatus.CompleteAutomaticExtension = append(autoFollowStatus.CompleteAutomaticExtension, 0)
											}
											WebsocketMessage := model.WebsocketMessage{
												Type:    "autoFollowStatus",
												Source:  0,
												Message: autoFollowStatus,
											}
											strings, _ := json.Marshal(WebsocketMessage)
											service.WebsocketManager.SendAll(strings)
										}
									}
									//fmt.Println("wifi查询自动化状态回复1", int(v[3]), len(randomAutoReceive), randomAutoReceive)
									//lock.Unlock()
								}
							}
						} else if fucntion == 66 {
							v := session.Buff.Next(NumberFuncCode66Byte)
							SendMsgTo(uint8(v[2]), v)
							//beego.Debug("方法码：", v[4], "源:", int(v[3]), "目的:", int(v[2]), "编组数量:", v[6], "CMD:", v[7], v)
						} else {
							session.Buff.Next(2)
						}
					} else {
						break
					}
				}
			}
		}
	}
}

// SendMsgTo 定向下发指令到支架控制器
func SendMsgTo(id uint8, msg []byte) {
	SmController.Lock.Lock()
	s, ok := SmController.Data[byte(id)]
	//SmController.Data[byte(id)].ResponseMsg <- msg
	SmController.Lock.Unlock()

	if ok {
		s.ResponseMsg <- msg
	}
}

func assembleWifiTime(TargetID byte) (data bytes.Buffer) {
	timeNow := time.Now().Local()
	h := timeNow.Hour()
	m := timeNow.Minute()
	s := timeNow.Second()
	var b bytes.Buffer
	b.Write([]byte{0xcd, 0xef})
	b.WriteByte(TargetID)
	b.WriteByte(0)
	b.WriteByte(0x10)
	b.WriteByte(byte(140 >> 8))
	b.WriteByte(byte(140 & 0x00ff))
	b.WriteByte(byte(2 >> 8))
	b.WriteByte(byte(2 & 0x00ff))
	b.WriteByte(byte(4))
	b.WriteByte(byte(m))
	b.WriteByte(byte(h))
	b.WriteByte(byte(0))
	b.WriteByte(byte(s))
	data = b
	return
}

// TimingWrite 定时下发tcp数据
func TimingWrite(ctx context.Context, mb *mbserver.Server) {
	MsgCmd = make(chan Cmd, 10)
	tickerWriteTime := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerWriteTime.C: //对支架控制器定时下发时间数据
			go func() {
				for _, v := range SmController.Data {
					msg := assembleWifiTime(byte(v.Key))
					SendMsgTo(byte(v.Key), utils.AddModbusCRC(msg.Bytes()))
				}
			}()
		}
	}
}

// SendToAll 统一向所有支架下发数据
func SendToAll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case tempCmd := <-MsgCmd:
			for _, v := range SmController.Data {
				tempCmd.ID = v.Key
				d := utils.AddModbusCRC(tempCmd.ToByte())
				v.ResponseMsg <- d
			}
		}
	}

}

func JBWC(ctx context.Context, mb *mbserver.Server, CanHeart, wifiHeart []int64) {
	var canStatus int
	var wifiStatus int
	tickerWriteTime := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerWriteTime.C:
			//canIsOk := true
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				// if time.Now().Unix()-CanHeart[i] > 600 && time.Now().Unix()-wifiHeart[i] > 60 {
				// 	mb.HoldingRegisters[4440+i] = uint16(1)<<8 | uint16(1)<<1 | uint16(1)
				// } else {
				if time.Now().Unix()-CanHeart[i] > 600 {
					canStatus = 1
				} else {
					canStatus = 0
				}
				if time.Now().Unix()-wifiHeart[i] > 60 {
					wifiStatus = 1
				} else {
					wifiStatus = 0
				}
				bsStatus := mb.HoldingRegisters[8000+i] >> 6 & 0x0001
				jtStatus := mb.HoldingRegisters[8000+i] >> 7 & 0x0001
				mb.HoldingRegisters[4440+i] = uint16(jtStatus)<<3 | uint16(bsStatus)<<2 | uint16(wifiStatus)<<1 | uint16(canStatus)

				//}
			}
			var faultStatus model.FaultStatus
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				faultStatus.Credible = append(faultStatus.WifiArr, int(mb.HoldingRegisters[4440+i]>>8&0x0001))
				faultStatus.WifiArr = append(faultStatus.WifiArr, int(mb.HoldingRegisters[4440+i]>>1&0x0001))
				faultStatus.CanArr = append(faultStatus.CanArr, int(mb.HoldingRegisters[4440+i]&0x0001))
				faultStatus.EmergencyStopArr = append(faultStatus.EmergencyStopArr, int(mb.HoldingRegisters[4440+i]>>3&0x0001))
				faultStatus.LockArr = append(faultStatus.LockArr, int(mb.HoldingRegisters[4440+i]>>2&0x0001))
				faultStatus.LinArr = append(faultStatus.LinArr, int(mb.HoldingRegisters[5200+i]))
			}

			WebsocketMessage := model.WebsocketMessage{
				Type:    "faultStatus",
				Source:  0,
				Message: faultStatus,
			}
			strings, _ := json.Marshal(WebsocketMessage)
			service.WebsocketManager.SendAll(strings)

			//fmt.Println("发送闭锁消息")
		}
	}
}

func AssembleWifiIDQuery(TargetID byte, address, number int) (data bytes.Buffer) {
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
