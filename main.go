package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"gocode/controller"
	service "gocode/services"

	"gocode/services/modbus"
	"gocode/services/mysql"

	//"gocode/services/pressalarm"
	"gocode/services/upload"
	"math/rand"
	"time"

	//"gocode/services/mysql"
	tcpService "gocode/services/tcpserver"
	udpService "gocode/services/udpserver"

	//upload "gocode/services/upload"
	"gocode/utils"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ffhelicopter/tmm/handler"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/tbrandon/mbserver"
)

var (
	// MBServer modbus instance
	MBServer *mbserver.Server

	CanHeart              []int64
	SimulationHeart       []int64
	IsAuto                []int
	thisBsStatus          []int
	lastBsStatus          []int
	isTableExist          []string
	WorkMode              []int
	WorkModeTime          []int64
	LastMode2Time         []int64
	Param1                []int
	Param2                []int
	Param3                []int
	Param4                []int
	PressStore            []int //压力存储数组用于
	PressLastTime         []time.Time
	PressLastTimebuzu     []time.Time
	PressLastTimebuzu_you []time.Time
	PressInterval         []int
	RandomAutoReceive     []int
)

func init() {
	fmt.Println("测试一下")

	utils.CurrentAbPath = getCurrentAbPath() //获取文件根路径
	utils.LoadConfig()                       //获取配置文件
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		CanHeart = append(CanHeart, 0)
		SimulationHeart = append(SimulationHeart, time.Now().Unix())
		IsAuto = append(IsAuto, 0)
		lastBsStatus = append(lastBsStatus, 0)
		WorkMode = append(WorkMode, 0)
		WorkModeTime = append(WorkModeTime, 0)
		LastMode2Time = append(LastMode2Time, 0)
		Param1 = append(Param1, 0)
		Param2 = append(Param2, 0)
		Param3 = append(Param3, 0)
		Param4 = append(Param4, 0)
		PressStore = append(PressStore, 0)
		PressLastTime = append(PressLastTime, time.Now())
		PressLastTimebuzu = append(PressLastTimebuzu, time.Now())
		PressLastTimebuzu_you = append(PressLastTimebuzu_you, time.Now())
		PressInterval = append(PressInterval, 0)
		RandomAutoReceive = append(RandomAutoReceive, 0)
	}
	log.SetFlags(log.LstdFlags | log.Ldate | log.Lmicroseconds)

	rl, _ := rotatelogs.New(path.Join(utils.CurrentAbPath, "static", "logs", "blockage.%Y%m%d.log"),
		rotatelogs.WithRotationCount(30),
		rotatelogs.WithRotationTime(24*time.Hour))
	log.SetOutput(rl)

}

func testzhijiacha() {
	a := 32989
	b := 32994
	c := 59
	var badValue int
	var goodValue int
	var c1 int
	badBit := a >> 15
	if badBit == 1 {
		badValue = -int(a & 0x7fff)
	} else {
		badValue = int(a & 0x7fff)
	}
	fmt.Println("原数据:", a, "解析后：", badValue)
	goodBit := b >> 15
	if goodBit == 1 {
		goodValue = -int(b & 0x7fff)
	} else {
		goodValue = int(b & 0x7fff)
	}
	fmt.Println("原数据:", b, "解析后：", goodValue)

	c1bit := c >> 15
	if c1bit == 1 {
		c1 = -int(c & 0x7fff)
	} else {
		c1 = int(c & 0x7fff)
	}
	fmt.Println("原数据:", c, "解析后：", c1, c1bit)
	value := 960 + goodValue - badValue
	fmt.Println("处理后行程：", badValue, goodValue, "计算的结果：", value)
}
func main() {
	fmt.Println("service is starting.")
	//testzhijiacha()
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	//解决跨域
	corsConf := cors.DefaultConfig()
	corsConf.AllowAllOrigins = true
	router.Use(cors.New(corsConf))

	ctx, cancel := context.WithCancel(context.Background())

	MBServer = mbserver.NewServer()
	go mysql.InitMysql(ctx, MBServer) //初始化mysql数据库
	//go mysql.Test(ctx)
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		MBServer.HoldingRegisters[1520+i*9] = uint16(252 + rand.Intn(50))
	}
	udpService.Controlcommand = make(chan uint16, 1000)
	upload.RealAciton = make(chan upload.RealData, 1024)
	m1 := utils.RemoteMap1()
	m2 := utils.RemoteMap2()
	m3 := utils.RemoteMap3()

	//监听外部给本地2502上写数据
	MBServer.RegisterFunctionHandler(6, func(s *mbserver.Server, frame mbserver.Framer) ([]byte, *mbserver.Exception) {
		data := frame.GetData()
		register := int64(binary.BigEndian.Uint16(data[0:2])) //被写哪一个寄存器
		value := binary.BigEndian.Uint16(data[2:4])           //写的值是多少
		s.HoldingRegisters[register] = value
		//udp处理
		if register == 0 { // 控制命令
			fmt.Println("收到控制命令", value)
			udpService.Controlcommand <- value
		} else if register == 79 { //记录数据
			fmt.Println("开启自动跟机")
			//udpService.CANSendAutoTraceShearer(value)
		} else if register == 5 && value&0x00ff != 0xff { // 更新私有参数静态部分*
			//udpService.CANRequestPrivateStaticData(int(value))
		} else if register == 4 && value&0x00ff != 0x00 { // 手动单地址配置
			//udpService.ConfigureAddress(-1)
		} else if register == 12 {
			udpService.CANRequestPublicData()
		} else if register > 14 && register < 72 || register == 9 { // 配置公共参数
			//udpService.CANConfigurationPublicParameter(register, value)
		} else if register == 70 {
			//udpService.AutoFollow() //自动跟机
		} else if register > 199 && register < 1520 { // 配置私有参数
			// support := (register - 194) / 6
			// remainder := (register - 194) % 6
			// udpService.CANConfigurationPrivateParameter(int(support), int(remainder))
		} else if register > 32767 && register < 32771 { //远控平台
			//fmt.Println("收到远控平台控制命令")
			if register == 32768 {
				udpService.RemoteControl(register, value, m1)
			} else if register == 32769 {
				udpService.RemoteControl(register, value, m2)
			} else if register == 32770 {
				udpService.RemoteControl(register, value, m3)
			}

		} else if register > 3999 && register < 4220 { //软件闭锁 按下为1 松开为0
			// if value == 1 {
			// 	udpService.BSOpen(byte(register - 3999))
			// } else if value == 0 {
			// 	udpService.BSClose(byte(register - 3999))
			// }
		} else if register > 5799 && register < 6020 { //调直支架之间差值
			fmt.Println("收到调直服务写的支架之间差值")
		}
		return frame.GetData()[0:4], &mbserver.Success
	})

	// MBServer.RegisterFunctionHandler(16, func(s *mbserver.Server, frame mbserver.Framer) ([]byte, *mbserver.Exception) {
	// 	if MBServer.HoldingRegisters[176] == 0 {
	// 		return frame.GetData()[0:4], &mbserver.Success
	// 	}
	// 	thisBsStatus = []int{}
	// 	data := frame.GetData()
	// 	register := int64(binary.BigEndian.Uint16(data[0:2]))
	// 	length := int64(binary.BigEndian.Uint16(data[2:4]))
	// 	//fmt.Println("人员接近防护", data, register, length)
	// 	for i := 0; i < int(length); i++ {
	// 		s.HoldingRegisters[register+int64(i)] = binary.BigEndian.Uint16(data[5+i*2 : 7+i*2])
	// 		if i < int(length)-1 {
	// 			for n := 0; n < 16; n++ {
	// 				thisBsStatus = append(thisBsStatus, int(s.HoldingRegisters[register+int64(i)]>>n&0x0001))
	// 			}
	// 		} else {
	// 			for n := 0; n < 4; n++ {
	// 				thisBsStatus = append(thisBsStatus, int(s.HoldingRegisters[register+int64(i)]>>n&0x0001))
	// 			}
	// 		}

	// 	}
	// 	for i := 0; i < len(lastBsStatus); i++ {
	// 		if lastBsStatus[i] != thisBsStatus[i] {
	// 			if lastBsStatus[i] == 0 && thisBsStatus[i] == 1 { //按下软件闭锁
	// 				fmt.Println(i+1, "号支架按下软件闭锁")
	// 				udpService.BSOpen(byte(i + 1))
	// 			} else if lastBsStatus[i] == 1 && thisBsStatus[i] == 0 { //解除软件闭锁
	// 				fmt.Println(i+1, "号支架解除软件闭锁")
	// 				udpService.BSClose(byte(i + 1))
	// 			}
	// 		}
	// 	}
	// 	//fmt.Println("----------------------------------------------")
	// 	lastBsStatus = thisBsStatus
	// 	return frame.GetData()[0:4], &mbserver.Success
	// })

	fmt.Println(utils.Conf.MODBUSSLAVE.SlaveIp, utils.Conf.MODBUSSLAVE.SlavePort)

	err := MBServer.ListenTCP(":" + utils.Conf.MODBUSSLAVE.SlavePort)
	if err != nil {
		fmt.Println("service blockage is running.")
		fmt.Println(err)
		return
	}

	MBServer.HoldingRegisters[176] = 1
	defer MBServer.Close()
	fmt.Println(http.Dir(path.Join(utils.CurrentAbPath, "static")))
	router.LoadHTMLGlob(path.Join(utils.CurrentAbPath, "views/*"))
	router.StaticFS("/static", http.Dir(path.Join(utils.CurrentAbPath, "static")))
	// //router.StaticFile("/favicon.ico", path.Join(utils.ConfigPath, "favicon.ico"))
	router.GET("/", handler.IndexHandler)
	router.GET("/ws", service.WebsocketManager.WsClient)
	v := router.Group("/api")
	{
		//私参查询
		v.GET("/private/query", controller.PrivateQuery)
		//公参查询
		v.GET("/public/query", controller.PublicQuery)
		//私参修改
		v.POST("/private/modify", controller.PrivateModify)
		//公参修改
		v.POST("/public/modify", controller.PublicModify)
		//控制命令
		v.POST("/control", controller.Control)
		//地址配置
		v.GET("/address", controller.Address)
		//软件闭锁
		v.GET("/lock", controller.Lock)
		//软件解锁
		v.GET("/unlock", controller.UnLock)
		//查询状态
		v.GET("/lock/query", controller.QueryLock)
		//煤机参数设置
		v.POST("/setshearer", controller.Setshearer)
		//登录
		v.POST("/login", controller.Login)

		//查询压力行程报表
		v.POST("/tablequery", controller.TableQuery)
		//查询故障报表
		v.POST("/faultquery", controller.FaultQuery)
		//查询支架总数
		v.GET("/supportnum", controller.SupportNumQuery)
		//查询煤机当前位置
		v.GET("/supportposition", controller.SupportPositionQuery)
		//人员定位使能
		v.GET("/person/enable", controller.PersonEnable)
		//支架全自动化使能
		v.GET("/auto/enable", controller.AutoEnable)
		//查询自动跟机状态报表
		v.POST("/autodataquery", controller.AutoDataQuery)
		//查询自动跟机状态单条记录
		v.POST("/oneautodataquery", controller.OneAutoDataQuery)
		//查询下一条自动跟机状态单条记录
		v.POST("/nextoneautodataquery", controller.NextOneAutoDataQuery)
		//查询上一条自动跟机状态单条记录
		v.POST("/lastoneautodataquery", controller.LastOneAutoDataQuery)
		//查询热力图数据
		v.POST("/getheatmap", controller.GetHeatMap)
		//查询热力图数据
		v.POST("/sendtestdata", controller.SendTestData)
		v.POST("/stoptestdata", controller.StopTestData)

		//给王总取证
		v.POST("/getwmldata", controller.GetWmlData)
		v.POST("/getrealdata", controller.GetRealData)
		v.POST("/realquery", controller.GetSupRealtime)
		v.POST("/warningquery", controller.GetSupWarning)

		v.POST("/getrecordcommand", controller.RecordCommandQuery1)
		v.POST("/getrecordcommandwithpagination", controller.RecordCommandQuery1)
	}

	go func() {
		router.Run(":" + utils.Conf.GLOBAL.HttpPort)
	}()

	go service.WebsocketManager.Start(ctx)                                                                                                                                                                                                  //websocket
	go tcpService.Run(ctx, MBServer, CanHeart, SimulationHeart, PressInterval, PressLastTime, WorkMode, LastMode2Time, Param1, Param2, Param3, Param4)                                                                                      //wifi监听
	go udpService.Run(ctx, MBServer, utils.Conf.SYSTEM.SupportNum, CanHeart, PressInterval, PressLastTime, WorkMode, WorkModeTime, LastMode2Time, IsAuto, SimulationHeart, isTableExist, Param1, Param2, Param3, Param4, RandomAutoReceive) // can2udp模块收发支架控制器数据

	go udpService.CANSendCommand1(ctx) //远程控制和调直 //力控控制命令

	//煤机数据设置
	go udpService.SetShearerParam(ctx, MBServer)
	//煤机数据读取
	go modbus.Run(ctx, MBServer)
	go udpService.FirstPublicQuery()                                                                                                             //程序启动后主动问询一次公参
	go upload.StartDataUpLoad(ctx, MBServer, IsAuto, WorkMode, Param1, Param2, Param3, Param4, cancel, PressLastTimebuzu, PressLastTimebuzu_you) //数据上传
	//go upload.StartRealDataUpLoad(ctx, MBServer, cancel)       //王总
	// go udpService.StartDBWorker()            //王总
	// go udpService.StartDBSendDataWorker()    //王总
	go udpService.StartRecordCommandWorker() //支架动作指令记录

	//go upload.RecordUploadError(ctx, PressInterval, PressLastTime)                                     //清空上传命令
	//go udpService.TrafficLight(ctx, MBServer)                                                   //机头支架动作红绿灯
	//go pressalarm.UpPressAlarm(ctx, MBServer, PressStore)
	go upload.RecordPressMysql(ctx)
	go upload.RecordAutoActionData(ctx, MBServer) //自动化状态历史查询
	go udpService.SendWebsocket(ctx, PressLastTime, RandomAutoReceive)
	go udpService.CanLoadRate(ctx, MBServer)         //can负载率
	go udpService.CanAutoState(ctx, MBServer)        //can自动化上传
	go udpService.ReadRemoteControl(ctx, m1, m2, m3) //远控平台
	go udpService.CANCalibrationTime(ctx)            //修改控制器时间
	go udpService.GetUwb_Three(ctx)                  //人员定位

	//go upload.TestRecordCommandMysql(ctx) //用于测试支架运行数据记录的
	//go mysql.SQLTest(ctx)
	quit := make(chan os.Signal, 1)
	// 优雅Shutdown（或重启）服务
	signal.Notify(quit, os.Interrupt) // syscall.SIGKILL
	<-quit

	cancel()
	select {
	case <-ctx.Done():
		//time.Sleep(time.Second * 1)
		log.Println("Server exited")
	}
}

// 最终方案-全兼容
func getCurrentAbPath() string {
	dir := getCurrentAbPathByExecutable()
	if strings.Contains(dir, getTmpDir()) {
		return getCurrentAbPathByCaller()
	}
	return dir
}

// 获取系统临时目录，兼容go run
func getTmpDir() string {
	dir := os.Getenv("TEMP")
	if dir == "" {
		dir = os.Getenv("TMP")
	}
	res, _ := filepath.EvalSymlinks(dir)
	return res
}

// 获取当前执行文件绝对路径
func getCurrentAbPathByExecutable() string {
	exePath, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	res, _ := filepath.EvalSymlinks(filepath.Dir(exePath))
	return res
}

// 获取当前执行文件绝对路径（go run）
func getCurrentAbPathByCaller() string {
	var abPath string
	_, filename, _, ok := runtime.Caller(0)
	if ok {
		abPath = path.Dir(filename)
	}
	return abPath
}
