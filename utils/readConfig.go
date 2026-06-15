package utils

import (
	"log"

	"github.com/spf13/viper"
)

// Conf  读取config.ini,全局使用
var Conf config
var CurrentAbPath string
var SupportSort int

type GLOBAL struct {
	HttpPort      string
	ControlEnable int //0不能控，1可以控
	Cqlj          int
}
type MODBUSSLAVE struct {
	SlaveIp   string
	SlavePort string
}
type MODBUSSHEARER struct {
	SlaveEnable int
	SlaveId     int
	SlaveIp     string
	SlavePort   int
	SlavePoint  uint16
	Sort        int
}

type REMOTECONTROL struct {
	SlaveId   int
	SlaveIp   string
	SlavePort int
}

type CAN struct {
	Can2udpIp1      string
	Can2udpIp2      string
	Can2udpIp3      string
	Can2udpIpExtra  string
	Can2udpIpExtra1 string
	Point1          int
	Point2          int
	PointExtra      int
	PointExtra1     int
	Can2udpPort     int
}

type SYSTEM struct {
	SupportNum int
	Mode       int //0为纯can模式  1为纯wifi模式 2位can/wifi混合模式
}

type MODEPARAM struct {
	Intervention    int
	ActionCode1     int
	ActionCode2     int
	ActionCode3     int
	ActionCode4     int
	ActionCode5     int
	ActionCode6     int
	ActionCode7     int
	ActionCode8     int
	TimeInterval    int
	PositionLimit   int
	ProbabilityHand int
	ProbabilityAuto int
}

type DATABASE struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
}

type PRESSUREPARAM struct {
	Enable         int
	PressThreshold int
}

type config struct {
	GLOBAL
	MODBUSSLAVE
	MODBUSSHEARER
	REMOTECONTROL
	CAN
	SYSTEM
	DATABASE
	MODEPARAM
	PRESSUREPARAM
}

func LoadConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(CurrentAbPath)
	viper.SetDefault("GLOBAL.HttpPort", "8080")
	viper.SetDefault("GLOBAL.ControlEnable", 0)
	viper.SetDefault("GLOBAL.Cqlj", 200)
	viper.SetDefault("MODBUSSLAVE.SlaveIp", "192.168.5.101")
	viper.SetDefault("MODBUSSLAVE.SlavePort", "2502")
	viper.SetDefault("MODBUSSHEARER.SlaveEnable", 1)
	viper.SetDefault("MODBUSSHEARER.SlaveId", 1)
	viper.SetDefault("MODBUSSHEARER.SlaveIp", "172.16.1.211")
	viper.SetDefault("MODBUSSHEARER.SlavePort", 502)
	viper.SetDefault("MODBUSSHEARER.SlavePoint", 32768)
	viper.SetDefault("MODBUSSHEARER.Sort", 0)
	viper.SetDefault("REMOTECONTROL.SlaveId", 1)
	viper.SetDefault("REMOTECONTROL.SlaveIp", "172.16.1.211")
	viper.SetDefault("REMOTECONTROL.SlavePort", 502)
	viper.SetDefault("CAN.Can2udpIp1", "10.246.78.180")
	viper.SetDefault("CAN.Can2udpIp2", "10.246.79.197")
	viper.SetDefault("CAN.Can2udpIp3", "10.246.79.198")
	viper.SetDefault("CAN.Can2udpIpExtra", "172.16.1.240")
	viper.SetDefault("CAN.Point1", 75)
	viper.SetDefault("CAN.Point2", 141)
	viper.SetDefault("CAN.PonitExtra", 141)
	viper.SetDefault("CAN.Can2udpPort", 8900)
	viper.SetDefault("SYSTEM.SupportNum", 220)
	viper.SetDefault("SYSTEM.Mode", 0)
	viper.SetDefault("DATABASE.Username", "root")
	viper.SetDefault("DATABASE.Password", "lianli")
	viper.SetDefault("DATABASE.Host", "loaclhost")
	viper.SetDefault("DATABASE.Port", 3306)
	viper.SetDefault("DATABASE.Database", "dyk")
	viper.SetDefault("MODEPARAM.Intervention", 7)
	viper.SetDefault("MODEPARAM.ActionCode1", 3)
	viper.SetDefault("MODEPARAM.ActionCode2", 4)
	viper.SetDefault("MODEPARAM.ActionCode3", 5)
	viper.SetDefault("MODEPARAM.ActionCode4", 6)
	viper.SetDefault("MODEPARAM.ActionCode5", 1000)
	viper.SetDefault("MODEPARAM.ActionCode6", 1001)
	viper.SetDefault("MODEPARAM.ActionCode7", 1002)
	viper.SetDefault("MODEPARAM.ActionCode8", 1003)
	viper.SetDefault("MODEPARAM.TimeInterval", 1200)
	viper.SetDefault("MODEPARAM.PositionLimit", 9)
	viper.SetDefault("MODEPARAM.ProbabilityHand", 0)
	viper.SetDefault("MODEPARAM.ProbabilityAuto", 0)
	viper.SetDefault("PRESSUREPARAM.Enable", 0)
	viper.SetDefault("PRESSUREPARAM.PressThreshold", 252)
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal("read config failed: ", err)
	}
	viper.Unmarshal(&Conf)
}
