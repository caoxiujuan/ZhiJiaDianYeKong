package model

import (
	"time"
)

type CanData struct {
	ID          int       `json:"id" gorm:"size:32;auto_increment;primary_key;"`
	Time        time.Time `json:"time"  gorm:"type:datetime(3)"`
	Type        string    `json:"type" gorm:"type:varchar(50)"`
	CanId       string    `json:"can_id" gorm:"type:varchar(50)"`
	Len         int       `json:"len" gorm:"size:32"`
	Information string    `json:"information" gorm:"type:varchar(50)"`
}

func (CanData) TableName() string {
	return "can_data_" + time.Now().Format("2006-01-02")
}

type LoginParams struct {
	Id       int    `json:"id" gorm:"primary_key" sql:"auto_increment;primary_key;"`
	Username string `json:"username"`
	Password string `json:"password"`
	Uuid     string `json:"uuid"`
}

type CanActionData struct {
	Id          int       `json:"id" gorm:"primary_key" sql:"auto_increment;primary_key;"`
	Time        time.Time `json:"time"`
	CanId       string    `json:"can_id"`
	Len         int       `json:"len"`
	Information string    `json:"information"`
}

//	type CanData struct {
//		Id          int       `json:"id"`
//		Time        time.Time `json:"time"`
//		Type        string    `json:"type"`
//		CanId       string    `json:"can_id"`
//		Len         int       `json:"len"`
//		Information string    `json:"information"`
//	}
type CanAutoData struct {
	Id          int       `json:"id"`
	Time        time.Time `json:"time"`
	Type        string    `json:"type"`
	CanId       string    `json:"can_id"`
	Len         int       `json:"len"`
	Information string    `json:"information"`
}

type ModeData struct {
	Id           int       `json:"id"`
	Time         time.Time `json:"time"`
	Intervention int       `json:"intervention"`
	ActionCode   int       `json:"action_code"`
}

type LeftColumnPressure struct {
	Support  int       `json:"support"`
	Time     time.Time `json:"time"`
	Pressure int       `json:"pressure"`
	Distance int       `json:"distance"`
}

type LeftColumnPressureResult struct {
	Support  int    `json:"support"`
	Time     string `json:"time"`
	Pressure int    `json:"pressure"`
	Distance int    `json:"distance"`
}

type Fault struct {
	Id      int       `json:"id" sql:"auto_increment;primary_key;"`
	Time    time.Time `json:"time"`
	Support int       `json:"support"`
	Kind    string    `json:"kind"`
}

type Pressure struct {
	Support     int       `json:"support"`
	Type1       string    `json:"type1"`
	LeftValue1  int       `json:"left_value1"`
	RightValue1 int       `json:"reight_value1"`
	Time1       time.Time `json:"time1"`
	Interval1   int       `json:"interval1"`
	Type2       string    `json:"type2"`
	LeftValue2  int       `json:"left_value2"`
	RightValue2 int       `json:"right_value2"`
	Time2       time.Time `json:"time2"`
}

type Abnormal struct {
	Support     int       `json:"support"`
	Truth       int       `json:"truth"`
	Upload      int       `json:"upload"`
	LeftValue1  int       `json:"left_value1"`
	RightValue1 int       `json:"reight_value1"`
	Time1       time.Time `json:"time1"`
	LeftValue2  int       `json:"left_value2"`
	RightValue2 int       `json:"right_value2"`
	Time2       time.Time `json:"time2"`
}

type UploadError struct {
	Id              int       `json:"id" sql:"auto_increment;primary_key;"`
	ErrorSupport    int       `json:"error_support"`
	ErrorType       string    `json:"error_type"`
	ErrorRecordTime time.Time `json:"error_record_time"`
	ErrorTime       time.Time `json:"error_time"`
	ErrorInterval   int       `json:"error_interval"`
}

type AutoAction struct {
	Id             int       `json:"id" sql:"auto_increment;primary_key;"`
	Time           time.Time `json:"time"`
	AutoActionData []byte    `json:"auto_action_data"`
}

type AutoActionTime struct {
	Id   int       `json:"id" sql:"auto_increment;primary_key;"`
	Time time.Time `json:"time"`
}

type OneAutoActionJson struct {
	AutoActionData []byte `json:"auto_action_data"`
}

type NextOneAutoActionJson struct {
	Id             int       `json:"id" sql:"auto_increment;primary_key;"`
	Time           time.Time `json:"time"`
	AutoActionData []byte    `json:"auto_action_data"`
}

type PressureFaultDiagnosis struct {
	Id             int       `json:"id" sql:"auto_increment;primary_key;"`
	Date           time.Time `json:"date"`
	Support        int       `json:"support"`
	HighAlarmTimes int       `json:"high_alarm_times"`
	LowAlarmTimes  int       `json:"low_alarm_times"`
}

type RealData struct {
	Xh        int    `json:"xh"`        //序号
	Dd        string `json:"dd"`        //地点
	Sbmc      string `json:"sbmc"`      //设备名称
	Zxzt      string `json:"zxzt"`      //在线状态
	Sfsbdmj   string `json:"sfsbdmj"`   //是否识别到煤机
	Zjdz      string `json:"zjdz"`      //支架动作
	Lzyl      int    `json:"lzyl"`      //立柱压力
	Mjwz      int    `json:"mjwz"`      //煤机位置
	Tyxc      int    `json:"tyxc"`      // 推移行程
	Dingbxzqj string `json:"dingbxzqj"` //顶板X轴倾角
	Dingbyzqj string `json:"dingbyzqj"` //顶板Y轴倾角
	Dibxzqj   string `json:"dibxzqj"`   //底板X轴倾角
	Dibyzqj   string `json:"dibyzqj"`   //底板Y轴倾角
	Sjgxsj    string `json:"sjgxsj"`    //数据更新时间
	Bjzt      string `json:"bjzt"`      //报警状态
	Bjyy      string `json:"bjyy"`      //报警原因
}

type RecordCommand struct {
	Id                     int       `json:"id" sql:"auto_increment;primary_key;"`
	Time                   time.Time `json:"time"`
	CurrentCommandSource   string    `json:"current_command_source"`    //当前命令来源
	ControlCommandDeviceId int       `json:"control_command_device_id"` //控制命令设备ID
	CommandType            string    `json:"command_type"`              //当前执行的动作
	SourceId               int       `json:"source_id"`
	IsRun                  int       `json:"is_run"`            //启停标志
	IsManual               int       `json:"is_manual"`         //是否手动干预标志
	AutoState              int       `json:"auto_state"`        //自动化状态
	AutoBreakSource        int       `json:"auto_break_source"` //中断源
	ShearerPosition        int       `json:"shearer_position"`  //煤机位置
	ShearerStep            int       `json:"shearer_step"`      //煤机工步
	TableTime              time.Time `json:"-"`                 // 新增字段
}

func (r RecordCommand) TableName() string {
	useTime := r.TableTime
	if useTime.IsZero() {
		useTime = time.Now()
	}
	//fmt.Println("--表名时间", useTime.Format("2006-01-02 15:04:05"))
	return "record_command" + useTime.Format("200601")
}

type ModbusUploadCommuction struct {
	Id        int       `json:"id" sql:"auto_increment;primary_key;"`
	Ip        string    `json:"ip"`
	Port      int       `json:"port"`
	FirstTime time.Time `json:"first_time"`
	LastTime  time.Time `json:"last_time"`
}

type WjwRecord struct {
	Time        time.Time
	SendData    string
	ReceiveData string
}
type WjwSendRecord struct {
	Time     time.Time
	SendData string
}

type FaultRecord struct {
	Id        int       `json:"id" sql:"auto_increment;primary_key;"`
	Time      time.Time `json:"time"`
	SourceId  int       `json:"source_id"`  //支架ID
	FaultType string    `json:"fault_type"` //故障类型
	TableTime string    // 新增字段
}

func (fr FaultRecord) TableName() string {
	useTime := fr.TableTime
	if useTime == "" {
		useTime = time.Now().Format("200601")
	}
	//fmt.Println("--表名时间", useTime.Format("2006-01-02 15:04:05"))
	return "fault_record" + useTime
}
