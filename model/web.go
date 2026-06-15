package model

import "time"

// WebsocketMessage websocket json 消息封装
type WebsocketMessage struct {
	Type    string      `json:"type"`
	Source  int         `json:"source"`
	Message interface{} `json:"msg"`
}

type PrivateParam struct {
	BackwashValve                  int `json:"backwash_valve"`                     //反冲洗阀
	RearPlateCylinder              int `json:"rear_plate_cylinder"`                //后插板油缸
	RearPillarCylinder             int `json:"rear_pillar_cylinder"`               //后立柱油缸
	BottomAdjustmentCylinder       int `json:"bottom_adjustment_cylinder"`         //底调油缸
	SideGuardCylinder              int `json:"side_guard_cylinder"`                //侧护板油缸
	SprayValve                     int `json:"spray_valve"`                        //喷雾阀
	BottomingCylinder              int `json:"bottoming_cylinder"`                 //起底油缸
	PushCylinder                   int `json:"push_cylinder"`                      //推移油缸
	FrontPillarCylinder            int `json:"front_pillar_cylinder"`              //前立柱油缸
	BalanceCylinder                int `json:"balance_cylinder"`                   //平衡油缸
	FrontBeamCylinder              int `json:"front_beam_cylinder"`                //前梁油缸
	ThreeStageGuardPlateCylinder   int `json:"three_stage_guard_plate_cylinder"`   //三级护帮板油缸
	SecondaryGuardPlateCylinder    int `json:"secondary_guard_plate_cylinder"`     //二级护帮板油缸
	FirstClassGuardPlateCylinder   int `json:"first_class_guard_plate_cylinder"`   //一级护帮板油缸
	AutoStraightenSensorEnable     int `json:"auto_straighten_sensor_enable"`      //自动调直传感器使能
	ShearerPositionSensorEnable    int `json:"shearer_position_sensor_enable"`     //采煤机位置传感器使能
	GuardPlateLimitSensorEnable    int `json:"guard_plate_limit_sensor_enable"`    //护帮板限位传感器使能
	TopBeamInclinationSensorEnable int `json:"top_beam_inclination_sensor_enable"` //顶梁倾角传感器使能
	TopPlateHeightSensorEnable     int `json:"top_plate_height_sensor_enable"`     //顶板高度传感器使能
	PushDisplacementSensorEnable   int `json:"push_displacement_sensor_enable"`    //推移位移传感器使能
	RPillarPressureSensorEnable    int `json:"r_pillar_pressure_sensor_enable"`    //右立柱压力传感器使能
	LPillarPressureSensorEnable    int `json:"l_pillar_pressure_sensor_enable"`    //左立柱压力传感器使能
	HeightOfAltimeterCase          int `json:"height_of_altimeter_case"`           //采高仪底座高度
	RX                             int `json:"rx"`                                 //顶板倾角传感器X轴
	RY                             int `json:"ry"`                                 //顶板倾角传感器Y轴
	FX                             int `json:"fx"`                                 //底座倾角传感器X轴
	FY                             int `json:"fy"`                                 //底座倾角传感器X轴
	Version                        int `json:"version"`                            //版本号
}

type PrivateParams struct {
	From         int `json:"from"`
	To           int `json:"to"`
	PrivateParam `json:"private_param"`
}

type PublicParam struct {
	BracingMethod                         int `json:"bracing_method"`                            //拉架方式
	ShearerDataEnable                     int `json:"shearer_data_enable"`                       //采煤机数据使能
	RemoteControlEnable                   int `json:"remote_control_enable"`                     //远控使能
	TelecontrolEnable                     int `json:"telecontrol_enable"`                        //遥控使能
	WifiEnable                            int `json:"wifi_enable"`                               //Wifi使能
	AutomaticPushEndEnable                int `json:"automatic_push_end_enable"`                 //自动推端头使能
	AutoFollowMachineEnable               int `json:"auto_follow_machine_enable"`                //自动跟机使能
	AutomaticBackwashEnable               int `json:"automatic_backwash_enable"`                 //自动反冲洗使能
	AutomaticSprayEnable                  int `json:"automatic_spray_enable"`                    //自动喷雾使能
	AutomaticGuardBoardEnable             int `json:"automatic_guardboard_enable"`               //自动护帮板使能
	AutomaticPushAndSlideEnable           int `json:"automatic_push_and_slide_enable"`           //自动推溜使能
	AutomaticRackTransferEnable           int `json:"automatic_rack_transfer_enable"`            //自动移架使能
	AutoCompensationEnable                int `json:"auto_compensation_enable"`                  //自动补压使能
	LoweringColumnLiftingBottom           int `json:"lowering_column_lifting_bottom"`            //降柱抬底使能
	SimultaneousAutomaticRackTransfer     int `json:"simultaneous_automatic_rack_transfer"`      //多架同时自动移架
	GuardPlateControl                     int `json:"guard_plate_control"`                       //护帮板控制
	SidePanelControls                     int `json:"side_panel_controls"`                       //侧护板控制
	BalanceControlEnable                  int `json:"balance_control_enable"`                    //平衡控制使能
	BottomLifterEnable                    int `json:"bottom_lifter_enable"`                      //抬底移架使能
	FrontBeamControlEnable                int `json:"front_beam_control_enable"`                 //前梁控制使能
	PressureTransferFrameEnable           int `json:"pressure_transfer_frame_enable"`            //带压移架使能
	AdjacentFrameAssistEnable             int `json:"adjacent_frame_assist_enable"`              //移架时邻架助推使能
	AdjacentRackPressureCorrelationEnable int `json:"adjacent_rack_pressure_correlation_enable"` //移架时邻架压力关联使能
	SSIDBYTE2                             int `json:"ssidbyte_2"`                                //SSID高八位
	SSIDBYTE1                             int `json:"ssidbyte_1"`                                //SSID低八位
	SupportSorting                        int `json:"support_sorting"`                           //支架排序
	TailSupportID                         int `json:"tail_support_id"`                           //尾支架ID
	FirstSupportID                        int `json:"first_support_id"`                          //首支架ID
	AutoTailSupportID                     int `json:"auto_tail_support_id"`                      //自动跟机尾支架号
	AutoFirstSupportID                    int `json:"auto_first_support_id"`                     //自动跟机首支架号
	TailTurningPoint                      int `json:"tail_turning_point"`                        //机尾折返点
	MachineHeadTurningPoint               int `json:"machine_head_turning_point"`                //机头折返点
	TailCutThroughPoint                   int `json:"tail_cut_through_point"`                    //机尾割透点
	MachineHeadCutThroughPoint            int `json:"machine_head_cut_through_point"`            //机头割透点
	SuspensionStopThreshold               int `json:"suspension_stop_threshold"`                 //补压停止阈值
	SuspensionStartThreshold              int `json:"suspension_start_threshold"`                //补压开始阈值
	RackTransferPressureSetting           int `json:"rack_transfer_pressure_setting"`            //移架压力设定值
	TransitionPressureSetting             int `json:"transition_pressure_setting"`               //过渡压力设定值
	InitialPressureSetting                int `json:"initial_pressure_setting"`                  //初撑压力设定值
	PushSlipAllowablePressure             int `json:"push_slip_allowable_pressure"`              //推溜允许压力
	MoveDistanceSettingValue              int `json:"move_distance_setting_value"`               //推移距离设定值
	PinHysteresisCompensation             int `json:"pin_Hysteresis_compensation"`               //销轴滞回补偿量

	ShiftDistanceZeroOffset               int `json:"shift_distance_zero_off_set"`                //推移距离零点偏移量
	JumpProtectionDistance                int `json:"jump_protection_distance"`                   //跳架保护距离
	FarthestControlDistance               int `json:"farthest_control_distance"`                  //最远控制距离
	GuardPlateInterval                    int `json:"guard_plate_interval"`                       //护帮板组内间隔支架数量
	GuardPlateDelay                       int `json:"guard_plate_delay"`                          //护帮板架间延迟时间
	GuardPlateGrouping                    int `json:"guard_plate_grouping"`                       //护帮板编组数量
	ColumnInterval                        int `json:"column_interval"`                            //立柱组内间隔支架数量
	ColumnDelay                           int `json:"column_delay"`                               //立柱架间延迟时间
	ColumnGrouping                        int `json:"column_grouping"`                            //前立柱升降编组数量
	TransferRackInterval                  int `json:"transfer_rack_interval"`                     //移架组内间隔支架数量
	TransferRackDelay                     int `json:"transfer_rack_delay"`                        //移架架间延迟时间
	TransferRackGrouping                  int `json:"transfer_rack_grouping"`                     //移架编组数量
	ShoveInterval                         int `json:"shove_interval"`                             //推溜组内间隔支架数量
	ShoveDelay                            int `json:"shove_delay"`                                //推溜架间延迟时间
	ShoveGrouping                         int `json:"shove_grouping"`                             //推溜编组数量
	SprayDurationGrouping                 int `json:"spray_duration_grouping"`                    //编组喷雾持续时间
	SprayGrouping                         int `json:"spray_grouping"`                             //编组喷雾数量
	StopLevel1Duration                    int `json:"stop_level_1_duration"`                      //编组收一级持续时间
	StartLevel1Duration                   int `json:"start_level_1_duration"`                     //编组伸一级持续时间
	StopLevel2Duration                    int `json:"stop_level_2_duration"`                      //编组收二级持续时间
	StartLevel2Duration                   int `json:"start_level_2_duration"`                     //编组伸二级持续时间
	StopLevel3Duration                    int `json:"stop_level_3_duration"`                      //编组收三级持续时间
	StartLevel3Duration                   int `json:"start_level_3_duration"`                     //编组伸三级持续时间
	StopFrontBeamDuration                 int `json:"stop_front_beam_duration"`                   //收前梁持续时间
	StartFrontBeamDuration                int `json:"start_front_beam_duration"`                  //伸前梁持续时间
	ColumnDropTime                        int `json:"column_drop_time"`                           //降柱时间
	RackTransferTime                      int `json:"rack_transfer_time"`                         //移架时间
	ColumnRiseTime                        int `json:"column_rise_time"`                           //升柱时间
	PushTime                              int `json:"push_time"`                                  //推溜时间
	BottomLiftingDelayTime                int `json:"bottom_lifting_delay_time"`                  //抬底延迟时间
	AutomaticBackwashCycle                int `json:"automatic_backwash_cycle"`                   //自动反冲洗周期（分钟）
	AutomaticRefillTimes                  int `json:"automatic_refill_times"`                     //自动补压次数
	AutomaticRefillInterval               int `json:"automatic_refill_interval"`                  //自动补压间隔
	AutomaticRefillCycle                  int `json:"automatic_refill_cycle"`                     //自动补压周期
	SprayDuration                         int `json:"spray_duration"`                             //喷雾持续时间
	BottomLiftDuration                    int `json:"bottom_lift_duration"`                       //抬底持续时间
	LowColumnStopBlanceDuration           int `json:"low_column_stop_blance_duration"`            //降柱时收平衡持续时间
	LowColumnStopBlanceStartTime          int `json:"low_column_stop_blance_start_time"`          //降柱时收平衡开始动作时间
	RiseColumnStartBlanceDuration         int `json:"rise_column_start_blance_duration"`          //升柱时伸平衡开始动作时间				升柱时伸平衡持续时间
	RiseColumnStartBlanceStartTime        int `json:"rise_column_start_blance_start_time"`        //升柱时伸平衡开始动作时间
	AutomaticRackTransferEarlyWarningTime int `json:"automatic_rack_transfer_early_warning_time"` //自动移架前预警时间
	SpacingDistanceBetweenStretchGuards   int `json:"spacing_distance_between_stretch_guards"`    //伸护帮板间隔距离
	SpacingDistanceBetweenStopGuards      int `json:"spacing_distance_between_stop_guards"`       //收护帮板间隔距离
	PushingIntervalDistance               int `json:"pushing_interval_distance"`                  //推溜间隔距离
	MovingIntervalDistance                int `json:"moving_interval_distance"`                   //移架间隔距离
	SprayIntervalDistance                 int `json:"spray_interval_distance"`                    //喷雾间隔距离
	AdjacentBracketCenterDistance         int `json:"adjacent_bracket_center_distance"`           //相邻支架中心距
	ShearerLength                         int `json:"shearer_length"`                             //采煤机长度
	PullBackDistance                      int `json:"pull_back_distance"`                         //拉后溜间隔距离
	PostFallColumnTime                    int `json:"post_fall_column_time"`                      //降后柱时间
	ColumnTimeAfterRise                   int `json:"column_time_after_rise"`                     //升后柱时间
	CloseBoardTime                        int `json:"close_board_time"`                           //关插板时间
	OpenBoardTime                         int `json:"open_board_time"`                            //开插板时间
	BackSlipTime                          int `json:"back_slip_time"`                             //拉后溜时间
	DataSheetCRC                          int `json:"data_sheet_crc"`                             //数据表18~68CRC
	AutoBackwashRemainingH                int `json:"auto_backwash_remaining_h"`                  //启动自动反冲洗剩余时间：小时
	AutoBackwashRemainingM                int `json:"auto_backwash_remaining_m"`                  //启动自动反冲洗剩余时间：分
	AutoRefillRemainingH                  int `json:"auto_refill_remaining_h"`                    //启动自动补压剩余时间：小时
	AutoRefillRemainingM                  int `json:"auto_refill_remaining_m"`                    //启动自动补压剩余时间：分

}

type AutoFollowStatus struct {
	IsAutoFollow                  []int `json:"is_auto_follow"`                   //是否自动跟机
	ShearerPosition               int   `json:"shearer_position"`                 //煤机位置
	CompleteAutomaticPush         []int `json:"complete_automatic_push"`          //完成自动推溜
	CompleteAutomaticRackTransfer []int `json:"complete_automatic_rack_transfer"` //完成自动移架
	CompleteAutomaticCare         []int `json:"complete_automatic_care"`          //完成自动收护帮
	CompleteAutomaticExtension    []int `json:"complete_automatic_extension"`     //完成自动伸护帮
	ShearerStep                   int   `json:"shearer_step"`                     //煤机工步
	Auto_tuiliu                   int   `json:"auto_tuiliu"`                      //自动推溜使能
	Auto_yijia                    int   `json:"auto_yijia"`                       //自动移架使能
	Auto_hubang                   int   `json:"auto_hubang"`                      //自动伸收护帮使能
	Cqlj                          []int `json:"cqlj"`                             //超前拉架显示
}

type SimulationStatus struct {
	ColumnPressureLeft             []int    `json:"column_pressure_left"`               //左立柱压力
	ColumnPressureLeftTime         []string `json:"column_pressure_left_time"`          //最新左立柱压力上传时间
	ColumnPressureLeftTimeInterval []int    `json:"column_pressure_left_time_interval"` //最新左立柱压力上传与上次上传时间间隔
	ColumnPressureRight            []int    `json:"column_pressure_right"`              //右立柱压力
	PushItinerary                  []int    `json:"push_itinerary"`                     //推移行程
	RoofHeight                     []int    `json:"roof_height"`                        //顶板高度
	RoofXAxis                      []int    `json:"roof_x_axis"`                        //支架顶板X轴倾角
	RoofYAxis                      []int    `json:"roof_y_axis"`                        //支架顶板Y轴倾角
	BaseXAxis                      []int    `json:"base_x_axis"`                        //支架底座X轴倾角
	BaseYAxis                      []int    `json:"base_y_axis"`                        //支架底座Y轴倾角
	BatteryVoltage                 []int    `json:"battery_voltage"`                    //电池电压
	Voltage12V                     []int    `json:"voltage_12v"`                        //12V电源电压
}

type FaultStatus struct {
	Credible         []int `json:"credible"`           //数据是否可信
	WifiArr          []int `json:"wifi_arr"`           //wifi状态
	CanArr           []int `json:"can_arr"`            //Can状态
	EmergencyStopArr []int `json:"emergency_stop_arr"` //急停状态
	LockArr          []int `json:"lock_arr"`           //闭锁状态
	LinArr           []int `json:"lin_arr"`            //lin总线状态
}

type Control struct {
	TargetID int `json:"target_id"` //目标控制器
	EndID    int `json:"end_id"`
	Command  int `json:"command"`  //命令码
	IsRun    int `json:"is_run"`   //1启 0停
	IsGroup  int `json:"is_group"` //0编组 1升序编组 2降序编组
}

type Shearer struct {
	Step            int    `json:"step"`              //煤机工步
	Position        int    `json:"position"`          //煤机位置
	Speed           int    `json:"speed"`             //煤机速度
	Direction       int    `json:"direction"`         //煤机方向
	LeftRollHeight  int    `json:"left_roll_height"`  //左滚筒高度
	RightRollHeight int    `json:"right_roll_height"` //右滚筒高度
	Heart           string `json:"heart"`             //心跳
	PersonEnable    int    `json:"person_enable"`     //人员接近闭锁使能
	CanLoadRate1    int    `json:"can_load_rate1"`    //can1负载率
	CanLoadRate2    int    `json:"can_load_rate2"`    //can2负载率
	Sort            int    `json:"sort"`              //正反工作面
	AutoStatus      int    `json:"auto_status"`       //支架自动化状态
	AutoEnable      int    `json:"auto_enable"`       //支架是否全自动使能

}

type LockStatus struct {
	KeyStatus int `json:"key_status"` //按键状态
	Status    int `json:"status"`     //显示状态
}

type ActionStatus struct {
	ID     int    `json:"id"`     //按键状态
	Action string `json:"action"` //执行动作
}

type QueryTableParams struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Support   int    `json:"support"`
}

type QueryAutoTableParams struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

type QueryHeatMapParams struct {
	Days int `json:"days"`
}

type QueryPressureFaultDiagnosisParams struct {
	Days int `json:"days"`
}

type QueryOneAutoTableParams struct {
	Id int `json:"id"`
}

type QueryFault struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

type ReturnNext struct {
	ID     int              `json:"id"`
	Time   time.Time        `json:"time"`
	Action AutoFollowStatus `json:"action"`
}

type QueryRecordCommand struct {
	StartTime              string `json:"start_time"`
	EndTime                string `json:"end_time"`
	SourceId               int    `json:"source_id"`
	CommandType            string `json:"command_type"`
	ControlCommandDeviceId int    `json:"control_command_device_id"`
	CurrentCommandSource   string `json:"current_command_source"`
	Page                   int    `json:"page"`
	PageSize               int    `json:"page_size"`
}
