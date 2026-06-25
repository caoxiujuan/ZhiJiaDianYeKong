package controller

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gocode/model"
	service "gocode/services"
	"gocode/services/mysql"
	mysqlService "gocode/services/mysql"
	udpService "gocode/services/udpserver"
	"gocode/utils"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
	"gorm.io/gorm"
)

// 私有参数查询
func PrivateQuery(c *gin.Context) {
	targetID := c.DefaultQuery("targetid", "1")
	id, _ := strconv.Atoi(targetID)
	udpService.CANRequestPrivateStaticData(id)
	c.JSON(http.StatusOK, gin.H{"code": 0})
}

// 公共参数查询
func PublicQuery(c *gin.Context) {
	udpService.CANRequestPublicData()
	c.JSON(http.StatusOK, gin.H{"code": 0})
}

// 私有参数修改
func PrivateModify(c *gin.Context) {
	clientUuid := c.GetHeader("uuid")
	var temp model.LoginParams
	result := mysqlService.Mysqlclient.Debug().Model(&model.LoginParams{}).Where("uuid=?", clientUuid).Find(&temp)
	if result.RowsAffected == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 1})
		return
	}
	var privateParams model.PrivateParams
	if err := c.ShouldBindJSON(&privateParams); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	from := privateParams.From
	to := privateParams.To
	for i := from; i <= to; i++ {
		param13 := (uint16(privateParams.PrivateParam.BackwashValve) << 13) |
			(uint16(privateParams.PrivateParam.RearPlateCylinder) << 12) |
			(uint16(privateParams.PrivateParam.RearPillarCylinder) << 11) |
			(uint16(privateParams.PrivateParam.BottomAdjustmentCylinder) << 10) |
			(uint16(privateParams.PrivateParam.SideGuardCylinder) << 9) |
			(uint16(privateParams.PrivateParam.SprayValve) << 8) |
			(uint16(privateParams.PrivateParam.BottomingCylinder) << 7) |
			(uint16(privateParams.PrivateParam.PushCylinder) << 6) |
			(uint16(privateParams.PrivateParam.FrontPillarCylinder) << 5) |
			(uint16(privateParams.PrivateParam.BalanceCylinder) << 4) |
			(uint16(privateParams.PrivateParam.FrontBeamCylinder) << 3) |
			(uint16(privateParams.PrivateParam.ThreeStageGuardPlateCylinder) << 2) |
			(uint16(privateParams.PrivateParam.SecondaryGuardPlateCylinder) << 1) |
			uint16(privateParams.PrivateParam.FirstClassGuardPlateCylinder)
		param14 := (uint16(privateParams.PrivateParam.AutoStraightenSensorEnable) << 7) |
			(uint16(privateParams.PrivateParam.ShearerPositionSensorEnable) << 6) |
			(uint16(privateParams.PrivateParam.GuardPlateLimitSensorEnable) << 5) |
			(uint16(privateParams.PrivateParam.TopBeamInclinationSensorEnable) << 4) |
			(uint16(privateParams.PrivateParam.TopPlateHeightSensorEnable) << 3) |
			(uint16(privateParams.PrivateParam.PushDisplacementSensorEnable) << 2) |
			(uint16(privateParams.PrivateParam.RPillarPressureSensorEnable) << 1) |
			uint16(privateParams.PrivateParam.LPillarPressureSensorEnable)
		udpService.CANConfigurationPrivateParameterWeb(i, param13, param14)
	}
	c.JSON(http.StatusOK, gin.H{"code": 0})
}

func PublicModify(c *gin.Context) {
	clientUuid := c.GetHeader("uuid")
	var temp model.LoginParams
	result := mysqlService.Mysqlclient.Debug().Model(&model.LoginParams{}).Where("uuid=?", clientUuid).Find(&temp)
	if result.RowsAffected == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 1})
		return
	}
	var publicParam model.PublicParam
	if err := c.ShouldBindJSON(&publicParam); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	param15 := (uint16(publicParam.ShearerDataEnable) << 11) |
		(uint16(publicParam.RemoteControlEnable) << 10) |
		(uint16(publicParam.TelecontrolEnable) << 9) |
		(uint16(publicParam.WifiEnable) << 8) |
		(uint16(publicParam.AutomaticPushEndEnable) << 7) |
		(uint16(publicParam.AutoFollowMachineEnable) << 6) |
		(uint16(publicParam.AutomaticBackwashEnable) << 5) |
		(uint16(publicParam.AutomaticSprayEnable) << 4) |
		(uint16(publicParam.AutomaticGuardBoardEnable) << 3) |
		(uint16(publicParam.AutomaticPushAndSlideEnable) << 2) |
		(uint16(publicParam.AutomaticRackTransferEnable) << 1) |
		uint16(publicParam.AutoCompensationEnable)
	if udpService.Mb.HoldingRegisters[15] != param15 {
		udpService.CANConfigurationPublicParameter(int64(15), param15)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param16 :=
		(uint16(publicParam.LoweringColumnLiftingBottom) << 9) |
			(uint16(publicParam.SimultaneousAutomaticRackTransfer) << 8) |
			(uint16(publicParam.GuardPlateControl) << 7) |
			(uint16(publicParam.SidePanelControls) << 6) |
			(uint16(publicParam.BalanceControlEnable) << 5) |
			(uint16(publicParam.BottomLifterEnable) << 4) |
			(uint16(publicParam.FrontBeamControlEnable) << 3) |
			(uint16(publicParam.PressureTransferFrameEnable) << 2) |
			(uint16(publicParam.AdjacentFrameAssistEnable) << 1) |
			uint16(publicParam.AdjacentRackPressureCorrelationEnable)
	if udpService.Mb.HoldingRegisters[16] != param16 {
		udpService.CANConfigurationPublicParameter(int64(16), param16)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param17 := (uint16(publicParam.SSIDBYTE2) << 8) | uint16(publicParam.SSIDBYTE1)&0x00ff
	if udpService.Mb.HoldingRegisters[17] != param17 {
		udpService.CANConfigurationPublicParameter(int64(17), param17)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param18 := uint16(publicParam.SupportSorting) & 0x0001
	if udpService.Mb.HoldingRegisters[18] != param18 {
		udpService.CANConfigurationPublicParameter(int64(18), param18)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	udpService.Mb.HoldingRegisters[18] = param18
	utils.SupportSort = int(param18)
	param19 := (uint16(publicParam.TailSupportID) << 8) | uint16(publicParam.FirstSupportID)&0x00ff
	if udpService.Mb.HoldingRegisters[19] != param19 {
		udpService.CANConfigurationPublicParameter(int64(19), param19)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param20 := (uint16(publicParam.AutoTailSupportID) << 8) | uint16(publicParam.AutoFirstSupportID)&0x00ff
	if udpService.Mb.HoldingRegisters[20] != param20 {
		udpService.CANConfigurationPublicParameter(int64(20), param20)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param21 := (uint16(publicParam.TailTurningPoint) << 8) | uint16(publicParam.MachineHeadTurningPoint)&0x00ff
	if udpService.Mb.HoldingRegisters[21] != param21 {
		udpService.CANConfigurationPublicParameter(int64(21), param21)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param24 := (uint16(publicParam.TailCutThroughPoint) << 8) | uint16(publicParam.MachineHeadCutThroughPoint)&0x00ff
	if udpService.Mb.HoldingRegisters[24] != param24 {
		udpService.CANConfigurationPublicParameter(int64(24), param24)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param25 := uint16(publicParam.SuspensionStartThreshold)
	if udpService.Mb.HoldingRegisters[25] != param25 {
		udpService.CANConfigurationPublicParameter(int64(25), param25)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param26 := uint16(publicParam.SuspensionStopThreshold)
	if udpService.Mb.HoldingRegisters[26] != param26 {
		udpService.CANConfigurationPublicParameter(int64(26), param26)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param27 := uint16(publicParam.RackTransferPressureSetting)
	if udpService.Mb.HoldingRegisters[27] != param27 {
		udpService.CANConfigurationPublicParameter(int64(27), param27)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param28 := uint16(publicParam.TransitionPressureSetting)
	if udpService.Mb.HoldingRegisters[28] != param28 {
		udpService.CANConfigurationPublicParameter(int64(28), param28)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param29 := uint16(publicParam.InitialPressureSetting)
	if udpService.Mb.HoldingRegisters[29] != param29 {
		udpService.CANConfigurationPublicParameter(int64(29), param29)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	param31 := uint16(publicParam.PushSlipAllowablePressure)
	if udpService.Mb.HoldingRegisters[31] != param31 {
		udpService.CANConfigurationPublicParameter(int64(31), param31)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param32 := uint16(publicParam.MoveDistanceSettingValue)
	if udpService.Mb.HoldingRegisters[32] != param32 {
		udpService.CANConfigurationPublicParameter(int64(32), param32)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param33 := uint16(publicParam.PinHysteresisCompensation) & 0x00ff
	if udpService.Mb.HoldingRegisters[33] != param33 {
		udpService.CANConfigurationPublicParameter(int64(33), param33)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	// param34 := uint16(publicParam.HeightOfAltimeterCase)
	// if udpService.Mb.HoldingRegisters[34] != param34 {
	// 	udpService.CANConfigurationPublicParameter(int64(34), param34)
	// 	time.Sleep(time.Duration(10) * time.Millisecond)
	// }
	param35 := uint16(publicParam.ShiftDistanceZeroOffset)
	if udpService.Mb.HoldingRegisters[35] != param35 {
		udpService.CANConfigurationPublicParameter(int64(35), param35)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param36 := (uint16(publicParam.JumpProtectionDistance) << 8) |
		uint16(publicParam.FarthestControlDistance)
	if udpService.Mb.HoldingRegisters[36] != param36 {
		udpService.CANConfigurationPublicParameter(int64(36), param36)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param38 := (uint16(publicParam.GuardPlateInterval) << 12) |
		(uint16(publicParam.GuardPlateDelay) << 8) |
		uint16(publicParam.GuardPlateGrouping)
	if udpService.Mb.HoldingRegisters[38] != param38 {
		udpService.CANConfigurationPublicParameter(int64(38), param38)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param39 := (uint16(publicParam.ColumnInterval) << 12) |
		(uint16(publicParam.ColumnDelay) << 8) |
		uint16(publicParam.ColumnGrouping)
	if udpService.Mb.HoldingRegisters[39] != param39 {
		udpService.CANConfigurationPublicParameter(int64(39), param39)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param40 := (uint16(publicParam.TransferRackInterval) << 12) |
		(uint16(publicParam.TransferRackDelay) << 8) |
		uint16(publicParam.TransferRackGrouping)
	if udpService.Mb.HoldingRegisters[40] != param40 {
		udpService.CANConfigurationPublicParameter(int64(40), param40)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param41 := (uint16(publicParam.ShoveInterval) << 12) |
		(uint16(publicParam.ShoveDelay) << 8) |
		uint16(publicParam.ShoveGrouping)
	if udpService.Mb.HoldingRegisters[41] != param41 {
		udpService.CANConfigurationPublicParameter(int64(41), param41)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param42 := (uint16(publicParam.SprayDurationGrouping) << 8) | uint16(publicParam.SprayGrouping)&0x00ff
	if udpService.Mb.HoldingRegisters[42] != param42 {
		udpService.CANConfigurationPublicParameter(int64(42), param42)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param43 := (uint16(publicParam.StopLevel1Duration) << 8) | uint16(publicParam.StartLevel1Duration)&0x00ff
	if udpService.Mb.HoldingRegisters[43] != param43 {
		udpService.CANConfigurationPublicParameter(int64(43), param43)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param44 := (uint16(publicParam.StopLevel2Duration) << 8) | uint16(publicParam.StartLevel2Duration)&0x00ff
	if udpService.Mb.HoldingRegisters[44] != param44 {
		udpService.CANConfigurationPublicParameter(int64(44), param44)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param45 := (uint16(publicParam.StopLevel3Duration) << 8) | uint16(publicParam.StartLevel3Duration)&0x00ff
	if udpService.Mb.HoldingRegisters[45] != param45 {
		udpService.CANConfigurationPublicParameter(int64(45), param45)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param46 := (uint16(publicParam.StopFrontBeamDuration) << 8) | uint16(publicParam.StartFrontBeamDuration)&0x00ff
	if udpService.Mb.HoldingRegisters[46] != param46 {
		udpService.CANConfigurationPublicParameter(int64(46), param46)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param47 := (uint16(publicParam.ColumnRiseTime) << 8) | uint16(publicParam.ColumnDropTime)&0x00ff
	if udpService.Mb.HoldingRegisters[47] != param47 {
		udpService.CANConfigurationPublicParameter(int64(47), param47)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param48 := (uint16(publicParam.PushTime) << 8) | uint16(publicParam.RackTransferTime)&0x00ff
	if udpService.Mb.HoldingRegisters[48] != param48 {
		udpService.CANConfigurationPublicParameter(int64(48), param48)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	param50 := uint16(publicParam.AutomaticBackwashCycle)
	if udpService.Mb.HoldingRegisters[50] != param50 {
		udpService.CANConfigurationPublicParameter(int64(50), param50)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param51 := (uint16(publicParam.AutomaticRefillTimes) << 13) |
		(uint16(publicParam.AutomaticRefillInterval) << 8) |
		uint16(publicParam.AutomaticRefillCycle)
	if udpService.Mb.HoldingRegisters[51] != param51 {
		udpService.CANConfigurationPublicParameter(int64(51), param51)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param52 := (uint16(publicParam.SprayDuration) << 8) | uint16(publicParam.BottomLiftDuration)&0x00ff
	if udpService.Mb.HoldingRegisters[52] != param52 {
		udpService.CANConfigurationPublicParameter(int64(52), param52)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param53 := (uint16(publicParam.LowColumnStopBlanceDuration) << 12) |
		(uint16(publicParam.LowColumnStopBlanceStartTime) << 8) |
		(uint16(publicParam.RiseColumnStartBlanceDuration) << 4) |
		uint16(publicParam.RiseColumnStartBlanceStartTime)
	if udpService.Mb.HoldingRegisters[53] != param53 {
		udpService.CANConfigurationPublicParameter(int64(53), param53)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param54 := uint16(publicParam.AutomaticRackTransferEarlyWarningTime) << 8
	if udpService.Mb.HoldingRegisters[54] != param54 {
		udpService.CANConfigurationPublicParameter(int64(54), param54)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param55 := (uint16(publicParam.SpacingDistanceBetweenStretchGuards) << 8) | uint16(publicParam.SpacingDistanceBetweenStopGuards)&0x00ff
	if udpService.Mb.HoldingRegisters[55] != param55 {
		udpService.CANConfigurationPublicParameter(int64(55), param55)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param57 := (uint16(publicParam.PushingIntervalDistance) << 8) | uint16(publicParam.MovingIntervalDistance)&0x00ff
	if udpService.Mb.HoldingRegisters[57] != param57 {
		udpService.CANConfigurationPublicParameter(int64(57), param57)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param58 := uint16(publicParam.SprayIntervalDistance) & 0x00ff
	if udpService.Mb.HoldingRegisters[58] != param58 {
		udpService.CANConfigurationPublicParameter(int64(58), param58)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param59 := (uint16(publicParam.AdjacentBracketCenterDistance) << 8) | uint16(publicParam.ShearerLength)&0x00ff
	if udpService.Mb.HoldingRegisters[59] != param59 {
		udpService.CANConfigurationPublicParameter(int64(59), param59)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param60 := uint16(publicParam.PullBackDistance) & 0x00ff
	if udpService.Mb.HoldingRegisters[60] != param60 {
		udpService.CANConfigurationPublicParameter(int64(60), param60)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param61 := uint16(publicParam.PostFallColumnTime) & 0x00ff
	if udpService.Mb.HoldingRegisters[61] != param61 {
		udpService.CANConfigurationPublicParameter(int64(61), param61)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param62 := uint16(publicParam.ColumnTimeAfterRise) & 0x00ff
	if udpService.Mb.HoldingRegisters[62] != param62 {
		udpService.CANConfigurationPublicParameter(int64(62), param62)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param63 := (uint16(publicParam.CloseBoardTime) << 8) | uint16(publicParam.OpenBoardTime)&0x00ff
	if udpService.Mb.HoldingRegisters[63] != param63 {
		udpService.CANConfigurationPublicParameter(int64(63), param63)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param64 := uint16(publicParam.BackSlipTime) & 0x00ff
	if udpService.Mb.HoldingRegisters[64] != param64 {
		udpService.CANConfigurationPublicParameter(int64(64), param64)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param69 := uint16(publicParam.DataSheetCRC) & 0x00ff
	if udpService.Mb.HoldingRegisters[69] != param69 {
		udpService.CANConfigurationPublicParameter(int64(69), param69)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param72 := (uint16(publicParam.AutoBackwashRemainingH) << 8) | uint16(publicParam.AutoBackwashRemainingM)&0x00ff
	if udpService.Mb.HoldingRegisters[72] != param72 {
		udpService.CANConfigurationPublicParameter(int64(72), param72)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	param73 := (uint16(publicParam.AutoRefillRemainingH) << 8) | uint16(publicParam.AutoRefillRemainingM)&0x00ff
	if udpService.Mb.HoldingRegisters[73] != param73 {
		udpService.CANConfigurationPublicParameter(int64(73), param73)
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	c.JSON(http.StatusOK, gin.H{"code": 0})
}

func Control(c *gin.Context) {
	clientUuid := c.GetHeader("uuid")
	var temp model.LoginParams
	result := mysqlService.Mysqlclient.Debug().Model(&model.LoginParams{}).Where("uuid=?", clientUuid).Find(&temp)
	if result.RowsAffected == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 1})
		return
	}
	var control model.Control
	if err := c.ShouldBindJSON(&control); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	go udpService.ControlWeb(control.TargetID, control.EndID, control.Command, control.IsRun)
	c.JSON(http.StatusOK, gin.H{"code": 0})
}
func Address(c *gin.Context) {
	clientUuid := c.GetHeader("uuid")
	var temp model.LoginParams
	result := mysqlService.Mysqlclient.Debug().Model(&model.LoginParams{}).Where("uuid=?", clientUuid).Find(&temp)
	if result.RowsAffected == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 1})
		return
	}
	targetID := c.DefaultQuery("targetid", "1")
	id, _ := strconv.Atoi(targetID)
	udpService.ConfigureAddress(id)
	c.JSON(http.StatusOK, gin.H{"code": 0})
}

func Lock(c *gin.Context) {
	targetID := c.DefaultQuery("targetid", "1")
	id, _ := strconv.Atoi(targetID)
	clientUuid := c.GetHeader("uuid")
	var temp model.LoginParams
	result := mysqlService.Mysqlclient.Debug().Model(&model.LoginParams{}).Where("uuid=?", clientUuid).Find(&temp)
	if result.RowsAffected == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 2})
		return
	}
	if udpService.Mb.HoldingRegisters[8000+int(id-1)]>>7&0x0001 == 1 || udpService.Mb.HoldingRegisters[8000+int(id-1)]>>6&0x0001 == 1 {
		c.JSON(http.StatusOK, gin.H{"code": 1})
	} else {
		udpService.BSOpen(byte(id))
		c.JSON(http.StatusOK, gin.H{"code": 0})
	}
}
func UnLock(c *gin.Context) {
	clientUuid := c.GetHeader("uuid")
	var temp model.LoginParams
	result := mysqlService.Mysqlclient.Debug().Model(&model.LoginParams{}).Where("uuid=?", clientUuid).Find(&temp)
	if result.RowsAffected == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 1})
		return
	}
	targetID := c.DefaultQuery("targetid", "1")
	id, _ := strconv.Atoi(targetID)
	udpService.BSClose(byte(id))
	c.JSON(http.StatusOK, gin.H{"code": 0})
}

func QueryLock(c *gin.Context) {
	targetID := c.DefaultQuery("targetid", "1")
	id, _ := strconv.Atoi(targetID)
	var lockStatus model.LockStatus
	lockStatus.KeyStatus = int(udpService.Mb.HoldingRegisters[4000+int(id-1)])
	lockStatus.Status = int(udpService.Mb.HoldingRegisters[4220+int(id-1)])
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": lockStatus})
}

func Setshearer(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0})
}

func Login(c *gin.Context) {
	var loginParams model.LoginParams
	if err := c.ShouldBindJSON(&loginParams); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	var sql_loginParams model.LoginParams
	result := mysqlService.Mysqlclient.Debug().Model(&model.LoginParams{}).Where("username=?", loginParams.Username).Find(&sql_loginParams)
	if result.RowsAffected < 1 {
		c.JSON(http.StatusOK, gin.H{"code": 1})
		return
	} else if sql_loginParams.Password != loginParams.Password {
		c.JSON(http.StatusOK, gin.H{"code": 1})
		return
	} else {
		uuid := uuid.NewV4().String()
		mysqlService.Mysqlclient.Debug().Model(&model.LoginParams{}).Where("username=?", loginParams.Username).Update("uuid", uuid)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": uuid})
	}
}

func TableQuery(c *gin.Context) {
	var queryTableParams model.QueryTableParams
	if err := c.ShouldBindJSON(&queryTableParams); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	fmt.Println("左支架压力查询", queryTableParams)
	var count int64
	mysqlService.Mysqlclient.Model(&model.LeftColumnPressure{}).Where("time BETWEEN ? AND ? AND support=?", queryTableParams.StartTime, queryTableParams.EndTime, queryTableParams.Support).Count(&count)
	fmt.Println(queryTableParams, count)
	if count > 0 {
		var leftColumnPressure []model.LeftColumnPressure
		//var LeftColumnPressureResult []model.LeftColumnPressureResult
		mysqlService.Mysqlclient.Model(&model.LeftColumnPressure{}).Where("time BETWEEN ? AND ? AND support=?", queryTableParams.StartTime, queryTableParams.EndTime, queryTableParams.Support).Find(&leftColumnPressure)
		//  for i := 0; i < len(leftColumnPressure); i++ {
		//  	var temp model.LeftColumnPressureResult
		// // 	temp.Support = leftColumnPressure[i].Support
		// 	temp.Time = leftColumnPressure[i].Time.Format("2006-01-02 15:04:05")
		// // 	temp.Pressure = leftColumnPressure[i].Pressure
		// // 	temp.Distance = leftColumnPressure[i].Distance
		// // 	LeftColumnPressureResult = append(LeftColumnPressureResult, temp)
		// }
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": leftColumnPressure})
		//fmt.Println("LeftColumnPressureResult",LeftColumnPressureResult)
	} else {
		c.JSON(http.StatusOK, gin.H{"code": 1})
	}
}

func FaultQuery(c *gin.Context) {
	var queryFault model.QueryFault
	if err := c.ShouldBindJSON(&queryFault); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}

	var count int64
	mysqlService.Mysqlclient.Model(&model.Fault{}).Where("time BETWEEN ? AND ?", queryFault.StartTime, queryFault.EndTime).Count(&count)
	if count > 0 {
		var fault []model.Fault
		mysqlService.Mysqlclient.Model(&model.Fault{}).Where("time BETWEEN ? AND ?", queryFault.StartTime, queryFault.EndTime).Find(&fault)

		c.JSON(http.StatusOK, gin.H{"code": 0, "message": fault})
	} else {
		c.JSON(http.StatusOK, gin.H{"code": 1})
	}
}
func SupportNumQuery(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": utils.Conf.SYSTEM.SupportNum})
}

// 查询所有支架程序版本号
func SupportVersionQuery(c *gin.Context) {
	versions := make([]int, 0, utils.Conf.SYSTEM.SupportNum)
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		versions = append(versions, int(udpService.Mb.HoldingRegisters[7600+i]))
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": versions})
}

func SupportPositionQuery(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code1": 0, "message1": int(udpService.Mb.HoldingRegisters[180])})
}

func PersonEnable(c *gin.Context) {
	value := c.DefaultQuery("value", "1")

	fmt.Println("开关的值为", value)
	myvalue, err := strconv.Atoi(value)
	if err == nil {
		udpService.Mb.HoldingRegisters[176] = uint16(myvalue)
	}
	var shearer model.Shearer
	shearer.Step = int(udpService.Mb.HoldingRegisters[179])
	shearer.Position = int(udpService.Mb.HoldingRegisters[180])
	shearer.Speed = int(udpService.Mb.HoldingRegisters[181])
	shearer.Direction = int(udpService.Mb.HoldingRegisters[182])
	shearer.LeftRollHeight = int(udpService.Mb.HoldingRegisters[183])
	shearer.RightRollHeight = int(udpService.Mb.HoldingRegisters[184])
	shearer.Heart = udpService.HeartString
	shearer.PersonEnable = int(udpService.Mb.HoldingRegisters[176])
	WebsocketMessage := model.WebsocketMessage{
		Type:    "shearer",
		Source:  0,
		Message: shearer,
	}
	strings, _ := json.Marshal(WebsocketMessage)
	service.WebsocketManager.SendAll(strings)

}

func AutoEnable(c *gin.Context) {
	//柳塔借用为
	value := c.DefaultQuery("value", "1")
	fmt.Println("支架自动化开关的值为", value)
	myvalue, err := strconv.Atoi(value)
	if err == nil {
		udpService.Mb.HoldingRegisters[193] = uint16(myvalue)
		// if udpService.Mb.HoldingRegisters[187] == 1 {
		// 	udpService.Mb.HoldingRegisters[177] = 1
		// }
	}
	var shearer model.Shearer
	shearer.Step = int(udpService.Mb.HoldingRegisters[179])
	shearer.Position = int(udpService.Mb.HoldingRegisters[180])
	shearer.Speed = int(udpService.Mb.HoldingRegisters[181])
	shearer.Direction = int(udpService.Mb.HoldingRegisters[182])
	shearer.LeftRollHeight = int(udpService.Mb.HoldingRegisters[183])
	shearer.RightRollHeight = int(udpService.Mb.HoldingRegisters[184])
	shearer.Heart = udpService.HeartString
	shearer.PersonEnable = int(udpService.Mb.HoldingRegisters[176])
	shearer.CanLoadRate1 = int(udpService.Mb.HoldingRegisters[185])
	shearer.CanLoadRate2 = int(udpService.Mb.HoldingRegisters[186])
	shearer.Sort = utils.Conf.MODBUSSHEARER.Sort
	shearer.AutoStatus = int(udpService.Mb.HoldingRegisters[177])
	shearer.AutoEnable = int(udpService.Mb.HoldingRegisters[193])
	WebsocketMessage := model.WebsocketMessage{
		Type:    "shearer",
		Source:  0,
		Message: shearer,
	}
	strings, _ := json.Marshal(WebsocketMessage)
	service.WebsocketManager.SendAll(strings)
}

func AutoDataQuery(c *gin.Context) {
	var queryAutoTableParams model.QueryAutoTableParams
	var count int64
	if err := c.ShouldBindJSON(&queryAutoTableParams); err != nil {
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	//fmt.Println("自动跟机查询", queryAutoTableParams.StartTime, queryAutoTableParams.EndTime)
	mysqlService.Mysqlclient.Model(&model.AutoAction{}).Where("time BETWEEN ? AND ?", queryAutoTableParams.StartTime, queryAutoTableParams.EndTime).Count(&count)
	if count > 0 {
		var autoActionTime []model.AutoActionTime
		mysqlService.Mysqlclient.Model(&model.AutoAction{}).Where("time BETWEEN ? AND ?", queryAutoTableParams.StartTime, queryAutoTableParams.EndTime).Find(&autoActionTime)
		//fmt.Println(autoActionTime)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": autoActionTime})
		return
	} else {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}
}

func OneAutoDataQuery(c *gin.Context) {
	var queryOneAutoTableParams model.QueryOneAutoTableParams
	if err := c.ShouldBindJSON(&queryOneAutoTableParams); err != nil {
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	var oneAutoActionJson model.OneAutoActionJson

	mysqlService.Mysqlclient.Model(&model.AutoAction{}).Where("id = ?", queryOneAutoTableParams.Id).Find(&oneAutoActionJson)

	var autoFollowStatus model.AutoFollowStatus
	json.Unmarshal([]byte(oneAutoActionJson.AutoActionData), &autoFollowStatus)

	if len(autoFollowStatus.CompleteAutomaticCare) > 0 {
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": autoFollowStatus})
		return
	} else {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}
}

func NextOneAutoDataQuery(c *gin.Context) {
	var queryOneAutoTableParams model.QueryOneAutoTableParams
	if err := c.ShouldBindJSON(&queryOneAutoTableParams); err != nil {
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}

	var nextOneAutoActionJson model.NextOneAutoActionJson
	mysqlService.Mysqlclient.Model(&model.AutoAction{}).Last(&nextOneAutoActionJson)

	fmt.Println("最后一条数据", nextOneAutoActionJson.Id)
	for {
		if queryOneAutoTableParams.Id+1 > nextOneAutoActionJson.Id {
			c.JSON(http.StatusOK, gin.H{"code": 2, "message": "已经是最后一条数据了"})
			return
		} else {
			var nextOneAutoActionJson model.NextOneAutoActionJson
			mysqlService.Mysqlclient.Model(&model.AutoAction{}).Where("id = ?", queryOneAutoTableParams.Id+1).Find(&nextOneAutoActionJson)
			// fmt.Println("比较字符串")
			// fmt.Println(string(oneAutoActionJson.AutoActionData))
			// fmt.Println( string(nextOneAutoActionJson.AutoActionData))
			// fmt.Println( string(oneAutoActionJson.AutoActionData) == string(nextOneAutoActionJson.AutoActionData) )
			var autoFollowStatus model.AutoFollowStatus
			json.Unmarshal([]byte(nextOneAutoActionJson.AutoActionData), &autoFollowStatus)
			var returnNext model.ReturnNext
			returnNext.ID = nextOneAutoActionJson.Id
			returnNext.Time = nextOneAutoActionJson.Time
			returnNext.Action = autoFollowStatus
			c.JSON(http.StatusOK, gin.H{"code": 0, "message": returnNext})
			return

		}
	}
}

func LastOneAutoDataQuery(c *gin.Context) {
	var queryOneAutoTableParams model.QueryOneAutoTableParams
	if err := c.ShouldBindJSON(&queryOneAutoTableParams); err != nil {
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	var nextOneAutoActionJson model.NextOneAutoActionJson
	mysqlService.Mysqlclient.Model(&model.AutoAction{}).First(&nextOneAutoActionJson)

	fmt.Println("第一条数据", nextOneAutoActionJson.Id)
	for {
		if queryOneAutoTableParams.Id-1 < nextOneAutoActionJson.Id {
			c.JSON(http.StatusOK, gin.H{"code": 2, "message": "已经是第一条数据了"})
			return
		} else {
			var nextOneAutoActionJson model.NextOneAutoActionJson
			mysqlService.Mysqlclient.Model(&model.AutoAction{}).Where("id = ?", queryOneAutoTableParams.Id-1).Find(&nextOneAutoActionJson)
			// fmt.Println("比较字符串")
			// fmt.Println(string(oneAutoActionJson.AutoActionData))
			// fmt.Println( string(nextOneAutoActionJson.AutoActionData))
			// fmt.Println( string(oneAutoActionJson.AutoActionData) == string(nextOneAutoActionJson.AutoActionData) )
			var autoFollowStatus model.AutoFollowStatus
			json.Unmarshal([]byte(nextOneAutoActionJson.AutoActionData), &autoFollowStatus)
			var returnNext model.ReturnNext
			returnNext.ID = nextOneAutoActionJson.Id
			returnNext.Time = nextOneAutoActionJson.Time
			returnNext.Action = autoFollowStatus
			c.JSON(http.StatusOK, gin.H{"code": 0, "message": returnNext})
			return

		}
	}
}

func GetHeatMap(c *gin.Context) {

	var queryHeatMapParams model.QueryHeatMapParams
	if err := c.ShouldBindJSON(&queryHeatMapParams); err != nil {
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}

	var count int64
	mysqlService.Mysqlclient.Model(&model.LeftColumnPressure{}).Where("time > ? AND support<181", time.Now().AddDate(0, 0, -queryHeatMapParams.Days).Format("2006-01-02")).Count(&count)
	fmt.Println("获取热力图数据", queryHeatMapParams, count)
	if count > 0 {
		var heatMapdatas [][]int
		var heatMap []model.LeftColumnPressure
		mysqlService.Mysqlclient.Model(&model.LeftColumnPressure{}).Where("time > ? AND support<181", time.Now().AddDate(0, 0, -queryHeatMapParams.Days).Format("2006-01-02")).Order("time DESC").Limit(220 * utils.Conf.SYSTEM.SupportNum).Find(&heatMap)
		fmt.Println(len(heatMap), len(heatMap)%utils.Conf.SYSTEM.SupportNum)
		if len(heatMap)%utils.Conf.SYSTEM.SupportNum == 0 {
			for i := 0; i < len(heatMap); i++ {
				var a []int
				a = append(a, i%utils.Conf.SYSTEM.SupportNum)
				a = append(a, i/utils.Conf.SYSTEM.SupportNum)
				a = append(a, heatMap[i].Pressure)
				heatMapdatas = append(heatMapdatas, a)
			}
			// for i := 0; i < len(heatMap); i++ {
			// 	heatMapdatas[i] = append(heatMapdatas[i],heatMap[i].Pressure)
			// }
			fmt.Println("数据可以被180整除", len(heatMapdatas), len(heatMapdatas[0]))
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": heatMapdatas})
		return
	} else {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}
}

func GetPressureFaultDiagnosis(c *gin.Context) {
	var queryPressureFaultDiagnosisParams model.QueryPressureFaultDiagnosisParams
	if err := c.ShouldBindJSON(&queryPressureFaultDiagnosisParams); err != nil {
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	fmt.Println(time.Now().AddDate(0, 0, -queryPressureFaultDiagnosisParams.Days).Format("2006-01-02"))
	var count int64
	mysqlService.Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().AddDate(0, 0, -queryPressureFaultDiagnosisParams.Days).Format("2006-01-02"), int(1)).Count(&count)
	if count > 0 {
		var pressureFaultDiagnosis []model.PressureFaultDiagnosis
		for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
			var pressureFaultDiagnosisOne model.PressureFaultDiagnosis
			mysqlService.Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().AddDate(0, 0, -queryPressureFaultDiagnosisParams.Days).Format("2006-01-02"), int(i+1)).Find(&pressureFaultDiagnosisOne)

			pressureFaultDiagnosis = append(pressureFaultDiagnosis, pressureFaultDiagnosisOne)
		}
		fmt.Println(pressureFaultDiagnosis)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": pressureFaultDiagnosis})
	} else {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}
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

var StartFlag bool

type Sendnterval struct {
	Sendinterval int
	IsFan        bool
}

func SendTestData(c *gin.Context) {
	var sendInterval Sendnterval
	if err := c.ShouldBindJSON(&sendInterval); err != nil {
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	fmt.Println("发送间隔", sendInterval)
	udpService.IsQF = sendInterval.IsFan
	StartFlag = true

	for {
		if !StartFlag {
			break
		} else {
			udpService.DataLocker.Lock()
			const MaxPendingPackets = 100 // 建议定义为全局常量
			if len(udpService.SendTestData) > MaxPendingPackets {
				// 计算要丢弃的数量
				removeCount := 10
				if len(udpService.SendTestData) < removeCount {
					removeCount = len(udpService.SendTestData)
				}

				udpService.SendTestData = udpService.SendTestData[removeCount:]

				fmt.Printf("警告：队列积压过多，强制清理了 %d 个旧包\n", removeCount)

				// LostPacketCount += int64(removeCount)
			}
			udpService.SendTestDataNum += 1
			//fmt.Println("收到测试数据", udpService.SendTestDataNum)
			length := 8
			randomBytes := make([]byte, length)
			for i := range randomBytes {
				//randomBytes[i] = byte(255)
				randomBytes[i] = byte(rand.Intn(256))
			}
			cf := udpService.CanFrame{}
			cf.Length = 8
			//int(tempCmd.Position/10)
			cf.FrameID = assembleCanID(0x07, 0x00, byte(1), 0xff)
			cf.Data[0] = randomBytes[0]
			cf.Data[1] = randomBytes[1]
			cf.Data[2] = randomBytes[2]
			cf.Data[3] = randomBytes[3]
			cf.Data[4] = randomBytes[4]
			cf.Data[5] = randomBytes[5]
			cf.Data[6] = randomBytes[6]
			cf.Data[7] = randomBytes[7]
			udpService.CanSendChannel1 <- cf.ToByte()
			// var
			// for _, temp := range randomBytes {
			// 		ReceiveTestDataText += fmt.Sprintf("%02x", temp) + " "
			// 	}
			sendDB := model.WjwSendRecord{Time: time.Now(), SendData: hex.EncodeToString(randomBytes)}
			select {
			case udpService.DBSChan <- sendDB:
				// 发送成功
			default:
				// 通道满了，丢弃记录或打印警告
				fmt.Println("Warning: DB Log Channel full, dropping record")
			}
			udpService.SendTestData = append(udpService.SendTestData, randomBytes)
			//SendTestDataNew = append(SendTestDataNew, randomBytes)
			udpService.DataLocker.Unlock()
			//fmt.Println(randomBytes)

			//fmt.Println((randomBytes[0]&0x0f)<<4 | (randomBytes[0]&0xf0)>>4)
			time.Sleep(time.Duration(sendInterval.Sendinterval) * time.Millisecond)

		}
	}
}

func StopTestData(c *gin.Context) {

	StartFlag = false

}

type Wml struct {
	SendDataNum            int64
	ReceiveDataRightNum    int64
	ReceiveDataNum         int64
	ReceiveDataErrorNum    int64
	SendDataBitNum         int64
	ReceiveDataRightBitNum int64
	ReceiveDataBitNum      int64
	ReceiveDataErrorBitNum int64
	ErrorRate              int64
	SendText               string
	ReceiveText            string
}

func GetWmlData(c *gin.Context) {
	var wmlData Wml
	wmlData.SendDataNum = udpService.SendTestDataNum
	wmlData.ReceiveDataNum = udpService.ReceiveTestDataNum
	wmlData.ReceiveDataRightNum = udpService.ReceiveTestDataRightNum
	wmlData.ReceiveDataErrorNum = udpService.ReceiveTestDataErrorNum
	wmlData.ReceiveDataRightBitNum = udpService.ReceiveTestDataRightBitNum
	wmlData.ReceiveDataErrorBitNum = udpService.ReceiveTestDataErrorBitNum
	wmlData.SendText = udpService.SendTestDataText
	wmlData.ReceiveText = udpService.ReceiveTestDataText
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": wmlData})
}

type BaoJing struct {
	Bjsj   time.Time //报警时间
	Sbbh   int       //设备编号
	Bjlx   string    //报警类型
	Bjkssj time.Time // 报警开始时间
	Bjjssj time.Time //报警结束时间
}

func GetRealData(c *gin.Context) {
	var realDatas []model.RealData
	// var lastwarning bool
	// lastwarning = false
	for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
		var realData model.RealData
		realData.Xh = i + 1
		realData.Dd = "22213工作面"
		realData.Sbmc = strconv.Itoa(i+1) + "号支架"
		if int(udpService.Mb.HoldingRegisters[4440+i]&0x0001) == 1 {
			realData.Zxzt = "离线"
		} else {
			realData.Zxzt = "在线"
		}

		if udpService.Mb.HoldingRegisters[180] == 0 {
			realData.Sfsbdmj = "否"
		} else {
			if i >= int(udpService.Mb.HoldingRegisters[180]/10-5) && i <= int(udpService.Mb.HoldingRegisters[180]/10+3) {
				realData.Sfsbdmj = "是"
			} else {
				realData.Sfsbdmj = "否"
			}
		}

		realData.Zjdz = "静止"
		if i < 16 {
			realData.Lzyl = int(252 + rand.Intn(50))
			realData.Tyxc = int(500 + rand.Intn(50))
			realData.Dingbxzqj = strconv.Itoa(int(rand.Intn(50))) + "°"
			realData.Dingbyzqj = strconv.Itoa(int(rand.Intn(50))) + "°"
			realData.Dibxzqj = strconv.Itoa(int(rand.Intn(50))) + "°"
			realData.Dibyzqj = strconv.Itoa(int(rand.Intn(50))) + "°"
		} else {
			realData.Lzyl = int(udpService.Mb.HoldingRegisters[1520+i*9]) //左压力
			realData.Tyxc = int(udpService.Mb.HoldingRegisters[1522+i*9]) //推移行程
			realData.Dingbxzqj = strconv.Itoa(int(udpService.Mb.HoldingRegisters[1524+i*9])) + "°"
			realData.Dingbyzqj = strconv.Itoa(int(udpService.Mb.HoldingRegisters[1525+i*9])) + "°"
			realData.Dibxzqj = strconv.Itoa(int(udpService.Mb.HoldingRegisters[1526+i*9])) + "°"
			realData.Dibyzqj = strconv.Itoa(int(udpService.Mb.HoldingRegisters[1527+i*9])) + "°"
		}
		realData.Mjwz = int(udpService.Mb.HoldingRegisters[180]) //煤机位置

		realData.Sjgxsj = time.Now().Format("2006-01-02 15:04:05")

		realData.Bjzt = "正常"
		realData.Bjyy = ""

		if realData.Lzyl > 500 {
			realData.Bjzt = "报警"
			realData.Bjyy = "压力过大"

			var count int64
			var bj BaoJing
			mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Count(&count)

			if count > 0 {
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Order("bjsj desc").Limit(1).Find(&bj)
				if bj.Bjkssj != bj.Bjjssj { //开始时间和结束时间不相等  说明上次报警已结束 要添加
					var BjData BaoJing
					BjData.Bjsj = time.Now()
					BjData.Sbbh = i + 1
					BjData.Bjlx = "压力过大"
					BjData.Bjkssj = time.Now()
					BjData.Bjjssj = time.Now()
					user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
						Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
					}
					mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
				}

			} else {
				var BjData BaoJing
				BjData.Bjsj = time.Now()
				BjData.Sbbh = i + 1
				BjData.Bjlx = "压力过大"
				BjData.Bjkssj = time.Now()
				BjData.Bjjssj = time.Now()
				user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
					Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
				}
				mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
			}

		}
		if realData.Lzyl < 252 {
			realData.Bjzt = "报警"
			realData.Bjyy = "压力过小"

			var count int64
			var bj BaoJing
			mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Count(&count)

			if count > 0 {
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Order("bjsj desc").Limit(1).Find(&bj)
				if bj.Bjkssj != bj.Bjjssj { //开始时间和结束时间不相等  说明上次报警已结束 要添加
					var BjData BaoJing
					BjData.Bjsj = time.Now()
					BjData.Sbbh = i + 1
					BjData.Bjlx = "压力过小"
					BjData.Bjkssj = time.Now()
					BjData.Bjjssj = time.Now()
					user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
						Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
					}
					mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
				}

			} else {
				var BjData BaoJing
				BjData.Bjsj = time.Now()
				BjData.Sbbh = i + 1
				BjData.Bjlx = "压力过小"
				BjData.Bjkssj = time.Now()
				BjData.Bjjssj = time.Now()
				user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
					Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
				}
				mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
			}

		}

		if int(udpService.Mb.HoldingRegisters[4440+i]>>2&0x0001) == 1 {
			realData.Bjzt = "报警"
			realData.Bjyy = "闭锁"

			var count int64
			var bj BaoJing
			mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Count(&count)

			if count > 0 {
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Order("bjsj desc").Limit(1).Find(&bj)
				if bj.Bjkssj != bj.Bjjssj { //开始时间和结束时间不相等  说明上次报警已结束 要添加
					var BjData BaoJing
					BjData.Bjsj = time.Now()
					BjData.Sbbh = i + 1
					BjData.Bjlx = "闭锁"
					BjData.Bjkssj = time.Now()
					BjData.Bjjssj = time.Now()
					user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
						Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
					}
					mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
				}

			} else {
				var BjData BaoJing
				BjData.Bjsj = time.Now()
				BjData.Sbbh = i + 1
				BjData.Bjlx = "闭锁"
				BjData.Bjkssj = time.Now()
				BjData.Bjjssj = time.Now()
				user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
					Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
				}
				mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
			}

		}

		if int(udpService.Mb.HoldingRegisters[4440+i]>>3&0x0001) == 1 {
			realData.Bjzt = "报警"
			realData.Bjyy = "急停"

			var count int64
			var bj BaoJing
			mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Count(&count)

			if count > 0 {
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Order("bjsj desc").Limit(1).Find(&bj)
				if bj.Bjkssj != bj.Bjjssj { //开始时间和结束时间不相等  说明上次报警已结束 要添加
					var BjData BaoJing
					BjData.Bjsj = time.Now()
					BjData.Sbbh = i + 1
					BjData.Bjlx = "急停"
					BjData.Bjkssj = time.Now()
					BjData.Bjjssj = time.Now()
					user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
						Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
					}
					mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
				}
			} else {
				var BjData BaoJing
				BjData.Bjsj = time.Now()
				BjData.Sbbh = i + 1
				BjData.Bjlx = "急停"
				BjData.Bjkssj = time.Now()
				BjData.Bjjssj = time.Now()
				user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
					Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
				}
				mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
			}

		}

		if realData.Zxzt == "离线" {
			realData.Bjzt = "报警"
			realData.Bjyy = "设备离线"

			var count int64
			var bj BaoJing
			mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Count(&count)
			//fmt.Println("设备离线count",count)
			if count > 0 {
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=? ", realData.Xh, realData.Bjyy).Order("bjsj desc").Limit(1).Find(&bj)
				//fmt.Println("bj",bj)
				if bj.Bjkssj != bj.Bjjssj { //开始时间和结束时间不相等  说明上次报警已结束 要添加
					var BjData BaoJing
					BjData.Bjsj = time.Now()
					BjData.Sbbh = i + 1
					BjData.Bjlx = "设备离线"
					BjData.Bjkssj = time.Now()
					BjData.Bjjssj = time.Now()
					user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
						Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
					}
					mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
				}

			} else {
				var BjData BaoJing
				BjData.Bjsj = time.Now()
				BjData.Sbbh = i + 1
				BjData.Bjlx = "设备离线"
				BjData.Bjkssj = time.Now()
				BjData.Bjjssj = time.Now()
				user := BaoJing{Bjsj: BjData.Bjsj, Sbbh: BjData.Sbbh, Bjlx: BjData.Bjlx,
					Bjkssj: BjData.Bjkssj, Bjjssj: BjData.Bjjssj,
				}
				mysql.Mysqlclient.Select("Bjsj", "Sbbh", "Bjlx", "Bjkssj", "Bjjssj").Create(&user)
			}

		}
		//fmt.Println("添加数据")
		realDatas = append(realDatas, realData)

		warning_code := []string{"压力过小", "压力过大", "闭锁", "急停", "设备离线"}

		if realData.Bjyy == "" {
			for j := 0; j < len(warning_code); j++ {
				var count int64
				var bj BaoJing
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Count(&count)
				if count > 0 {
					mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Order("bjsj desc").Limit(1).Find(&bj)
					if bj.Bjkssj == bj.Bjjssj { //说明应该结束报警了
						mysql.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Order("bjsj desc").Limit(1).Update("bjjssj", time.Now())

					}
				}
			}
		}

		if realData.Bjyy == "压力过小" {
			for j := 1; j < len(warning_code); j++ {
				var count int64
				var bj BaoJing
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Count(&count)
				//fmt.Println("压力过小", count, warning_code[j])
				if count > 0 {
					mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Order("bjsj desc").Limit(1).Find(&bj)
					if bj.Bjkssj == bj.Bjjssj { //说明应该结束报警了
						mysql.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Order("bjsj desc").Limit(1).Update("bjjssj", time.Now())

					}
				}
			}
		}

		if realData.Bjyy == "压力过大" {
			for j := 2; j < len(warning_code); j++ {
				var count int64
				var bj BaoJing
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Count(&count)
				if count > 0 {
					mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Limit(1).Find(&bj)
					if bj.Bjkssj == bj.Bjjssj { //说明应该结束报警了
						mysql.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Order("bjsj desc").Limit(1).Update("bjjssj", time.Now())

					}
				}
			}
		}

		if realData.Bjyy == "闭锁" {
			for j := 3; j < len(warning_code); j++ {
				var count int64
				var bj BaoJing
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Count(&count)
				if count > 0 {
					mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Limit(1).Find(&bj)
					if bj.Bjkssj == bj.Bjjssj { //说明应该结束报警了
						mysql.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Order("bjsj desc").Limit(1).Update("bjjssj", time.Now())

					}
				}
			}
		}

		if realData.Bjyy == "急停" {
			for j := 4; j < len(warning_code); j++ {
				var count int64
				var bj BaoJing
				mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Count(&count)
				if count > 0 {
					mysqlService.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Limit(1).Find(&bj)
					if bj.Bjkssj == bj.Bjjssj { //说明应该结束报警了
						mysql.Mysqlclient.Model(&BaoJing{}).Where("sbbh = ? AND bjlx=?", realData.Xh, warning_code[j]).Order("bjsj desc").Limit(1).Update("bjjssj", time.Now())

					}
				}
			}
		}

	}
	//fmt.Println("实时数据", realDatas)
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": realDatas})
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

func GetSupRealtime(c *gin.Context) {
	var queryTableParams model.QueryTableParams
	if err := c.ShouldBindJSON(&queryTableParams); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	fmt.Println("支架压力实时查询", queryTableParams)

	var count int64
	mysqlService.Mysqlclient.Model(&RealData1{}).Where("sjgxsj BETWEEN ? AND ? AND xh=?", queryTableParams.StartTime, queryTableParams.EndTime, queryTableParams.Support).Count(&count)
	fmt.Println("支架压力实时查询数量", count)
	if count > 0 {
		var realdata []RealData1
		mysqlService.Mysqlclient.Model(&RealData1{}).Where("sjgxsj BETWEEN ? AND ? AND xh=?", queryTableParams.StartTime, queryTableParams.EndTime, queryTableParams.Support).Order("sjgxsj desc").Find(&realdata)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": realdata})
	} else {
		c.JSON(http.StatusOK, gin.H{"code": 1})
	}
}

func GetSupWarning(c *gin.Context) {
	var queryTableParams model.QueryTableParams
	if err := c.ShouldBindJSON(&queryTableParams); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	var count int64
	mysqlService.Mysqlclient.Model(&BaoJing{}).Where("bjsj BETWEEN ? AND ?", queryTableParams.StartTime, queryTableParams.EndTime).Count(&count)
	fmt.Println("支架压力实时查询数量", count)
	if count > 0 {
		var warningdata []BaoJing
		mysqlService.Mysqlclient.Model(&BaoJing{}).Where("bjsj BETWEEN ? AND ?", queryTableParams.StartTime, queryTableParams.EndTime).Order("bjsj desc").Find(&warningdata)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": warningdata})
	} else {
		c.JSON(http.StatusOK, gin.H{"code": 1})
	}
}

// 不分页查询
func RecordCommandQuery(c *gin.Context) {
	var queryRecordCommandParams model.QueryRecordCommand

	if err := c.ShouldBindJSON(&queryRecordCommandParams); err != nil {
		fmt.Println(err)
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}
	db := mysqlService.Mysqlclient.Model(&model.RecordCommand{})

	// 时间范围查询
	db = db.Where("time >= ? AND time <= ?", queryRecordCommandParams.StartTime, queryRecordCommandParams.EndTime)

	// 可选条件查询
	if queryRecordCommandParams.SourceId != 0 {
		db = db.Where("source_id = ?", queryRecordCommandParams.SourceId)
	}
	if queryRecordCommandParams.CommandType != "" {
		db = db.Where("command_type = ?", queryRecordCommandParams.CommandType)
	}
	if queryRecordCommandParams.ControlCommandDeviceId != 0 {
		db = db.Where("control_command_device_id = ?", queryRecordCommandParams.ControlCommandDeviceId)
	}
	if queryRecordCommandParams.CurrentCommandSource != "" {
		db = db.Where("current_command_source = ?", queryRecordCommandParams.CurrentCommandSource)
	}
	db = db.Select("DISTINCT DATE_FORMAT(time, '%Y-%m-%d %H:%i:%s') as time_second, *")
	var count int64
	db.Count(&count)

	if count == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}

	// 使用DISTINCT和日期函数进行秒级去重
	var uniqueData []model.RecordCommand
	db.Order("time asc").Find(&uniqueData)

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "查询成功",
		"data":    uniqueData,
		"total":   count,
		"unique":  len(uniqueData),
	})

}

// 分页查询版本
func RecordCommandQueryWithPagination(c *gin.Context) {
	var queryRecordCommandParams model.QueryRecordCommand
	if err := c.ShouldBindJSON(&queryRecordCommandParams); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	offset := (page - 1) * pageSize

	db := mysqlService.Mysqlclient.Model(&model.RecordCommand{})
	db = buildQueryConditions1(db, queryRecordCommandParams)

	var total int64
	db.Count(&total)

	if total == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}

	var records []model.RecordCommand
	db.Offset(offset).Limit(pageSize).Order("time DESC").Find(&records)

	c.JSON(http.StatusOK, gin.H{
		"code":      0,
		"message":   "查询成功",
		"data":      records,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"pages":     (total + int64(pageSize) - 1) / int64(pageSize),
	})
}

// 构建查询条件的辅助函数
func buildQueryConditions1(db *gorm.DB, params model.QueryRecordCommand) *gorm.DB {
	db = db.Where("time >= ? AND time <= ?", params.StartTime, params.EndTime)

	if params.SourceId != 0 {
		db = db.Where("source_id = ?", params.SourceId)
	}
	if params.CommandType != "" {
		db = db.Where("command_type = ?", params.CommandType)
	}
	if params.ControlCommandDeviceId != 0 {
		db = db.Where("control_command_device_id = ?", params.ControlCommandDeviceId)
	}
	if params.CurrentCommandSource != "" {
		db = db.Where("current_command_source = ?", params.CurrentCommandSource)
	}
	db = db.Select("DISTINCT DATE_FORMAT(time, '%Y-%m-%d %H:%i:%s') as time_second, *")
	return db
}

// 不分页跨表查询所有数据
func RecordCommandQuery1(c *gin.Context) {

	var queryRecordCommandParams model.QueryRecordCommand

	if err := c.ShouldBindJSON(&queryRecordCommandParams); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		//return
	}
	fmt.Println("111", queryRecordCommandParams)
	// 解析时间范围，确定需要查询的表
	startTime, err := time.Parse("2006-01-02 15:04:05", queryRecordCommandParams.StartTime)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": "开始时间格式错误"})
		return
	}

	endTime, err := time.Parse("2006-01-02 15:04:05", queryRecordCommandParams.EndTime)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": "结束时间格式错误"})
		return
	}

	// 获取需要查询的所有表名
	tableNames := getTableNames(startTime, endTime)
	if len(tableNames) == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}

	// 使用UNION ALL查询所有相关表
	unionQuery, args := buildUnionQuery(tableNames, queryRecordCommandParams)

	var results []model.RecordCommand
	var totalCount int64

	// 执行查询
	err = mysqlService.Mysqlclient.Raw(unionQuery, args...).Scan(&results).Error
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": "查询失败: " + err.Error()})
		return
	}

	// 获取总数
	totalCount = int64(len(results))

	if totalCount == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "查询成功",
		"data":    results,
		"total":   totalCount,
		"unique":  len(results),
	})
}

// 构建UNION ALL查询
func buildUnionQuery(tableNames []string, query model.QueryRecordCommand) (string, []interface{}) {
	var args []interface{}
	var unionParts []string

	// 为每个表构建查询
	for _, tableName := range tableNames {
		tableQuery := fmt.Sprintf(`
			SELECT id, time, current_command_source, control_command_device_id, command_type, source_id 
			FROM %s 
			WHERE time BETWEEN ? AND ?`, tableName)

		// 添加时间参数
		args = append(args, query.StartTime, query.EndTime)

		// 添加可选条件
		if query.SourceId != 0 {
			tableQuery += " AND source_id = ?"
			args = append(args, query.SourceId)
		}
		if query.CommandType != "" {
			tableQuery += " AND command_type = ?"
			args = append(args, query.CommandType)
		}
		if query.ControlCommandDeviceId != 0 {
			tableQuery += " AND control_command_device_id = ?"
			args = append(args, query.ControlCommandDeviceId)
		}
		if query.CurrentCommandSource != "" {
			tableQuery += " AND current_command_source = ?"
			args = append(args, query.CurrentCommandSource)
		}

		unionParts = append(unionParts, tableQuery)
	}

	unionSQL := "(" + strings.Join(unionParts, " UNION ALL ") + ") AS combined_tables"

	// 最终查询：去重和排序
	// finalQuery := `
	// 	SELECT DISTINCT
	// 		id, time, current_command_source, control_command_device_id, command_type, source_id
	// 	FROM ` + unionSQL + `
	// 	GROUP BY UNIX_TIMESTAMP(time)
	// 	ORDER BY time ASC
	// `

	finalQuery := `
		SELECT 
			MIN(id) as id,
			MIN(time) as time,
			current_command_source,
			control_command_device_id,
			command_type,
			source_id
		FROM ` + unionSQL + `
		GROUP BY 
			current_command_source,
			control_command_device_id,
			command_type,
			source_id,
			UNIX_TIMESTAMP(time) DIV 1
		ORDER BY time DESC
	`

	return finalQuery, args
}

// 获取需要查询的表名列表
func getTableNames(startTime, endTime time.Time) []string {
	var tables []string

	// 从开始时间的月份到结束时间的月份
	current := time.Date(startTime.Year(), startTime.Month(), 1, 0, 0, 0, 0, startTime.Location())
	endMonth := time.Date(endTime.Year(), endTime.Month(), 1, 0, 0, 0, 0, endTime.Location())

	for !current.After(endMonth) {
		tableName := "record_command" + current.Format("200601")
		tables = append(tables, tableName)
		current = current.AddDate(0, 1, 0) // 下个月
	}

	return tables
}

// 分页查询版本（支持跨表查询）
func RecordCommandQueryWithPagination1(c *gin.Context) {
	var queryRecordCommandParams model.QueryRecordCommand
	if err := c.ShouldBindJSON(&queryRecordCommandParams); err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": err.Error()})
		return
	}

	// 验证时间参数
	if queryRecordCommandParams.StartTime == "" || queryRecordCommandParams.EndTime == "" {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": "开始时间和结束时间不能为空"})
		return
	}

	// 解析时间范围，确定需要查询的表
	startTime, err := time.Parse("2006-01-02 15:04:05", queryRecordCommandParams.StartTime)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": "开始时间格式错误"})
		return
	}

	endTime, err := time.Parse("2006-01-02 15:04:05", queryRecordCommandParams.EndTime)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": "结束时间格式错误"})
		return
	}

	page := queryRecordCommandParams.Page
	pageSize := queryRecordCommandParams.PageSize
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize
	// 获取需要查询的表名
	tableNames := getTableNames(startTime, endTime)
	if len(tableNames) == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}

	// 构建跨表查询
	total, records, err := queryWithPagination(tableNames, queryRecordCommandParams, pageSize, offset)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"code": 1, "message": "查询失败: " + err.Error()})
		return
	}

	if total == 0 {
		c.JSON(http.StatusOK, gin.H{"code": 2, "message": "查无数据"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":      0,
		"message":   "查询成功",
		"data":      records,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"pages":     (total + int64(pageSize) - 1) / int64(pageSize),
	})
}

// 构建跨表分页查询
func queryWithPagination(tableNames []string, params model.QueryRecordCommand, pageSize, offset int) (int64, []model.RecordCommand, error) {
	// 构建UNION ALL查询
	unionQuery, countQuery, args, countArgs := buildPaginationQuery(tableNames, params, pageSize, offset)

	var total int64
	var records []model.RecordCommand

	// 先查询总数
	err := mysqlService.Mysqlclient.Raw(countQuery, countArgs...).Scan(&total).Error
	if err != nil {
		return 0, nil, err
	}

	if total == 0 {
		return 0, nil, nil
	}

	// 再查询分页数据
	err = mysqlService.Mysqlclient.Raw(unionQuery, args...).Scan(&records).Error
	if err != nil {
		return 0, nil, err
	}

	return total, records, nil
}

// 构建分页查询SQL
func buildPaginationQuery(tableNames []string, params model.QueryRecordCommand, pageSize, offset int) (string, string, []interface{}, []interface{}) {
	var args []interface{}
	var unionParts []string
	var countArgs []interface{}
	// 构建每个表的查询
	for _, tableName := range tableNames {
		tableQuery := fmt.Sprintf(`
			SELECT id, time, current_command_source, control_command_device_id, command_type, source_id 
			FROM %s 
			WHERE time BETWEEN ? AND ?`, tableName)

		args = append(args, params.StartTime, params.EndTime)
		countArgs = append(countArgs, params.StartTime, params.EndTime)
		// 添加可选条件
		conditions, newArgs := buildConditions(params)
		if len(conditions) > 0 {
			tableQuery += " AND " + strings.Join(conditions, " AND ")
		}
		args = append(args, newArgs...)
		countArgs = append(countArgs, newArgs...)
		unionParts = append(unionParts, tableQuery)
	}

	unionSQL := "(" + strings.Join(unionParts, " UNION ALL ") + ") AS combined_tables"

	// 数据查询SQL（带分页）
	// dataQuery := `
	// 	SELECT * FROM (
	// 		SELECT *,
	// 			ROW_NUMBER() OVER (
	// 				PARTITION BY
	// 					UNIX_TIMESTAMP(time),
	// 					current_command_source,
	// 					control_command_device_id,
	// 					command_type,
	// 					source_id
	// 				ORDER BY time DESC
	// 			) as rn
	// 		FROM ` + unionSQL + `
	// 	) AS ranked_data
	// 	WHERE rn = 1
	// 	ORDER BY time DESC
	// 	LIMIT ? OFFSET ?`
	dataQuery := `
    SELECT * FROM (
        SELECT 
            id,
            time,
            current_command_source,
            control_command_device_id,
            command_type,
            source_id,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    DATE_FORMAT(time, '%Y-%m-%d %H:%i:%s'),
                    current_command_source,
                    control_command_device_id,
                    command_type,
                    source_id
                ORDER BY time DESC
            ) as rn
        FROM ` + unionSQL + `
    ) AS ranked_data
    WHERE rn = 1
    ORDER BY time DESC
    LIMIT ? OFFSET ?`

	args = append(args, pageSize, offset)

	// 总数查询SQL
	// countQuery := `
	// 	SELECT COUNT(*) FROM (
	// 		SELECT
	// 			current_command_source,
	// 			control_command_device_id,
	// 			command_type,
	// 			source_id,
	// 			UNIX_TIMESTAMP(time)
	// 		FROM ` + unionSQL + `
	// 		GROUP BY
	// 			current_command_source,
	// 			control_command_device_id,
	// 			command_type,
	// 			source_id,
	// 			UNIX_TIMESTAMP(time)
	// 	) AS distinct_records
	// `
	countQuery := `
		SELECT COUNT(*) FROM (
			SELECT DISTINCT
				current_command_source,
				control_command_device_id,
				command_type,
				source_id,
				UNIX_TIMESTAMP(time) DIV 1
			FROM ` + unionSQL + `
		) AS distinct_records`

	return dataQuery, countQuery, args, countArgs
}

// 构建查询条件
func buildConditions(params model.QueryRecordCommand) ([]string, []interface{}) {
	var conditions []string
	var newArgs []interface{}
	if params.SourceId != 0 {
		conditions = append(conditions, "source_id = ?")
		newArgs = append(newArgs, params.SourceId)
	}
	if params.CommandType != "" {
		conditions = append(conditions, "command_type = ?")
		newArgs = append(newArgs, params.CommandType)
	}
	if params.ControlCommandDeviceId != 0 {
		conditions = append(conditions, "control_command_device_id = ?")
		newArgs = append(newArgs, params.ControlCommandDeviceId)
	}
	if params.CurrentCommandSource != "" {
		conditions = append(conditions, "current_command_source = ?")
		newArgs = append(newArgs, params.CurrentCommandSource)
	}

	return conditions, newArgs
}
