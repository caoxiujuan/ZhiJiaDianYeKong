package mysql

import (
	"context"
	"fmt"
	"gocode/model"
	"gocode/utils"
	"log"
	"strconv"
	"time"

	"github.com/tbrandon/mbserver"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var (
	Mysqlclient *gorm.DB
)

func InitMysql(ctx context.Context, mbServer *mbserver.Server) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			c := utils.Conf.DATABASE
			dataSource := c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + strconv.Itoa(c.Port) + ")/" + c.Database + "?charset=utf8&parseTime=True&loc=Local"
			db, err := gorm.Open(mysql.Open(dataSource), &gorm.Config{
				NamingStrategy: schema.NamingStrategy{
					SingularTable: true,
				},
			})
			if err != nil {
				log.Println("数据库连接异常", err)
				time.Sleep(1 * time.Second)
				continue
			}
			sqlDB, err := db.DB()
			if err != nil {
				log.Println("数据库初始化异常", err)
				time.Sleep(1 * time.Second)
				continue
			}
			sqlDB.SetMaxIdleConns(10)
			sqlDB.SetMaxOpenConns(100)

			// user := model.Pressure{}
			// for i := 1; i <= utils.Conf.SYSTEM.SupportNum; i++ {
			// 	result := db.Where("support = ?", i).First(&user)
			// 	if result.RowsAffected == 0 {
			// 		user.Support = i
			// 		db.Select("Support").Create(&user)
			// 	}
			// }
			record := model.RecordCommand{TableTime: time.Now()}
			tableName := record.TableName()
			if !db.Migrator().HasTable(tableName) {
				fmt.Printf("正在创建表: %s\n", tableName)
				if err := db.Table(tableName).AutoMigrate(&model.RecordCommand{}); err != nil {
					log.Println("创建指令记录表异常: ", err)
					time.Sleep(1 * time.Second)
					continue
				} else {

					// 检查索引是否存在，如果不存在则创建
					indexName := "idx_time"
					migrator := db.Migrator()
					if !migrator.HasIndex(tableName, indexName) {
						// 如果不存在，创建索引
						sql := fmt.Sprintf("CREATE INDEX %s ON %s (time)", indexName, tableName)

						if err := db.Exec(sql).Error; err != nil {
							log.Printf("创建索引 %s 失败: %v\n", indexName, err)
						} else {
							fmt.Printf("成功创建索引: %s\n", indexName)
						}
					}
				}
			}
			fault := model.FaultRecord{TableTime: time.Now().Format("200601")}
			faultTableName := fault.TableName()
			if !db.Migrator().HasTable(faultTableName) {
				fmt.Printf("正在创建表: %s\n", faultTableName)
				if err := db.Table(faultTableName).AutoMigrate(&model.FaultRecord{}); err != nil {
					log.Println("创建故障记录表异常: ", err)
					time.Sleep(1 * time.Second)
					continue
				} else {

					// 检查索引是否存在，如果不存在则创建
					indexName := "idx_source_time"
					migrator := db.Migrator()
					if !migrator.HasIndex(faultTableName, indexName) {
						// 如果不存在，创建索引
						sql := fmt.Sprintf("CREATE INDEX %s ON %s (source_id,time)", indexName, faultTableName)

						if err := db.Exec(sql).Error; err != nil {
							log.Printf("创建索引 %s 失败: %v\n", indexName, err)
						} else {
							fmt.Printf("成功创建索引: %s\n", indexName)
						}
					}
				}
			}

			Mysqlclient = db

			fmt.Println("退出", Mysqlclient)
			return
		}
	}

}

type Mytest struct {
	ID       int
	Username int
	Password int
}

func Test(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if Mysqlclient != nil {
				user := model.CanActionData{Time: time.Now(), CanId: "0184", Len: 6, Information: "00 00 01 02"}
				Mysqlclient.Select("Time", "Type", "CanId", "Len", "Information").Create(&user)

			} else {
				fmt.Println("数据库未初始化")
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// func Test(ctx context.Context, mbServer *mbserver.Server) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		default:
// 			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
// 				//user := model.LeftColumnPressure{Support: i + 1, Time: time.Now(), Pressure: rand.Intn(1000), Distance: rand.Intn(960)}
// 				if math.Abs(float64(mbServer.HoldingRegisters[1520+i*9]-mbServer.HoldingRegisters[1521+i*9])) > 100 {
// 					if mbServer.HoldingRegisters[1520+i*9] >= mbServer.HoldingRegisters[1521+i*9] {
// 						user := model.LeftColumnPressure{Support: i + 1, Time: time.Now(), Pressure: int(mbServer.HoldingRegisters[1520+i*9]), Distance: int(mbServer.HoldingRegisters[1522+i*9])}
// 						Mysqlclient.Select("Support", "Time", "Pressure", "Distance").Create(&user)
// 					} else {
// 						user := model.LeftColumnPressure{Support: i + 1, Time: time.Now(), Pressure: int(mbServer.HoldingRegisters[1521+i*9]), Distance: int(mbServer.HoldingRegisters[1522+i*9])}
// 						Mysqlclient.Select("Support", "Time", "Pressure", "Distance").Create(&user)
// 					}
// 				} else {
// 					user := model.LeftColumnPressure{Support: i + 1, Time: time.Now(), Pressure: int((mbServer.HoldingRegisters[1520+i*9] + mbServer.HoldingRegisters[1521+i*9]) / 2), Distance: int(mbServer.HoldingRegisters[1522+i*9])}
// 					Mysqlclient.Select("Support", "Time", "Pressure", "Distance").Create(&user)
// 				}
// 			}
// 			fmt.Println("记录一次支架压力值")
// 			time.Sleep(time.Second * 300)

// 		}
// 	}
// }

// func SQLTest(ctx context.Context) {
// 	tickerTime := time.NewTicker(1 * time.Second)
// 	//
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case <-tickerTime.C:

// 			for i := 1; i <= 10; i++ {
// 				// /time.Now().Format("2006-01-02")
// 				var count int64
// 				Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), i).Count(&count)
// 				if count == 0 {
// 					user := model.PressureFaultDiagnosis{Support: i, Date: time.Now(), AlarmTimes: 1}
// 					Mysqlclient.Select("Support", "Date", "AlarmTimes").Create(&user)
// 				} else {
// 					var pressureFaultDiagnosis model.PressureFaultDiagnosis
// 					Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), i).Find(&pressureFaultDiagnosis)
// 					Mysqlclient.Model(&model.PressureFaultDiagnosis{}).Where("date= ? AND support= ?", time.Now().Format("2006-01-02"), i).Update("alarm_times", pressureFaultDiagnosis.AlarmTimes+1)
// 				}

// 			}
// 		}
// 	}
// }
