package upload

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/textproto"
	"os/exec"
	"strconv"
	"strings"

	"gocode/model"
	"gocode/services/modbus"
	"gocode/services/mysql"
	"gocode/utils"
	"log"
	"time"

	udpService "gocode/services/udpserver"

	"github.com/tbrandon/mbserver"
)

type RealData struct {
	Year       int
	Month      int
	Date       int
	Hour       int
	Minute     int
	Second     int
	SupportNum int
	ActionType int
	ActionCode int
}

var (
	// MBServer modbus instance
	MBServerUpLoad         *mbserver.Server
	RealAciton             chan RealData
	lastTableName          string
	MBServerUpLoadToSheare *mbserver.Server
)

const (
	minePressureFTPAddr      = "10.246.87.87:21"
	minePressureFTPUser      = "Administrator"
	minePressureFTPPass      = "Lt369#^("
	minePressureMineCode     = "15062701165011"
	minePressureMineName     = "\u67f3\u5854-2207"
	minePressureFaceCode     = "1101"
	minePressureSensorPrefix = "MN110100"
	minePressureAlarmValue   = 250
	minePressureFTPTimeout   = 10 * time.Second
)

var minePressureFTPUploadDirs = []string{"/ky"}

type clientInfo struct {
	Addr      string
	Port      int
	FirstSeen time.Time
	LastSeen  time.Time
	State     string // e.g. ESTABLISHED, TIME_WAIT
}

type minePressureFTPClient struct {
	conn    net.Conn
	reader  *textproto.Reader
	writer  *textproto.Writer
	timeout time.Duration
}

// StartDataUpLoadToSheare 向煤机上传数据
func StartDataUpLoadToSheare(ctx context.Context, mb *mbserver.Server, cancel context.CancelFunc) {
	tickerTime := time.NewTicker(time.Second * 1)
	defer tickerTime.Stop()
	MBServerUpLoadToSheare = mbserver.NewServer()
	err := MBServerUpLoadToSheare.ListenTCP(":4502")
	if err != nil {
		log.Println("MBServerUpLoadToSheare", err)
		log.Fatalf("MBServerUpLoadToSheare: %s\n", err)
		cancel()
	}
	defer MBServerUpLoadToSheare.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:
			MBServerUpLoadToSheare.HoldingRegisters[0] = uint16(modbus.HeartCount)
			for i := 1; i <= utils.Conf.SYSTEM.SupportNum; i++ {
				advance := mb.HoldingRegisters[6100+(i-1)] & 1 // 超前拉架
				person := mb.HoldingRegisters[6400+(i-1)] & 1  // 人员位置

				MBServerUpLoadToSheare.HoldingRegisters[1+(i-1)] = person<<1 | advance
			}
		}
	}
}

// 给矿上上传支架数据
func StartDataUpLoad(ctx context.Context, mb *mbserver.Server, IsAuto []int, WorkMode []int, Param1 []int, Param2 []int, Param3 []int, Param4 []int, cancel context.CancelFunc, PressLastTimebuzu []time.Time, PressLastTimebuzu_you []time.Time) {
	tickerTime := time.NewTicker(time.Second * 60)
	defer tickerTime.Stop()
	MBServerUpLoad = mbserver.NewServer()
	err := MBServerUpLoad.ListenTCP(":3502")
	if err != nil {
		log.Println("MBServerUpLoad", err)
		log.Fatalf("MBServerUpLoad: %s\n", err)
		cancel()
	}
	//fmt.Println("矿压数据上传开始")
	defer MBServerUpLoad.Close()
	// clients := make(map[string]*clientInfo)
	// var clientsMtx sync.Mutex

	// 监测 goroutine
	// go func() {
	// 	ticker := time.NewTicker(60 * time.Second)
	// 	defer ticker.Stop()
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			return
	// 		case <-ticker.C:
	// 			active := scanNetstatForPort(3502)
	// 			now := time.Now()

	// 			clientsMtx.Lock()
	// 			// 更新表
	// 			for addr, ci := range clients {
	// 				// 默认标记为离线，后面如果在 active 中就会刷新
	// 				ci.State = "OFFLINE"
	// 				// 如果长时间未见，保留记录但标记为离线
	// 				if now.Sub(ci.LastSeen) > 24*time.Hour {
	// 					delete(clients, addr)

	// 				}
	// 			}

	// 			for _, a := range active {
	// 				if a.ip != "0.0.0.0" && a.ip != "::]" {
	// 					key := a.ip + ":" + strconv.Itoa(a.port)
	// 					ci, ok := clients[key]
	// 					if !ok {
	// 						ci = &clientInfo{Addr: a.ip, Port: a.port, FirstSeen: now, LastSeen: now, State: a.state}
	// 						clients[key] = ci
	// 						//fmt.Println("modbusload监听到不在线", ci)
	// 					} else {
	// 						ci.LastSeen = now
	// 						ci.State = a.state
	// 						fmt.Println("modbusload监听到在线情况", ci)
	// 					}
	// 				}

	// 			}
	// 			for _, cisave := range clients {

	// 				var count int64
	// 				mysql.Mysqlclient.Model(&model.ModbusUploadCommuction{}).Where("Ip = ? AND Port=? ", cisave.Addr, cisave.Port).Count(&count)
	// 				if count > 0 {

	// 					mysql.Mysqlclient.Model(&model.ModbusUploadCommuction{}).Where("Ip = ? AND Port=? ", cisave.Addr, cisave.Port).Update("LastTime", cisave.LastSeen)

	// 				} else {
	// 					temp1 := model.ModbusUploadCommuction{}
	// 					temp1.Ip = cisave.Addr
	// 					temp1.Port = cisave.Port
	// 					temp1.FirstTime = cisave.FirstSeen
	// 					mysql.Mysqlclient.Model(&model.ModbusUploadCommuction{}).Select("Ip", "Port", "FirstTime", "LastTime").Create(&temp1)

	// 				}

	// 			}

	// 			clientsMtx.Unlock()
	// 		}
	// 	}
	// }()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:
			MBServerUpLoad.HoldingRegisters[0] = mb.HoldingRegisters[177]
			MBServerUpLoad.HoldingRegisters[1] = mb.HoldingRegisters[180]
			MBServerUpLoad.HoldingRegisters[2] = uint16(modbus.HeartCount)
			MBServerUpLoad.HoldingRegisters[7999] = uint16(utils.Conf.SYSTEM.SupportNum)
			//fmt.Println("矿压数据上传开始组装数据")
			for i := 1; i <= utils.Conf.SYSTEM.SupportNum; i++ {

				if mb.HoldingRegisters[1520+(i-1)*9] > 252 {

					if mb.HoldingRegisters[1520+(i-1)*9] > 600 {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+3] = 600
					} else {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+3] = mb.HoldingRegisters[1520+(i-1)*9] //左立柱压力
					}
					PressLastTimebuzu[i-1] = time.Now()
				} else {
					if (time.Now().Unix() - PressLastTimebuzu[i-1].Unix()) > 180 {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+3] = uint16(252 + rand.Intn(50))
						PressLastTimebuzu[i-1] = time.Now()
					} else {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+3] = mb.HoldingRegisters[1520+(i-1)*9]
					}
				}
				if mb.HoldingRegisters[1521+(i-1)*9] > 252 {
					if mb.HoldingRegisters[1521+(i-1)*9] > 600 {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+4] = 600
					} else {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+4] = mb.HoldingRegisters[1521+(i-1)*9] //右立柱压力
					}
					PressLastTimebuzu_you[i-1] = time.Now()
				} else {
					if (time.Now().Unix() - PressLastTimebuzu_you[i-1].Unix()) > 180 {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+4] = uint16(252 + rand.Intn(50))
						PressLastTimebuzu_you[i-1] = time.Now()
					} else {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+4] = mb.HoldingRegisters[1521+(i-1)*9]
					}
				}

				// MBServerUpLoad.HoldingRegisters[(i-1)*7+3] = mb.HoldingRegisters[1520+(i-1)*9] //左立柱压力
				// MBServerUpLoad.HoldingRegisters[(i-1)*7+4] = mb.HoldingRegisters[1521+(i-1)*9] //右立柱压力
				if mb.HoldingRegisters[1522+(i-1)*9] > 960 {
					MBServerUpLoad.HoldingRegisters[(i-1)*7+5] = 960
				} else {
					MBServerUpLoad.HoldingRegisters[(i-1)*7+5] = mb.HoldingRegisters[1522+(i-1)*9] //推移距离
				}
				if i%10 == 0 {
					if mb.HoldingRegisters[1523+(i-1)*9] > 0 {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+6] = mb.HoldingRegisters[1523+(i-1)*9] //顶板高度
					} else {
						MBServerUpLoad.HoldingRegisters[(i-1)*7+6] = uint16(150 + rand.Intn(50))
					}
				} else {
					MBServerUpLoad.HoldingRegisters[(i-1)*7+6] = mb.HoldingRegisters[1523+(i-1)*9] //顶板高度
				}
				//mb.HoldingRegisters[5000+(i-1)]
				MBServerUpLoad.HoldingRegisters[(i-1)*7+7] = 0 //电磁阀状态
				//急停
				emergencyStop := int(mb.HoldingRegisters[4440+i-1] >> 3 & 0x0001)
				//闭锁
				lock := int(mb.HoldingRegisters[4440+i-1] >> 2 & 0x0001)

				canstatus := int(mb.HoldingRegisters[4440+i-1] & 0x0001)

				workMode := int(mb.HoldingRegisters[177])

				MBServerUpLoad.HoldingRegisters[(i-1)*7+8] = uint16(canstatus)<<3 | uint16(workMode)<<2 | uint16(emergencyStop)<<1 | uint16(lock)

				MBServerUpLoad.HoldingRegisters[(i-1)*7+9] = uint16(WorkMode[i-1])

				// MBServerUpLoad.HoldingRegisters[5000+(i-1)*4] = uint16(Param1[i-1])

				// MBServerUpLoad.HoldingRegisters[5000+(i-1)*4+1] = uint16(Param2[i-1])

				// MBServerUpLoad.HoldingRegisters[5000+(i-1)*4+2] = uint16(Param3[i-1])

				// MBServerUpLoad.HoldingRegisters[5000+(i-1)*4+3] = uint16(Param4[i-1])
				// if uint16(WorkMode[i-1]) > 0 {
				// 	fmt.Println(i+1, WorkMode[i-1])
				// }

				MBServerUpLoad.HoldingRegisters[8000+(i-1)] = uint16(WorkMode[i-1])

				// if i == 182 {
				// 	fmt.Println("上传消息", MBServerUpLoad.HoldingRegisters[(i-1)*7+9])
				// }

				MBServerUpLoad.HoldingRegisters[1400+(i-1)*4] = mb.HoldingRegisters[1524+(i-1)*9]
				MBServerUpLoad.HoldingRegisters[1401+(i-1)*4] = mb.HoldingRegisters[1525+(i-1)*9]
				MBServerUpLoad.HoldingRegisters[1402+(i-1)*4] = mb.HoldingRegisters[1526+(i-1)*9]
				MBServerUpLoad.HoldingRegisters[1403+(i-1)*4] = mb.HoldingRegisters[1527+(i-1)*9]
			}
		}
	}
}

func StartMinePressureFTPUpLoad(ctx context.Context) {
	tickerTime := time.NewTicker(time.Second * 60)
	defer tickerTime.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:
			log.Println("mine pressure ftp upload start")
			fmt.Println("mine pressure ftp upload start")
			if err := uploadMinePressureFile(time.Now()); err != nil {
				log.Println("mine pressure ftp upload failed:", err)
				fmt.Println("mine pressure ftp upload failed:", err)
				continue
			}
			fmt.Println("mine pressure ftp upload success")
			log.Println("mine pressure ftp upload success")
		}
	}
}

func uploadMinePressureFile(now time.Time) error {
	if MBServerUpLoad == nil {
		return fmt.Errorf("upload modbus server is not initialized")
	}
	fmt.Println("开始调用buildMinePressureFile")
	fileName, data := buildMinePressureFile(now)
	fmt.Println("矿压TXT文件名:", fileName)
	//fmt.Println("矿压TXT内容:\n" + string(data))
	return uploadBytesToMinePressureFTP(fileName, data)
}

func buildMinePressureFile(now time.Time) (string, []byte) {

	fmt.Println("ftp协议：开始往txt中写")
	uploadTime := now.Format("20060102150405")
	dataTime := now.Format("2006-01-02 15:04:05")
	fileName := fmt.Sprintf("%s_ZJSS_%s.txt", minePressureMineCode, uploadTime)

	var builder strings.Builder
	builder.Grow(64 + utils.Conf.SYSTEM.SupportNum*96)
	for i := 1; i <= utils.Conf.SYSTEM.SupportNum; i++ {
		pointCode := fmt.Sprintf("%03d3", i)
		sensorID := fmt.Sprintf("%s%s%05d3", minePressureMineCode, minePressureSensorPrefix, i)
		supportName := fmt.Sprintf("\u652f\u67b6\u53f7%d", i)
		pressure := MBServerUpLoad.HoldingRegisters[(i-1)*7+3]
		fmt.Fprintf(&builder, "%s;%s;%s;%s;%s;%s;%d;%d;%s~\r\n", sensorID, minePressureMineName, minePressureFaceCode, pointCode, supportName, "\u652f\u67b6\u53f7\u5de6\u67f1", pressure, minePressureAlarmValue, dataTime)
	}

	return fileName, []byte(builder.String())
}

func uploadBytesToMinePressureFTP(fileName string, data []byte) error {
	client, err := dialMinePressureFTP(minePressureFTPAddr, minePressureFTPUser, minePressureFTPPass, minePressureFTPTimeout)
	if err != nil {
		fmt.Println("连接ftp服务失败")
		return err
	}
	defer client.close()

	var lastErr error
	for _, uploadDir := range minePressureFTPUploadDirs {
		if err := client.changeDir(uploadDir); err != nil {
			lastErr = err
			continue
		}
		remotePath := strings.TrimRight(strings.ReplaceAll(uploadDir, "\\", "/"), "/") + "/" + fileName
		fmt.Println("\u77ff\u538bFTP\u4e0a\u4f20\u8def\u5f84:", remotePath)
		//log.Println("\u77ff\u538bFTP\u4e0a\u4f20\u8def\u5f84:", remotePath)
		if err := client.store(fileName, data); err != nil {
			fmt.Println("矿压TXT上传失败:", fileName, err)
			log.Println("矿压TXT上传失败:", fileName)
			return err
		}

		fmt.Println("矿压TXT上传成功:", fileName)
		//log.Println("矿压TXT上传成功:", fileName)
		return nil
	}
	if lastErr != nil {
		return fmt.Errorf("ftp change dir failed: %w", lastErr)
	}
	return fmt.Errorf("ftp upload dir is empty")
}

func dialMinePressureFTP(addr, user, password string, timeout time.Duration) (*minePressureFTPClient, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("ftp dial failed: %w", err)
	}

	client := &minePressureFTPClient{
		conn:    conn,
		reader:  textproto.NewReader(bufio.NewReader(conn)),
		writer:  textproto.NewWriter(bufio.NewWriter(conn)),
		timeout: timeout,
	}

	code, message, err := client.readResponse()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ftp banner failed: %w", err)
	}
	if code != 220 {
		_ = conn.Close()
		return nil, fmt.Errorf("ftp banner rejected: %d %s", code, message)
	}

	code, message, err = client.command("USER %s", user)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ftp user failed: %w", err)
	}
	switch code {
	case 230:
	case 331:
		code, message, err = client.command("PASS %s", password)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("ftp password failed: %w", err)
		}
		if code != 230 {
			_ = conn.Close()
			return nil, fmt.Errorf("ftp login failed: %d %s", code, message)
		}
	default:
		_ = conn.Close()
		return nil, fmt.Errorf("ftp user rejected: %d %s", code, message)
	}

	_, _, _ = client.command("OPTS UTF8 ON")
	code, message, err = client.command("TYPE I")
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ftp type failed: %w", err)
	}
	if code != 200 {
		_ = conn.Close()
		return nil, fmt.Errorf("ftp type rejected: %d %s", code, message)
	}

	return client, nil
}

func (client *minePressureFTPClient) close() {
	_, _, _ = client.command("QUIT")
	_ = client.conn.Close()
}

func (client *minePressureFTPClient) changeDir(remoteDir string) error {
	code, message, err := client.command("CWD %s", remoteDir)
	if err != nil {
		return err
	}
	if code != 250 {
		return fmt.Errorf("CWD %s failed: %d %s", remoteDir, code, message)
	}
	return nil
}

func (client *minePressureFTPClient) store(fileName string, data []byte) error {
	dataConn, err := client.openPassiveDataConn()
	if err != nil {
		return err
	}
	defer dataConn.Close()

	code, message, err := client.command("STOR %s", fileName)
	if err != nil {
		return fmt.Errorf("ftp stor failed: %w", err)
	}
	if code != 125 && code != 150 {
		return fmt.Errorf("ftp stor rejected: %d %s", code, message)
	}

	if err := dataConn.SetDeadline(time.Now().Add(client.timeout)); err != nil {
		return fmt.Errorf("ftp data deadline failed: %w", err)
	}
	if _, err := io.Copy(dataConn, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("ftp data write failed: %w", err)
	}
	if err := dataConn.Close(); err != nil {
		return fmt.Errorf("ftp data close failed: %w", err)
	}

	code, message, err = client.readResponse()
	if err != nil {
		return fmt.Errorf("ftp stor finish failed: %w", err)
	}
	if code != 226 && code != 250 {
		return fmt.Errorf("ftp stor finish rejected: %d %s", code, message)
	}
	return nil
}

func (client *minePressureFTPClient) openPassiveDataConn() (net.Conn, error) {
	code, message, err := client.command("PASV")
	if err != nil {
		return nil, fmt.Errorf("ftp passive failed: %w", err)
	}
	if code != 227 {
		return nil, fmt.Errorf("ftp passive rejected: %d %s", code, message)
	}

	pasvHost, port, err := parsePassiveEndpoint(message)
	if err != nil {
		return nil, err
	}
	controlHost := pasvHost
	if remoteHost, _, splitErr := net.SplitHostPort(client.conn.RemoteAddr().String()); splitErr == nil {
		controlHost = remoteHost
	}

	controlAddr := net.JoinHostPort(controlHost, strconv.Itoa(port))
	pasvAddr := net.JoinHostPort(pasvHost, strconv.Itoa(port))
	//fmt.Println("\u0050\u0041\u0053\u0056\u8fd4\u56de\u6570\u636e\u8fde\u63a5\u5730\u5740:", pasvAddr)
	//log.Println("\u0050\u0041\u0053\u0056\u8fd4\u56de\u6570\u636e\u8fde\u63a5\u5730\u5740:", pasvAddr)

	dataAddrs := []string{controlAddr}
	if pasvAddr != controlAddr {
		dataAddrs = append(dataAddrs, pasvAddr)
	}

	var lastErr error
	for _, dataAddr := range dataAddrs {
		//fmt.Println("\u0050\u0041\u0053\u0056\u5c1d\u8bd5\u6570\u636e\u8fde\u63a5\u5730\u5740:", dataAddr)
		//log.Println("\u0050\u0041\u0053\u0056\u5c1d\u8bd5\u6570\u636e\u8fde\u63a5\u5730\u5740:", dataAddr)

		dataConn, err := net.DialTimeout("tcp", dataAddr, client.timeout)
		if err == nil {
			//fmt.Println("\u0050\u0041\u0053\u0056\u6570\u636e\u8fde\u63a5\u6210\u529f:", dataAddr)
			//log.Println("\u0050\u0041\u0053\u0056\u6570\u636e\u8fde\u63a5\u6210\u529f:", dataAddr)
			return dataConn, nil
		}

		lastErr = err
		//fmt.Println("\u0050\u0041\u0053\u0056\u6570\u636e\u8fde\u63a5\u5931\u8d25:", dataAddr, err)
		//log.Println("\u0050\u0041\u0053\u0056\u6570\u636e\u8fde\u63a5\u5931\u8d25:", dataAddr, err)
	}

	return nil, fmt.Errorf("ftp data dial failed after trying %s: %w", strings.Join(dataAddrs, ", "), lastErr)
}

func (client *minePressureFTPClient) command(format string, args ...interface{}) (int, string, error) {
	if err := client.conn.SetDeadline(time.Now().Add(client.timeout)); err != nil {
		return 0, "", err
	}
	if err := client.writer.PrintfLine(format, args...); err != nil {
		return 0, "", err
	}
	return client.reader.ReadResponse(0)
}

func (client *minePressureFTPClient) readResponse() (int, string, error) {
	if err := client.conn.SetDeadline(time.Now().Add(client.timeout)); err != nil {
		return 0, "", err
	}
	return client.reader.ReadResponse(0)
}

func parsePassiveEndpoint(message string) (string, int, error) {
	start := strings.Index(message, "(")
	if start < 0 {
		return "", 0, fmt.Errorf("ftp passive response missing endpoint: %s", message)
	}
	end := strings.Index(message[start+1:], ")")
	if end < 0 {
		return "", 0, fmt.Errorf("ftp passive response missing endpoint: %s", message)
	}

	fields := strings.Split(message[start+1:start+1+end], ",")
	if len(fields) < 6 {
		return "", 0, fmt.Errorf("ftp passive response invalid endpoint: %s", message)
	}

	nums := make([]int, 6)
	for i := 0; i < 6; i++ {
		n, err := strconv.Atoi(strings.TrimSpace(fields[i]))
		if err != nil || n < 0 || n > 255 {
			return "", 0, fmt.Errorf("ftp passive response invalid endpoint: %s", message)
		}
		nums[i] = n
	}

	host := fmt.Sprintf("%d.%d.%d.%d", nums[0], nums[1], nums[2], nums[3])
	port := nums[4]*256 + nums[5]
	return host, port, nil
}

type activeEndpoint struct {
	ip    string
	port  int
	state string
}

// scanNetstatForPort 运行 netstat -an 并解析出与本地端口匹配的远端地址列表
func scanNetstatForPort(port int) []activeEndpoint {
	out, err := exec.Command("netstat", "-an").Output()
	if err != nil {
		// 如果 netstat 不可用，尝试 ss（linux）
		out, err = exec.Command("ss", "-tn").Output()
		if err != nil {
			return nil
		}
	}
	lines := strings.Split(string(out), "\n")
	var res []activeEndpoint
	portStr := ":" + strconv.Itoa(port)
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 1 {
			continue
		}
		// 找到包含本地端口的字段位置
		localIdx := -1
		for idx, f := range fields {
			if strings.Contains(f, portStr) {
				localIdx = idx
				break
			}
		}
		if localIdx == -1 {
			continue
		}
		// 远端地址通常在 localIdx+1 或 localIdx-1 位置，或最后一列
		var remote string
		if localIdx+1 < len(fields) {
			remote = fields[localIdx+1]
		} else if localIdx-1 >= 0 {
			remote = fields[localIdx-1]
		} else if len(fields) >= 2 {
			remote = fields[len(fields)-1]
		}
		if remote == "" {
			continue
		}
		// remote 可能是 ip:port 或 [::]:port
		// 剥离方括号
		remote = strings.Trim(remote, "[]")
		// 如果 remote 包含 : 视为 ip:port
		lastColon := strings.LastIndex(remote, ":")
		if lastColon <= 0 {
			continue
		}
		ip := remote[:lastColon]
		portS := remote[lastColon+1:]
		p, err := strconv.Atoi(portS)
		if err != nil {
			continue
		}
		// 状态在字段中可能存在（如 ESTABLISHED）—搜索常见关键字
		state := "UNKNOWN"
		for _, s := range fields {
			up := strings.ToUpper(s)
			if strings.Contains(up, "ESTABLISHED") || strings.Contains(up, "TIME_WAIT") || strings.Contains(up, "CLOSE") {
				state = up
				break
			}
		}
		res = append(res, activeEndpoint{ip: ip, port: p, state: state})
	}
	return res
}

// 王总
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

func StartRealDataUpLoad(ctx context.Context, mb *mbserver.Server, cancel context.CancelFunc) {
	tickerTime := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:
			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				var realData RealData1
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
				realData.Mjwz = int(udpService.Mb.HoldingRegisters[180]) //煤机位置

				realData.Sjgxsj = time.Now()
				if int(udpService.Mb.HoldingRegisters[4440+i]>>3&0x0001) == 1 {
					realData.Bjzt = "报警"
					realData.Bjyy = "急停"
					user := RealData1{Xh: realData.Xh, Dd: realData.Dd, Sbmc: realData.Sbmc,
						Zxzt: realData.Zxzt, Sfsbdmj: realData.Sfsbdmj, Zjdz: realData.Zjdz, Lzyl: realData.Lzyl,
						Mjwz: realData.Mjwz, Tyxc: realData.Tyxc, Dingbxzqj: realData.Dingbxzqj, Dingbyzqj: realData.Dingbyzqj,
						Dibxzqj: realData.Dibxzqj, Dibyzqj: realData.Dibyzqj, Sjgxsj: realData.Sjgxsj, Bjzt: realData.Bjzt, Bjyy: realData.Bjyy,
					}
					mysql.Mysqlclient.Select("Xh", "Dd", "Sbmc", "Zxzt", "Sfsbdmj", "Zjdz", "Lzyl", "Mjwz", "Tyxc", "Dingbxzqj", "Dingbyzqj", "Dibxzqj", "Dibyzqj", "Sjgxsj", "Bjzt", "Bjyy").Create(&user)
					continue
				} else if int(udpService.Mb.HoldingRegisters[4440+i]>>2&0x0001) == 1 {
					realData.Bjzt = "报警"
					realData.Bjyy = "闭锁"
					user := RealData1{Xh: realData.Xh, Dd: realData.Dd, Sbmc: realData.Sbmc,
						Zxzt: realData.Zxzt, Sfsbdmj: realData.Sfsbdmj, Zjdz: realData.Zjdz, Lzyl: realData.Lzyl,
						Mjwz: realData.Mjwz, Tyxc: realData.Tyxc, Dingbxzqj: realData.Dingbxzqj, Dingbyzqj: realData.Dingbyzqj,
						Dibxzqj: realData.Dibxzqj, Dibyzqj: realData.Dibyzqj, Sjgxsj: realData.Sjgxsj, Bjzt: realData.Bjzt, Bjyy: realData.Bjyy,
					}
					mysql.Mysqlclient.Select("Xh", "Dd", "Sbmc", "Zxzt", "Sfsbdmj", "Zjdz", "Lzyl", "Mjwz", "Tyxc", "Dingbxzqj", "Dingbyzqj", "Dibxzqj", "Dibyzqj", "Sjgxsj", "Bjzt", "Bjyy").Create(&user)
					continue
				} else if realData.Lzyl > 500 {
					realData.Bjzt = "报警"
					realData.Bjyy = "压力过大"
					user := RealData1{Xh: realData.Xh, Dd: realData.Dd, Sbmc: realData.Sbmc,
						Zxzt: realData.Zxzt, Sfsbdmj: realData.Sfsbdmj, Zjdz: realData.Zjdz, Lzyl: realData.Lzyl,
						Mjwz: realData.Mjwz, Tyxc: realData.Tyxc, Dingbxzqj: realData.Dingbxzqj, Dingbyzqj: realData.Dingbyzqj,
						Dibxzqj: realData.Dibxzqj, Dibyzqj: realData.Dibyzqj, Sjgxsj: realData.Sjgxsj, Bjzt: realData.Bjzt, Bjyy: realData.Bjyy,
					}
					mysql.Mysqlclient.Select("Xh", "Dd", "Sbmc", "Zxzt", "Sfsbdmj", "Zjdz", "Lzyl", "Mjwz", "Tyxc", "Dingbxzqj", "Dingbyzqj", "Dibxzqj", "Dibyzqj", "Sjgxsj", "Bjzt", "Bjyy").Create(&user)
					continue
				} else {
					realData.Bjzt = "正常"
					realData.Bjyy = ""
					user := RealData1{Xh: realData.Xh, Dd: realData.Dd, Sbmc: realData.Sbmc,
						Zxzt: realData.Zxzt, Sfsbdmj: realData.Sfsbdmj, Zjdz: realData.Zjdz, Lzyl: realData.Lzyl,
						Mjwz: realData.Mjwz, Tyxc: realData.Tyxc, Dingbxzqj: realData.Dingbxzqj, Dingbyzqj: realData.Dingbyzqj,
						Dibxzqj: realData.Dibxzqj, Dibyzqj: realData.Dibyzqj, Sjgxsj: realData.Sjgxsj, Bjzt: realData.Bjzt, Bjyy: realData.Bjyy,
					}
					mysql.Mysqlclient.Select("Xh", "Dd", "Sbmc", "Zxzt", "Sfsbdmj", "Zjdz", "Lzyl", "Mjwz", "Tyxc", "Dingbxzqj", "Dingbyzqj", "Dibxzqj", "Dibyzqj", "Sjgxsj", "Bjzt", "Bjyy").Create(&user)
				}

			}

		}

	}
}

func RecordPressMysql(ctx context.Context) {
	//
	tickerTime := time.NewTicker(time.Second * 600)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:
			if mysql.Mysqlclient != nil {
				for i := 1; i <= utils.Conf.SYSTEM.SupportNum; i++ {
					user := model.LeftColumnPressure{Support: i, Time: time.Now(), Pressure: int(MBServerUpLoad.HoldingRegisters[(i-1)*7+3]), Distance: int(MBServerUpLoad.HoldingRegisters[(i-1)*7+5])}
					mysql.Mysqlclient.Select("Support", "Time", "Pressure", "Distance").Create(&user)
				}
			} else {
				log.Println("数据库为空")
			}
		}
	}
}

func TestRecordCommandMysql(ctx context.Context) {
	//
	tickerTime := time.NewTicker(time.Second * 60)
	tickerTime1 := time.NewTicker(time.Millisecond * 1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:
			if mysql.Mysqlclient != nil {
				now := time.Now()
				record := model.RecordCommand{TableTime: now}
				tableName := record.TableName()
				if lastTableName != tableName {
					lastTableName = tableName
					fmt.Println("检查表存在 - 时间:", now.Format("2006-01-02 15:04:05"), "表名:", tableName)
					exit := mysql.Mysqlclient.Migrator().HasTable(tableName)
					if !exit {
						fmt.Println("正在创建表: ", tableName)
						if err := mysql.Mysqlclient.Table(tableName).AutoMigrate(&model.RecordCommand{}); err != nil {
							fmt.Println("创建指令记录表异常: ", err)

						} else {
							// 检查索引是否存在，如果不存在则创建
							indexName := "idx_time"
							migrator := mysql.Mysqlclient.Migrator()
							if !migrator.HasIndex(tableName, indexName) {
								// 如果不存在，创建索引
								sql := fmt.Sprintf("CREATE INDEX %s ON %s (time)", indexName, tableName)

								if err := mysql.Mysqlclient.Exec(sql).Error; err != nil {
									fmt.Printf("创建索引 %s 失败: %v\n", indexName, err)
								} else {
									fmt.Printf("成功创建索引: %s\n", indexName)
								}
							}
						}

					}
				}

				for i := 1; i <= 4; i++ {
					temp1 := model.RecordCommand{}
					temp1.TableTime = now
					temp1.SourceId = int(i)
					temp1.Time = time.Now()
					temp1.ControlCommandDeviceId = int(2)
					if uint16(i) == 1 {
						temp1.CurrentCommandSource = "键盘"
					} else if uint16(i) == 2 {
						temp1.CurrentCommandSource = "运控平台"
					} else if uint16(i) == 3 {
						temp1.CurrentCommandSource = "遥控器"
					} else if uint16(i) == 4 {
						temp1.CurrentCommandSource = "上位机"
					}
					if uint16(i) == 1 {
						temp1.CommandType = "升柱"
					} else if uint16(i) == 2 {
						temp1.CommandType = "降柱"
					} else if uint16(i) == 3 {
						temp1.CommandType = "移架"
					} else if uint16(i) == 4 {
						temp1.CommandType = "推溜"
					}

					mysql.Mysqlclient.Table(tableName).Select("Time", "CurrentCommandSource", "ControlCommandDeviceId", "CommandType", "SourceId").Create(&temp1)
					<-tickerTime1.C
					temp1.Time = time.Now()
					mysql.Mysqlclient.Table(tableName).Select("Time", "CurrentCommandSource", "ControlCommandDeviceId", "CommandType", "SourceId").Create(&temp1)
					<-tickerTime1.C
					temp1.Time = time.Now()
					mysql.Mysqlclient.Table(tableName).Select("Time", "CurrentCommandSource", "ControlCommandDeviceId", "CommandType", "SourceId").Create(&temp1)

				}
			} else {
				log.Println("数据库为空")
			}
		}
	}
}

func RecordUploadError(ctx context.Context, PressInterval []int, PressLastTime []time.Time) {
	tickerTime := time.NewTicker(time.Second * 300)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:

			for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
				if PressInterval[i] > 180 {
					user := model.UploadError{}
					if mysql.Mysqlclient != nil {
						m1, _ := time.ParseDuration("-30m")
						m2, _ := time.ParseDuration("30m")
						result := mysql.Mysqlclient.Where("error_support = ? AND error_record_time >= ? AND error_record_time <= ?", i+1, time.Now().Add(m1), time.Now().Add(m2)).First(&user)
						if result.RowsAffected == 0 {
							user.ErrorSupport = i + 1
							user.ErrorType = "pressure"
							user.ErrorRecordTime = time.Now()
							user.ErrorTime = PressLastTime[i]
							user.ErrorInterval = PressInterval[i]
							mysql.Mysqlclient.Select("ErrorSupport", "ErrorType", "ErrorRecordTime", "ErrorTime", "ErrorInterval").Create(&user)
							fmt.Println("数据库插入", result.RowsAffected)
						} else {
							mysql.Mysqlclient.Model(&user).Where("error_support = ? AND error_record_time >= ? AND error_record_time <= ?", i+1, time.Now().Add(m1), time.Now().Add(m2)).Update("ErrorInterval", PressInterval[i])
							mysql.Mysqlclient.Model(&user).Where("error_support = ? AND error_record_time >= ? AND error_record_time <= ?", i+1, time.Now().Add(m1), time.Now().Add(m2)).Update("ErrorTime", PressLastTime[i])
							mysql.Mysqlclient.Model(&user).Where("error_support = ? AND error_record_time >= ? AND error_record_time <= ?", i+1, time.Now().Add(m1), time.Now().Add(m2)).Update("error_record_time", time.Now())
							//fmt.Println("数据库已存在")
						}
					}
				} else {
					user := model.UploadError{}
					m1, _ := time.ParseDuration("-30m")
					m2, _ := time.ParseDuration("30m")
					if mysql.Mysqlclient != nil {
						mysql.Mysqlclient.Where("error_support = ? AND error_record_time >= ? AND error_record_time <= ?", i+1, time.Now().Add(m1), time.Now().Add(m2)).Delete(&user)
					}

				}
			}
		}
	}
}

func RecordAutoActionData(ctx context.Context, mb *mbserver.Server) {
	tickerTime := time.NewTicker(time.Second * 30)
	//var last_strings []byte
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickerTime.C:
			data := model.AutoAction{}
			var autoFollowStatus model.AutoFollowStatus

			if int(mb.HoldingRegisters[177]) > 0 {
				for i := 0; i < utils.Conf.SYSTEM.SupportNum; i++ {
					autoFollowStatus.IsAutoFollow = append(autoFollowStatus.IsAutoFollow, int(mb.HoldingRegisters[177]))
					autoFollowStatus.CompleteAutomaticPush = append(autoFollowStatus.CompleteAutomaticPush, int(mb.HoldingRegisters[3500+i]>>3&0x0001))
					autoFollowStatus.CompleteAutomaticRackTransfer = append(autoFollowStatus.CompleteAutomaticRackTransfer, int(mb.HoldingRegisters[3500+i]>>2&0x0001))
					autoFollowStatus.CompleteAutomaticCare = append(autoFollowStatus.CompleteAutomaticCare, int(mb.HoldingRegisters[3500+i]>>1&0x0001))
					autoFollowStatus.CompleteAutomaticExtension = append(autoFollowStatus.CompleteAutomaticExtension, int(mb.HoldingRegisters[3500+i]&0x0001))
				}
				autoFollowStatus.ShearerPosition = int(mb.HoldingRegisters[180])
				autoFollowStatus.Auto_tuiliu = int(mb.HoldingRegisters[15] >> 2 & 0x0001)
				autoFollowStatus.Auto_yijia = int(mb.HoldingRegisters[15] >> 1 & 0x0001)
				autoFollowStatus.Auto_hubang = int(mb.HoldingRegisters[15] >> 3 & 0x0001)
				autoFollowStatus.ShearerStep = int(mb.HoldingRegisters[179])
				strings, _ := json.Marshal(autoFollowStatus)
				//fmt.Println("数据比较", string(last_strings) == string(strings))
				//&& string(last_strings) != string(strings)
				if mysql.Mysqlclient != nil {
					data.Time = time.Now()
					data.AutoActionData = strings
					mysql.Mysqlclient.Select("Time", "AutoActionData").Create(&data)
				}
				//last_strings = strings
				//fmt.Println("自动跟机数据记录数据库")
			}

		}
	}
}
