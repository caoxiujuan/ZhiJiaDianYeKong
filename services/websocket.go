package service

import (
	"context"
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	uuid "github.com/satori/go.uuid"
)

func (manager *Manager) Start(ctx context.Context) {
	log.Printf("websocket manage start")
	for {
		select {
		case <-ctx.Done():
			log.Printf("websocket manage stop")
			return
		// 注册
		case client := <-manager.Register:
			//log.Printf("client [%s] connect", client.Id)
			log.Printf("register client [%s] to group [%s]", client.Id, client.Group)

			manager.Lock.Lock()
			//log.Printf(" group length [%d]", len(manager.Group))

			manager.Group[client.Id] = client
			manager.clientCount += 1
			log.Printf(" group length [%d]", len(manager.Group))
			manager.Lock.Unlock()

		// 注销
		case client := <-manager.UnRegister:
			log.Printf("unregister client [%s] from group [%s]", client.Id, client.Group)
			manager.Lock.Lock()
			// close(client.Message)
			// delete(manager.Group, client.Id)
			// manager.clientCount -= 1
			// if manager.clientCount == 0 {
			// 	log.Printf("delete empty group [%s]", client.Group)
			// 	delete(manager.Group, client.Group)
			// }
			if _, ok := manager.Group[client.Id]; ok {
				delete(manager.Group, client.Id)
				manager.clientCount -= 1
				// 关闭通道通知 Write 协程退出
				close(client.Message)
			}
			manager.Lock.Unlock()

			// 发送广播数据到某个组的 channel 变量 Send 中
		case data := <-manager.BroadCastMessage:
			manager.Lock.Lock()
			// 预分配切片容量，减少内存分配
			clients := make([]*Client, 0, len(manager.Group))
			for _, conn := range manager.Group {
				clients = append(clients, conn)
			}
			manager.Lock.Unlock()
			for _, conn := range clients {
				// --- 增加 recover 保护，防止向已关闭 channel 发送导致的 panic ---
				func(c *Client) {
					defer func() {
						if r := recover(); r != nil {
							// 捕获 panic，防止服务崩溃
							log.Printf("Recovered from broadcast panic for client [%s]: %v", c.Id, r)
						}
					}()

					// 检查缓冲区是否已满
					if len(c.Message) >= cap(c.Message) {
						log.Printf("client [%s] channel full, try unregister", c.Id)
						// 使用 select default 防止阻塞导致死锁
						select {
						case manager.UnRegister <- c:
						default:
						}
						return
					}

					// 发送消息
					select {
					case c.Message <- data.Message:
					default:
						// 发送失败（缓冲区满或已关闭），尝试注销
						select {
						case manager.UnRegister <- c:
						default:
						}
					}
				}(conn)
			}
			// for _, conn := range manager.Group {
			// 	if len(conn.Message) == cap(conn.Message) {
			// 		log.Println("无法正常发送websocket消息,主动注销客户端")
			// 		WebsocketManager.UnRegister <- conn
			// 		continue
			// 	} else {
			// 		conn.Message <- data.Message
			// 	}

			// }

		case data := <-manager.Message:
			manager.Lock.Lock()
			if client, ok := manager.Group[data.Id]; ok {
				// 检查客户端的消息队列是否已满，防止阻塞
				if len(client.Message) < cap(client.Message) {
					client.Message <- data.Message
				} else {
					log.Printf("无法向客户端 [%s] 发送消息，队列已满", data.Id)
				}
			} else {
				log.Printf("目标客户端 [%s] 不存在或已断线", data.Id)
			}
			manager.Lock.Unlock()
		}
	}
}

// 注册
func (manager *Manager) RegisterClient(client *Client) {
	manager.Register <- client
}

// 注销
func (manager *Manager) UnRegisterClient(client *Client) {
	manager.UnRegister <- client
}

// 向指定的 client 发送数据
func (manager *Manager) Send(id string, message []byte) {
	data := &MessageData{
		Id:      id,
		Message: message,
	}
	manager.Message <- data
}

// 广播
func (manager *Manager) SendAll(message []byte) {
	data := &BroadCastMessageData{
		Message: message,
	}
	//fmt.Println("广播消息", len(manager.BroadCastMessage))
	manager.BroadCastMessage <- data
}

// Client 单个 websocket 信息
type Client struct {
	Id, Group string
	Socket    *websocket.Conn
	Message   chan []byte
}

// MessageData 单个发送数据信息
type MessageData struct {
	Id      string
	Message []byte
}

// 广播发送数据信息
type BroadCastMessageData struct {
	Message []byte
}
type Manager struct {
	Group                map[string]*Client
	clientCount          int
	Lock                 sync.Mutex
	Register, UnRegister chan *Client
	Message              chan *MessageData
	BroadCastMessage     chan *BroadCastMessageData
}

var (
	// 初始化 wsManager 管理器
	WebsocketManager = Manager{
		Group:            make(map[string]*Client),
		Register:         make(chan *Client, 1024),
		UnRegister:       make(chan *Client, 1024),
		Message:          make(chan *MessageData, 1024),
		BroadCastMessage: make(chan *BroadCastMessageData, 512),
		clientCount:      0,
	}
	upgrader = websocket.Upgrader{
		// 解决跨域问题
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

// func (c *Client) Read() {
// 	defer func() {
// 		// 防止 UnRegister channel 阻塞导致协程泄露，使用 try-send
// 		select {
// 		case WebsocketManager.UnRegister <- c:
// 		default:
// 			log.Printf("UnRegister channel full, client [%s] read exit maybe pending", c.Id)
// 		}

// 		// 即使注销失败也要关闭 Socket，释放资源
// 		_ = c.Socket.Close()
// 	}()

// 	for {
// 		// 设置读取超时，防止长时间未通讯的僵尸连接
// 		_ = c.Socket.SetReadDeadline(time.Now().Add(60 * time.Second))

// 		messageType, _, err := c.Socket.ReadMessage()
// 		if err != nil || messageType == websocket.CloseMessage {
// 			// 移除 string(message) 转换，避免大内存分配和无效日志
// 			if err != nil {
// 				log.Printf("client [%s] read error: %v", c.Id, err)
// 			} else {
// 				log.Printf("client [%s] receive close message", c.Id)
// 			}
// 			break
// 		}
// 		// 如果需要处理客户端发来的消息，在这里处理
// 	}
// }

// func (c *Client) Write() {
// 	defer func() {
// 		log.Printf("client [%s] disconnect", c.Id)
// 		if err := c.Socket.Close(); err != nil {
// 			log.Printf("client write [%s] disconnect err: %s", c.Id, err)
// 		}
// 	}()

// 	for {
// 		select {
// 		case message, ok := <-c.Message:
// 			if !ok {
// 				// Channel 被关闭，正常退出
// 				return
// 			}

// 			// 设置写入超时
// 			_ = c.Socket.SetWriteDeadline(time.Now().Add(10 * time.Second))
// 			err := c.Socket.WriteMessage(websocket.TextMessage, message)
// 			if err != nil {
// 				log.Printf("client [%s] write message error: %v", c.Id, err)
// 				return
// 			}
// 		}
// 	}
// }

// 读信息，从 websocket 连接直接读取数据
func (c *Client) Read() {
	defer func() {
		select {
		case WebsocketManager.UnRegister <- c:
		default:
			//log.Printf("UnRegister channel full, client [%s] read exit maybe pending", c.Id)
		}
		//log.Printf("client [%s] disconnect", c.Id)
		if err := c.Socket.Close(); err != nil {
			log.Printf("client read [%s] disconnect err: %s", c.Id, err)
		}
	}()

	for {
		messageType, message, err := c.Socket.ReadMessage()
		if err != nil || messageType == websocket.CloseMessage {
			log.Printf("client [%s] receive read message: %s, err: %s", c.Id, string(message), err)
			break
		}

		//回发给websocket
		//c.Message <- message
	}
}

// 写信息，从 channel 变量 Send 中读取数据写入 websocket 连接
func (c *Client) Write() {
	defer func() {
		log.Printf("client [%s] disconnect", c.Id)
		if err := c.Socket.Close(); err != nil {
			log.Printf("client write [%s] disconnect err: %s", c.Id, err)
		}
	}()

	for {
		select {
		case message, ok := <-c.Message:
			if !ok {
				_ = c.Socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			// log.Printf("client [%s] write message: %s", c.Id, string(message))
			err := c.Socket.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				log.Printf("client [%s] writemessage err: %s", c.Id, message, err)
				//log.Printf("client [%s] writemessage err: %s", c.Id, err)
				return
			}
		}
	}
}
func (manager *Manager) WsClient(c *gin.Context) {
	// use default options
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	client := &Client{
		Id:      uuid.NewV4().String(),
		Socket:  ws,
		Message: make(chan []byte, 256),
	}
	manager.RegisterClient(client)
	go client.Write()
	go client.Read()

}
