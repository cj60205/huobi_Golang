package websocketclientbase

import (
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/huobirdcenter/huobi_golang/internal/gzip"
	"github.com/huobirdcenter/huobi_golang/internal/model"
	"github.com/huobirdcenter/huobi_golang/internal/requestbuilder"
	"github.com/huobirdcenter/huobi_golang/logging/applogger"
	"github.com/huobirdcenter/huobi_golang/pkg/model/auth"
	"github.com/huobirdcenter/huobi_golang/pkg/model/base"
)

const (
	websocketV2Path = "/ws/v2"
)

// It will be invoked after websocket v2 authentication response received
type AuthenticationV2ResponseHandler func(resp *auth.WebSocketV2AuthenticationResponse)

// The base class that responsible to get data from websocket authentication v2
type WebSocketV2ClientBase struct {
	host string
	conn *websocket.Conn
	tag  string

	authenticationResponseHandler AuthenticationV2ResponseHandler
	messageHandler                MessageHandler
	responseHandler               ResponseHandler

	stopReadChannel   chan int
	stopTickerChannel chan int
	ticker            *time.Ticker
	lastReceivedTime  time.Time
	mutex             *sync.Mutex
	sendMutex         *sync.Mutex

	requestBuilder *requestbuilder.WebSocketV2RequestBuilder
}

// Initializer
func (p *WebSocketV2ClientBase) Init(accessKey string, secretKey string, host string) *WebSocketV2ClientBase {
	p.host = host
	p.stopReadChannel = make(chan int, 1)
	p.stopTickerChannel = make(chan int, 1)
	p.requestBuilder = new(requestbuilder.WebSocketV2RequestBuilder).Init(accessKey, secretKey, host, websocketV2Path)
	p.mutex = &sync.Mutex{}
	p.sendMutex = &sync.Mutex{}
	return p
}

// Set callback handler
func (p *WebSocketV2ClientBase) SetHandler(authHandler AuthenticationV2ResponseHandler, msgHandler MessageHandler, repHandler ResponseHandler) {
	p.authenticationResponseHandler = authHandler
	p.messageHandler = msgHandler
	p.responseHandler = repHandler
}

// Connect to websocket server
// if autoConnect is true, then the connection can be re-connect if no data received after the pre-defined timeout
func (p *WebSocketV2ClientBase) Connect(autoConnect bool, tag string) {
	// set tag
	p.tag = fmt.Sprintf("[%s]", tag)

	// connect to websocket
	p.connectWebSocket()

	// start loop to read and handle message
	p.startReadLoop()

	// start ticker to manage connection
	if autoConnect {
		p.startTicker()
	}

}

// Send data to websocket server
func (p *WebSocketV2ClientBase) Send(data string) {
	if p.conn == nil {
		applogger.Error("%s WebSocket sent error: no connection available", p.tag)
		return
	}

	p.sendMutex.Lock()
	err := p.conn.WriteMessage(websocket.TextMessage, []byte(data))
	p.sendMutex.Unlock()

	if err != nil {
		applogger.Error("%s WebSocket sent error: data=%s, error=%s", p.tag, data, err)
	}
}

// Close the connection to server
func (p *WebSocketV2ClientBase) Close() {
	p.stopReadLoop()
	p.stopTicker()
	p.disconnectWebSocket()
}

// connect to server
func (p *WebSocketV2ClientBase) connectWebSocket() bool {
	var err error

	url := fmt.Sprintf("wss://%s%s", p.host, websocketV2Path)
	applogger.Debug("%s WebSocket connecting...", p.tag)
	p.conn, _, err = websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		applogger.Error("%s WebSocket connected error: %s", p.tag, err)
		return false
	}
	applogger.Info("%s WebSocket connected", p.tag)

	auth, err := p.requestBuilder.Build()
	if err != nil {
		applogger.Error("%s Signature generated error: %s", p.tag, err)
		return false
	}

	p.Send(auth)
	return true
}

// disconnect with server
func (p *WebSocketV2ClientBase) disconnectWebSocket() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.conn == nil {
		return
	}

	applogger.Debug("%s WebSocket disconnecting...", p.tag)
	err := p.conn.Close()
	if err != nil {
		applogger.Error("%s WebSocket disconnect error: %s", p.tag, err)
		return
	}

	applogger.Info("%s WebSocket disconnected", p.tag)
}

func (p *WebSocketV2ClientBase) getLastReceivedTime() time.Time {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.lastReceivedTime
}

func (p *WebSocketV2ClientBase) setLastReceivedTime(t time.Time) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.lastReceivedTime = t
}

// initialize a ticker and start a goroutine tickerLoop()
func (p *WebSocketV2ClientBase) startTicker() {
	p.ticker = time.NewTicker(TimerIntervalSecond * time.Second)
	p.setLastReceivedTime(time.Now())

	go p.tickerLoop()
}

// stop ticker and stop the goroutine
func (p *WebSocketV2ClientBase) stopTicker() {
	p.ticker.Stop()
	p.stopTickerChannel <- 1
}

// defines a for loop that will run based on ticker's frequency
// It checks the last data that received from server, if it is longer than the threshold,
// it will force disconnect server and connect again.
func (p *WebSocketV2ClientBase) tickerLoop() {
	applogger.Debug("%s tickerLoop started", p.tag)
	for {
		select {
		// start a goroutine readLoop()
		case <-p.stopTickerChannel:
			applogger.Debug("%s tickerLoop stopped", p.tag)
			return

		// Receive tick from tickChannel
		case <-p.ticker.C:
			elapsedSecond := time.Since(p.getLastReceivedTime()).Seconds()
			applogger.Debug("%s WebSocket received data %f sec ago", p.tag, elapsedSecond)

			if elapsedSecond > ReconnectWaitSecond {
				applogger.Warn("%s WebSocket reconnect...", p.tag)
				p.disconnectWebSocket()
				time.Sleep(TimerIntervalSecond * time.Second)
				// reset last received time as now
				p.setLastReceivedTime(time.Now())
			}
		}
	}
}

// start a goroutine readLoop()
func (p *WebSocketV2ClientBase) startReadLoop() {
	go p.readLoop()
}

// stop the goroutine readLoop()
func (p *WebSocketV2ClientBase) stopReadLoop() {
	p.stopReadChannel <- 1
}

// defines a for loop to read data from server
// it will stop once it receives the signal from stopReadChannel
func (p *WebSocketV2ClientBase) readLoop() {
	applogger.Debug("%s readLoop started", p.tag)
	var reading = true
	for {
		select {
		// Receive data from stopChannel
		case <-p.stopReadChannel:
			applogger.Debug("%s readLoop stopped", p.tag)
			return

		default:
			if p.conn == nil || !reading {
				applogger.Error("%s Read error: no connection available", p.tag)
				for !p.connectWebSocket() {
					time.Sleep(TimerIntervalSecond * time.Second)
				}
				reading = true
				continue
			}

			msgType, buf, err := p.conn.ReadMessage()
			if err != nil {
				applogger.Error("%s Read error: %s", p.tag, err)
				p.disconnectWebSocket()
				reading = false
				time.Sleep(TimerIntervalSecond * time.Second)
				continue
			}

			p.setLastReceivedTime(time.Now())

			// decompress gzip data if it is binary message
			var message string
			if msgType == websocket.BinaryMessage {
				message, err = gzip.GZipDecompress(buf)
				if err != nil {
					applogger.Error("%s UnGZip data error: %s", p.tag, err)
					return
				}
			} else if msgType == websocket.TextMessage {
				message = string(buf)
			}

			// Try to pass as PingV2Message
			// If it is Ping then respond Pong
			pingV2Msg := model.ParsePingV2Message(message)
			if pingV2Msg.IsPing() {
				applogger.Debug("%s Received Ping: %d", p.tag, pingV2Msg.Data.Timestamp)
				pongMsg := fmt.Sprintf("{\"action\": \"pong\", \"data\": { \"ts\": %d } }", pingV2Msg.Data.Timestamp)
				p.Send(pongMsg)
				applogger.Debug("%s Respond Pong: %d", p.tag, pingV2Msg.Data.Timestamp)
			} else {
				// Try to pass as websocket v2 authentication response
				// If it is then invoke authentication handler
				wsV2Resp := base.ParseWSV2Resp(message)
				if wsV2Resp != nil {
					switch wsV2Resp.Action {
					case "req":
						authResp := auth.ParseWSV2AuthResp(message)
						if authResp != nil && p.authenticationResponseHandler != nil {
							p.authenticationResponseHandler(authResp)
						}

					case "sub", "push":
						{
							result, err := p.messageHandler(message)
							if err != nil {
								applogger.Error("%s Handle message error: %s", p.tag, err)
								continue
							}
							if p.responseHandler != nil {
								p.responseHandler(result)
							}
						}
					}
				}
			}
		}
	}
}
