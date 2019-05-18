package discover

import (
	"fmt"
	"github.com/ssgo/config"
	"github.com/ssgo/log"
	"github.com/ssgo/standard"
	"github.com/ssgo/u"
	"strconv"
	"strings"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/ssgo/httpclient"
	"github.com/ssgo/redis"
)

var serverRedisPool *redis.Redis
var clientRedisPool *redis.Redis
var pubsubRedisPool *redis.Redis
var isServer = false
var isClient = false
var daemonRunning = false
var syncerRunning = false
var syncerStopChan chan bool
var daemonStopChan chan bool
//var pingStopChan chan bool

var myAddr = ""
var appNodes = map[string]map[string]*NodeInfo{}

type NodeInfo struct {
	Addr        string
	Weight      int
	UsedTimes   uint64
	FailedTimes int
	Data        interface{}
}

var settedLoadBalancer LoadBalancer = &DefaultLoadBalancer{}
var appSubscribeKeys []interface{}
var appClientPools = map[string]*httpclient.ClientPool{}

func IsServer() bool {
	return isServer
}
func IsClient() bool {
	return isClient
}

var logger = log.New(u.ShortUniqueId())
var inited = false

func logError(error string, extra ...interface{}) {
	if extra == nil {
		extra = make([]interface{}, 0)
	}
	extra = append(extra, "app", Config.App, "addr", myAddr, "weight", Config.Weight)
	logger.Error("Discover: "+error, extra...)
}

func logInfo(info string, extra ...interface{}) {
	if extra == nil {
		extra = make([]interface{}, 0)
	}
	extra = append(extra, "app", Config.App, "addr", myAddr, "weight", Config.Weight)
	logger.Info("Discover: "+info, extra...)
}

func Init() {
	if !inited {
		inited = true
		config.LoadConfig("discover", &Config)

		if Config.Registry == "" {
			Config.Registry = standard.DiscoverDefaultRegistry // 127.0.0.1:6379::15
		}
		if Config.CallRetryTimes <= 0 {
			Config.CallRetryTimes = 10
		}

		//if Config.App != "" && Config.App[0] == '_' {
		//	logError("bad app name")
		//	Config.App = ""
		//}

		if Config.Weight <= 0 {
			Config.Weight = 100
		}
	}
}

func Start(addr string) bool {
	Init()
	myAddr = addr

	isServer = Config.App != "" && Config.Weight > 0
	if isServer {
		serverRedisPool = redis.GetRedis(Config.Registry, logger)
		if serverRedisPool.Error != nil {
			logError(serverRedisPool.Error.Error())
		}

		// 注册节点
		if serverRedisPool.HSET(Config.App, addr, Config.Weight) {
		//if r := serverRedisPool.Do("HSET " + Config.App, addr, Config.Weight); r.Error == nil {
			logInfo("registered")
			serverRedisPool.Do("PUBLISH", "CH_"+Config.App, fmt.Sprintf("%s %d", addr, Config.Weight))
			daemonRunning = true
			//fmt.Println("  ####1", r.Error, r.String())
			go daemon()
		} else {
			logError("register failed") // TODO ????????
			//return false
		}
	}

	if Config.Calls != nil && len(Config.Calls) > 0 {
		for app, conf := range Config.Calls {
			addApp(app, conf, false)
		}
		if Restart() == false {
			return false
		}
	}
	return true
}

func daemon() {
	logInfo("daemon thread started")

	for {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second * 1)
			if !daemonRunning {
				break
			}
		}
		if isServer && !serverRedisPool.HEXISTS(Config.App, myAddr) {
			logInfo("lost app registered info")
			// 注册节点
			if serverRedisPool.HSET(Config.App, myAddr, Config.Weight) {
				logInfo("registered on daemon")
				serverRedisPool.Do("PUBLISH", "CH_"+Config.App, fmt.Sprintf("%s %d", myAddr, Config.Weight))
			} else {
				logError("register failed on daemon")
			}
		}
		if !daemonRunning {
			break
		}
	}

	logInfo("daemon thread stopped")

	if daemonStopChan != nil {
		daemonStopChan <- true
	}
}

func Restart() bool {
	if clientRedisPool == nil {
		clientRedisPool = redis.GetRedis(Config.Registry, logger)
	}

	confForPubSub := *clientRedisPool.Config
	confForPubSub.IdleTimeout = -1
	confForPubSub.ReadTimeout = -1
	if pubsubRedisPool == nil {
		newLogger := logger.New(u.ShortUniqueId())
		pubsubRedisPool = redis.NewRedis(&confForPubSub, newLogger)
	}

	if isClient == false {
		isClient = true
	}

	// 如果之前没有启动
	if syncConn != nil {
		logInfo("stopping", "appSubscribeKeys", appSubscribeKeys)
		//log.Print("DISCOVER	stopping")
		_ = syncConn.Unsubscribe(appSubscribeKeys)
		_ = syncConn.Close()
		syncConn = nil
		logInfo("stopped", "appSubscribeKeys", appSubscribeKeys)
		//log.Print("DISCOVER	stopped")
	}

	// 如果之前没有启动
	if syncerRunning == false {
		logInfo("starting", "appSubscribeKeys", appSubscribeKeys)
		//log.Print("DISCOVER	starting")
		syncerRunning = true
		initedChan := make(chan bool)
		go syncDiscover(initedChan)
		<-initedChan
		//go pingRedis()
		logInfo("started", "appSubscribeKeys", appSubscribeKeys)
		//log.Print("DISCOVER	started")
	}
	return true
}

func Stop() {
	if isClient {
		syncerRunning = false
		if syncConn != nil {
			logInfo("unsubscribing", "appSubscribeKeys", appSubscribeKeys)
			tmpConn := syncConn
			syncConn = nil
			//log.Print("DISCOVER	unsubscribing	", appSubscribeKeys)
			_ = tmpConn.Unsubscribe(appSubscribeKeys)
			logInfo("closing sync connection", "appSubscribeKeys", appSubscribeKeys)
			//log.Print("DISCOVER	closing syncConn")
			go func() {
				_ = tmpConn.Close()
				logInfo("sync connection closed", "appSubscribeKeys", appSubscribeKeys)
				//syncerStopChan <- true
				//pingStopChan <- true
				//log.Print("DISCOVER	closed syncConn")
			}()
		}
	}

	if isServer {
		daemonRunning = false
		if serverRedisPool.HDEL(Config.App, myAddr) > 0 {
			logInfo("unregistered", "appSubscribeKeys", appSubscribeKeys)
			//log.Printf("DISCOVER	Unregistered	%s	%s	%d", Config.App, myAddr, 0)
			serverRedisPool.Do("PUBLISH", "CH_"+Config.App, fmt.Sprintf("%s %d", myAddr, 0))
		}
	}
}

func Wait() {
	if isClient {
		logInfo("waiting for client close")
		if syncerStopChan != nil {
			<-syncerStopChan
			syncerStopChan = nil
		}
		logInfo("client close done")
		//if pingStopChan != nil {
		//	<-pingStopChan
		//	pingStopChan = nil
		//}
	}

	if isServer {
		logInfo("waiting for server close")
		if daemonStopChan != nil {
			<-daemonStopChan
			daemonStopChan = nil
		}
		logInfo("server close done")
	}
}

func AddExternalApp(app string, callInfo string) bool {
	return addApp(app, callInfo, true)
}

func addApp(app string, callInfo string, fetch bool) bool {
	if appClientPools[app] != nil {
		return false
	}
	if Config.Calls == nil {
		Config.Calls = make(map[string]string)
	}
	if Config.Calls[app] == "" {
		Config.Calls[app] = callInfo
	}

	appNodes[app] = map[string]*NodeInfo{}
	appSubscribeKeys = append(appSubscribeKeys, "CH_"+app)

	callInfoArr := u.SplitTrim(callInfo, ":")
	callTimeout := 0
	if len(callInfoArr) > 0 {
		callTimeout = u.Int(callInfoArr[0])
	}
	if callTimeout <= 0 {
		callTimeout = 10000
	}
	callToken := ""
	callHttpVersion := 2
	if len(callInfoArr) > 1 {
		callToken = callInfoArr[1]
	}
	if len(callInfoArr) > 2 {
		if callInfoArr[2] == "1" {
			callHttpVersion = 1
		}
	}

	var cp *httpclient.ClientPool
	if callHttpVersion == 1 {
		cp = httpclient.GetClient(time.Duration(callTimeout) * time.Millisecond)
	} else {
		cp = httpclient.GetClientH2C(time.Duration(callTimeout) * time.Millisecond)
	}
	if callToken != "" {
		cp.SetGlobalHeader("Access-Token", callToken)
	}
	appClientPools[app] = cp

	// 立刻获取一次应用信息
	if fetch {
		fetchApp(app)
	}

	return true
}

var syncConn *redigo.PubSubConn

func fetchApp(app string) {
	if clientRedisPool == nil {
		clientRedisPool = redis.GetRedis(Config.Registry, logger)
	}

	appResults := clientRedisPool.Do("HGETALL", app).ResultMap()
	for _, node := range appNodes[app] {
		if appResults[node.Addr] == nil {
			logInfo("remove node", "node", node, "nodes", appNodes[app])
			//log.Printf("DISCOVER	Remove When Reset	%s	%s	%d", app, node.Addr, 0)
			pushNode(app, node.Addr, 0)
		}
	}
	for addr, weightResult := range appResults {
		weight := weightResult.Int()
		logInfo("update node", "nodes", appNodes[app])
		//log.Printf("DISCOVER	Reset	%s	%s	%d", app, addr, weight)
		pushNode(app, addr, weight)
	}
}

func syncDiscover(initedChan chan bool) {
	inited := false
	for {
		syncConn = &redigo.PubSubConn{Conn: pubsubRedisPool.GetConnection()}
		err := syncConn.Subscribe(appSubscribeKeys...)
		if err != nil {
			logError(err.Error(), "appSubscribeKeys", appSubscribeKeys)
			//log.Print("REDIS SUBSCRIBE	", err)
			_ = syncConn.Close()
			syncConn = nil

			if !inited {
				logInfo("sync thread started")

				inited = true
				initedChan <- true
			}
			time.Sleep(time.Second * 1)
			if !syncerRunning {
				break
			}
			continue
		}

		// 第一次或断线后重新获取（订阅开始后再获取全量确保信息完整）
		for app := range Config.Calls {
			fetchApp(app)
		}
		if !inited {
			inited = true
			initedChan <- true
		}
		if !syncerRunning {
			break
		}

		// 开始接收订阅数据
		for {
			isErr := false
			receiveObj := syncConn.Receive()
			switch v := receiveObj.(type) {
			case redigo.Message:
				a := strings.Split(string(v.Data), " ")
				addr := a[0]
				weight := 0
				if len(a) == 2 {
					weight, _ = strconv.Atoi(a[1])
				}
				app := strings.Replace(v.Channel, "CH_", "", 1)
				logInfo("received new registered info", "nodes", appNodes[app], "appSubscribeKeys", appSubscribeKeys)
				//log.Printf("DISCOVER	Received	%s	%s	%d", app, addr, weight)
				pushNode(app, addr, weight)
			case redigo.Subscription:
			case redigo.Pong:
				//log.Print("	-0-0-0-0-0-0-	Pong")
			case error:
				if !strings.Contains(v.Error(), "connection closed") {
					logInfo(v.Error(), "appSubscribeKeys", appSubscribeKeys)
					//log.Printf("REDIS RECEIVE ERROR	%s", v)
				}
				isErr = true
				break
			}
			if isErr {
				break
			}
			if !syncerRunning {
				break
			}
		}
		if !syncerRunning {
			break
		}
		time.Sleep(time.Second * 1)
		if !syncerRunning {
			break
		}
	}

	if syncConn != nil {
		_ = syncConn.Unsubscribe(appSubscribeKeys)
		//考虑goroutine的并发性，再做一次判断
		if syncConn != nil {
			_ = syncConn.Close()
			syncConn = nil
		}
	}

	logInfo("sync thread stopped")

	if syncerStopChan != nil {
		syncerStopChan <- true
	}
}

func pushNode(app, addr string, weight int) {
	if weight == 0 {
		// 删除节点
		if appNodes[app][addr] != nil {
			delete(appNodes[app], addr)
		}
	} else if appNodes[app][addr] == nil {
		// 新节点
		var avgScore float64 = 0
		for _, node := range appNodes[app] {
			avgScore = float64(node.UsedTimes) / float64(weight)
		}
		usedTimes := uint64(avgScore) * uint64(weight)
		appNodes[app][addr] = &NodeInfo{Addr: addr, Weight: weight, UsedTimes: usedTimes}
	} else if appNodes[app][addr].Weight != weight {
		// 修改权重
		node := appNodes[app][addr]
		node.Weight = weight
		node.UsedTimes = uint64(float64(node.UsedTimes) / float64(node.Weight) * float64(weight))
	}
}
