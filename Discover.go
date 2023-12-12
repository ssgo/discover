package discover

import (
	"fmt"
	"github.com/ssgo/config"
	"github.com/ssgo/log"
	"github.com/ssgo/standard"
	"github.com/ssgo/u"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

type callInfoType struct {
	Timeout     time.Duration
	HttpVersion int
	Token       string
	SSL         bool
}

var _calls = map[string]*callInfoType{}

var myAddr = ""
var _appNodes = map[string]map[string]*NodeInfo{}

type NodeInfo struct {
	Addr        string
	Weight      int
	UsedTimes   uint64
	FailedTimes int
	Data        sync.Map
	//Data        map[string]interface{}
}

var settedRoute func(*AppClient, *http.Request) = nil
var settedLoadBalancer LoadBalancer = &DefaultLoadBalancer{}
var appSubscribeKeys []interface{}
var appClientPools = map[string]*httpclient.ClientPool{}

func IsServer() bool {
	return isServer
}
func IsClient() bool {
	return isClient
}

var _logger = log.New(u.ShortUniqueId())
var _inited = false

func logError(error string, extra ...interface{}) {
	if extra == nil {
		extra = make([]interface{}, 0)
	}
	extra = append(extra, "app", Config.App, "addr", myAddr)
	_logger.Error("Discover: "+error, extra...)
}

func logInfo(info string, extra ...interface{}) {
	if extra == nil {
		extra = make([]interface{}, 0)
	}
	extra = append(extra, "app", Config.App, "addr", myAddr)
	_logger.Info("Discover: "+info, extra...)
}

func SetLogger(logger *log.Logger) {
	_logger = logger
}

func Init() {
	if !_inited {
		_inited = true
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
		serverRedisPool = redis.GetRedis(Config.Registry, _logger)
		if serverRedisPool.Error != nil {
			logError(serverRedisPool.Error.Error())
		}

		// 注册节点
		if serverRedisPool.HSET(Config.App, addr, Config.Weight) {
			serverRedisPool.SETEX(Config.App+"_"+addr, 10, "1")
			//if r := serverRedisPool.Do("HSET " + Config.App, addr, Config.Weight); r.Error == nil {
			logInfo("registered")
			serverRedisPool.Do("PUBLISH", "CH_"+Config.App, fmt.Sprintf("%s %d", addr, Config.Weight))
			daemonRunning = true
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
		// 每1秒检查一次
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 100)
			if !daemonRunning {
				break
			}
		}
		if !daemonRunning {
			break
		}
		if isServer {
			if !serverRedisPool.HEXISTS(Config.App, myAddr) {
				logInfo("lost app registered info")
				// 注册节点
				if serverRedisPool.HSET(Config.App, myAddr, Config.Weight) {
					serverRedisPool.SETEX(Config.App+"_"+myAddr, 10, "1")
					logInfo("registered on daemon")
					serverRedisPool.Do("PUBLISH", "CH_"+Config.App, fmt.Sprintf("%s %d", myAddr, Config.Weight))
				} else {
					logError("register failed on daemon")
				}
			} else {
				// 保持存活
				serverRedisPool.SETEX(Config.App+"_"+myAddr, 10, "1")
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
		clientRedisPool = redis.GetRedis(Config.Registry, _logger)
	}

	confForPubSub := *clientRedisPool.Config
	confForPubSub.IdleTimeout = -1
	confForPubSub.ReadTimeout = -1
	if pubsubRedisPool == nil {
		newLogger := _logger.New(u.ShortUniqueId())
		pubsubRedisPool = redis.NewRedis(&confForPubSub, newLogger)
	}

	if isClient == false {
		isClient = true
	}

	// 如果之前已经启动
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
			serverRedisPool.DEL(Config.App + "_" + myAddr)
			logInfo("unregistered", "appSubscribeKeys", appSubscribeKeys)
			//log.Printf("DISCOVER	Unregistered	%s	%s	%d", Config.App, myAddr, 0)
			serverRedisPool.Do("PUBLISH", "CH_"+Config.App, fmt.Sprintf("%s %d", myAddr, 0))
		}
	}
}

//外部框架使用discover
func EasyStart() (string, int) {
	listener, err := net.Listen("tcp", os.Getenv("DISCOVER_LISTEN"))
	if err != nil {
		_logger.Error(err.Error())
		return "", 0
	}

	addrInfo := listener.Addr().(*net.TCPAddr)
	_ = listener.Close()

	ip := addrInfo.IP
	port := addrInfo.Port
	if !ip.IsGlobalUnicast() {
		// 如果监听的不是外部IP，使用第一个外部IP
		addrs, _ := net.InterfaceAddrs()
		//_logger.Warning("====1", Config.IpPrefix)
		for _, a := range addrs {
			an := a.(*net.IPNet)
			//_logger.Warning("====2", an.IP.To4().String())
			// 显式匹配网段
			if Config.IpPrefix != "" && strings.HasPrefix(an.IP.To4().String(), Config.IpPrefix) {
				ip = an.IP.To4()
				break
			}

			// 忽略 Docker 私有网段，匹配最后一个
			//_logger.Warning("====3", an.IP.To4().String(), an.IP.IsGlobalUnicast())
			if an.IP.IsGlobalUnicast() && !strings.HasPrefix(an.IP.To4().String(), "172.17.") {
				ip = an.IP.To4()
			}
		}
	}
	//_logger.Warning("====4", ip, port)
	addr := fmt.Sprintf("%s:%d", ip.String(), port)

	if Start(addr) == false {
		logError("failed to start discover")
		return "", 0
	}
	closeEsayChan := make(chan os.Signal, 2)
	signal.Notify(closeEsayChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-closeEsayChan
		if IsClient() || IsServer() {
			Stop()
		}
		//signal.Stop(closeEsayChan)
	}()

	return ip.String(), port
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

func AddExternalApp(app string, callConf string) bool {
	return addApp(app, callConf, true)
}

var numberMatcher, _ = regexp.Compile("^\\d+(s|ms|us|µs|ns?)?$")
var appLock = sync.RWMutex{}

func getCallInfo(app string) *callInfoType {
	appLock.RLock()
	info := _calls[app]
	appLock.RUnlock()
	return info
}

func addApp(app string, callConf string, fetch bool) bool {
	if appClientPools[app] != nil {
		return false
	}

	callInfo := callInfoType{
		Timeout:     10 * time.Second,
		HttpVersion: 2,
		SSL:         false,
		Token:       "",
	}
	for _, v := range u.SplitTrim(callConf, ":") {
		if v == "1" || v == "2" {
			callInfo.HttpVersion = u.Int(v)
		} else if v == "s" {
			callInfo.SSL = true
		} else if numberMatcher.MatchString(v) {
			callInfo.Timeout = u.Duration(v)
		} else {
			callInfo.Token = v
		}
	}

	var cp *httpclient.ClientPool
	if callInfo.HttpVersion == 1 || callInfo.SSL {
		cp = httpclient.GetClient(callInfo.Timeout)
	} else {
		cp = httpclient.GetClientH2C(callInfo.Timeout)
	}

	appLock.Lock()
	if Config.Calls == nil {
		Config.Calls = make(map[string]string)
	}
	Config.Calls[app] = callConf

	_appNodes[app] = map[string]*NodeInfo{}
	appSubscribeKeys = append(appSubscribeKeys, "CH_"+app)

	_calls[app] = &callInfo

	//if callInfo.Token != "" {
	//	cp.SetGlobalHeader("Access-Token", callInfo.Token)
	//}
	appClientPools[app] = cp
	appLock.Unlock()

	// 立刻获取一次应用信息
	if fetch {
		fetchApp(app)
	}
	return true
}

var syncConn *redigo.PubSubConn
var lastFetchTimeTag int64

func fetchApp(app string) {
	if clientRedisPool == nil {
		clientRedisPool = redis.GetRedis(Config.Registry, _logger)
	}

	appResults := clientRedisPool.Do("HGETALL", app).ResultMap()

	// 有调用时每3秒检查一次node的可用性
	fetchTimeTag := time.Now().Unix() // / 3
	if fetchTimeTag != lastFetchTimeTag {
		lastFetchTimeTag = fetchTimeTag
		for addr, _ := range appResults {
			checkKey := app + "_" + addr
			if !clientRedisPool.EXISTS(checkKey) {
				// 删除不存在了的节点
				clientRedisPool.HDEL(app, addr)
				delete(appResults, addr)
			}
		}
	}

	nodes := getAppNodes(app)

	for _, node := range nodes {
		if appResults[node.Addr] == nil {
			logInfo("remove node", "node", node, "nodes", nodes)
			//log.Printf("DISCOVER	Remove When Reset	%s	%s	%d", app, node.Addr, 0)
			pushNode(app, node.Addr, 0)
		}
	}
	for addr, weightResult := range appResults {
		weight := weightResult.Int()
		logInfo("update node", "app", app, "addr", addr, "weight", weightResult.Int())
		//log.Printf("DISCOVER	Reset	%s	%s	%d", app, addr, weight)
		pushNode(app, addr, weight)
	}
}

func getAppNodes(app string) map[string]*NodeInfo{
	appLock.RLock()
	nodes := map[string]*NodeInfo{}
	for k, v := range _appNodes[app] {
		nodes[k] = v
	}
	appLock.RUnlock()
	return nodes
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
			time.Sleep(time.Millisecond * 500)
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
				logInfo("received new registered info", "nodes", getAppNodes(app), "appSubscribeKeys", appSubscribeKeys)
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
		time.Sleep(time.Millisecond * 500)
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

//var nodesLock = sync.Mutex{}

func pushNode(app, addr string, weight int) {
	nodes := getAppNodes(app)
	//nodesLock.Lock()
	if weight == 0 {
		// 删除节点
		appLock.Lock()
		if _appNodes[app][addr] != nil {
			delete(_appNodes[app], addr)
		}
		appLock.Unlock()
	} else if nodes[addr] == nil {
		// 新节点
		var avgScore float64 = 0
		for _, node := range nodes {
			avgScore = float64(node.UsedTimes) / float64(node.Weight)
			break
		}
		usedTimes := uint64(avgScore) * uint64(weight)
		appLock.Lock()
		_appNodes[app][addr] = &NodeInfo{Addr: addr, Weight: weight, UsedTimes: usedTimes, Data: sync.Map{}}
		appLock.Unlock()
	} else if nodes[addr].Weight != weight {
		// 修改权重
		node := nodes[addr]
		node.Weight = weight
		node.UsedTimes = uint64(float64(node.UsedTimes) / float64(node.Weight) * float64(weight))
		appLock.Lock()
		_appNodes[app][addr] = node
		appLock.Unlock()
	}
	//nodesLock.Unlock()
}
