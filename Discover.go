package discover

import (
	"fmt"
	"github.com/ssgo/log"
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
	FailedTimes uint8
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

func Start(addr string, conf Config) bool {
	myAddr = addr
	config = conf

	if config.CallTimeout <= 0 {
		config.CallTimeout = 5000
	}

	if config.Registry == "" {
		config.Registry = "discover:15"
	}
	if config.RegistryCalls == "" {
		config.RegistryCalls = config.Registry
	}
	if config.CallRetryTimes <= 0 {
		config.CallRetryTimes = 10
	}

	if config.App != "" && config.App[0] == '_' {
		log.Error("DC", map[string]interface{}{
			"error": "bad app name",
			"app":   config.App,
		})
		config.App = ""
	}

	if config.Weight <= 0 {
		config.Weight = 1
	}

	isServer = config.App != "" && config.Weight > 0
	if isServer {
		serverRedisPool = redis.GetRedis(config.Registry)

		// 注册节点
		if serverRedisPool.HSET(config.RegistryPrefix+config.App, addr, config.Weight) {
			log.Info("DC", map[string]interface{}{
				"info":   "registered",
				"app":    config.App,
				"addr":   addr,
				"weight": config.Weight,
			})
			serverRedisPool.Do("PUBLISH", config.RegistryPrefix+"CH_"+config.App, fmt.Sprintf("%s %d", addr, config.Weight))
			daemonRunning = true
			go daemon()
		} else {
			log.Error("DC", map[string]interface{}{
				"error":  "register failed",
				"app":    config.App,
				"addr":   addr,
				"weight": config.Weight,
			})
			return false
		}
	}

	if len(config.Calls) > 0 {
		for app, conf := range config.Calls {
			addApp(app, *conf, false)
		}
		if Restart() == false {
			return false
		}
	}
	return true
}

func daemon() {
	log.Info("DC", map[string]interface{}{
		"info":   "daemon thread started",
		"app":    config.App,
		"addr":   myAddr,
		"weight": config.Weight,
	})

	for {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second * 1)
			if !daemonRunning {
				break
			}
		}
		if isServer && !serverRedisPool.HEXISTS(config.RegistryPrefix+config.App, myAddr) {
			log.Info("DC", map[string]interface{}{
				"info":   "lost app registered info",
				"app":    config.App,
				"addr":   myAddr,
				"weight": config.Weight,
			})
			// 注册节点
			if serverRedisPool.HSET(config.RegistryPrefix+config.App, myAddr, config.Weight) {
				log.Info("DC", map[string]interface{}{
					"info":   "registered on daemon",
					"app":    config.App,
					"addr":   myAddr,
					"weight": config.Weight,
				})
				serverRedisPool.Do("PUBLISH", config.RegistryPrefix+"CH_"+config.App, fmt.Sprintf("%s %d", myAddr, config.Weight))
			} else {
				log.Error("DC", map[string]interface{}{
					"error":  "register failed on daemon",
					"app":    config.App,
					"addr":   myAddr,
					"weight": config.Weight,
				})
			}
		}
		if !daemonRunning {
			break
		}
	}

	log.Info("DC", map[string]interface{}{
		"info":   "daemon thread stopped",
		"app":    config.App,
		"addr":   myAddr,
		"weight": config.Weight,
	})

	if daemonStopChan != nil {
		daemonStopChan <- true
	}
}

func Restart() bool {
	//log.Info("DC", map[string]interface{}{
	//	"info":             "restarting",
	//	"app":              config.App,
	//	"addr":             myAddr,
	//	"weight":           config.Weight,
	//	"appSubscribeKeys": appSubscribeKeys,
	//})
	if clientRedisPool == nil {
		clientRedisPool = redis.GetRedis(config.RegistryCalls)
	}

	confForPubSub := *clientRedisPool.Config
	confForPubSub.IdleTimeout = -1
	confForPubSub.ReadTimeout = -1
	if pubsubRedisPool == nil {
		pubsubRedisPool = redis.NewRedis(&confForPubSub)
	}

	if isClient == false {
		isClient = true
	}

	// 如果之前没有启动
	if syncConn != nil {
		log.Info("DC", map[string]interface{}{
			"info":             "stopping",
			"app":              config.App,
			"addr":             myAddr,
			"weight":           config.Weight,
			"appSubscribeKeys": appSubscribeKeys,
		})
		//log.Print("DISCOVER	stopping")
		syncConn.Unsubscribe(appSubscribeKeys)
		syncConn.Close()
		syncConn = nil
		log.Info("DC", map[string]interface{}{
			"info":             "stopped",
			"app":              config.App,
			"addr":             myAddr,
			"weight":           config.Weight,
			"appSubscribeKeys": appSubscribeKeys,
		})
		//log.Print("DISCOVER	stopped")
	}

	// 如果之前没有启动
	if syncerRunning == false {
		log.Info("DC", map[string]interface{}{
			"info":             "starting",
			"app":              config.App,
			"addr":             myAddr,
			"weight":           config.Weight,
			"appSubscribeKeys": appSubscribeKeys,
		})
		//log.Print("DISCOVER	starting")
		syncerRunning = true
		initedChan := make(chan bool)
		go syncDiscover(initedChan)
		<-initedChan
		//go pingRedis()
		log.Info("DC", map[string]interface{}{
			"info":             "started",
			"app":              config.App,
			"addr":             myAddr,
			"weight":           config.Weight,
			"appSubscribeKeys": appSubscribeKeys,
		})
		//log.Print("DISCOVER	started")
	}
	return true
}

func Stop() {
	if isClient {
		syncerRunning = false
		if syncConn != nil {
			log.Info("DC", map[string]interface{}{
				"info":             "unsubscribing",
				"app":              config.App,
				"addr":             myAddr,
				"weight":           config.Weight,
				"appSubscribeKeys": appSubscribeKeys,
			})
			tmpConn := syncConn
			syncConn = nil
			//log.Print("DISCOVER	unsubscribing	", appSubscribeKeys)
			tmpConn.Unsubscribe(appSubscribeKeys)
			log.Info("DC", map[string]interface{}{
				"info":             "closing sync connection",
				"app":              config.App,
				"addr":             myAddr,
				"weight":           config.Weight,
				"appSubscribeKeys": appSubscribeKeys,
			})
			//log.Print("DISCOVER	closing syncConn")
			go func() {
				tmpConn.Close()
				log.Info("DC", map[string]interface{}{
					"info":             "sync connection closed",
					"app":              config.App,
					"addr":             myAddr,
					"weight":           config.Weight,
					"appSubscribeKeys": appSubscribeKeys,
				})
				//syncerStopChan <- true
				//pingStopChan <- true
				//log.Print("DISCOVER	closed syncConn")
			}()
		}
	}

	if isServer {
		daemonRunning = false
		if serverRedisPool.HDEL(config.RegistryPrefix+config.App, myAddr) > 0 {
			log.Info("DC", map[string]interface{}{
				"info":             "unregistered",
				"app":              config.App,
				"addr":             myAddr,
				"weight":           config.Weight,
				"appSubscribeKeys": appSubscribeKeys,
			})
			//log.Printf("DISCOVER	Unregistered	%s	%s	%d", config.App, myAddr, 0)
			serverRedisPool.Do("PUBLISH", config.RegistryPrefix+"CH_"+config.App, fmt.Sprintf("%s %d", myAddr, 0))
		}
	}
}

func Wait() {
	if isClient {
		log.Info("DC", "info", "waiting for client close")
		if syncerStopChan != nil {
			<-syncerStopChan
			syncerStopChan = nil
		}
		log.Info("DC", "info", "client close done")
		//if pingStopChan != nil {
		//	<-pingStopChan
		//	pingStopChan = nil
		//}
	}

	if isServer {
		log.Info("DC", "info", "waiting for server close")
		if daemonStopChan != nil {
			<-daemonStopChan
			daemonStopChan = nil
		}
		log.Info("DC", "info", "server close done")
	}
}

func AddExternalApp(app string, conf CallInfo) bool {
	return addApp(app, conf, true)
}

func addApp(app string, conf CallInfo, fetch bool) bool {
	if appClientPools[app] != nil {
		return false
	}
	if len(config.Calls) == 0 {
		config.Calls = make(map[string]*CallInfo)
	}
	if config.Calls[app] == nil {
		config.Calls[app] = &conf
	}

	appNodes[app] = map[string]*NodeInfo{}
	appSubscribeKeys = append(appSubscribeKeys, config.RegistryPrefix+"CH_"+app)

	timeout := conf.Timeout
	if timeout <= 0 {
		timeout = config.CallTimeout
	}
	var cp *httpclient.ClientPool
	if conf.HttpVersion == 1 {
		cp = httpclient.GetClient(time.Duration(timeout) * time.Millisecond)
	} else {
		cp = httpclient.GetClientH2C(time.Duration(timeout) * time.Millisecond)
	}
	appClientPools[app] = cp

	// 立刻获取一次应用信息
	if fetch {
		fetchApp(app)
	}

	return true
}

var syncConn *redigo.PubSubConn

//// 保持 redis 链接，否则会因为超时而发生错误
//func pingRedis() {
//	n := 15
//	if clientRedisPool.ReadTimeout > 2000 {
//		n = clientRedisPool.ReadTimeout / 1000 / 2
//	} else if clientRedisPool.ReadTimeout > 0 {
//		n = 1
//	}
//	for {
//		for i := 0; i < n; i++ {
//			time.Sleep(time.Second * 1)
//			if !syncerRunning {
//				break
//			}
//		}
//		if !syncerRunning {
//			break
//		}
//		if syncConn != nil {
//			//syncConn.Ping("1")
//		}
//		if !syncerRunning {
//			break
//		}
//		if isServer && !serverRedisPool.HEXISTS(config.RegistryPrefix+config.App, myAddr) {
//			log.Info("DC", map[string]interface{}{
//				"info":   "lost app registered info",
//				"app":    config.App,
//				"addr":   myAddr,
//				"weight": config.Weight,
//			})
//			// 注册节点
//			if serverRedisPool.HSET(config.RegistryPrefix+config.App, myAddr, config.Weight) {
//				log.Info("DC", map[string]interface{}{
//					"info":   "registered",
//					"app":    config.App,
//					"addr":   myAddr,
//					"weight": config.Weight,
//				})
//				serverRedisPool.Do("PUBLISH", config.RegistryPrefix+"CH_"+config.App, fmt.Sprintf("%s %d", myAddr, config.Weight))
//			} else {
//				log.Error("DC", map[string]interface{}{
//					"error":  "register failed",
//					"app":    config.App,
//					"addr":   myAddr,
//					"weight": config.Weight,
//				})
//			}
//		}
//		if !syncerRunning {
//			break
//		}
//	}
//	//if pingStopChan != nil {
//	//	pingStopChan <- true
//	//}
//}

func fetchApp(app string) {
	if clientRedisPool == nil {
		clientRedisPool = redis.GetRedis(config.RegistryCalls)
	}

	appResults := clientRedisPool.Do("HGETALL", config.RegistryPrefix+app).ResultMap()
	for _, node := range appNodes[app] {
		if appResults[node.Addr] == nil {
			log.Info("DC", map[string]interface{}{
				"info":   "remove node",
				"app":    app,
				"addr":   node.Addr,
				"weight": node.Weight,
				"node":   node,
				"nodes":  appNodes[app],
			})
			//log.Printf("DISCOVER	Remove When Reset	%s	%s	%d", app, node.Addr, 0)
			pushNode(app, node.Addr, 0)
		}
	}
	for addr, weightResult := range appResults {
		weight := weightResult.Int()
		log.Info("DC", map[string]interface{}{
			"info":   "update node",
			"app":    app,
			"addr":   addr,
			"weight": weight,
			"nodes":  appNodes[app],
		})
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
			log.Error("DC", map[string]interface{}{
				"error":            err.Error(),
				"appSubscribeKeys": appSubscribeKeys,
			})
			//log.Print("REDIS SUBSCRIBE	", err)
			syncConn.Close()
			syncConn = nil

			if !inited {
				log.Info("DC", map[string]interface{}{
					"info":   "sync thread started",
					"app":    config.App,
					"addr":   myAddr,
					"weight": config.Weight,
				})

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
		for app := range config.Calls {
			fetchApp(app)
			//appResults := clientRedisPool.Do("HGETALL", config.RegistryPrefix+app).ResultMap()
			//for _, node := range appNodes[app] {
			//	if appResults[node.Addr] == nil {
			//		log.Printf("DISCOVER	Remove When Reset	%s	%s	%d", app, node.Addr, 0)
			//		pushNode(app, node.Addr, 0)
			//	}
			//}
			//for addr, weightResult := range appResults {
			//	weight := weightResult.Int()
			//	log.Printf("DISCOVER	Reset	%s	%s	%d", app, addr, weight)
			//	pushNode(app, addr, weight)
			//}
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
				app := strings.Replace(v.Channel, config.RegistryPrefix+"CH_", "", 1)
				log.Info("DC", map[string]interface{}{
					"info":             "received new registered info",
					"app":              app,
					"weight":           weight,
					"nodes":            appNodes[app],
					"appSubscribeKeys": appSubscribeKeys,
				})
				//log.Printf("DISCOVER	Received	%s	%s	%d", app, addr, weight)
				pushNode(app, addr, weight)
			case redigo.Subscription:
			case redigo.Pong:
				//log.Print("	-0-0-0-0-0-0-	Pong")
			case error:
				if !strings.Contains(v.Error(), "connection closed") {
					log.Info("DC", map[string]interface{}{
						"error":            v.Error(),
						"appSubscribeKeys": appSubscribeKeys,
					})
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
		syncConn.Unsubscribe(appSubscribeKeys)
		//考虑goroutine的并发性，再做一次判断
		if syncConn != nil {
			syncConn.Close()
			syncConn = nil
		}
	}

	log.Info("DC", map[string]interface{}{
		"info":   "sync thread stopped",
		"app":    config.App,
		"addr":   myAddr,
		"weight": config.Weight,
	})

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
