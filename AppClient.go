package discover

import (
	"net/http"

	"github.com/ssgo/log"
)

type AppClient struct {
	excludes map[string]bool
	tryTimes int
	Logger   *log.Logger
	App      string
	Method   string
	Path     string
	Data     *map[string]interface{}
	Headers  *map[string]string
}

func (appClient *AppClient) logError(error string, extra ...interface{}) {
	if appClient.Logger == nil {
		appClient.Logger = log.DefaultLogger
	}
	appClient.Logger.Error("Discover Client: "+error, extra...)
}

func (appClient *AppClient) Next(app string, request *http.Request) *NodeInfo {
	return appClient.NextWithNode(app, "", request)
}

func (appClient *AppClient) CheckApp(app string) bool {
	nodes := getAppNodes(app)
	if nodes == nil {
		if !addApp(app, "", true) {
			appClient.logError("app not found", "app", app, "calls", Config.Calls)
			return false
			//} else {
			//	// 新app，重启订阅
			//	Restart()
		}
	}
	return true
}

func (appClient *AppClient) NextWithNode(app, withNode string, request *http.Request) *NodeInfo {
	if appClient.excludes == nil {
		appClient.excludes = map[string]bool{}
	}

	allNodes := getAppNodes(app)
	if allNodes == nil {
		appClient.logError("app not found", "app", app, "calls", Config.Calls)
		return nil
	}
	if len(allNodes) == 0 {
		appClient.logError("node not found", "app", app, "nodes", allNodes)
		return nil
	}

	appClient.tryTimes++
	if withNode != "" {
		appClient.excludes[withNode] = true
		return allNodes[withNode]
	}

	var node *NodeInfo
	readyNodes := make([]*NodeInfo, 0)
	for _, node := range allNodes {
		if appClient.excludes[node.Addr] || node.FailedTimes >= Config.CallRetryTimes {
			continue
		}
		readyNodes = append(readyNodes, node)
	}
	if len(readyNodes) == 0 {
		// 没有可用节点的情况下，尝试已经失败多次的节点
		for _, node := range allNodes {
			if appClient.excludes[node.Addr] {
				continue
			}
			readyNodes = append(readyNodes, node)
		}
	}
	if len(readyNodes) > 0 {
		node = settedLoadBalancer.Next(appClient, readyNodes, request)
		appClient.excludes[node.Addr] = true
	}
	if node == nil {
		appClient.logError("node not found", "app", app, "tryTimes", appClient.tryTimes, "nodes", allNodes)
	}

	return node
}
