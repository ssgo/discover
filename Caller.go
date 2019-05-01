package discover

import (
	"fmt"
	"github.com/ssgo/standard"
	"net/http"
	"time"

	"github.com/ssgo/httpclient"
	"github.com/ssgo/log"
)

type Caller struct {
	Request *http.Request
	NoBody  bool
	logger  *log.Logger
}

func NewCaller(request *http.Request, logger *log.Logger) *Caller {
	return &Caller{Request: request, logger: logger}
}

func (caller *Caller) logError(error string, extra ...interface{}) {
	if caller.logger == nil {
		caller.logger = log.DefaultLogger
	}
	caller.logger.Error("Discover Caller: "+error, extra...)
}

func (caller *Caller) Get(app, path string, headers ...string) *httpclient.Result {
	return caller.Do("GET", app, path, nil, headers...)
}
func (caller *Caller) Post(app, path string, data interface{}, headers ...string) *httpclient.Result {
	return caller.Do("POST", app, path, data, headers...)
}
func (caller *Caller) Put(app, path string, data interface{}, headers ...string) *httpclient.Result {
	return caller.Do("PUT", app, path, data, headers...)
}
func (caller *Caller) Delete(app, path string, data interface{}, headers ...string) *httpclient.Result {
	return caller.Do("DELETE", app, path, data, headers...)
}
func (caller *Caller) Head(app, path string, data interface{}, headers ...string) *httpclient.Result {
	return caller.Do("HEAD", app, path, data, headers...)
}
func (caller *Caller) Do(method, app, path string, data interface{}, headers ...string) *httpclient.Result {
	r, _ := caller.DoWithNode(method, app, "", path, data, headers...)
	return r
}
func (caller *Caller) DoWithNode(method, app, withNode, path string, data interface{}, headers ...string) (*httpclient.Result, string) {
	appConf := config.Calls[app]
	if headers == nil {
		headers = []string{}
	}
	if appConf != nil && appConf.Headers != nil {
		for k, v := range appConf.Headers {
			headers = append(headers, k, v)
		}
	}

	if isServer {
		headers = append(headers, standard.DiscoverHeaderFromApp, config.App)
		headers = append(headers, standard.DiscoverHeaderFromNode, myAddr)
	}

	var r *httpclient.Result
	appClient := AppClient{logger: caller.logger}
	for {
		node := appClient.NextWithNode(app, withNode, caller.Request)
		if node == nil {
			break
		}

		// 请求节点
		startTime := time.Now()
		node.UsedTimes++
		scheme := "http"
		if appConf.WithSSL {
			scheme += "s"
		}
		if appClientPools[app].NoBody != caller.NoBody {
			appClientPools[app].NoBody = caller.NoBody
		}
		if caller.Request == nil {
			r = appClientPools[app].Do(method, fmt.Sprintf("%s://%s%s", scheme, node.Addr, path), data, headers...)
		} else {
			r = appClientPools[app].DoByRequest(caller.Request, method, fmt.Sprintf("%s://%s%s", scheme, node.Addr, path), data, headers...)
		}
		settedLoadBalancer.Response(node, r.Error, r.Response, startTime.UnixNano()-time.Now().UnixNano())

		if r.Error != nil || r.Response.StatusCode == 502 || r.Response.StatusCode == 503 || r.Response.StatusCode == 504 {
			statusCode := 0
			if r.Response != nil {
				statusCode = r.Response.StatusCode
			}
			caller.logError(r.Error.Error(),
				"app", app,
				"statusCode", statusCode,
				"path", path,
				"tryTimes", appClient.tryTimes,
				"node", node,
				"nodes", appNodes[app],
			)
			//log.Printf("DISCOVER	Failed	%s	%s	%d	%d	%d / %d	%d / %d	%d	%s", node.Addr, path, node.Weight, node.UsedTimes, appClient.tryTimes, len(appNodes[app]), node.FailedTimes, config.CallRetryTimes, statusCode, r.Error)
			// 错误处理
			node.FailedTimes++
			if node.FailedTimes >= config.CallRetryTimes {
				caller.logError(fmt.Sprint("call failed on ", node.FailedTimes, " times"),
					"app", app,
					"addr", node.Addr,
					"path", path,
					"weight", node.Weight,
					"usedTimes", node.UsedTimes,
					"tryTimes", appClient.tryTimes,
					"appNum", len(appNodes[app]),
					"failedTimes", node.FailedTimes,
					"retryLimit", config.CallRetryTimes,
					"statusCode", statusCode,
				)
				//log.Printf("DISCOVER	Removed	%s	%s	%d	%d	%d / %d	%d / %d	%d	%s", node.Addr, path, node.Weight, node.UsedTimes, appClient.tryTimes, len(appNodes[app]), node.FailedTimes, config.CallRetryTimes, statusCode, r.Error)
				if clientRedisPool.HDEL(config.RegistryPrefix+app, node.Addr) > 0 {
					clientRedisPool.Do("PUBLISH", config.RegistryPrefix+"CH_"+app, fmt.Sprintf("%s %d", node.Addr, 0))
				}
			}
		} else {
			// 成功
			return r, node.Addr
		}
	}

	// 全部失败，返回最后一个失败的结果
	return &httpclient.Result{Error: fmt.Errorf("CALL	%s	%s	No node avaliable	(%d / %d)", app, path, appClient.tryTimes, len(appNodes[app]))}, ""
}
