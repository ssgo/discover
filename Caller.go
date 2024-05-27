package discover

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/ssgo/standard"
	"github.com/ssgo/u"
	"net/http"
	"reflect"
	"strings"
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
func (caller *Caller) Head(app, path string, headers ...string) *httpclient.Result {
	return caller.Do("HEAD", app, path, nil, headers...)
}
func (caller *Caller) Do(method, app, path string, data interface{}, headers ...string) *httpclient.Result {
	r, _ := caller.DoWithNode(method, app, "", path, data, headers...)
	return r
}
func (caller *Caller) ManualDo(method, app, path string, data interface{}, headers ...string) *httpclient.Result {
	r, _ := caller.DoWithNode(method, app, "", path, data, headers...)
	return r
}
func (caller *Caller) Open(app, path string, headers ...string) *websocket.Conn {
	r, _ := caller.doWithNode(false, "WS", app, "", path, nil, headers...)
	if v, ok := r.(*websocket.Conn); ok {
		return v
	} else {
		return nil
	}
}
func (caller *Caller) DoWithNode(method, app, withNode, path string, data interface{}, headers ...string) (*httpclient.Result, string) {
	r, nodeAddr := caller.doWithNode(false, method, app, withNode, path, data, headers...)
	if v, ok := r.(*httpclient.Result); ok {
		return v, nodeAddr
	} else {
		return nil, nodeAddr
	}
}
func (caller *Caller) ManualDoWithNode(method, app, withNode, path string, data interface{}, headers ...string) (*httpclient.Result, string) {
	r, nodeAddr := caller.doWithNode(true, method, app, withNode, path, data, headers...)
	if v, ok := r.(*httpclient.Result); ok {
		return v, nodeAddr
	} else {
		return nil, nodeAddr
	}
}
func (caller *Caller) doWithNode(manualDo bool, method, app, withNode, path string, data interface{}, headers ...string) (interface{}, string) {
	callerHeaders := map[string]string{}
	if headers != nil {
		for i := 1; i < len(headers); i += 2 {
			callerHeaders[headers[i-1]] = headers[i]
		}
	}

	if isServer {
		callerHeaders[standard.DiscoverHeaderFromApp] = Config.App
		callerHeaders[standard.DiscoverHeaderFromNode] = myAddr
	}

	callData := map[string]interface{}{}
	if data != nil && caller.NoBody == false && (settedRoute != nil || settedLoadBalancer != nil) {
		t := u.FinalType(reflect.ValueOf(data))
		if t.Kind() == reflect.Map || t.Kind() == reflect.Struct {
			u.Convert(data, &callData)
		}
	}

	var r *httpclient.Result
	var r2Conn *websocket.Conn
	appClient := AppClient{Logger: caller.logger, App: app, Method: method, Path: path, Data: &callData, Headers: &callerHeaders}
	if settedRoute != nil {
		settedRoute(&appClient, caller.Request)
		app = appClient.App
		method = appClient.Method
		path = appClient.Path
		if data != nil && caller.NoBody == false {
			data = callData
		}
	}

	if appClient.CheckApp(app) {
		callInfo := getCallInfo(app)
		//if callInfo != nil && callInfo.Token != "" && callerHeaders["Access-Token"] == "" {
		if callInfo != nil && callInfo.Token != "" {
			callerHeaders["Access-Token"] = callInfo.Token
		}

		settedHeaders := make([]string, 0)
		for k, v := range callerHeaders {
			settedHeaders = append(settedHeaders, k, v)
		}

		for {
			node := appClient.NextWithNode(app, withNode, caller.Request)
			if node == nil {
				break
			}
			// 请求节点
			startTime := time.Now()
			node.UsedTimes++
			scheme := "http"
			if callInfo != nil && callInfo.SSL {
				scheme += "s"
			}
			if appClientPools[app].NoBody != caller.NoBody {
				appClientPools[app].NoBody = caller.NoBody
			}

			if strings.ToLower(method) == "WS" {
				reqHeader := http.Header{}
				if settedHeaders != nil {
					for i := 1; i < len(settedHeaders); i += 2 {
						reqHeader.Set(settedHeaders[i-1], settedHeaders[i])
					}
				}
				if scheme == "https" {
					scheme = "wss"
				} else {
					scheme = "ws"
				}
				conn, r2Resp, r2Err := websocket.DefaultDialer.Dial(fmt.Sprintf("%s://%s%s", scheme, node.Addr, path), reqHeader)
				r2Conn = conn
				r = &httpclient.Result{
					Error:    r2Err,
					Response: r2Resp,
				}
			} else {
				if caller.Request == nil {
					if manualDo {
						r = appClientPools[app].ManualDo(method, fmt.Sprintf("%s://%s%s", scheme, node.Addr, path), data, settedHeaders...)
					} else {
						r = appClientPools[app].Do(method, fmt.Sprintf("%s://%s%s", scheme, node.Addr, path), data, settedHeaders...)
					}
				} else {
					if manualDo {
						r = appClientPools[app].ManualDoByRequest(caller.Request, method, fmt.Sprintf("%s://%s%s", scheme, node.Addr, path), data, settedHeaders...)
					} else {
						r = appClientPools[app].DoByRequest(caller.Request, method, fmt.Sprintf("%s://%s%s", scheme, node.Addr, path), data, settedHeaders...)
					}
				}
			}
			settedLoadBalancer.Response(&appClient, node, r.Error, r.Response, startTime.UnixNano()-time.Now().UnixNano())
			if r.Error != nil || r.Response.StatusCode == 502 || r.Response.StatusCode == 503 || r.Response.StatusCode == 504 {
				statusCode := 0
				if r.Response != nil {
					statusCode = r.Response.StatusCode
				}
				errStr := ""
				if r.Error != nil {
					errStr = r.Error.Error()
				} else {
					errStr = r.Response.Status
				}

				nodes := getAppNodes(app)

				caller.logError(errStr,
					"app", app,
					"statusCode", statusCode,
					"path", path,
					"tryTimes", appClient.tryTimes,
					"node", node,
					"nodes", nodes,
				)
				//log.Printf("DISCOVER	Failed	%s	%s	%d	%d	%d / %d	%d / %d	%d	%s", node.Addr, path, node.Weight, node.UsedTimes, appClient.tryTimes, len(nodes), node.FailedTimes, Config.CallRetryTimes, statusCode, r.Error)
				// 错误处理
				node.FailedTimes++
				if node.FailedTimes >= Config.CallRetryTimes {
					caller.logError(fmt.Sprint("call failed on ", node.FailedTimes, " times"),
						"app", app,
						"addr", node.Addr,
						"path", path,
						"weight", node.Weight,
						"usedTimes", node.UsedTimes,
						"tryTimes", appClient.tryTimes,
						"appNum", len(nodes),
						"failedTimes", node.FailedTimes,
						"retryLimit", Config.CallRetryTimes,
						"statusCode", statusCode,
					)
					//log.Printf("DISCOVER	Removed	%s	%s	%d	%d	%d / %d	%d / %d	%d	%s", node.Addr, path, node.Weight, node.UsedTimes, appClient.tryTimes, len(appNodes[app]), node.FailedTimes, Config.CallRetryTimes, statusCode, r.Error)
					if clientRedisPool.HDEL(app, node.Addr) > 0 {
						clientRedisPool.Do("PUBLISH", "CH_"+app, fmt.Sprintf("%s %d", node.Addr, 0))
					}
				}
			} else {
				// 成功
				if strings.ToLower(method) == "WS" {
					return r2Conn, node.Addr
				} else {
					return r, node.Addr
				}
			}
		}
	}

	// 全部失败，返回最后一个失败的结果
	errStr := fmt.Errorf("CALL	%s	%s	%s	No node avaliable	(%d / %d)", method, app, path, appClient.tryTimes, len(getAppNodes(app)))
	if strings.ToLower(method) == "WS" {
		caller.logger.Error(errStr.Error())
		return nil, ""
	} else {
		return &httpclient.Result{Error: errStr}, ""
	}
}
