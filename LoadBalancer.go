package discover

import "net/http"

// 设置一个负载均衡算法
func SetLoadBalancer(lb LoadBalancer) {
	settedLoadBalancer = lb
}

type LoadBalancer interface {

	// 每个请求完成后提供信息
	Response(appClient *AppClient, node *NodeInfo, err error, response *http.Response, responseTimeing int64)

	// 请求时根据节点的得分取最小值发起请求
	Next(appClient *AppClient, nodes []*NodeInfo, request *http.Request) *NodeInfo
}

type DefaultLoadBalancer struct{}

func (lba *DefaultLoadBalancer) Response(appClient *AppClient, node *NodeInfo, err error, response *http.Response, responseTimeing int64) {
	//node.Data["score"] = float64(node.UsedTimes) / float64(node.Weight)
	node.Data.Store("score", float64(node.UsedTimes)/float64(node.Weight))
}

func (lba *DefaultLoadBalancer) Next(appClient *AppClient, nodes []*NodeInfo, request *http.Request) *NodeInfo {
	var minScore float64 = -1
	var minNode *NodeInfo = nil
	for _, node := range nodes {
		scoreValue, ok := node.Data.Load("score")
		if scoreValue == nil || !ok {
			scoreValue = float64(node.UsedTimes) / float64(node.Weight)
			node.Data.Store("score", scoreValue)
		}
		score := scoreValue.(float64)
		if minScore == -1 || score < minScore {
			minScore = score
			minNode = node
		}
	}
	return minNode
}
