package discover

var Config = struct {
	Registry       string            // redis://:@127.0.0.1:6379/15
	App            string            // register to a app service
	Weight         int               // 100
	Calls          map[string]string // defines which apps will call
	CallRetryTimes int               // 10
	IpPrefix       string            // 指定使用的IP网段，默认排除 172.17
}{}
