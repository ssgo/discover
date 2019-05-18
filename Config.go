package discover

var Config = struct {
	Registry       string            // "127.0.0.1:6379:15"、"127.0.0.1:6379:15:password"
	App            string            // register to a app service
	Weight         int               // 1
	Calls          map[string]string // defines which apps will call
	CallRetryTimes int               // 10
}{}
