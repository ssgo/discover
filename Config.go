package discover

var config Config

type Config struct {
	Registry             string               // "127.0.0.1:6379:15"、"127.0.0.1:6379:15:password"
	RegistryPrefix       string               // ""
	RegistryCalls        string               // same with Registry
	App                  string               // register to a app service
	Weight               uint                 // 1
	Calls                map[string]*CallInfo // defines which apps will call
	CallRetryTimes       uint8                // 10
	CallTimeout          int                  // 10000
}

type CallInfo struct {
	Headers     map[string]string
	Timeout     int
	HttpVersion int
	WithSSL     bool
}
