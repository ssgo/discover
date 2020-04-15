# a simple discover based redis

### configuration

```go
type Config struct {
    Registry       string            // a redis url, default is redis://:@127.0.0.1:6379/15
    App            string            // register to a app service
    Calls          map[string]string // defines which apps will call
}
```

### configure App will be registered for request


### configure Calls will subscribe app nodes for call

Calls value will detection value type to config

1 or 2 for http version

s for https

number + time unit (for example: 10s、300ms) for timeout

other for Access-Token header

```go
type callInfoType struct {
	Timeout     time.Duration
	HttpVersion int
	Token       string
	SSL         bool
}
```

```go
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
```