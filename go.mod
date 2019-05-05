module github.com/ssgo/discover

go 1.12

require (
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/ssgo/config v0.1.3
	github.com/ssgo/httpclient v0.1.3
	github.com/ssgo/log v0.1.3
	github.com/ssgo/redis v0.1.3
	github.com/ssgo/standard v0.1.3
	github.com/ssgo/u v0.1.3
)

replace github.com/ssgo/redis => ../redis
