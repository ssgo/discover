package discover_test

import (
	"github.com/ssgo/discover"
	"github.com/ssgo/log"
	"github.com/ssgo/redis"
	"github.com/ssgo/u"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestBase(t *testing.T) {

	l, _ := net.Listen("tcp", ":18001")
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("OK"))
	})
	go func() {
		_ = http.Serve(l, nil)
	}()

	discover.Config.App = "app1"
	//discover.Config.Calls = map[string]string{
	//	"app1": "5000ms:xxxx:1",
	//}

	discover.Start("127.0.0.1:18001")
	discover.AddExternalApp("app1", "1")

	time.Sleep(100 * time.Millisecond)

	appClient := discover.AppClient{}
	node := appClient.Next("app1", nil)
	if node == nil {
		t.Fatal("node is nil ")
	}
	if node.Addr != "127.0.0.1:18001" {
		t.Fatal(node.Addr, " != 127.0.0.1:18001")
	}

	caller := discover.NewCaller(nil, log.New(u.ShortUniqueId()))
	result := caller.Get("app1", "/").String()
	if result != "OK" {
		t.Fatal(result, " != OK")
	}

	result = caller.Get("app2", "/").String()
	if result == "OK" {
		t.Fatal(result, " == OK")
	}

	rd := redis.GetRedis(discover.Config.Registry, nil)
	rd.PUBLISH("CH_app1", "127.0.0.1:18001")
	rd.PUBLISH("CH_app1", "127.0.0.1:18000 100")

	time.Sleep(100 * time.Millisecond)

	result = caller.Get("app1", "/").String()
	if result == "OK" {
		t.Fatal(result, " == OK")
	}

	rd.PUBLISH("CH_app1", "127.0.0.1:18000")
	rd.PUBLISH("CH_app1", "127.0.0.1:18001 100")

	time.Sleep(100 * time.Millisecond)

	result = caller.Get("app1", "/").String()
	if result != "OK" {
		t.Fatal(result, " != OK")
	}

	discover.AddExternalApp("app1", "h2c")

	result = caller.Get("app1", "/").String()
	if result == "OK" {
		t.Fatal(result, " == OK")
	}

	discover.AddExternalApp("app1", "1")

	result = caller.Get("app1", "/").String()
	if result != "OK" {
		t.Fatal(result, " == OK")
	}

	_ = l.Close()
	discover.Stop()
	discover.Wait()
}
