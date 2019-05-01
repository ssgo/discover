package discover_test

import (
	"github.com/ssgo/discover"
	"github.com/ssgo/log"
	"github.com/ssgo/u"
	"net"
	"net/http"
	"testing"
)

func TestBase(t *testing.T) {

	l, _ := net.Listen("tcp", ":18001")
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("OK"))
	})
	go func() {
		_ = http.Serve(l, nil)
	}()

	discover.Start("127.0.0.1:18001", discover.Config{
		App: "app1",
		Calls: map[string]*discover.CallInfo{
			"app1": &discover.CallInfo{
				Headers: map[string]string{
					"Access-Token": "xxxx",
				},
				HttpVersion: 1,
			},
		},
	})

	//time.Sleep(100 * time.Millisecond)
	appClient := discover.AppClient{}
	node := appClient.Next("app1", nil)
	if node == nil {
		t.Error("node is nil ")
	}
	if node.Addr != "127.0.0.1:18001" {
		t.Error(node.Addr, " != 127.0.0.1:18001")
	}

	caller := discover.NewCaller(nil, log.New(u.ShortUniqueId()))
	result := caller.Get("app1", "/").String()
	if result != "OK" {
		t.Error(result, " != OK")
	}

	result = caller.Get("app2", "/").String()
	if result == "OK" {
		t.Error(result, " == OK")
	}

	_ = l.Close()
	discover.Stop()
	discover.Wait()
}
