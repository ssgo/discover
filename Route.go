package discover

import "net/http"

func SetRoute(route func(appClient *AppClient, request *http.Request)) {
	settedRoute = route
}
