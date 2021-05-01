module github.com/abergmeier/terraform-provider-exasol

go 1.16

require (
	cloud.google.com/go/storage v1.10.0
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/gorilla/websocket v1.4.2
	github.com/grantstreetgroup/go-exasol-client v0.0.0-20210226220253-73218e4f3e92
	github.com/hashicorp/terraform-plugin-sdk/v2 v2.6.1
)

replace github.com/grantstreetgroup/go-exasol-client => github.com/abergmeier/go-exasol-client v0.0.0-20210430191731-1bb220807540
