module github.com/jmulyadi/kv-store

go 1.23

toolchain go1.24.0

require (
	google.golang.org/grpc v1.74.2
	google.golang.org/protobuf v1.36.6
// other deps
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	golang.org/x/net v0.40.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.25.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250528174236-200df99c418a // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/jmulyadi/kv-store/proto => ./proto
