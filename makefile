all: *.go data/data.pb.go 
	go test ./

data/data.pb.go: data.proto
	protoc -I ./ data.proto --go_out=plugins=grpc:data
