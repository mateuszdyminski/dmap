
# Requirements 
* Golang >= 1.5
* Grpc 
## Install Grpc
```
go get google.golang.org/grpc
```

# Generate service 
```
protoc -I protos protos/msg.proto --go_out=plugins=grpc:protos
```