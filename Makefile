CC=go build
export GOOS=linux
export GOARCH=amd64
#export GOPATH=$(GOPATH):$(PWD)/timkr.si
CFLAGS=

.PHONY: clean
	
all: clientExe controllerExe replicatorExe

clean:
	-rm -f clientExe controllerExe replicatorExe
	-$(shell find . -name "*.pb.go" -type f -delete)

replicatorExe: proto/*.proto replicator/*.go
	protoc --go_out=replicator/ --go-grpc_out=replicator/ $(filter %.proto,$^)
	$(CC) -C replicator -o ../$@

controllerExe: proto/*.proto controller/*.go
	protoc --go_out=controller/ --go-grpc_out=controller/ $(filter %.proto,$^)
	$(CC) -C controller -o ../$@

clientExe: proto/*.proto client/*.go
	protoc --go_out=client/ --go-grpc_out=client/ $(filter %.proto,$^)
	$(CC) -C client -o ../$@

raftadminCli: raftadmin/cmd/raftadmin/raftadmin.go
	$(CC) -C raftadmin/cmd/raftadmin -o $(PWD)/$@