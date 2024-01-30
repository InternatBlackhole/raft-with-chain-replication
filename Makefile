CC=go build
GOOS=linux
GOARCH=amd64
MODULE=tkNaloga04
CFLAGS=

$(shell mkdir -p $(PWD)/bin)

PACKAGES=client replicator controller
TARGETS=$(addprefix ./bin/,$(PACKAGES))

.PHONY: clean
	
all: $(TARGETS)
	echo $(TARGETS)

clean:
	-rm -rf ./bin/*
	-rm -f ./rpc/*.pb.go

proto: proto/*.proto
	protoc --go_out=$(PWD) --go-grpc_out=$(PWD) $^

$(TARGETS): proto
	-$(CC) -o $@ $(MODULE)/$(notdir $@)