all: *.proto
	protoc --go_out=$(PWD)/.. --go-grpc_out=$(PWD)/.. $^

clean: