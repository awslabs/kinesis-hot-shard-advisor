.PHONY: build clean install test

TARGET=
EXTENSION=
SUFFIX=$(GOOS)_$(GOARCH)
ifneq ("${SUFFIX}", "_")
TARGET=_$(SUFFIX)
endif
EXTENSION=$(FILEEXT)

AWS_REGION=
ifeq ("${AWS_REGION}", "")
AWS_REGION="us-west-1"
endif

MOD_NAME="github.com/awslabs/kinesis-hot-shard-advisor"

build:
	CGO_ENABLED=0 go build -o "./build/khs${TARGET}${EXTENSION}"

clean:
	rm -rf ./build

test: build
	CGO_ENABLED=0 AWS_REGION="${AWS_REGION}" go test -timeout 5s -covermode=count -coverpkg="${MOD_NAME}/analyse,${MOD_NAME}/analyse/aggregator,${MOD_NAME}/analyse/service" -coverprofile=build/cover.out ./...

install: test build
	cp "./build/khs${TARGET}" /usr/local/bin/
