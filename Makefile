.PHONY: build test

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-kafka.version=${VERSION}'" -o conduit-connector-kafka cmd/connector/main.go

test-kafka:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose-kafka.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose-kafka.yml down; \
		exit $$ret

test-redpanda:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose-redpanda.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose-redpanda.yml down; \
		exit $$ret

test: test-kafka test-redpanda

generate:
	go generate ./...

lint:
	golangci-lint run
