.PHONY: build test

build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-kafka.version=${VERSION}'" -o conduit-connector-kafka cmd/connector/main.go

test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down; \
		exit $$ret

generate:
	go generate ./...

lint:
	golangci-lint run
