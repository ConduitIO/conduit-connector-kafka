.PHONY: build test

build:
	go build -o conduit-plugin-kafka cmd/kafka/main.go

test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose-kafka.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose-kafka.yml down; \
		exit $$ret

