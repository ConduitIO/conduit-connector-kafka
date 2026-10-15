VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-kafka.version=${VERSION}'" -o conduit-connector-kafka cmd/connector/main.go

# KAFKA_PORT is the host port the test broker is published on. Tests reach it
# through KAFKA_TEST_SERVERS (test.Servers). Override both to run next to
# another broker, for example: make test-kafka KAFKA_PORT=19092.
KAFKA_PORT ?= 9092
export KAFKA_PORT
export KAFKA_TEST_SERVERS ?= localhost:$(KAFKA_PORT)

# COMPOSE_PROJECT names the compose project, so the test stack never shares
# containers, networks or volumes with another project that has a test/
# directory. "down -v" also removes the broker's anonymous volume.
COMPOSE_PROJECT ?= conduit-connector-kafka

# ACCEPTANCE_REPORT_DIR, when set, makes TestAcceptanceV2 write its report to
# $(ACCEPTANCE_REPORT_DIR)/acceptance-report-<broker>.json. Check it with
# scripts/check-acceptance-report.sh.
ACCEPTANCE_REPORT_DIR ?=

.PHONY: test-kafka
test-kafka:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -p $(COMPOSE_PROJECT)-kafka -f test/compose-kafka.yaml up --quiet-pull -d --wait || \
		{ docker compose -p $(COMPOSE_PROJECT)-kafka -f test/compose-kafka.yaml down -v; exit 1; }
	$(if $(ACCEPTANCE_REPORT_DIR),rm -f $(ACCEPTANCE_REPORT_DIR)/acceptance-report-kafka.json;) \
	$(if $(ACCEPTANCE_REPORT_DIR),CONDUIT_ACCEPTANCE_REPORT=$(ACCEPTANCE_REPORT_DIR)/acceptance-report-kafka.json) \
		go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -p $(COMPOSE_PROJECT)-kafka -f test/compose-kafka.yaml down -v; \
		exit $$ret

.PHONY: test-redpanda
test-redpanda:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -p $(COMPOSE_PROJECT)-redpanda -f test/compose-redpanda.yaml up --quiet-pull -d --wait || \
		{ docker compose -p $(COMPOSE_PROJECT)-redpanda -f test/compose-redpanda.yaml down -v; exit 1; }
	$(if $(ACCEPTANCE_REPORT_DIR),rm -f $(ACCEPTANCE_REPORT_DIR)/acceptance-report-redpanda.json;) \
	$(if $(ACCEPTANCE_REPORT_DIR),CONDUIT_ACCEPTANCE_REPORT=$(ACCEPTANCE_REPORT_DIR)/acceptance-report-redpanda.json) \
		go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -p $(COMPOSE_PROJECT)-redpanda -f test/compose-redpanda.yaml down -v; \
		exit $$ret

.PHONY: test
test: test-kafka test-redpanda

# The kill -9 chaos test (test/chaos, build tag "chaos"): SIGKILLs a
# Kafka-to-Kafka pipeline and checks invariants 1 to 3 on recovery.
.PHONY: test-chaos-kafka
test-chaos-kafka:
	docker compose -p $(COMPOSE_PROJECT)-chaos-kafka -f test/compose-kafka.yaml up --quiet-pull -d --wait || \
		{ docker compose -p $(COMPOSE_PROJECT)-chaos-kafka -f test/compose-kafka.yaml down -v; exit 1; }
	go test $(GOTEST_FLAGS) -tags chaos -race -count=1 -timeout 30m ./test/chaos/; ret=$$?; \
		docker compose -p $(COMPOSE_PROJECT)-chaos-kafka -f test/compose-kafka.yaml down -v; \
		exit $$ret

.PHONY: test-chaos-redpanda
test-chaos-redpanda:
	docker compose -p $(COMPOSE_PROJECT)-chaos-redpanda -f test/compose-redpanda.yaml up --quiet-pull -d --wait || \
		{ docker compose -p $(COMPOSE_PROJECT)-chaos-redpanda -f test/compose-redpanda.yaml down -v; exit 1; }
	go test $(GOTEST_FLAGS) -tags chaos -race -count=1 -timeout 30m ./test/chaos/; ret=$$?; \
		docker compose -p $(COMPOSE_PROJECT)-chaos-redpanda -f test/compose-redpanda.yaml down -v; \
		exit $$ret

.PHONY: generate
generate:
	go generate ./...
	conn-sdk-cli readmegen -w

.PHONY: fmt
fmt: ## Format Go files using gofumpt and gci.
	gofumpt -l -w .
	gci write --skip-generated  .

.PHONY: lint
lint:
	golangci-lint run

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
