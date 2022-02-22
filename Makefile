.PHONY: test

test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose-kafka.yml up --quiet-pull -d --wait
	go test -v -count=1 -race ./...; ret=$$?; \
		docker compose -f test/docker-compose-kafka.yml down; \
		exit $$ret

