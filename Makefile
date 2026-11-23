GOLANGCI_LINT_VERSION := v2.15.0

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "$(LDFLAGS) -s -w" -o ./bin/main ./cmd

lint:
	@(if ! type golangci-lint >/dev/null 2>&1; then curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/$(GOLANGCI_LINT_VERSION)/install.sh | sh -s -- -b $$(go env GOPATH)/bin $(GOLANGCI_LINT_VERSION) ;fi)
	golangci-lint run ./...

start-local:
	docker run -d --name dynamo -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb -inMemory
	timeout 15 sh -c "until curl -s -o /dev/null -w "%{http_code}" localhost:8000  | grep -q 400; do sleep 1; done"

stop-local:
	docker rm -f dynamo

generate:
	go generate ./...

test:
	go test ./...

cover:
	go test -coverpkg=./... -coverprofile=cover.out ./...
	go tool cover -html=cover.out -o cover.html

.PHONY: build lint start-local stop-local generate test cover
