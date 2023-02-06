GOLANGCI_LINT_VERSION := v1.51.0
export GO111MODULE=on

lint:
	@(if ! type golangci-lint >/dev/null 2>&1; then curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin ${GOLANGCI_LINT_VERSION} ;fi)
	golangci-lint run ./...

start-local:
	docker run -d -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb

generate:
	go generate ./...

test:
	go test ./...

test-ci: export SKIP_DOCKER_TEST="true"
test-ci:
	go test ./...
	
build:
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS) -s -w" -o ./bin/mmain ./cmd

cover:
	go test -coverprofile=cover.out ./...
	go tool cover -html=cover.out -o cover.html
