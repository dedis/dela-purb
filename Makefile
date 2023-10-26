.PHONY: all tidy generate lint vet test coverage pushdoc

# Default "make" target to check locally that everything is ok, BEFORE pushing remotely
all: lint vet test
	@echo "Done with the standard checks"

tidy:
	go mod tidy

# Some packages are excluded from staticcheck due to deprecated warnings: #208.
lint: tidy
	# Coding style static check.
	@go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck `go list ./... | grep -Ev "(go\.dedis\.ch/dela/internal/testing|go\.dedis\.ch/dela/mino/minogrpc/ptypes)"`

vet: tidy
	@echo "⚠️ Warning: the following only works with go >= 1.14" && \
	go install go.dedis.ch/dela/internal/mcheck && \
	go vet -vettool=`go env GOPATH`/bin/mcheck -commentLen -ifInit ./...

# test runs all tests in DELA without coverage
test: tidy
	go test ./...

# test runs all tests in DELA and generate a coverage output (to be used by sonarcloud)
coverage: tidy
	go test -json -covermode=count -coverprofile=profile.cov ./... | tee report.json
