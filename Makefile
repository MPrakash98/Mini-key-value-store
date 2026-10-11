.PHONY: fmt vet test bench clean

fmt:
	gofmt -l -w .

vet:
	go vet ./...

test:
	go test -race ./...

bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf data/ bench-results/
