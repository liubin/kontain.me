build:
	go build -mod=vendor -o bin/estargz cmd/estargz/main.go
run: build
	bin/estargz