PACKAGES=$(shell go list ./... | grep -v '^github.com/sevein/nfdmp2rds/vendor/')

test:
	@go test $(PACKAGES)

.PHONY: test
