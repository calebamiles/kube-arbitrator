BIN_DIR=_output/bin

kube-arbitrator: init
	go build -o ${BIN_DIR}/kube-batchd ./cmd/kube-batchd/
	go build -o ${BIN_DIR}/kube-quotalloc ./cmd/kube-quotalloc/

verify: generate-code
	hack/verify-gofmt.sh
	hack/verify-golint.sh
	hack/verify-gencode.sh

init:
	mkdir -p ${BIN_DIR}

generate-code:
	go build -o ${BIN_DIR}/deepcopy-gen ./cmd/deepcopy-gen/
	${BIN_DIR}/deepcopy-gen -i ./pkg/batchd/apis/v1/ -O zz_generated.deepcopy
	${BIN_DIR}/deepcopy-gen -i ./pkg/quotalloc/apis/v1/ -O zz_generated.deepcopy

test-integration:
	hack/make-rules/test-integration.sh $(WHAT)

run-test:
	hack/make-rules/test.sh $(WHAT) $(TESTS)

clean:
	rm -rf _output/
	rm -f kube-arbitrator
