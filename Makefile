BIN_DIR=_output/bin
RELEASE_VER=v0.4.2
REPO_PATH=github.com/kubernetes-sigs/kube-batch
GitSHA=`git rev-parse HEAD`
Date=`date "+%Y-%m-%d %H:%M:%S"`
REL_OSARCH="linux/amd64"
LD_FLAGS=" \
    -X '${REPO_PATH}/pkg/version.GitSHA=${GitSHA}' \
    -X '${REPO_PATH}/pkg/version.Built=${Date}'   \
    -X '${REPO_PATH}/pkg/version.Version=${RELEASE_VER}'"

default: kube-batch webhook-server

kube-batch: init
	go build -ldflags ${LD_FLAGS} -o=${BIN_DIR}/kube-batch ./cmd/kube-batch

webhook-server: init
	go build -ldflags ${LD_FLAGS} -o=${BIN_DIR}/webhook-server ./cmd/webhook-server

verify: generate-code
	hack/verify-gofmt.sh
	hack/verify-golint.sh
	hack/verify-gencode.sh
	hack/verify-boilerplate.sh
	hack/verify-spelling.sh

init:
	mkdir -p ${BIN_DIR}

generate-code: init
	go build -o ${BIN_DIR}/deepcopy-gen ./cmd/deepcopy-gen/
	${BIN_DIR}/deepcopy-gen -i ./pkg/apis/scheduling/v1alpha1/ -O zz_generated.deepcopy

rel_bins:
	go get github.com/mitchellh/gox
	CGO_ENABLED=0 gox -osarch=${REL_OSARCH} -ldflags ${LD_FLAGS} \
	-output=${BIN_DIR}/{{.OS}}/{{.Arch}}/kube-batch ./cmd/kube-batch
	CGO_ENABLED=0 gox -osarch=${REL_OSARCH} -ldflags ${LD_FLAGS} \
	-output=${BIN_DIR}/{{.OS}}/{{.Arch}}/webhook-server ./cmd/webhook-server

images: rel_bins
	cp ./_output/bin/${REL_OSARCH}/kube-batch ./deployment/images/kube-batch/
	docker build ./deployment/images/kube-batch -t kubesigs/kube-batch:${RELEASE_VER}
	rm -f ./deployment/images/kube-batch/kube-batch
	cp ./_output/bin/${REL_OSARCH}/webhook-server ./deployment/images/webhook-server/
	docker build ./deployment/images/webhook-server -t kubesigs/kube-batch-webhook-server:${RELEASE_VER}
	rm -f ./deployment/images/webhook-server/webhook-server

run-test:
	hack/make-rules/test.sh $(WHAT) $(TESTS)

e2e: kube-batch
	hack/run-e2e.sh

coverage:
	KUBE_COVER=y hack/make-rules/test.sh $(WHAT) $(TESTS)

clean:
	rm -rf _output/
	rm -f kube-batch
