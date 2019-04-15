![volcano-logo](doc/images/volcano-logo.png)


# Volcano

[![Build Status](https://travis-ci.org/kubernetes-sigs/volcano.svg?branch=master)](https://travis-ci.org/kubernetes-sigs/volcano)
[![Go Report Card](https://goreportcard.com/badge/github.com/kubernetes-sigs/volcano)](https://goreportcard.com/report/github.com/kubernetes-sigs/volcano)
[![RepoSize](https://img.shields.io/github/repo-size/kubernetes-sigs/volcano.svg)](https://github.com/kubernetes-sigs/volcano)
[![Release](https://img.shields.io/github/release/kubernetes-sigs/kube-batch.svg)](https://github.com/kubernetes-sigs/volcano/releases)
[![LICENSE](https://img.shields.io/github/license/kubernetes-sigs/volcano.svg)](https://github.com/kubernetes-sigs/volcano/blob/master/LICENSE)

Volcano is system for runnning high performance workloads on
Kubernetes.  It provides a suite of mechanisms currently missing from
Kubernetes that are commonly required by many classes of high
performance workload including:

1. machine learning/deep learning,
2. bioinformatics/genomics, and 
3. other "big data" applications.

These types of applications typically run on generalized domain
frameworks like Tensorflow, Spark, PyTorch, MPI, etc, which Volcano integrates with.

Some examples of the mechanisms and features that Volcano adds to Kubernetes are:

1. Job management extensions and improvements, e.g:
    1. Multi-pod jobs
	2. Lifecycle management extensions including suspend/resume and
       restart.
	3. Improved error handling
	4. Indexed jobs
	5. Task dependencies
2. Scheduling extensions, e.g:
    1. Co-scheduling
	2. Fair-share scheduling
	3. Queue scheduling
	4. Preemption and reclaims
	5. Reservartions and backfills
	6. Topology-based scheduling
3. Runtime extensions, e.g:
    1. Support for specialized continer runtimes like Singularity,
       with GPU accelerator extensions and enhanced security features.
4. Other
    1. Data locality awareness and intelligent scheduling
	2. Optimizations for data throughput, round-trip latency, etc.
	

Volcano builds upon a decade and a half of experience running a wide
variety of high performance workloads at scale using several systems
and platforms, combined with best-of-breed ideas and practices from
the open source community.

## Overall Architecture

![volcano](doc/images/volcano-intro.png)

## Quick Start Guide

The easiest way to deploy Volcano is to use the Helm chart.

### Pre-requisites

First of all, clone the repo to your local path:

```
# mkdir -p $GOPATH/src/github.com/kubernetes-sigs
# cd $GOPATH/src/github.com/kubernetes-sigs
# git clone https://github.com/kubernetes-sigs/volcano
```

### 1. Volcano Image

Official images are available on [DockerHub](https://hub.docker.com/u/kubesigs), however you can
build them locally with the command:

```
cd $GOPATH/src/volcano.sh/volcano
make images

## Verify your images
# docker images
REPOSITORY                                                 TAG                                        IMAGE ID            CREATED             SIZE
kubesigs/vk-admission                                      v0.4.2                                     166dfdd01733        30 minutes ago      39.38 MB
kubesigs/vk-controllers                                    v0.4.2                                     df8ad74b0787        30 minutes ago      42.17 MB
kubesigs/kube-batch                                        v0.4.2                                     c77d9c9ee8a8        30 minutes ago      47.7 MB

``` 

**NOTE**: You need ensure the images are correctly loaded in your kubernetes cluster, for
example, if you are using [kind cluster](https://github.com/kubernetes-sigs/kind), 
try command ```kind load docker-image <image-name>:<tag> ``` for each of the images.

### 2. Helm charts
Second, install the required helm plugin and generate valid
certificate, volcano uses a helm plugin **gen-admission-secret** to
generate certificate for admission service to communicate with
kubernetes API server.

```
#1. Install helm plugin
helm plugin install deployment/volcano/plugins/gen-admission-secret

#2. Generate secret within service name
helm gen-admission-secret --service <specified-name>-admission-service --namespace <namespace>

## For eg: 
kubectl create namespace volcano-trial

helm gen-admission-secret --service volcano-trial-admission-service --namespace volcano-trial

```

Finally, install helm chart.

```
helm install deployment/volcano --namespace <namespace> --name <specified-name>

For eg :
helm install deployment/volcano --namespace volcano-trial --name volcano-trial

```

**NOTE**:The ```<specified-name>``` used in the two commands above should be identical.


To Verify your installation run the following commands:

```
#1. Verify the Running Pods
# kubectl get pods --namespace <namespace> 
NAME                                                READY   STATUS    RESTARTS   AGE
<specified-name>-admission-84fd9b9dd8-9trxn          1/1     Running   0          43s
<specified-name>-controllers-75dcc8ff89-42v6r        1/1     Running   0          43s
<specified-name>-scheduler-b94cdb867-89pm2           1/1     Running   0          43s

#2. Verify the Services
# kubectl get services --namespace <namespace> 
NAME                                     TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)   AGE
<specified-name>-admission-service       ClusterIP   10.105.78.53   <none>        443/TCP   91s

```

## Developing

### E2E Test
Volcano also utilize [kind cluster](https://github.com/kubernetes-sigs/kind) to provide a simple way to
cover E2E tests. Make sure you have kubectl and kind binary installed on your local environment
before running tests. Command as below:
```
make e2e-kind
```
In case of debugging, you can keep the kubernetes cluster environment after tests via:
```
CLEANUP_CLUSTER=-1 make e2e-kind
```
And if only parts of the tests are focused, please execute:
```
TEST_FILE=<test-file-name> make e2e-kind
```
Command above will finally be translated
into: ``go test ./test/e2e/volcano -v -timeout 30m -args --ginkgo.regexScansFilePath=true --ginkgo.focus=<test-file-name>``


## Community, discussion, contribution, and support

You can reach the maintainers of this project at:

Slack: [#volcano-sh](http://t.cn/Efa7LKx)

Mailing List: https://groups.google.com/forum/#!forum/volcano-sh
