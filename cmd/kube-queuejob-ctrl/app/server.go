/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/kubernetes-incubator/kube-arbitrator/cmd/kube-queuejob-ctrl/app/options"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/queuejob-ctrl/controller/queuejob"
)

func buildConfig(master, kubeconfig string) (*rest.Config, error) {
	if master != "" || kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	return rest.InClusterConfig()
}

func Run(opt *options.ServerOption) error {
	config, err := buildConfig(opt.Master, opt.Kubeconfig)
	if err != nil {
		return err
	}

	neverStop := make(chan struct{})

	// Start QueueJob Controller to create CRD and manage QueueJob lifecycle.
	queuejobController := queuejob.NewController(config)
	queuejobController.Run(neverStop)


	<-neverStop

	return nil
}
