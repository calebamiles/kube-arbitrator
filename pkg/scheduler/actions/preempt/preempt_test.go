/*
Copyright 2018 The Kubernetes Authors.

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

package preempt

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	kbv1 "github.com/kubernetes-sigs/kube-batch/pkg/apis/scheduling/v1alpha1"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/cache"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/conf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/conformance"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/gang"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/nodeorder"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/predicates"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/priority"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/util"
)

func TestPreempt(t *testing.T) {
	framework.RegisterPluginBuilder("conformance", conformance.New)
	framework.RegisterPluginBuilder("gang", gang.New)
	defer framework.CleanupPluginBuilders()

	tests := []struct {
		name      string
		podGroups []*kbv1.PodGroup
		pods      []*v1.Pod
		nodes     []*v1.Node
		queues    []*kbv1.Queue
		expected  int
	}{
		{
			name: "do not preempt if there are enough idle resources",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember: 3,
						Queue:     "q1",
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			// If there are enough idle resources on the node, then there is no need to preempt anything.
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("10", "10G"), make(map[string]string)),
			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 0,
		},
		{
			name: "one Job with two Pods on one node",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember: 3,
						Queue:     "q1",
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptor2", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("3", "3Gi"), make(map[string]string)),
			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 0,
		},
		{
			name: "two Jobs on one node",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						Queue: "q1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember: 2,
						Queue:     "q1",
					},
				},
			},

			pods: []*v1.Pod{
				// running pod with pg1, under c1
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				// running pod with pg1, under c1
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				// pending pod with pg2, under c1
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
				// pending pod with pg2, under c1
				util.BuildPod("c1", "preemptor2", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("2", "2G"), make(map[string]string)),
			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 2,
		},
		{
			name: "preempt one task of different job to fit both jobs on one node",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember:         1,
						Queue:             "q1",
						PriorityClassName: "low-priority",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember:         1,
						Queue:             "q1",
						PriorityClassName: "high-priority",
					},
				},
			},

			pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptor2", "", v1.PodPending, util.BuildResourceList("1", "1G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("2", "2G"), make(map[string]string)),
			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 1,
		},
		{
			name: "preempt enough tasks to fit large task of different job",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember:         1,
						Queue:             "q1",
						PriorityClassName: "low-priority",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember:         1,
						Queue:             "q1",
						PriorityClassName: "high-priority",
					},
				},
			},
			// There are 3 cpus and 3G of memory idle and 3 tasks running each consuming 1 cpu and 1G of memory.
			// Big task requiring 5 cpus and 5G of memory should preempt 2 of 3 running tasks to fit into the node.
			pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n1", v1.PodRunning, util.BuildResourceList("1", "1G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, util.BuildResourceList("5", "5G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceList("6", "6G"), make(map[string]string)),
			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 2,
		},
	}

	allocate := New()

	for j, test := range tests {
		binder := &util.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string),
		}
		evictor := &util.FakeEvictor{
			Evicts:  make([]string, 0),
			Channel: make(chan string),
		}
		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			Evictor:       evictor,
			StatusUpdater: &util.FakeStatusUpdater{},
			VolumeBinder:  &util.FakeVolumeBinder{},

			Recorder: record.NewFakeRecorder(100),
		}
		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}
		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, ss := range test.podGroups {
			schedulerCache.AddPodGroupAlpha1(ss)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueuev1alpha1(q)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:               "conformance",
						EnabledPreemptable: &trueValue,
					},
					{
						Name:                "gang",
						EnabledPreemptable:  &trueValue,
						EnabledJobPipelined: &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)

		allocate.Execute(ssn)

		// for i := 0; i < test.expected; i++ {
		// 	select {
		// 	case <-evictor.Channel:
		// 	case <-time.After(3 * time.Second):
		// 		t.Errorf("Failed to get evicting request.")
		// 	}
		// }

		// if test.expected != len(evictor.Evicts) {
		// 	t.Errorf("case %d (%s): expected: %v, got %v ", i, test.name, test.expected, len(evictor.Evicts))
		// }

		for i := 0; i < test.expected; i++ {
			select {
			case key := <-evictor.Channel:
				t.Log("get evictee ", key)
			case <-time.After(1 * time.Second):
				t.Errorf("case %d(%vs): not enough evictions", j, test.name)
			}
		}
		select {
		case key, opened := <-evictor.Channel:
			if opened {
				t.Errorf("case [%v]: unexpected eviction: %s", test.name, key)
			}
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func TestPreemptBetweenJobs(t *testing.T) {
	framework.RegisterPluginBuilder("conformance", conformance.New)
	framework.RegisterPluginBuilder("gang", gang.New)
	framework.RegisterPluginBuilder("predicates", predicates.New)
	framework.RegisterPluginBuilder("nodeorder", nodeorder.New)
	defer framework.CleanupPluginBuilders()

	tests := []struct {
		name      string
		podGroups []*kbv1.PodGroup
		pods      []*v1.Pod
		nodes     []*v1.Node
		queues    []*kbv1.Queue
		expected  int
	}{
		{
			name: "idle resouce is enought for 1 preemptor and need preempt another one preemptee for 2 preemtor",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember: 1,
						Queue:     "q1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg2",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember: 3,
						Queue:     "q1",
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPod("c1", "preemptee1", "n1", v1.PodRunning, util.BuildResourceList("700m", "0G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee2", "n2", v1.PodRunning, util.BuildResourceList("700m", "0G"), "pg1", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptee3", "n2", v1.PodRunning, util.BuildResourceList("700m", "0G"), "pg1", make(map[string]string), make(map[string]string)),
				// can run on n1 with idle cpu 300m
				util.BuildPod("c1", "preemptor1", "", v1.PodPending, util.BuildResourceList("300m", "0G"), "pg2", make(map[string]string), make(map[string]string)),
				// preempt only 1 pod with 700m is enough for 2 preemptor
				util.BuildPod("c1", "preemptor2", "", v1.PodPending, util.BuildResourceList("300m", "0G"), "pg2", make(map[string]string), make(map[string]string)),
				util.BuildPod("c1", "preemptor3", "", v1.PodPending, util.BuildResourceList("300m", "0G"), "pg2", make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceListWithPods("1050m", "2Gi", "100"), make(map[string]string)),
				util.BuildNode("n2", util.BuildResourceListWithPods("1500m", "2Gi", "100"), make(map[string]string)),
			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 2, // TODO： consider left resource when score, and only 1 pod should preempted
		},
	}

	preempt := New()
	for j, test := range tests {
		binder := &util.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string, test.expected),
		}
		evictor := &util.FakeEvictor{
			Evicts:  make([]string, 0),
			Channel: make(chan string, test.expected),
		}
		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			Evictor:       evictor,
			StatusUpdater: &util.FakeStatusUpdater{},
			VolumeBinder:  &util.FakeVolumeBinder{},

			Recorder: record.NewFakeRecorder(100),
		}
		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}
		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, ss := range test.podGroups {
			schedulerCache.AddPodGroupAlpha1(ss)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueuev1alpha1(q)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:               "conformance",
						EnabledPreemptable: &trueValue,
					},
					{
						Name:                "gang",
						EnabledPreemptable:  &trueValue,
						EnabledJobPipelined: &trueValue,
					},
					{
						Name:             "predicates",
						EnabledPredicate: &trueValue,
					},
					{
						Name:             "nodeorder",
						EnabledNodeOrder: &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)
		preempt.Execute(ssn)

		for i := 0; i < test.expected; i++ {
			select {
			case key := <-evictor.Channel:
				t.Log("get evictee ", key)
			case <-time.After(1 * time.Second):
				t.Errorf("case %d %v: not enough evictions", j, test.name)
			}
		}
		select {
		case key, opened := <-evictor.Channel:
			if opened {
				t.Errorf("case %d [%v]: unexpected eviction: %s", j, test.name, key)
			}
		case <-time.After(300 * time.Millisecond):
			// TODO: Active waiting here is not optimal, but there is no better way currently.
			//	 Ideally we would like to wait for evict and bind request goroutines to finish first.
		}
	}
}

func TestPreemptInJobs(t *testing.T) {
	framework.RegisterPluginBuilder("conformance", conformance.New)
	framework.RegisterPluginBuilder("priority", priority.New)
	framework.RegisterPluginBuilder("predicates", predicates.New)
	framework.RegisterPluginBuilder("nodeorder", nodeorder.New)
	defer framework.CleanupPluginBuilders()

	highPriority, lowPriority := int32(2000), int32(100)
	tests := []struct {
		name      string
		podGroups []*kbv1.PodGroup
		pods      []*v1.Pod
		nodes     []*v1.Node
		queues    []*kbv1.Queue
		expected  int
	}{
		{
			name: "preempt in same jobs: can not preempt in same job",
			podGroups: []*kbv1.PodGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pg1",
						Namespace: "c1",
					},
					Spec: kbv1.PodGroupSpec{
						MinMember: 2,
						Queue:     "q1",
					},
				},
			},
			pods: []*v1.Pod{
				util.BuildPodWithPrio("c1", "preemptee1", "n1", v1.PodRunning, util.BuildResourceList("1000m", "1Gi"), "pg1", &lowPriority, make(map[string]string), make(map[string]string)),
				util.BuildPodWithPrio("c1", "preemptee1", "n2", v1.PodRunning, util.BuildResourceList("1000m", "1Gi"), "pg1", &lowPriority, make(map[string]string), make(map[string]string)),
				util.BuildPodWithPrio("c1", "preemptor1", "", v1.PodPending, util.BuildResourceList("1000m", "1Gi"), "pg1", &highPriority, make(map[string]string), make(map[string]string)),
				util.BuildPodWithPrio("c1", "preemptor2", "", v1.PodPending, util.BuildResourceList("1000m", "1Gi"), "pg1", &highPriority, make(map[string]string), make(map[string]string)),
			},
			nodes: []*v1.Node{
				util.BuildNode("n1", util.BuildResourceListWithPods("1000m", "1Gi", "100"), make(map[string]string)),
				util.BuildNode("n2", util.BuildResourceListWithPods("1000m", "1Gi", "100"), make(map[string]string)),
			},
			queues: []*kbv1.Queue{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "q1",
					},
					Spec: kbv1.QueueSpec{
						Weight: 1,
					},
				},
			},
			expected: 2,
		},
	}

	preempt := New()
	for j, test := range tests {
		binder := &util.FakeBinder{
			Binds:   map[string]string{},
			Channel: make(chan string, test.expected),
		}
		evictor := &util.FakeEvictor{
			Evicts:  make([]string, 0),
			Channel: make(chan string, test.expected),
		}
		schedulerCache := &cache.SchedulerCache{
			Nodes:         make(map[string]*api.NodeInfo),
			Jobs:          make(map[api.JobID]*api.JobInfo),
			Queues:        make(map[api.QueueID]*api.QueueInfo),
			Binder:        binder,
			Evictor:       evictor,
			StatusUpdater: &util.FakeStatusUpdater{},
			VolumeBinder:  &util.FakeVolumeBinder{},

			Recorder: record.NewFakeRecorder(100),
		}
		for _, node := range test.nodes {
			schedulerCache.AddNode(node)
		}
		for _, pod := range test.pods {
			schedulerCache.AddPod(pod)
		}

		for _, ss := range test.podGroups {
			schedulerCache.AddPodGroupAlpha1(ss)
		}

		for _, q := range test.queues {
			schedulerCache.AddQueuev1alpha1(q)
		}

		trueValue := true
		ssn := framework.OpenSession(schedulerCache, []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:               "conformance",
						EnabledPreemptable: &trueValue,
					},
					{
						Name:               "priority",
						EnabledJobOrder:    &trueValue,
						EnabledTaskOrder:   &trueValue,
						EnabledPreemptable: &trueValue,
					},
					{
						Name:             "predicates",
						EnabledPredicate: &trueValue,
					},
					{
						Name:             "nodeorder",
						EnabledNodeOrder: &trueValue,
					},
				},
			},
		})
		defer framework.CloseSession(ssn)
		preempt.Execute(ssn)

		for i := 0; i < test.expected; i++ {
			select {
			case key := <-evictor.Channel:
				t.Log("get evictee ", key)
			case <-time.After(1 * time.Second):
				t.Errorf("case %d %v: not enough evictions", j, test.name)
			}
		}
		select {
		case key, opened := <-evictor.Channel:
			if opened {
				t.Errorf("case %d [%v]: unexpected eviction: %s", j, test.name, key)
			}
		case <-time.After(300 * time.Millisecond):
			// TODO: Active waiting here is not optimal, but there is no better way currently.
			//	 Ideally we would like to wait for evict and bind request goroutines to finish first.
		}
	}
}
