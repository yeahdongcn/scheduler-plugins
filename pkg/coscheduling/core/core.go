/*
Copyright 2020 The Kubernetes Authors.

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

package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	gocache "github.com/patrickmn/go-cache"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	informerv1 "k8s.io/client-go/informers/core/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	resourcehelper "k8s.io/kubernetes/pkg/api/v1/resource"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/coscheduling/core/slurm/topology/tree"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

type Status string

const (
	// PodGroupNotSpecified denotes no PodGroup is specified in the Pod spec.
	PodGroupNotSpecified Status = "PodGroup not specified"
	// PodGroupNotFound denotes the specified PodGroup in the Pod spec is
	// not found in API server.
	PodGroupNotFound Status = "PodGroup not found"
	Success          Status = "Success"
	Wait             Status = "Wait"

	permitStateKey = "PermitCoscheduling"
)

type PermitState struct {
	Activate bool
}

func (s *PermitState) Clone() framework.StateData {
	return &PermitState{Activate: s.Activate}
}

// Manager defines the interfaces for PodGroup management.
type Manager interface {
	PreFilter(context.Context, *corev1.Pod) (sets.Set[string], error)
	Permit(context.Context, *framework.CycleState, *corev1.Pod) Status
	GetPodGroup(context.Context, *corev1.Pod) (string, *v1alpha1.PodGroup)
	GetCreationTimestamp(*corev1.Pod, time.Time) time.Time
	DeletePermittedPodGroup(string)
	CalculateAssignedPods(string, string) int
	ActivateSiblings(pod *corev1.Pod, state *framework.CycleState)
	BackoffPodGroup(string, time.Duration)
}

// PodGroupManager defines the scheduling operation called
type PodGroupManager struct {
	// client is a generic controller-runtime client to manipulate both core resources and PodGroups.
	client client.Client
	// snapshotSharedLister is pod shared list
	snapshotSharedLister framework.SharedLister
	// scheduleTimeout is the default timeout for podgroup scheduling.
	// If podgroup's scheduleTimeoutSeconds is set, it will be used.
	scheduleTimeout *time.Duration
	// permittedPG stores the podgroup name which has passed the pre resource check.
	permittedPG      *gocache.Cache
	nominatedNodeSet *gocache.Cache
	// backedOffPG stores the podgorup name which failed scheduling recently.
	backedOffPG *gocache.Cache
	// podLister is pod lister
	podLister listerv1.PodLister
	sync.RWMutex
}

// NewPodGroupManager creates a new operation object.
func NewPodGroupManager(client client.Client, snapshotSharedLister framework.SharedLister, scheduleTimeout *time.Duration, podInformer informerv1.PodInformer) *PodGroupManager {
	pgMgr := &PodGroupManager{
		client:               client,
		snapshotSharedLister: snapshotSharedLister,
		scheduleTimeout:      scheduleTimeout,
		podLister:            podInformer.Lister(),
		permittedPG:          gocache.New(3*time.Second, 3*time.Second),
		nominatedNodeSet:     gocache.New(3*time.Second, 3*time.Second),
		backedOffPG:          gocache.New(10*time.Second, 10*time.Second),
	}
	return pgMgr
}

func (pgMgr *PodGroupManager) BackoffPodGroup(pgName string, backoff time.Duration) {
	if backoff == time.Duration(0) {
		return
	}
	pgMgr.backedOffPG.Add(pgName, nil, backoff)
}

// ActivateSiblings stashes the pods belonging to the same PodGroup of the given pod
// in the given state, with a reserved key "kubernetes.io/pods-to-activate".
func (pgMgr *PodGroupManager) ActivateSiblings(pod *corev1.Pod, state *framework.CycleState) {
	pgName := util.GetPodGroupLabel(pod)
	if pgName == "" {
		return
	}

	// Only proceed if it's explicitly requested to activate sibling pods.
	if c, err := state.Read(permitStateKey); err != nil {
		return
	} else if s, ok := c.(*PermitState); !ok || !s.Activate {
		return
	}

	pods, err := pgMgr.podLister.Pods(pod.Namespace).List(
		labels.SelectorFromSet(labels.Set{v1alpha1.PodGroupLabel: pgName}),
	)
	if err != nil {
		klog.ErrorS(err, "Failed to obtain pods belong to a PodGroup", "podGroup", pgName)
		return
	}

	for i := range pods {
		if pods[i].UID == pod.UID {
			pods = append(pods[:i], pods[i+1:]...)
			break
		}
	}

	if len(pods) != 0 {
		if c, err := state.Read(framework.PodsToActivateKey); err == nil {
			if s, ok := c.(*framework.PodsToActivate); ok {
				s.Lock()
				for _, pod := range pods {
					namespacedName := GetNamespacedName(pod)
					s.Map[namespacedName] = pod
				}
				s.Unlock()
			}
		}
	}
}

// PreFilter filters out a pod if
// 1. it belongs to a podgroup that was recently denied or
// 2. the total number of pods in the podgroup is less than the minimum number of pods
// that is required to be scheduled.
func (pgMgr *PodGroupManager) PreFilter(ctx context.Context, pod *corev1.Pod) (sets.Set[string], error) {
	klog.V(5).InfoS("Pre-filter", "pod", klog.KObj(pod))
	pgFullName, pg := pgMgr.GetPodGroup(ctx, pod)
	if pg == nil {
		return nil, nil
	}

	if _, exist := pgMgr.backedOffPG.Get(pgFullName); exist {
		return nil, fmt.Errorf("podGroup %v failed recently", pgFullName)
	}

	pods, err := pgMgr.podLister.Pods(pod.Namespace).List(
		labels.SelectorFromSet(labels.Set{v1alpha1.PodGroupLabel: util.GetPodGroupLabel(pod)}),
	)
	if err != nil {
		return nil, fmt.Errorf("podLister list pods failed: %w", err)
	}

	if len(pods) < int(pg.Spec.MinMember) {
		return nil, fmt.Errorf("pre-filter pod %v cannot find enough sibling pods, "+
			"current pods number: %v, minMember of group: %v", pod.Name, len(pods), pg.Spec.MinMember)
	}

	if pg.Spec.MinResources == nil {
		return nil, nil
	}

	// TODO(cwdsuzhou): This resource check may not always pre-catch unschedulable pod group.
	// It only tries to PreFilter resource constraints so even if a PodGroup passed here,
	// it may not necessarily pass Filter due to other constraints such as affinity/taints.
	if _, ok := pgMgr.permittedPG.Get(pgFullName); ok {
		var set sets.Set[string]
		if c, ok := pgMgr.nominatedNodeSet.Get(pgFullName); ok {
			set = c.(sets.Set[string])
		}
		return set, nil
	}

	nodes, err := pgMgr.snapshotSharedLister.NodeInfos().List()
	if err != nil {
		return nil, err
	}

	minResources := pg.Spec.MinResources.DeepCopy()
	podQuantity := resource.NewQuantity(int64(pg.Spec.MinMember), resource.DecimalSI)
	minResources[corev1.ResourcePods] = *podQuantity
	err = CheckClusterResource(ctx, nodes, minResources, pgFullName)
	if err != nil {
		klog.ErrorS(err, "Failed to PreFilter (CheckClusterResource)", "podGroup", klog.KObj(pg))
		return nil, err
	}
	pgMgr.permittedPG.Add(pgFullName, pgFullName, *pgMgr.scheduleTimeout)

	set, err := pgMgr.filterNodes(ctx, nodes, pgFullName, pg, pod)
	if err != nil {
		klog.ErrorS(err, "Failed to PreFilter (filterNodes)", "podGroup", klog.KObj(pg))
		return nil, err
	}
	pgMgr.nominatedNodeSet.Add(pgFullName, set, *pgMgr.scheduleTimeout)
	return set, nil
}

// Permit permits a pod to run, if the minMember match, it would send a signal to chan.
func (pgMgr *PodGroupManager) Permit(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) Status {
	pgFullName, pg := pgMgr.GetPodGroup(ctx, pod)
	if pgFullName == "" {
		return PodGroupNotSpecified
	}
	if pg == nil {
		// A Pod with a podGroup name but without a PodGroup found is denied.
		return PodGroupNotFound
	}

	assigned := pgMgr.CalculateAssignedPods(pg.Name, pg.Namespace)
	// The number of pods that have been assigned nodes is calculated from the snapshot.
	// The current pod in not included in the snapshot during the current scheduling cycle.
	if int32(assigned)+1 >= pg.Spec.MinMember {
		return Success
	}

	if assigned == 0 {
		// Given we've reached Permit(), it's mean all PreFilter checks (minMember & minResource)
		// already pass through, so if assigned == 0, it could be due to:
		// - minResource get satisfied
		// - new pods added
		// In either case, we should and only should use this 0-th pod to trigger activating
		// its siblings.
		// It'd be in-efficient if we trigger activating siblings unconditionally.
		// See https://github.com/kubernetes-sigs/scheduler-plugins/issues/682
		state.Write(permitStateKey, &PermitState{Activate: true})
	}

	return Wait
}

// GetCreationTimestamp returns the creation time of a podGroup or a pod.
func (pgMgr *PodGroupManager) GetCreationTimestamp(pod *corev1.Pod, ts time.Time) time.Time {
	pgName := util.GetPodGroupLabel(pod)
	if len(pgName) == 0 {
		return ts
	}
	var pg v1alpha1.PodGroup
	if err := pgMgr.client.Get(context.TODO(), types.NamespacedName{Namespace: pod.Namespace, Name: pgName}, &pg); err != nil {
		return ts
	}
	return pg.CreationTimestamp.Time
}

// DeletePermittedPodGroup deletes a podGroup that passes Pre-Filter but reaches PostFilter.
func (pgMgr *PodGroupManager) DeletePermittedPodGroup(pgFullName string) {
	pgMgr.permittedPG.Delete(pgFullName)
	pgMgr.nominatedNodeSet.Delete(pgFullName)
}

// GetPodGroup returns the PodGroup that a Pod belongs to in cache.
func (pgMgr *PodGroupManager) GetPodGroup(ctx context.Context, pod *corev1.Pod) (string, *v1alpha1.PodGroup) {
	pgName := util.GetPodGroupLabel(pod)
	if len(pgName) == 0 {
		return "", nil
	}
	var pg v1alpha1.PodGroup
	if err := pgMgr.client.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pgName}, &pg); err != nil {
		return fmt.Sprintf("%v/%v", pod.Namespace, pgName), nil
	}
	return fmt.Sprintf("%v/%v", pod.Namespace, pgName), &pg
}

// CalculateAssignedPods returns the number of pods that has been assigned nodes: assumed or bound.
func (pgMgr *PodGroupManager) CalculateAssignedPods(podGroupName, namespace string) int {
	nodeInfos, err := pgMgr.snapshotSharedLister.NodeInfos().List()
	if err != nil {
		klog.ErrorS(err, "Cannot get nodeInfos from frameworkHandle")
		return 0
	}
	var count int
	for _, nodeInfo := range nodeInfos {
		for _, podInfo := range nodeInfo.Pods {
			pod := podInfo.Pod
			if util.GetPodGroupLabel(pod) == podGroupName && pod.Namespace == namespace && pod.Spec.NodeName != "" {
				count++
			}
		}
	}

	return count
}

func (pgMgr *PodGroupManager) filterNodes(
	ctx context.Context,
	nodeList []*framework.NodeInfo,
	desiredPodGroupName string, desiredPodGroup *v1alpha1.PodGroup,
	pod *corev1.Pod) (sets.Set[string], error) {
	// 1. Get the nodes that match the nodeSelector of the pod.
	labelSelector := labels.NewSelector()
	for k, v := range pod.Spec.NodeSelector {
		requirement, err := labels.NewRequirement(k, selection.Equals, []string{v})
		if err != nil {
			klog.ErrorS(err, "Failed to create label requirement", "key", k, "value", v)
			continue
		}
		labelSelector = labelSelector.Add(*requirement)
	}
	list := &corev1.NodeList{}
	pgMgr.client.List(ctx, list, &client.ListOptions{
		LabelSelector: labelSelector,
	})
	lookup := make(map[string]struct{})
	for _, node := range list.Items {
		lookup[node.Name] = struct{}{}
	}
	// XXX: If the nodeSelector is not satisfied, return nil.
	if len(lookup) == 0 {
		klog.InfoS("No nodes match the nodeSelector", "nodeSelector", pod.Spec.NodeSelector)
		return nil, nil
	}
	klog.InfoS("Nodes match the nodeSelector", "nodeSelector", pod.Spec.NodeSelector, "nodes", lookup)

	// 2. Check if the resource of the node can satisfy the pod.
	resourceAvailableLookup := make(map[string]struct{})
	resourceRequest := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
	for _, info := range nodeList {
		if info == nil || info.Node() == nil {
			continue
		}

		nodeResource := util.ResourceList(getNodeResource(ctx, info, desiredPodGroupName))
		add := true
		for name, quant := range resourceRequest {
			quant.Sub(nodeResource[name])
			if quant.Sign() > 0 {
				add = false
				break
			}
		}
		if add {
			if _, ok := lookup[info.Node().Name]; ok {
				resourceAvailableLookup[info.Node().Name] = struct{}{}
			}
		}
	}
	// XXX: If the resource is not satisfied, return nil.
	if len(resourceAvailableLookup) == 0 {
		klog.InfoS("No nodes have enough resource", "resourceRequest", resourceRequest)
		return nil, nil
	}
	klog.InfoS("Nodes have enough resource", "resourceRequest", resourceRequest, "nodes", resourceAvailableLookup)

	// 3. Get the nodes that match the taints of the pod.

	// 4. Get the nodes that match the affinity of the pod.

	// 5. Get the nodes that match the anti-affinity of the pod.

	err := tree.SwitchRecordValidate(tree.DefaultConfigPath)
	if err != nil {
		klog.ErrorS(err, "Failed to validate switch records", "configPath", tree.DefaultConfigPath)
		return nil, err
	}

	requested := uint32(desiredPodGroup.Spec.MinMember + desiredPodGroup.Spec.MinMember/10)
	if desiredPodGroup.Spec.MinMember%10 > 0 {
		requested += 1
	}
	availableNodes := []string{}
	for node := range resourceAvailableLookup {
		availableNodes = append(availableNodes, node)
	}
	selectedNodes, leafSwitchCount, err := tree.EvalNodesTree(availableNodes, []string{}, requested)
	if err != nil {
		klog.ErrorS(err, "Failed to evaluate nodes tree")
		return nil, err
	}
	klog.InfoS("Evaluated nodes tree", "selectedNodes", selectedNodes, "leafSwitchCount", leafSwitchCount)

	set := sets.Set[string]{}
	for _, node := range selectedNodes {
		set.Insert(node)
	}

	return set, nil
}

// CheckClusterResource checks if resource capacity of the cluster can satisfy <resourceRequest>.
// It returns an error detailing the resource gap if not satisfied; otherwise returns nil.
func CheckClusterResource(ctx context.Context, nodeList []*framework.NodeInfo, resourceRequest corev1.ResourceList, desiredPodGroupName string) error {
	for _, info := range nodeList {
		if info == nil || info.Node() == nil {
			continue
		}

		nodeResource := util.ResourceList(getNodeResource(ctx, info, desiredPodGroupName))
		for name, quant := range resourceRequest {
			quant.Sub(nodeResource[name])
			if quant.Sign() <= 0 {
				delete(resourceRequest, name)
				continue
			}
			resourceRequest[name] = quant
		}
		if len(resourceRequest) == 0 {
			return nil
		}
	}
	return fmt.Errorf("resource gap: %v", resourceRequest)
}

// GetNamespacedName returns the namespaced name.
func GetNamespacedName(obj metav1.Object) string {
	return fmt.Sprintf("%v/%v", obj.GetNamespace(), obj.GetName())
}

func getNodeResource(ctx context.Context, info *framework.NodeInfo, desiredPodGroupName string) *framework.Resource {
	nodeClone := info.Snapshot()
	logger := klog.FromContext(ctx)
	for _, podInfo := range info.Pods {
		if podInfo == nil || podInfo.Pod == nil {
			continue
		}
		if util.GetPodGroupFullName(podInfo.Pod) != desiredPodGroupName {
			continue
		}
		nodeClone.RemovePod(logger, podInfo.Pod)
	}

	leftResource := framework.Resource{
		ScalarResources: make(map[corev1.ResourceName]int64),
	}
	allocatable := nodeClone.Allocatable
	requested := nodeClone.Requested

	leftResource.AllowedPodNumber = allocatable.AllowedPodNumber - len(nodeClone.Pods)
	leftResource.MilliCPU = allocatable.MilliCPU - requested.MilliCPU
	leftResource.Memory = allocatable.Memory - requested.Memory
	leftResource.EphemeralStorage = allocatable.EphemeralStorage - requested.EphemeralStorage

	for k, allocatableEx := range allocatable.ScalarResources {
		requestEx, ok := requested.ScalarResources[k]
		if !ok {
			leftResource.ScalarResources[k] = allocatableEx
		} else {
			leftResource.ScalarResources[k] = allocatableEx - requestEx
		}
	}
	klog.V(4).InfoS("Node left resource", "node", klog.KObj(info.Node()), "resource", leftResource)
	return &leftResource
}
