package statefulset

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	patchtypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
	"github.com/argoproj/argo-rollouts/utils/record"
)

const (
	// StatefulSetRevisionAnnotation is the annotation key for StatefulSet revision
	StatefulSetRevisionAnnotation = "statefulset.kubernetes.io/revision"
	// StatefulSetHashAnnotation is the annotation key for StatefulSet hash
	StatefulSetHashAnnotation = "argo-rollouts.argoproj.io/statefulset-hash"
)

// CreateStatefulSetFromRollout creates a StatefulSet from a Rollout
func CreateStatefulSetFromRollout(rollout *v1alpha1.Rollout, revision string) (*appsv1.StatefulSet, error) {
	newStatefulSetTemplate := GetStatefulSetTemplateFromRollout(rollout)
	return NewStatefulSetFromTemplate(newStatefulSetTemplate, rollout, revision)
}

// GetStatefulSetTemplateFromRollout gets the StatefulSet template from a Rollout
func GetStatefulSetTemplateFromRollout(rollout *v1alpha1.Rollout) *appsv1.StatefulSet {
	template := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        rollout.Name,
			Namespace:   rollout.Namespace,
			Labels:      rollout.Labels,
			Annotations: rollout.Annotations,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    rollout.Spec.Replicas,
			ServiceName: rollout.Name, // Default service name
			Selector:    rollout.Spec.Selector,
			Template:    rollout.Spec.Template,
		},
	}
	return template
}

// NewStatefulSetFromTemplate creates a new StatefulSet from a template
func NewStatefulSetFromTemplate(template *appsv1.StatefulSet, rollout *v1alpha1.Rollout, revision string) (*appsv1.StatefulSet, error) {
	statefulset := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        template.Name,
			Namespace:   template.Namespace,
			Labels:      template.Labels,
			Annotations: template.Annotations,
		},
		Spec: template.Spec,
	}

	if statefulset.Labels == nil {
		statefulset.Labels = make(map[string]string)
	}
	if statefulset.Annotations == nil {
		statefulset.Annotations = make(map[string]string)
	}

	// Set revision annotation
	statefulset.Annotations[StatefulSetRevisionAnnotation] = revision

	// Set owner reference
	if rollout != nil {
		statefulset.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(rollout, v1alpha1.SchemeGroupVersion.WithKind("Rollout")),
		}
	}

	return statefulset, nil
}

// GetStatefulSetsByHash returns StatefulSets matching the provided hash
func GetStatefulSetsByHash(statefulsetLister appslisters.StatefulSetLister, rolloutName, namespace, hash string) ([]*appsv1.StatefulSet, error) {
	statefulsets, err := statefulsetLister.StatefulSets(namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var matchingStatefulSets []*appsv1.StatefulSet
	for _, ss := range statefulsets {
		if ss.Annotations[StatefulSetHashAnnotation] == hash {
			matchingStatefulSets = append(matchingStatefulSets, ss)
		}
	}
	return matchingStatefulSets, nil
}

// ScaleStatefulSet scales a StatefulSet to the specified replica count
func ScaleStatefulSet(ctx context.Context, client kubernetes.Interface, statefulset *appsv1.StatefulSet, newScale int32, recorder record.EventRecorder) (*appsv1.StatefulSet, error) {
	if *statefulset.Spec.Replicas == newScale {
		return statefulset, nil
	}

	logCtx := logutil.WithStatefulSet(statefulset)
	logCtx.Infof("Scaling StatefulSet from %d to %d replicas", *statefulset.Spec.Replicas, newScale)

	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, newScale)
	scaledStatefulSet, err := client.AppsV1().StatefulSets(statefulset.Namespace).Patch(
		ctx,
		statefulset.Name,
		patchtypes.MergePatchType,
		[]byte(patch),
		metav1.PatchOptions{},
	)
	if err != nil {
		logCtx.Errorf("Failed to scale StatefulSet: %v", err)
		recorder.Eventf(statefulset, record.EventOptions{EventType: corev1.EventTypeWarning, EventReason: "ScalingStatefulSet"}, "Failed to scale StatefulSet %s from %d to %d: %v", statefulset.Name, *statefulset.Spec.Replicas, newScale, err)
		return nil, err
	}

	recorder.Eventf(statefulset, record.EventOptions{EventType: corev1.EventTypeNormal, EventReason: "ScalingStatefulSet"}, "Scaled StatefulSet %s from %d to %d replicas", statefulset.Name, *statefulset.Spec.Replicas, newScale)
	return scaledStatefulSet, nil
}

// UpdateStatefulSetTemplate updates the StatefulSet template
func UpdateStatefulSetTemplate(ctx context.Context, client kubernetes.Interface, statefulset *appsv1.StatefulSet, newTemplate corev1.PodTemplateSpec, recorder record.EventRecorder) (*appsv1.StatefulSet, error) {
	logCtx := logutil.WithStatefulSet(statefulset)
	logCtx.Info("Updating StatefulSet template")

	statefulset.Spec.Template = newTemplate

	updatedStatefulSet, err := client.AppsV1().StatefulSets(statefulset.Namespace).Update(
		ctx,
		statefulset,
		metav1.UpdateOptions{},
	)
	if err != nil {
		logCtx.Errorf("Failed to update StatefulSet template: %v", err)
		recorder.Eventf(statefulset, record.EventOptions{EventType: corev1.EventTypeWarning, EventReason: "UpdatingStatefulSet"}, "Failed to update StatefulSet template: %v", err)
		return nil, err
	}

	recorder.Eventf(statefulset, record.EventOptions{EventType: corev1.EventTypeNormal, EventReason: "UpdatingStatefulSet"}, "Updated StatefulSet template")
	return updatedStatefulSet, nil
}

// GetStatefulSetRevision returns the revision of a StatefulSet
func GetStatefulSetRevision(statefulset *appsv1.StatefulSet) string {
	if statefulset.Annotations != nil {
		return statefulset.Annotations[StatefulSetRevisionAnnotation]
	}
	return ""
}

// EqualIgnoreHash returns true if two StatefulSets are equal, ignoring hash-related annotations
func EqualIgnoreHash(ss1, ss2 *appsv1.StatefulSet) bool {
	if ss1 == nil || ss2 == nil {
		return ss1 == ss2
	}
	
	// Basic comparison - for now we'll do a simple spec comparison
	// This can be enhanced later if needed
	if ss1.Spec.Replicas != nil && ss2.Spec.Replicas != nil {
		if *ss1.Spec.Replicas != *ss2.Spec.Replicas {
			return false
		}
	} else if ss1.Spec.Replicas != ss2.Spec.Replicas {
		return false
	}
	
	// Compare labels
	if !labels.Equals(ss1.Labels, ss2.Labels) {
		return false
	}
	
	// Compare annotations (excluding hash-related ones)
	ann1 := make(map[string]string)
	ann2 := make(map[string]string)
	
	for k, v := range ss1.Annotations {
		if k != StatefulSetHashAnnotation && k != StatefulSetRevisionAnnotation {
			ann1[k] = v
		}
	}
	
	for k, v := range ss2.Annotations {
		if k != StatefulSetHashAnnotation && k != StatefulSetRevisionAnnotation {
			ann2[k] = v
		}
	}
	
	return labels.Equals(ann1, ann2)
}

// GetStatefulSetHash returns the hash of a StatefulSet
func GetStatefulSetHash(statefulset *appsv1.StatefulSet) string {
	if statefulset.Annotations != nil {
		return statefulset.Annotations[StatefulSetHashAnnotation]
	}
	return ""
}

// SetStatefulSetHash sets the hash annotation on a StatefulSet
func SetStatefulSetHash(statefulset *appsv1.StatefulSet, hash string) {
	if statefulset.Annotations == nil {
		statefulset.Annotations = make(map[string]string)
	}
	statefulset.Annotations[StatefulSetHashAnnotation] = hash
}

// GetDesiredStatefulSetName returns the desired name for a StatefulSet based on rollout and hash
func GetDesiredStatefulSetName(rollout *v1alpha1.Rollout, hash string) string {
	return fmt.Sprintf("%s-%s", rollout.Name, hash)
}

// IsStatefulSetAvailable checks if a StatefulSet is available
func IsStatefulSetAvailable(statefulset *appsv1.StatefulSet) bool {
	if statefulset == nil {
		return false
	}
	
	// StatefulSet is available if it has the desired number of ready replicas
	return statefulset.Status.ReadyReplicas == *statefulset.Spec.Replicas
}

// StatefulSetsByCreationTimestamp sorts StatefulSets by creation timestamp
type StatefulSetsByCreationTimestamp []*appsv1.StatefulSet

func (o StatefulSetsByCreationTimestamp) Len() int      { return len(o) }
func (o StatefulSetsByCreationTimestamp) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o StatefulSetsByCreationTimestamp) Less(i, j int) bool {
	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}

// StatefulSetsByRevisionNumber sorts StatefulSets by revision number
type StatefulSetsByRevisionNumber []*appsv1.StatefulSet

func (o StatefulSetsByRevisionNumber) Len() int      { return len(o) }
func (o StatefulSetsByRevisionNumber) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
func (o StatefulSetsByRevisionNumber) Less(i, j int) bool {
	revision1, err1 := strconv.Atoi(GetStatefulSetRevision(o[i]))
	revision2, err2 := strconv.Atoi(GetStatefulSetRevision(o[j]))
	if err1 != nil || err2 != nil {
		return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
	}
	return revision1 < revision2
}
