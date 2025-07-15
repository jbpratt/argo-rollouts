package rollout

import (
	"context"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	"github.com/argoproj/argo-rollouts/utils/hash"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
	"github.com/argoproj/argo-rollouts/utils/record"
	statefulsetutil "github.com/argoproj/argo-rollouts/utils/statefulset"
)

const (
	// StatefulSetManagedByRolloutAnnotation is the annotation key for StatefulSets managed by rollouts
	StatefulSetManagedByRolloutAnnotation = "rollouts.argoproj.io/managed-by"
)

// getAllStatefulSets returns all StatefulSets associated with a Rollout
func (c *rolloutContext) getAllStatefulSets() ([]*appsv1.StatefulSet, error) {
	statefulsets, err := c.statefulSetLister.StatefulSets(c.rollout.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var ownedStatefulSets []*appsv1.StatefulSet
	for _, ss := range statefulsets {
		if metav1.IsControlledBy(ss, c.rollout) {
			ownedStatefulSets = append(ownedStatefulSets, ss)
		}
	}

	return ownedStatefulSets, nil
}

// getStatefulSetsByHashedName returns StatefulSets with names that match the hashed name pattern
func (c *rolloutContext) getStatefulSetsByHashedName() ([]*appsv1.StatefulSet, error) {
	statefulsets, err := c.getAllStatefulSets()
	if err != nil {
		return nil, err
	}

	// Filter StatefulSets that match the hashed name pattern
	var hashedStatefulSets []*appsv1.StatefulSet
	for _, ss := range statefulsets {
		if isHashedStatefulSetName(c.rollout.Name, ss.Name) {
			hashedStatefulSets = append(hashedStatefulSets, ss)
		}
	}

	return hashedStatefulSets, nil
}

// isHashedStatefulSetName checks if a StatefulSet name matches the hashed pattern
func isHashedStatefulSetName(rolloutName, statefulsetName string) bool {
	// Check if the name follows the pattern: rolloutName-hash
	if len(statefulsetName) <= len(rolloutName)+1 {
		return false
	}
	
	prefix := rolloutName + "-"
	if !strings.HasPrefix(statefulsetName, prefix) {
		return false
	}
	
	// The suffix should be a valid hash (alphanumeric)
	suffix := statefulsetName[len(prefix):]
	return len(suffix) > 0 && isValidHash(suffix)
}

// isValidHash checks if a string is a valid hash (alphanumeric)
func isValidHash(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

// createStatefulSet creates a new StatefulSet
func (c *rolloutContext) createStatefulSet(template *appsv1.StatefulSet, hash string) (*appsv1.StatefulSet, error) {
	newStatefulSet := template.DeepCopy()
	newStatefulSet.Name = statefulsetutil.GetDesiredStatefulSetName(c.rollout, hash)
	
	// Set hash annotation
	if newStatefulSet.Annotations == nil {
		newStatefulSet.Annotations = make(map[string]string)
	}
	newStatefulSet.Annotations[statefulsetutil.StatefulSetHashAnnotation] = hash
	newStatefulSet.Annotations[StatefulSetManagedByRolloutAnnotation] = c.rollout.Name
	
	// Set owner reference
	newStatefulSet.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(c.rollout, v1alpha1.SchemeGroupVersion.WithKind("Rollout")),
	}
	
	// Set revision based on observed generation
	revision := c.rollout.Status.ObservedGeneration
	if revision == "" {
		revision = "1"
	}
	newStatefulSet.Annotations[statefulsetutil.StatefulSetRevisionAnnotation] = revision
	
	c.log.WithField(logutil.StatefulSetKey, newStatefulSet.Name).Info("Creating StatefulSet")
	
	created, err := c.kubeclientset.AppsV1().StatefulSets(c.rollout.Namespace).Create(
		context.TODO(),
		newStatefulSet,
		metav1.CreateOptions{},
	)
	if err != nil {
		c.log.WithField(logutil.StatefulSetKey, newStatefulSet.Name).Errorf("Failed to create StatefulSet: %v", err)
		c.recorder.Eventf(c.rollout, record.EventOptions{EventType: corev1.EventTypeWarning, EventReason: "StatefulSetCreateError"}, "Failed to create StatefulSet %s: %v", newStatefulSet.Name, err)
		return nil, err
	}
	
	c.recorder.Eventf(c.rollout, record.EventOptions{EventType: corev1.EventTypeNormal, EventReason: "StatefulSetCreated"}, "Created StatefulSet %s", created.Name)
	return created, nil
}

// getNewStatefulSet returns the new StatefulSet this rollout should create
func (c *rolloutContext) getNewStatefulSet() (*appsv1.StatefulSet, error) {
	template := statefulsetutil.GetStatefulSetTemplateFromRollout(c.rollout)
	
	// Calculate hash of the template
	templateHash := hash.ComputePodTemplateHash(&template.Spec.Template, c.rollout.Status.CollisionCount)
	
	// Check if we already have a StatefulSet with this hash
	existingStatefulSets, err := c.getStatefulSetsByHashedName()
	if err != nil {
		return nil, err
	}
	
	for _, ss := range existingStatefulSets {
		if statefulsetutil.GetStatefulSetHash(ss) == templateHash {
			return ss, nil
		}
	}
	
	// Create new StatefulSet if none exists with this hash
	return c.createStatefulSet(template, templateHash)
}

// getStableStatefulSet returns the stable StatefulSet for the rollout
func (c *rolloutContext) getStableStatefulSet() (*appsv1.StatefulSet, error) {
	stableHash := c.rollout.Status.StableRS
	if stableHash == "" {
		return nil, nil
	}
	
	existingStatefulSets, err := c.getStatefulSetsByHashedName()
	if err != nil {
		return nil, err
	}
	
	for _, ss := range existingStatefulSets {
		if statefulsetutil.GetStatefulSetHash(ss) == stableHash {
			return ss, nil
		}
	}
	
	return nil, nil
}

// reconcileStatefulSets reconciles StatefulSets for the rollout
func (c *rolloutContext) reconcileStatefulSets() error {
	var err error
	
	// Get all StatefulSets
	c.allSSs, err = c.getAllStatefulSets()
	if err != nil {
		return err
	}
	
	// Get new StatefulSet
	c.newSS, err = c.getNewStatefulSet()
	if err != nil {
		return err
	}
	
	// Get stable StatefulSet
	c.stableSS, err = c.getStableStatefulSet()
	if err != nil {
		return err
	}
	
	// Determine older StatefulSets
	c.olderSSs = c.getOlderStatefulSets()
	
	return nil
}

// getOlderStatefulSets returns all StatefulSets that are not the new StatefulSet
func (c *rolloutContext) getOlderStatefulSets() []*appsv1.StatefulSet {
	var olderSSs []*appsv1.StatefulSet
	
	for _, ss := range c.allSSs {
		if c.newSS != nil && ss.Name == c.newSS.Name {
			continue
		}
		olderSSs = append(olderSSs, ss)
	}
	
	return olderSSs
}

// scaleStatefulSetAndRecordEvent scales a StatefulSet and records an event
func (c *rolloutContext) scaleStatefulSetAndRecordEvent(ss *appsv1.StatefulSet, newScale int32) (*appsv1.StatefulSet, error) {
	return statefulsetutil.ScaleStatefulSet(context.TODO(), c.kubeclientset, ss, newScale, c.recorder)
}

// cleanupOldStatefulSets removes old StatefulSets that are no longer needed
func (c *rolloutContext) cleanupOldStatefulSets() error {
	if len(c.olderSSs) == 0 {
		return nil
	}
	
	// Sort by creation timestamp to determine which ones to clean up
	sort.Sort(statefulsetutil.StatefulSetsByCreationTimestamp(c.olderSSs))
	
	// Keep the most recent ones based on revision history limit
	limit := defaults.GetRevisionHistoryLimitOrDefault(c.rollout)
	
	var toDelete []*appsv1.StatefulSet
	if len(c.olderSSs) > int(limit) {
		toDelete = c.olderSSs[:len(c.olderSSs)-int(limit)]
	}
	
	for _, ss := range toDelete {
		// Only delete if it's scaled down to 0
		if ss.Spec.Replicas != nil && *ss.Spec.Replicas == 0 {
			c.log.WithField(logutil.StatefulSetKey, ss.Name).Info("Deleting old StatefulSet")
			
			err := c.kubeclientset.AppsV1().StatefulSets(ss.Namespace).Delete(
				context.TODO(),
				ss.Name,
				metav1.DeleteOptions{},
			)
			if err != nil && !errors.IsNotFound(err) {
				c.log.WithField(logutil.StatefulSetKey, ss.Name).Errorf("Failed to delete old StatefulSet: %v", err)
				return err
			}
			
			c.recorder.Eventf(c.rollout, record.EventOptions{EventType: corev1.EventTypeNormal, EventReason: "StatefulSetDeleted"}, "Deleted old StatefulSet %s", ss.Name)
		}
	}
	
	return nil
}

// syncStatefulSetRevision syncs the revision annotation on a StatefulSet
func (c *rolloutContext) syncStatefulSetRevision(ss *appsv1.StatefulSet) error {
	if ss == nil {
		return nil
	}
	
	currentRevision := c.rollout.Status.ObservedGeneration
	if currentRevision == "" {
		currentRevision = "1"
	}
	
	if ss.Annotations == nil {
		ss.Annotations = make(map[string]string)
	}
	
	if ss.Annotations[statefulsetutil.StatefulSetRevisionAnnotation] != currentRevision {
		ss.Annotations[statefulsetutil.StatefulSetRevisionAnnotation] = currentRevision
		
		_, err := c.kubeclientset.AppsV1().StatefulSets(ss.Namespace).Update(
			context.TODO(),
			ss,
			metav1.UpdateOptions{},
		)
		if err != nil {
			c.log.WithField(logutil.StatefulSetKey, ss.Name).Errorf("Failed to update StatefulSet revision: %v", err)
			return err
		}
	}
	
	return nil
}

// isStatefulSetProgressing checks if a StatefulSet is progressing
func (c *rolloutContext) isStatefulSetProgressing(ss *appsv1.StatefulSet) bool {
	if ss == nil {
		return false
	}
	
	// StatefulSet is progressing if:
	// 1. It has pending updates (UpdatedReplicas < desired replicas)
	// 2. It has not yet achieved the desired ready replicas
	return ss.Status.UpdatedReplicas < *ss.Spec.Replicas ||
		   ss.Status.ReadyReplicas < *ss.Spec.Replicas
}

// calculatePartitionForCanary calculates the partition value for canary deployments
func (c *rolloutContext) calculatePartitionForCanary(totalReplicas int32, canaryWeight int32) int32 {
	if canaryWeight == 0 {
		return totalReplicas
	}
	if canaryWeight >= 100 {
		return 0
	}
	
	canaryReplicas := (totalReplicas * canaryWeight) / 100
	return totalReplicas - canaryReplicas
}

// updateStatefulSetPartition updates the partition value for StatefulSet rolling update
func (c *rolloutContext) updateStatefulSetPartition(ss *appsv1.StatefulSet, partition int32) error {
	if ss == nil {
		return nil
	}
	
	if ss.Spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		return fmt.Errorf("StatefulSet %s must use RollingUpdate strategy for canary deployments", ss.Name)
	}
	
	if ss.Spec.UpdateStrategy.RollingUpdate == nil {
		ss.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
	}
	
	currentPartition := int32(0)
	if ss.Spec.UpdateStrategy.RollingUpdate.Partition != nil {
		currentPartition = *ss.Spec.UpdateStrategy.RollingUpdate.Partition
	}
	
	if currentPartition != partition {
		ss.Spec.UpdateStrategy.RollingUpdate.Partition = &partition
		
		c.log.WithField(logutil.StatefulSetKey, ss.Name).
			Infof("Updating StatefulSet partition from %d to %d", currentPartition, partition)
		
		_, err := c.kubeclientset.AppsV1().StatefulSets(ss.Namespace).Update(
			context.TODO(),
			ss,
			metav1.UpdateOptions{},
		)
		if err != nil {
			c.log.WithField(logutil.StatefulSetKey, ss.Name).
				Errorf("Failed to update StatefulSet partition: %v", err)
			return err
		}
		
		c.recorder.Eventf(c.rollout, record.EventOptions{EventType: corev1.EventTypeNormal, EventReason: "StatefulSetPartitionUpdated"}, "Updated StatefulSet %s partition to %d", ss.Name, partition)
	}
	
	return nil
}
