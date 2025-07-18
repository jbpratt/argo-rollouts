package viewcontroller

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/argoproj/argo-rollouts/utils/queue"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/argoproj/argo-rollouts/pkg/apiclient/rollout"
	rolloutclientset "github.com/argoproj/argo-rollouts/pkg/client/clientset/versioned"
	rolloutinformers "github.com/argoproj/argo-rollouts/pkg/client/informers/externalversions"
	rolloutlisters "github.com/argoproj/argo-rollouts/pkg/client/listers/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/pkg/kubectl-argo-rollouts/info"
)

// viewController is a mini controller which allows printing of live updates to rollouts
// Allows subscribers to receive updates about
type viewController struct {
	name      string
	namespace string

	kubeInformerFactory     kubeinformers.SharedInformerFactory
	rolloutsInformerFactory rolloutinformers.SharedInformerFactory

	replicaSetLister  appslisters.ReplicaSetNamespaceLister
	podLister         corelisters.PodNamespaceLister
	rolloutLister     rolloutlisters.RolloutNamespaceLister
	experimentLister  rolloutlisters.ExperimentNamespaceLister
	analysisRunLister rolloutlisters.AnalysisRunNamespaceLister
	deploymentLister  appslisters.DeploymentNamespaceLister
	statefulSetLister appslisters.StatefulSetNamespaceLister

	cacheSyncs []cache.InformerSynced

	workqueue workqueue.RateLimitingInterface
	prevObj   any
	getObj    func() (any, error)
	callbacks []func(any)
	// acquire 'callbacksLock' before reading/writing to 'callbacks'
	callbacksLock sync.Mutex
}

type RolloutViewController struct {
	*viewController
}

type ExperimentViewController struct {
	*viewController
}

type RolloutInfoCallback func(*rollout.RolloutInfo)

type ExperimentInfoCallback func(*rollout.ExperimentInfo)

func NewRolloutViewController(namespace string, name string, kubeClient kubernetes.Interface, rolloutClient rolloutclientset.Interface) *RolloutViewController {
	vc := newViewController(namespace, name, kubeClient, rolloutClient)
	vc.cacheSyncs = append(
		vc.cacheSyncs,
		vc.rolloutsInformerFactory.Argoproj().V1alpha1().Rollouts().Informer().HasSynced,
	)
	rvc := RolloutViewController{
		viewController: vc,
	}
	vc.getObj = func() (any, error) {
		return rvc.GetRolloutInfo()
	}
	return &rvc
}

func NewExperimentViewController(namespace string, name string, kubeClient kubernetes.Interface, rolloutClient rolloutclientset.Interface) *ExperimentViewController {
	vc := newViewController(namespace, name, kubeClient, rolloutClient)
	evc := ExperimentViewController{
		viewController: vc,
	}
	vc.getObj = func() (any, error) {
		return evc.GetExperimentInfo()
	}
	return &evc
}

func newViewController(namespace string, name string, kubeClient kubernetes.Interface, rolloutClient rolloutclientset.Interface) *viewController {
	kubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient, 0, kubeinformers.WithNamespace(namespace))
	rolloutsInformerFactory := rolloutinformers.NewSharedInformerFactoryWithOptions(rolloutClient, 0, rolloutinformers.WithNamespace(namespace))

	controller := viewController{
		name:                    name,
		namespace:               namespace,
		kubeInformerFactory:     kubeInformerFactory,
		rolloutsInformerFactory: rolloutsInformerFactory,
		replicaSetLister:        kubeInformerFactory.Apps().V1().ReplicaSets().Lister().ReplicaSets(namespace),
		podLister:               kubeInformerFactory.Core().V1().Pods().Lister().Pods(namespace),
		rolloutLister:           rolloutsInformerFactory.Argoproj().V1alpha1().Rollouts().Lister().Rollouts(namespace),
		experimentLister:        rolloutsInformerFactory.Argoproj().V1alpha1().Experiments().Lister().Experiments(namespace),
		analysisRunLister:       rolloutsInformerFactory.Argoproj().V1alpha1().AnalysisRuns().Lister().AnalysisRuns(namespace),
		deploymentLister:        kubeInformerFactory.Apps().V1().Deployments().Lister().Deployments(namespace),
		statefulSetLister:       kubeInformerFactory.Apps().V1().StatefulSets().Lister().StatefulSets(namespace),
		workqueue:               workqueue.NewRateLimitingQueue(queue.DefaultArgoRolloutsRateLimiter()),
	}

	controller.cacheSyncs = append(controller.cacheSyncs,
		kubeInformerFactory.Apps().V1().ReplicaSets().Informer().HasSynced,
		kubeInformerFactory.Core().V1().Pods().Informer().HasSynced,
		rolloutsInformerFactory.Argoproj().V1alpha1().Experiments().Informer().HasSynced,
		rolloutsInformerFactory.Argoproj().V1alpha1().AnalysisRuns().Informer().HasSynced,
	)

	enqueueRolloutHandlerFuncs := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			controller.workqueue.Add(controller.name)
		},
		UpdateFunc: func(old, new any) {
			controller.workqueue.Add(controller.name)
		},
		DeleteFunc: func(obj any) {
			controller.workqueue.Add(controller.name)
		},
	}

	// changes to any of these resources will enqueue the rollout for refreshing
	kubeInformerFactory.Apps().V1().ReplicaSets().Informer().AddEventHandler(enqueueRolloutHandlerFuncs)
	kubeInformerFactory.Core().V1().Pods().Informer().AddEventHandler(enqueueRolloutHandlerFuncs)
	rolloutsInformerFactory.Argoproj().V1alpha1().Rollouts().Informer().AddEventHandler(enqueueRolloutHandlerFuncs)
	rolloutsInformerFactory.Argoproj().V1alpha1().Experiments().Informer().AddEventHandler(enqueueRolloutHandlerFuncs)
	rolloutsInformerFactory.Argoproj().V1alpha1().AnalysisRuns().Informer().AddEventHandler(enqueueRolloutHandlerFuncs)

	return &controller
}

func (c *viewController) Start(ctx context.Context) {
	c.kubeInformerFactory.Start(ctx.Done())
	c.rolloutsInformerFactory.Start(ctx.Done())
	cache.WaitForCacheSync(ctx.Done(), c.cacheSyncs...)
}

func (c *viewController) Run(ctx context.Context) error {
	go wait.Until(func() {
		for c.processNextWorkItem() {
		}
	}, time.Second, ctx.Done())
	<-ctx.Done()
	c.DeregisterCallbacks()
	return nil
}

func (c *viewController) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}
	defer c.workqueue.Done(obj)

	newObj, err := c.getObj()
	if err != nil {
		log.Warn(err.Error())
		return true
	}
	if !reflect.DeepEqual(c.prevObj, newObj) {

		// Acquire the mutex and make a thread-local copy of the list of callbacks
		c.callbacksLock.Lock()
		callbacks := append(make([]func(any), 0), c.callbacks...)
		c.callbacksLock.Unlock()

		for _, cb := range callbacks {
			cb(newObj)
		}
		c.prevObj = newObj
	}
	return true
}

func (c *viewController) DeregisterCallbacks() {
	c.callbacksLock.Lock()
	defer c.callbacksLock.Unlock()

	c.callbacks = nil
}

func (c *RolloutViewController) GetRolloutInfo() (*rollout.RolloutInfo, error) {
	ro, err := c.rolloutLister.Get(c.name)
	if err != nil {
		return nil, err
	}

	allReplicaSets, err := c.replicaSetLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	allPods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	allExps, err := c.experimentLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	allAnalysisRuns, err := c.analysisRunLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var workloadRef interface{}
	if ro.Spec.WorkloadRef != nil {
		switch ro.Spec.WorkloadRef.Kind {
		case "Deployment":
			workloadRef, err = c.deploymentLister.Get(ro.Spec.WorkloadRef.Name)
		case "StatefulSet":
			workloadRef, err = c.statefulSetLister.Get(ro.Spec.WorkloadRef.Name)
		default:
			return nil, fmt.Errorf("unsupported workload reference kind: %s", ro.Spec.WorkloadRef.Kind)
		}
		if err != nil {
			return nil, err
		}
	}

	roInfo := info.NewRolloutInfo(ro, allReplicaSets, allPods, allExps, allAnalysisRuns, workloadRef)
	return roInfo, nil
}

func (c *RolloutViewController) RegisterCallback(callback RolloutInfoCallback) {
	cb := func(i any) {
		callback(i.(*rollout.RolloutInfo))
	}
	c.callbacksLock.Lock()
	defer c.callbacksLock.Unlock()

	c.callbacks = append(c.callbacks, cb)
}

func (c *ExperimentViewController) GetExperimentInfo() (*rollout.ExperimentInfo, error) {
	exp, err := c.experimentLister.Get(c.name)
	if err != nil {
		return nil, err
	}
	allReplicaSets, err := c.replicaSetLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	allPods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	allAnalysisRuns, err := c.analysisRunLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	expInfo := info.NewExperimentInfo(exp, allReplicaSets, allAnalysisRuns, allPods)
	return expInfo, nil
}

func (c *ExperimentViewController) RegisterCallback(callback ExperimentInfoCallback) {
	cb := func(i any) {
		callback(i.(*rollout.ExperimentInfo))
	}
	c.callbacksLock.Lock()
	defer c.callbacksLock.Unlock()
	c.callbacks = append(c.callbacks, cb)
}
