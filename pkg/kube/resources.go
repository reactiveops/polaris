package kube

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	conf "github.com/fairwindsops/polaris/pkg/config"

	"github.com/sirupsen/logrus"
	"github.com/thoas/go-funk"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sYaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // Required for other auth providers like GKE.
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// ResourceProvider contains k8s resources to be audited
type ResourceProvider struct {
	ServerVersion string
	CreationTime  time.Time
	SourceName    string
	SourceType    string
	Nodes         []corev1.Node
	Namespaces    []corev1.Namespace
	Resources     resourceKindMap
}

type resourceKindMap map[string][]GenericResource

func (rkm resourceKindMap) addResource(r GenericResource) {
	gvk := r.Resource.GroupVersionKind()
	key := gvk.Group + "/" + gvk.Kind
	rkm[key] = append(rkm[key], r)
}

func (rkm resourceKindMap) addResources(rs []GenericResource) {
	for _, r := range rs {
		rkm.addResource(r)
	}
}

func (rkm resourceKindMap) GetLength() int {
	total := 0
	for _, rs := range rkm {
		total += len(rs)
	}
	return total
}

func (rkm resourceKindMap) GetNumberOfControllers() int {
	total := 0
	for _, rs := range rkm {
		for _, r := range rs {
			if r.PodSpec != nil {
				total++
			}
		}
	}
	return total
}

// This is here for backward compatibility reasons
func maybeTransformKindIntoGroupKind(k string) string {
	if k == "Ingress" {
		return "networking.k8s.io/Ingress"
	} else if k == "PodDisruptionBudget" {
		return "policy/PodDisruptionBudget"
	}
	return k
}

func parseGroupKind(gk string) schema.GroupKind {
	i := strings.Index(gk, "/")
	if i == -1 {
		return schema.GroupKind{Kind: gk}
	}

	group := gk[:i]
	kind := gk[i+1:]
	return schema.GroupKind{Group: group, Kind: kind}
}

func newResourceProvider(version, sourceType, sourceName string) ResourceProvider {
	return ResourceProvider{
		ServerVersion: version,
		SourceType:    sourceType,
		SourceName:    sourceName,
		CreationTime:  time.Now(),
		Nodes:         make([]corev1.Node, 0),
		Namespaces:    make([]corev1.Namespace, 0),
		Resources:     make(map[string][]GenericResource),
	}
}

type k8sResource struct {
	Kind string `yaml:"kind"`
}

var podSpecFields = []string{"jobTemplate", "spec", "template"}

// CreateResourceProvider returns a new ResourceProvider object to interact with k8s resources
func CreateResourceProvider(ctx context.Context, directory, workload string, c conf.Configuration) (*ResourceProvider, error) {
	if workload != "" {
		return CreateResourceProviderFromResource(ctx, workload)
	}
	if directory != "" {
		return CreateResourceProviderFromPath(directory)
	}
	return CreateResourceProviderFromCluster(ctx, c)
}

// CreateResourceProviderFromResource creates a new ResourceProvider that just contains one workload
func CreateResourceProviderFromResource(ctx context.Context, workload string) (*ResourceProvider, error) {
	kubeConf, configError := config.GetConfig()
	if configError != nil {
		logrus.Errorf("Error fetching KubeConfig: %v", configError)
		return nil, configError
	}
	kube, err := kubernetes.NewForConfig(kubeConf)
	if err != nil {
		logrus.Errorf("Error creating Kubernetes client: %v", err)
		return nil, err
	}
	serverVersion, err := kube.Discovery().ServerVersion()
	if err != nil {
		logrus.Errorf("Error fetching Cluster API version: %v", err)
		return nil, err
	}
	resources := newResourceProvider(serverVersion.Major+"."+serverVersion.Minor, "Resource", workload)

	parts := strings.Split(workload, "/")
	if len(parts) != 4 {
		return nil, fmt.Errorf("Invalid workload identifier %s. Should be in format namespace/kind/version/name, e.g. nginx-ingress/Deployment.apps/v1/default-backend", workload)
	}
	namespace := parts[0]
	kind := parts[1]
	version := parts[2]
	name := parts[3]

	dynamicInterface, err := dynamic.NewForConfig(kubeConf)
	if err != nil {
		logrus.Errorf("Error connecting to dynamic interface: %v", err)
		return nil, err
	}
	groupResources, err := restmapper.GetAPIGroupResources(kube.Discovery())
	if err != nil {
		logrus.Errorf("Error getting API Group resources: %v", err)
		return nil, err
	}
	restMapper := restmapper.NewDiscoveryRESTMapper(groupResources)
	obj, err := getObject(ctx, namespace, kind, version, name, &dynamicInterface, &restMapper)
	if err != nil {
		logrus.Errorf("Could not find workload %s: %v", workload, err)
		return nil, err
	}
	workloadObj, err := NewGenericResourceFromUnstructured(*obj)
	if err != nil {
		logrus.Errorf("Could not parse workload %s: %v", workload, err)
		return nil, err
	}

	resources.Resources.addResource(workloadObj)
	return &resources, nil
}

// CreateResourceProviderFromPath returns a new ResourceProvider using the YAML files in a directory
func CreateResourceProviderFromPath(directory string) (*ResourceProvider, error) {
	resources := newResourceProvider("unknown", "Path", directory)

	if directory == "-" {
		fi, err := os.Stdin.Stat()
		if err == nil && fi.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
			if err := resources.addResourcesFromReader(os.Stdin); err != nil {
				return nil, err
			}
			return &resources, nil
		}
	}

	visitFile := func(path string, f os.FileInfo, err error) error {
		if !strings.HasSuffix(path, ".yml") && !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		contents, err := ioutil.ReadFile(path)
		if err != nil {
			logrus.Errorf("Error reading file: %v", path)
			return err
		}
		return resources.addResourcesFromYaml(string(contents))
	}

	err := filepath.Walk(directory, visitFile)
	if err != nil {
		return nil, err
	}
	return &resources, nil
}

// CreateResourceProviderFromCluster creates a new ResourceProvider using live data from a cluster
func CreateResourceProviderFromCluster(ctx context.Context, c conf.Configuration) (*ResourceProvider, error) {
	kubeConf, configError := config.GetConfig()
	if configError != nil {
		logrus.Errorf("Error fetching KubeConfig: %v", configError)
		return nil, configError
	}
	api, err := kubernetes.NewForConfig(kubeConf)
	if err != nil {
		logrus.Errorf("Error creating Kubernetes client: %v", err)
		return nil, err
	}
	dynamicInterface, err := dynamic.NewForConfig(kubeConf)
	if err != nil {
		logrus.Errorf("Error connecting to dynamic interface: %v", err)
		return nil, err
	}
	return CreateResourceProviderFromAPI(ctx, api, kubeConf.Host, &dynamicInterface, c)
}

// CreateResourceProviderFromAPI creates a new ResourceProvider from an existing k8s interface
func CreateResourceProviderFromAPI(ctx context.Context, kube kubernetes.Interface, clusterName string, dynamic *dynamic.Interface, c conf.Configuration) (*ResourceProvider, error) {
	listOpts := metav1.ListOptions{}
	serverVersion, err := kube.Discovery().ServerVersion()
	if err != nil {
		logrus.Errorf("Error fetching Cluster API version: %v", err)
		return nil, err
	}
	provider := newResourceProvider(serverVersion.Major+"."+serverVersion.Minor, "Cluster", clusterName)

	nodes, err := kube.CoreV1().Nodes().List(ctx, listOpts)
	if err != nil {
		logrus.Errorf("Error fetching Nodes: %v", err)
		return nil, err
	}
	namespaces, err := kube.CoreV1().Namespaces().List(ctx, listOpts)
	if err != nil {
		logrus.Errorf("Error fetching Namespaces: %v", err)
		return nil, err
	}
	pods, err := kube.CoreV1().Pods("").List(ctx, listOpts)
	if err != nil {
		logrus.Errorf("Error fetching Pods: %v", err)
		return nil, err
	}

	resources, err := restmapper.GetAPIGroupResources(kube.Discovery())
	if err != nil {
		logrus.Errorf("Error getting API Group resources: %v", err)
		return nil, err
	}
	restMapper := restmapper.NewDiscoveryRESTMapper(resources)
	allChecks := []conf.SchemaCheck{}
	for _, check := range c.CustomChecks {
		allChecks = append(allChecks, check)
	}
	for _, check := range conf.BuiltInChecks {
		allChecks = append(allChecks, check)
	}

	var additionalKinds []conf.TargetKind
	for _, check := range allChecks {
		neededKinds := []conf.TargetKind{check.Target}
		for key := range check.AdditionalSchemas {
			neededKinds = append(neededKinds, conf.TargetKind(key))
		}
		for _, kind := range neededKinds {
			if !funk.Contains(conf.HandledTargets, kind) {
				additionalKinds = append(additionalKinds, kind)
			}
		}
	}

	for _, kind := range additionalKinds {
		groupKind := parseGroupKind(maybeTransformKindIntoGroupKind(string(kind)))
		mapping, err := (restMapper).RESTMapping(groupKind)
		if err != nil {
			logrus.Warnf("Error retrieving mapping of Kind %s because of error: %v", kind, err)
			return nil, err
		}

		objects, err := (*dynamic).Resource(mapping.Resource).Namespace("").List(ctx, metav1.ListOptions{})
		if err != nil {
			logrus.Warnf("Error retrieving parent object API %s and Kind %s because of error: %v", mapping.Resource.Version, mapping.Resource.Resource, err)
			return nil, err
		}
		for _, obj := range objects.Items {
			res, err := NewGenericResourceFromUnstructured(obj)
			if err != nil {
				return nil, err
			}
			provider.Resources.addResource(res)
		}
	}

	objectCache := map[string]unstructured.Unstructured{}

	controllers, err := LoadControllers(ctx, pods.Items, dynamic, &restMapper, objectCache)
	if err != nil {
		logrus.Errorf("Error loading controllers from pods: %v", err)
		return nil, err
	}
	provider.Nodes = nodes.Items
	provider.Namespaces = namespaces.Items
	provider.Resources.addResources(controllers)
	return &provider, nil
}

// LoadControllers loads a list of controllers from the kubeResources Pods
func LoadControllers(ctx context.Context, pods []corev1.Pod, dynamicClientPointer *dynamic.Interface, restMapperPointer *meta.RESTMapper, objectCache map[string]unstructured.Unstructured) ([]GenericResource, error) {
	interfaces := []GenericResource{}
	deduped := map[string]corev1.Pod{}
	for _, pod := range pods {
		owners := pod.ObjectMeta.OwnerReferences
		if len(owners) == 0 {
			deduped[pod.ObjectMeta.Namespace+"/Pod/"+pod.ObjectMeta.Name] = pod
			continue
		}
		deduped[pod.ObjectMeta.Namespace+"/"+owners[0].Kind+"/"+owners[0].Name] = pod
	}
	for _, pod := range deduped {
		workload, err := ResolveControllerFromPod(ctx, pod, dynamicClientPointer, restMapperPointer, objectCache)
		if err != nil {
			return nil, err
		}
		interfaces = append(interfaces, workload)
	}
	return deduplicateControllers(interfaces), nil
}

// Because the controllers with an Owner take on the name of the Owner, this eliminates any duplicates.
// In cases like CronJobs older children can hang around, so this takes the most recent.
func deduplicateControllers(inputControllers []GenericResource) []GenericResource {
	controllerMap := make(map[string]GenericResource)
	for _, controller := range inputControllers {
		key := controller.ObjectMeta.GetNamespace() + "/" + controller.Kind + "/" + controller.ObjectMeta.GetName()
		oldController, ok := controllerMap[key]
		if !ok || controller.ObjectMeta.GetCreationTimestamp().Time.After(oldController.ObjectMeta.GetCreationTimestamp().Time) {
			controllerMap[key] = controller
		}
	}
	results := make([]GenericResource, len(controllerMap))
	idx := 0
	for _, controller := range controllerMap {
		results[idx] = controller
		idx++
	}
	return results
}

func (resources *ResourceProvider) addResourcesFromReader(reader io.Reader) error {
	contents, err := ioutil.ReadAll(reader)
	if err != nil {
		logrus.Errorf("Error reading from %v: %v", reader, err)
		return err
	}
	if err := resources.addResourcesFromYaml(string(contents)); err != nil {
		return err
	}
	return nil
}

func (resources *ResourceProvider) addResourcesFromYaml(contents string) error {
	specs := regexp.MustCompile("[\r\n]-+[\r\n]").Split(string(contents), -1)
	for _, spec := range specs {
		if strings.TrimSpace(spec) == "" {
			continue
		}
		err := resources.addResourceFromString(spec)
		if err != nil {
			logrus.Errorf("Error parsing YAML: (%v)", err)
			return err
		}
	}
	return nil
}

func (resources *ResourceProvider) addResourceFromString(contents string) error {
	contentBytes := []byte(contents)
	decoder := k8sYaml.NewYAMLOrJSONDecoder(bytes.NewReader(contentBytes), 1000)
	resource := k8sResource{}
	err := decoder.Decode(&resource)
	decoder = k8sYaml.NewYAMLOrJSONDecoder(bytes.NewReader(contentBytes), 1000)

	if err != nil {
		logrus.Errorf("Invalid YAML: %s", string(contents))
		return err
	}
	if resource.Kind == "Namespace" {
		ns := corev1.Namespace{}
		err = decoder.Decode(&ns)
		resources.Namespaces = append(resources.Namespaces, ns)
	}

	if resource.Kind == "Pod" {
		pod := corev1.Pod{}
		err = decoder.Decode(&pod)
		if err != nil {
			return err
		}
		workload, err := NewGenericResourceFromPod(pod, pod)
		if err != nil {
			return err
		}
		resources.Resources.addResource(workload)
	} else {
		newResource, err := NewGenericResourceFromBytes(contentBytes)
		if err != nil {
			return err
		}
		resources.Resources.addResource(newResource)
	}
	return err
}
