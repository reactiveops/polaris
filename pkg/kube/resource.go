package kube

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	kubeAPICoreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	kubeAPIMetaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// GenericResource is a base implementation with some free methods for inherited structs
type GenericResource struct {
	Kind               string
	ObjectMeta         kubeAPIMetaV1.Object
	Resource           unstructured.Unstructured
	PodSpec            *kubeAPICoreV1.PodSpec
	OriginalObjectJSON []byte
}

// NewGenericResourceFromUnstructured creates a workload from an unstructured.Unstructured
func NewGenericResourceFromUnstructured(unst unstructured.Unstructured) (GenericResource, error) {
	workload := GenericResource{
		Kind:     unst.GetKind(),
		Resource: unst,
	}

	objMeta, err := meta.Accessor(&unst)
	if err != nil {
		return workload, err
	}
	workload.ObjectMeta = objMeta

	b, err := json.Marshal(&unst)
	if err != nil {
		return workload, err
	}
	workload.OriginalObjectJSON = b
	m := make(map[string]interface{})
	err = json.Unmarshal(b, &m)
	if err != nil {
		return workload, err
	}
	podSpecMap := GetPodSpec(m)
	if podSpecMap != nil {
		b, err = json.Marshal(podSpecMap)
		if err != nil {
			return workload, err
		}
		podSpec := kubeAPICoreV1.PodSpec{}
		err = json.Unmarshal(b, &podSpec)
		if err != nil {
			return workload, err
		}
		workload.PodSpec = &podSpec
	}
	return workload, nil
}

// NewGenericResourceFromPod builds a new workload for a given Pod without looking at parents
func NewGenericResourceFromPod(podResource kubeAPICoreV1.Pod, originalObject interface{}) (GenericResource, error) {
	workload := GenericResource{
		Kind:       "Pod",
		PodSpec:    &podResource.Spec,
		ObjectMeta: podResource.ObjectMeta.GetObjectMeta(),
	}
	if originalObject != nil {
		bytes, err := json.Marshal(originalObject)
		if err != nil {
			return workload, err
		}
		workload.OriginalObjectJSON = bytes

		err = json.Unmarshal(bytes, &workload.Resource.Object)
		if err != nil {
			logrus.Error("Couldn't marshal JSON for pod ", err)
			return workload, err
		}
		objMeta, err := meta.Accessor(&workload.Resource)
		if err != nil {
			logrus.Error("Couldn't create meta accessor for unstructred ", err)
			return workload, err
		}
		workload.ObjectMeta = objMeta
	}
	return workload, nil
}

// NewGenericResourceFromBytes parses a generic kubernetes resource
func NewGenericResourceFromBytes(contentBytes []byte) (GenericResource, error) {
	unst := unstructured.Unstructured{}
	err := yaml.Unmarshal(contentBytes, &unst.Object)
	if err != nil {
		return GenericResource{}, err
	}
	return NewGenericResourceFromUnstructured(unst)
}

// ResolveControllerFromPod builds a new workload for a given Pod
func ResolveControllerFromPod(ctx context.Context, podResource kubeAPICoreV1.Pod, dynamicClient *dynamic.Interface, restMapper *meta.RESTMapper, objectCache map[string]unstructured.Unstructured) (GenericResource, error) {
	workload, err := resolveControllerFromPod(ctx, podResource, dynamicClient, restMapper, objectCache)
	if err != nil {
		return workload, err
	}
	if len(workload.OriginalObjectJSON) == 0 {
		return NewGenericResourceFromPod(podResource, podResource)
	}
	return workload, err
}

func resolveControllerFromPod(ctx context.Context, podResource kubeAPICoreV1.Pod, dynamicClient *dynamic.Interface, restMapper *meta.RESTMapper, objectCache map[string]unstructured.Unstructured) (GenericResource, error) {
	podWorkload, err := NewGenericResourceFromPod(podResource, nil)
	if err != nil {
		return podWorkload, err
	}
	topKind := "Pod"
	topMeta := podWorkload.ObjectMeta
	owners := podResource.ObjectMeta.GetOwnerReferences()
	lastKey := ""
	for len(owners) > 0 {
		if len(owners) > 1 {
			logrus.Warn("More than 1 owner found")
		}
		firstOwner := owners[0]
		if firstOwner.Kind == "Node" {
			break
		}
		topKind = firstOwner.Kind
		key := fmt.Sprintf("%s/%s/%s", firstOwner.Kind, topMeta.GetNamespace(), firstOwner.Name)
		lastKey = key
		abstractObject, ok := objectCache[key]
		if !ok {
			err := cacheAllObjectsOfKind(ctx, firstOwner.APIVersion, firstOwner.Kind, dynamicClient, restMapper, objectCache)
			if err != nil {
				logrus.Warnf("Error caching objects of Kind %s %v", firstOwner.Kind, err)
				break
			}
			abstractObject, ok = objectCache[key]
			if !ok {
				logrus.Errorf("Cache missed %s again", key)
				break
			}
		}

		objMeta, err := meta.Accessor(&abstractObject)
		if err != nil {
			logrus.Warnf("Error retrieving parent metadata %s of API %s and Kind %s because of error: %v ", firstOwner.Name, firstOwner.APIVersion, firstOwner.Kind, err)
			return GenericResource{}, err
		}
		topMeta = objMeta
		owners = abstractObject.GetOwnerReferences()
	}

	if lastKey != "" {
		unst := objectCache[lastKey]
		return NewGenericResourceFromUnstructured(unst)
	}
	workload, err := NewGenericResourceFromPod(podResource, podResource)
	if err != nil {
		return workload, err
	}
	workload.Kind = topKind
	workload.ObjectMeta = topMeta
	return workload, nil
}

func cacheAllObjectsOfKind(ctx context.Context, apiVersion, kind string, dynamicClient *dynamic.Interface, restMapper *meta.RESTMapper, objectCache map[string]unstructured.Unstructured) error {
	fqKind := schema.FromAPIVersionAndKind(apiVersion, kind)
	mapping, err := (*restMapper).RESTMapping(fqKind.GroupKind(), fqKind.Version)
	if err != nil {
		logrus.Warnf("Error retrieving mapping of API %s and Kind %s because of error: %v", apiVersion, kind, err)
		return err
	}

	objects, err := (*dynamicClient).Resource(mapping.Resource).Namespace("").List(ctx, kubeAPIMetaV1.ListOptions{})
	if err != nil {
		logrus.Warnf("Error retrieving parent object API %s and Kind %s because of error: %v", mapping.Resource.Version, mapping.Resource.Resource, err)
		return err
	}
	for idx, object := range objects.Items {
		key := fmt.Sprintf("%s/%s/%s", object.GetKind(), object.GetNamespace(), object.GetName())
		objectCache[key] = objects.Items[idx]
	}
	return nil
}

func getObject(ctx context.Context, namespace, kind, version, name string, dynamicClient *dynamic.Interface, restMapper *meta.RESTMapper) (*unstructured.Unstructured, error) {
	fqKind := schema.ParseGroupKind(kind)
	mapping, err := (*restMapper).RESTMapping(fqKind, version)
	if err != nil {
		return nil, err
	}
	object, err := (*dynamicClient).Resource(mapping.Resource).Namespace(namespace).Get(ctx, name, kubeAPIMetaV1.GetOptions{})
	return object, err
}

// GetPodSpec looks inside arbitrary YAML for a PodSpec
func GetPodSpec(yaml map[string]interface{}) interface{} {
	for _, child := range podSpecFields {
		if childYaml, ok := yaml[child]; ok {
			return GetPodSpec(childYaml.(map[string]interface{}))
		}
	}
	if _, ok := yaml["containers"]; ok {
		return yaml
	}
	return nil
}
