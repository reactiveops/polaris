package kube

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sYaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // Required for GKE auth.
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// ResourceProvider contains k8s resources to be audited
type ResourceProvider struct {
	ServerVersion string
	Nodes         []corev1.Node
	Deployments   []appsv1.Deployment
	Namespaces    []corev1.Namespace
	Pods          map[string][]corev1.Pod
}

type k8sResource struct {
	Kind string `yaml:"kind"`
}

// CreateResourceProvider returns a new ResourceProvider object to interact with k8s resources
func CreateResourceProvider(directory string) (*ResourceProvider, error) {
	if directory != "" {
		return CreateResourceProviderFromDirectory(directory)
	}
	return CreateResourceProviderFromCluster()
}

// CreateResourceProviderFromDirectory returns a new ResourceProvider using the YAML files in a directory
func CreateResourceProviderFromDirectory(directory string) (*ResourceProvider, error) {
	resources := ResourceProvider{
		ServerVersion: "unknown",
		Nodes:         []corev1.Node{},
		Deployments:   []appsv1.Deployment{},
		Namespaces:    []corev1.Namespace{},
		Pods:          map[string][]corev1.Pod{},
	}
	visitFile := func(path string, f os.FileInfo, err error) error {
		if !strings.HasSuffix(path, ".yml") && !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		contents, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		decoder := k8sYaml.NewYAMLOrJSONDecoder(bytes.NewReader(contents), 1000)
		resource := k8sResource{}
		err = decoder.Decode(&resource)
		if err != nil {
			// TODO: should we panic if the YAML is bad?
			return nil
		}
		decoder = k8sYaml.NewYAMLOrJSONDecoder(bytes.NewReader(contents), 1000)
		if resource.Kind == "Deployment" {
			dep := appsv1.Deployment{}
			err = decoder.Decode(&dep)
			if err != nil {
				return err
			}
			resources.Deployments = append(resources.Deployments, dep)
		} else if resource.Kind == "Namespace" {
			ns := corev1.Namespace{}
			err = decoder.Decode(&ns)
			if err != nil {
				return err
			}
			resources.Namespaces = append(resources.Namespaces, ns)
		} else if resource.Kind == "Pod" {
			pod := corev1.Pod{}
			err = decoder.Decode(&pod)
			if err != nil {
				return err
			}
			namespace := pod.ObjectMeta.Namespace
			if namespace == "" {
				namespace = "default"
			}
			podGroup, exists := resources.Pods[namespace]
			if !exists {
				podGroup = []corev1.Pod{}
			}
			resources.Pods[namespace] = append(podGroup, pod)
		}
		return nil
	}
	err := filepath.Walk(directory, visitFile)
	if err != nil {
		return nil, err
	}
	return &resources, nil
}

// CreateResourceProviderFromCluster creates a new ResourceProvider using live data from a cluster
func CreateResourceProviderFromCluster() (*ResourceProvider, error) {
	kubeConf := config.GetConfigOrDie()
	api, err := kubernetes.NewForConfig(kubeConf)
	if err != nil {
		return nil, err
	}
	return CreateResourceProviderFromAPI(api)
}

// CreateResourceProviderFromAPI creates a new ResourceProvider from an existing k8s interface
func CreateResourceProviderFromAPI(kube kubernetes.Interface) (*ResourceProvider, error) {
	listOpts := metav1.ListOptions{}
	serverVersion, err := kube.Discovery().ServerVersion()
	if err != nil {
		return nil, err
	}
	deploys, err := kube.AppsV1().Deployments("").List(listOpts)
	if err != nil {
		return nil, err
	}
	nodes, err := kube.CoreV1().Nodes().List(listOpts)
	if err != nil {
		return nil, err
	}
	namespaces, err := kube.CoreV1().Namespaces().List(listOpts)
	if err != nil {
		return nil, err
	}
	podsByNamespace := map[string][]corev1.Pod{}
	for _, ns := range namespaces.Items {
		pods, err := kube.CoreV1().Pods(ns.Name).List(listOpts)
		if err != nil {
			return nil, err
		}
		podsByNamespace[ns.Name] = pods.Items
	}
	api := ResourceProvider{
		ServerVersion: serverVersion.Major + "." + serverVersion.Minor,
		Deployments:   deploys.Items,
		Nodes:         nodes.Items,
		Namespaces:    namespaces.Items,
		Pods:          podsByNamespace,
	}
	return &api, nil
}
