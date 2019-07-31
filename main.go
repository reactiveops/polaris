// Copyright 2019 FairwindsOps Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"k8s.io/apimachinery/pkg/runtime"

	conf "github.com/fairwindsops/polaris/pkg/config"
	"github.com/fairwindsops/polaris/pkg/dashboard"
	"github.com/fairwindsops/polaris/pkg/kube"
	"github.com/fairwindsops/polaris/pkg/validator"
	fwebhook "github.com/fairwindsops/polaris/pkg/webhook"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	apitypes "k8s.io/apimachinery/pkg/types"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // Required for other auth providers like GKE.
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/runtime/signals"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/yaml"
)

const (
	// Version represents the current release version of Polaris
	Version = "0.4.0"
)

func main() {
	// Load CLI Flags
	// TODO: Split up global flags vs dashboard/webhook/audit specific flags
	dashboard := flag.Bool("dashboard", false, "Runs the webserver for Polaris dashboard.")
	webhook := flag.Bool("webhook", false, "Runs the webhook webserver.")
	audit := flag.Bool("audit", false, "Runs a one-time audit.")
	auditPath := flag.String("audit-path", "", "If specified, audits one or more YAML files instead of a cluster")
	setExitCode := flag.Bool("set-exit-code-on-error", false, "When running with --audit, set an exit code of 3 when the audit contains error-level issues.")
	minScore := flag.Int("set-exit-code-below-score", 0, "When running with --audit, set an exit code of 4 when the score is below this threshold (1-100)")
	dashboardPort := flag.Int("dashboard-port", 8080, "Port for the dashboard webserver")
	dashboardBasePath := flag.String("dashboard-base-path", "/", "Path on which the dashboard is served")
	webhookPort := flag.Int("webhook-port", 9876, "Port for the webhook webserver")
	auditOutputURL := flag.String("output-url", "", "Destination URL to send audit results")
	auditOutputFile := flag.String("output-file", "", "Destination file for audit results")
	auditOutputFormat := flag.String("output-format", "json", "Output format for results - json, yaml, or score")
	displayName := flag.String("display-name", "", "An optional identifier for the audit")
	configPath := flag.String("config", "", "Location of Polaris configuration file")
	logLevel := flag.String("log-level", logrus.InfoLevel.String(), "Logrus log level")
	version := flag.Bool("version", false, "Prints the version of Polaris")
	disableWebhookConfigInstaller := flag.Bool("disable-webhook-config-installer", false,
		"disable the installer in the webhook server, so it won't install webhook configuration resources during bootstrapping")

	flag.Parse()

	// if version is specified anywhere, print and exit
	if *version {
		fmt.Printf("Polaris version %s\n", Version)
		os.Exit(0)
	}

	// Set logging level
	parsedLevel, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.Errorf("log-level flag has invalid value %s", *logLevel)
	} else {
		logrus.SetLevel(parsedLevel)
	}

	// Parse the config file
	c, err := conf.ParseFile(*configPath)
	if err != nil {
		logrus.Errorf("Error parsing config at %s: %v", *configPath, err)
		os.Exit(1)
	}

	// Override display name on reports if defined in CLI flags
	if *displayName != "" {
		c.DisplayName = *displayName
	}

	// default to run as audit if no "run-mode" is defined
	if !*dashboard && !*webhook && !*audit {
		*audit = true
	}

	// perform the action for the desired "run-mode"
	if *webhook {
		startWebhookServer(c, *disableWebhookConfigInstaller, *webhookPort)
	} else if *dashboard {
		startDashboardServer(c, *auditPath, *dashboardPort, *dashboardBasePath)
	} else if *audit {
		auditData := runAndReportAudit(c, *auditPath, *auditOutputFile, *auditOutputURL, *auditOutputFormat)

		// exit code 3 if any errors in the audit else if score is under desired minimum, exit 4
		if *setExitCode && auditData.ClusterSummary.Results.Totals.Errors > 0 {
			logrus.Infof("%d errors found in audit", auditData.ClusterSummary.Results.Totals.Errors)
			os.Exit(3)
		} else if *minScore != 0 && auditData.ClusterSummary.Score < uint(*minScore) {
			logrus.Infof("Audit score of %d is less than the provided minimum of %d", auditData.ClusterSummary.Score, *minScore)
			os.Exit(4)
		}
	}
}

func startDashboardServer(c conf.Configuration, auditPath string, port int, basePath string) {
	router := dashboard.GetRouter(c, auditPath, port, basePath)
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
	http.Handle("/", router)

	logrus.Infof("Starting Polaris dashboard server on port %d", port)
	logrus.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func startWebhookServer(c conf.Configuration, disableWebhookConfigInstaller bool, port int) {
	logrus.Debug("Setting up controller manager")
	mgr, err := manager.New(config.GetConfigOrDie(), manager.Options{})
	if err != nil {
		logrus.Errorf("Unable to set up overall controller manager: %v", err)
		os.Exit(1)
	}

	polarisAppName := "polaris"
	polarisResourceName := "polaris-webhook"
	polarisNamespaceBytes, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")

	if err != nil {
		// Not exiting here as we have fallback options
		logrus.Debugf("Error reading namespace information: %v", err)
	}

	polarisNamespace := string(polarisNamespaceBytes)
	if polarisNamespace == "" {
		polarisNamespace = polarisResourceName
		logrus.Debugf("Could not determine current namespace, creating resources in %s namespace", polarisNamespace)
	}

	logrus.Info("Setting up webhook server")
	as, err := webhook.NewServer(polarisResourceName, mgr, webhook.ServerOptions{
		Port:                          int32(port),
		CertDir:                       "/opt/cert",
		DisableWebhookConfigInstaller: &disableWebhookConfigInstaller,
		BootstrapOptions: &webhook.BootstrapOptions{
			ValidatingWebhookConfigName: polarisResourceName,
			Secret: &apitypes.NamespacedName{
				Namespace: polarisNamespace,
				Name:      polarisResourceName,
			},

			Service: &webhook.Service{
				Namespace: polarisNamespace,
				Name:      polarisResourceName,

				// Selectors should select the pods that runs this webhook server.
				Selectors: map[string]string{
					"app":       polarisAppName,
					"component": "webhook",
				},
			},
		},
	})

	if err != nil {
		logrus.Errorf("Error setting up webhook server: %v", err)
		os.Exit(1)
	}

	logrus.Infof("Polaris webhook server listening on port %d", port)

	// TODO: This logic really should probably live with the SupportedController struct in config - to house the supported versions and specs
	supportedControllersToScan := map[runtime.Object]string{
		&appsv1.Deployment{}:            "deployments",
		&extensionsv1beta1.Deployment{}: "deployments-ext",
		&appsv1.StatefulSet{}:           "statefulsets",
		&appsv1.DaemonSet{}:             "daemonsets",
		&batchv1.Job{}:                  "jobs",
		&batchv1beta1.CronJob{}:         "cronjobs",
		&corev1.ReplicationController{}: "replicationcontrollers",
	}
	logrus.Debug("Registering webhooks to the webhook server")
	for supportedAPIType, name := range supportedControllersToScan {
		webhook := fwebhook.NewWebhook(name, mgr, fwebhook.Validator{Config: c}, supportedAPIType)
		if err = as.Register(webhook); err != nil {
			logrus.Debugf("Unable to register webhooks in the admission server: %v", err)
			os.Exit(1)
		}
	}

	logrus.Debug("Starting webhook manager")
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		logrus.Errorf("Error starting manager: %v", err)
		os.Exit(1)
	}
}

func runAndReportAudit(c conf.Configuration, auditPath string, outputFile string, outputURL string, outputFormat string) validator.AuditData {
	// Create a kubernetes client resource provider
	k, err := kube.CreateResourceProvider(auditPath)
	if err != nil {
		logrus.Errorf("Error fetching Kubernetes resources %v", err)
		os.Exit(1)
	}
	auditData, err := validator.RunAudit(c, k)

	if err != nil {
		logrus.Errorf("Error while running audit on resources: %v", err)
		os.Exit(1)
	}

	var outputBytes []byte
	if outputFormat == "score" {
		outputBytes = []byte(fmt.Sprintf("%d\n", auditData.ClusterSummary.Score))
	} else if outputFormat == "yaml" {
		jsonBytes, err := json.Marshal(auditData)
		if err == nil {
			outputBytes, err = yaml.JSONToYAML(jsonBytes)
		}
	} else {
		outputBytes, err = json.MarshalIndent(auditData, "", "  ")
	}
	if err != nil {
		logrus.Errorf("Error marshalling audit: %v", err)
		os.Exit(1)
	}
	if outputURL == "" && outputFile == "" {
		os.Stdout.Write(outputBytes)
	} else {
		if outputURL != "" {
			req, err := http.NewRequest("POST", outputURL, bytes.NewBuffer(outputBytes))

			if err != nil {
				logrus.Errorf("Error building request for output: %v", err)
				os.Exit(1)
			}

			if outputFormat == "json" {
				req.Header.Set("Content-Type", "application/json")
			} else if outputFormat == "yaml" {
				req.Header.Set("Content-Type", "application/x-yaml")
			} else {
				req.Header.Set("Content-Type", "text/plain")
			}
			client := &http.Client{}
			resp, err := client.Do(req)

			if err != nil {
				logrus.Errorf("Error making request for output: %v", err)
				os.Exit(1)
			}

			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)

			if err != nil {
				logrus.Errorf("Error reading response: %v", err)
				os.Exit(1)
			}

			logrus.Infof("Received response: %v", body)
		}

		if outputFile != "" {
			err := ioutil.WriteFile(outputFile, []byte(outputBytes), 0644)
			if err != nil {
				logrus.Errorf("Error writing output to file: %v", err)
				os.Exit(1)
			}
		}
	}
	return auditData
}
