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

package validator

import (
	"encoding/json"
	"testing"

	conf "github.com/fairwindsops/polaris/pkg/config"
	"github.com/fairwindsops/polaris/pkg/kube"
	"github.com/stretchr/testify/assert"

	network "k8s.io/api/networking/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestValidatePDB(t *testing.T) {
	c := conf.Configuration{
		Checks: map[string]conf.Severity{
			"pdbDisruptionsIsZero": conf.SeverityWarning,
		},
	}
	pdb := unstructured.Unstructured{}
	res, err := kube.NewGenericResourceFromUnstructured(pdb)
	res.Kind = "PodDisruptionBudget"

	actualResult, err := applyNonControllerSchemaChecks(&c, nil, res)
	if err != nil {
		panic(err)
	}
	results := actualResult.Results["pdbDisruptionsIsZero"]

	assert.False(t, results.Success)
	assert.Equal(t, conf.SeverityWarning, results.Severity)
	assert.Equal(t, "Reliability", results.Category)
	assert.EqualValues(t, "disruptionsAllowed is not greater than zero", results.Message)
}

func TestValidateIngress(t *testing.T) {
	c := conf.Configuration{
		Checks: map[string]conf.Severity{
			"tlsSettingsMissing": conf.SeverityWarning,
		},
	}
	tls := network.IngressTLS{
		Hosts:      []string{"test"},
		SecretName: "secret",
	}

	ingress := network.Ingress{}
	ingress.Spec.TLS = []network.IngressTLS{tls}
	b, err := json.Marshal(ingress)
	if err != nil {
		panic(err)
	}
	unst := unstructured.Unstructured{}
	err = json.Unmarshal(b, &unst.Object)
	if err != nil {
		panic(err)
	}
	res, err := kube.NewGenericResourceFromUnstructured(unst)
	if err != nil {
		panic(err)
	}
	res.Kind = "Ingress"

	actualResult, err := applyNonControllerSchemaChecks(&c, nil, res)
	if err != nil {
		panic(err)
	}
	results := actualResult.Results["tlsSettingsMissing"]

	assert.True(t, results.Success)
	assert.Equal(t, conf.SeverityWarning, results.Severity)
	assert.Equal(t, "Security", results.Category)
	assert.EqualValues(t, "Ingress has TLS configured", results.Message)
}
