<div align="center">
  <img src="/img/polaris-logo.png" alt="Polaris Logo" />
  <br>

  [![Version][version-image]][version-link] [![CircleCI][circleci-image]][circleci-link] [![Go Report Card][goreport-image]][goreport-link]
</div>

[version-image]: https://img.shields.io/static/v1.svg?label=Version&message=1.2.0&color=239922
[version-link]: https://github.com/FairwindsOps/polaris

[goreport-image]: https://goreportcard.com/badge/github.com/FairwindsOps/polaris
[goreport-link]: https://goreportcard.com/report/github.com/FairwindsOps/polaris

[circleci-image]: https://circleci.com/gh/FairwindsOps/polaris.svg?style=svg
[circleci-link]: https://circleci.com/gh/FairwindsOps/polaris.svg

Fairwinds' Polaris keeps your clusters sailing smoothly. It runs a variety of checks to ensure that
Kubernetes pods and controllers are configured using best practices, helping you avoid
problems in the future. Polaris can be run in a few different modes:

Polaris can be run in three different modes:
* As a [dashboard](https://polaris.docs.fairwinds.com/dashboard), so you can audit what's running inside your cluster.
* As an [admission controller](https://polaris.docs.fairwinds.com/admission-controller), so you can automatically reject workloads that don't adhere to your organization's policies.
* As a [command-line tool](https://polaris.docs.fairwinds.com/infrastructure-as-code), so you can test local YAML files, e.g. as part of a CI/CD process.

**Want to learn more?** Reach out on [the Slack channel](https://fairwindscommunity.slack.com/messages/polaris) ([request invite](https://join.slack.com/t/fairwindscommunity/shared_invite/zt-e3c6vj4l-3lIH6dvKqzWII5fSSFDi1g)), send an email to `opensource@fairwinds.com`, or join us for [office hours on Zoom](https://fairwindscommunity.slack.com/messages/office-hours)

---

**Get more from Polaris** with [Fairwinds Insights](https://www.fairwinds.com/insights?utm_campaign=Hosted%20Polaris%20&utm_source=polaris&utm_term=polaris&utm_content=polaris) -
Insights can help you track Polaris findings over time, send new findings to Slack and Datadog, and integrate other
Kubernetes auditing tools such as
[Trivy](https://github.com/aquasecurity/trivy) and [Goldilocks](https://github.com/FairwindsOps/goldilocks/)

---

## Documentation
Check out the [docs at docs.fairwinds.com](PROJECT-NAME.docs.fairwinds.com), or view the [markdown](./docs-md)

## Dashboard
Here's a quick preview of what the dashboard looks like.

<p align="center">
  <img src="/docs-md/.vuepress/public/img/dashboard-screenshot.png" alt="Polaris Dashboard" width="550"/>
</p>

