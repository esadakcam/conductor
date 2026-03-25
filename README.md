# Conductor

Automates operations across multiple Kubernetes clusters based on declarative when/then rules. You define conditions to watch (HTTP endpoints, Prometheus metrics, deployment readiness) and actions to take when they're met (scale deployments, update ConfigMaps, exec into pods, etc.).

Built for scenarios like migrating 5G core workloads between clusters based on load, but works for any multi-cluster orchestration need.

## How it works

Tasks are YAML-defined pairs of conditions and actions:

```yaml
tasks:
  - name: "Migrate workload to cluster 2"
    when:
      - type: "prometheus_metric"
        endpoint: "http://cluster1:30092/metrics"
        metric_name: "ues_active"
        operator: "lt"
        value: 10
    then:
      - type: "k8s_scale_deployment"
        member: "https://cluster1:6443"
        deployment: "amf"
        namespace: "open5gs"
        replicas: 0
      - type: "k8s_scale_deployment"
        member: "https://cluster2:6443"
        deployment: "amf"
        namespace: "open5gs"
        replicas: 1
```

Conductor polls conditions every 15 seconds. When all conditions for a task are met, it runs the actions in order. Each action gets a UUID tracked in etcd so it won't run twice, even after a crash.

## Two modes

**Centralized** -- A single Conductor instance talks directly to all clusters via their Kubernetes APIs. Simple, no coordination needed.

**Distributed** -- One Conductor instance per cluster. They elect a leader via etcd. The leader evaluates conditions and tells followers to execute actions through a REST API. Epoch-based fencing prevents stale leaders from issuing commands after a new leader is elected.

## Conditions

| Type | What it checks |
|------|----------------|
| `endpoint_value` | HTTP response body against a numeric value (`eq`, `lt`, `gt`, ...) |
| `endpoint_success` | Whether an HTTP endpoint returns a successful response |
| `prometheus_metric` | A Prometheus metric value against a threshold |
| `k8s_deployment_ready` | Whether a deployment has the expected replica count |
| `always_true` | Always passes (useful for one-shot tasks) |

## Actions

| Type | What it does |
|------|--------------|
| `k8s_scale_deployment` | Scale a deployment's replicas |
| `k8s_restart_deployment` | Rolling restart a deployment |
| `k8s_wait_deployment_rollout` | Block until rollout completes |
| `k8s_update_configmap` | Update a key in a ConfigMap |
| `k8s_exec_deployment` | Run a command inside a pod |
| `config_value_sum` | Distribute a numeric value across ConfigMaps on multiple clusters |
| `endpoint` | Make an HTTP request |
| `delay` | Wait for a duration |
| `echo` | Log a message |

## Getting started

Requires Go 1.25.2+ and a running etcd instance (docker-compose provided).

```bash
# Start a local etcd cluster
make docker-up

# Build
make build

# Run in centralized mode
./bin/conductor centralized config/config.centralized.yaml

# Or distributed mode
./bin/conductor distributed config/config.distributed.yaml
```

## Configuration

### Centralized

```yaml
kubeconfig_locations:
  - /path/to/cluster1-kubeconfig.yml
  - /path/to/cluster2-kubeconfig.yml

db:
  - http://localhost:2379

tasks:
  # ...
```

### Distributed

```yaml
name: instance-1

db:
  - http://etcd1:2379
  - http://etcd2:2379

server:
  port: 8080

tasks:
  # ...
```

In distributed mode, `member` fields in actions point to follower HTTP endpoints (e.g. `http://192.168.1.111:8080`) instead of Kubernetes API servers directly.

## Project layout

```
cmd/              Entry point
internal/
  cluster/        Core polling loop and outbox interface
    centralized/  Centralized mode outbox + execution
    distributed/  Leader election, distributed outbox
  task/           Condition/action types and marshalling
    common/       Shared condition/action implementations
    centralized/  Centralized-specific implementations
    distributed/  Distributed-specific (HTTP-delegated) implementations
  k8s/            Kubernetes client wrapper
  server/         HTTP server for distributed mode followers
  utils/          Config loading
config/           Example configs
deployment/       Docker-compose for etcd, K8s manifests
docs/             Architecture and flow diagrams
```

## Development

```bash
make test             # Run tests
make test-coverage    # Tests + HTML coverage report
make fmt              # Format code
make vet              # Run go vet
```

## Docs

- [Centralized architecture](docs/architecture-centralized.md)
- [Distributed architecture](docs/architecture-distributed.md)
- [Outbox flow (centralized)](docs/outbox-centralized.md)
- [Outbox flow (distributed)](docs/outbox-distributed.md)
- [Task execution flow](docs/flow-distributed.md)
- [Zombie leader prevention](docs/zombi-leader.md)
