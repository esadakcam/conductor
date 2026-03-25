# Conductor

A distributed Kubernetes cluster orchestration system that automates cross-cluster operations with guaranteed exactly-once execution semantics.

## Overview

Conductor is designed to manage and automate operations across multiple Kubernetes clusters. It supports two deployment modes:

- **Centralized**: Single controller managing all member clusters
- **Distributed**: Leader-follower architecture with leader election and fault tolerance

Key features include:
- **Leader election** via etcd for distributed coordination
- **Idempotent execution** to ensure safe retries
- **Zombie leader prevention** using epoch-based fencing
- **Recovery from failures** via outbox pattern
- **Flexible condition-based triggers** (HTTP endpoints, Prometheus metrics, Kubernetes resources)
- **Diverse action types** (Kubernetes operations, HTTP calls, delays)

## Architecture

### Centralized Mode

A single Conductor instance running in a central cluster that:
- Directly manages all member clusters via their Kubernetes APIs
- Uses etcd for idempotency tracking and state management
- Simple deployment with a single point of control

![Centralized Architecture](docs/architecture-centralized.md)

### Distributed Mode

Multiple Conductor instances deployed across member clusters with:
- **Leader election**: One leader coordinates operations across all clusters
- **Epoch-based fencing**: Prevents zombie leaders from executing stale commands
- **Outbox pattern**: Reliable task execution with crash recovery
- **Leader-to-follower communication**: REST API for distributed task execution

![Distributed Architecture](docs/architecture-distributed.md)

## Quick Start

### Prerequisites

- Go 1.25.2 or later
- Docker and docker-compose (for local etcd cluster)
- Access to Kubernetes clusters (or kind clusters for testing)

### Building

```bash
# Build for your platform
make build

# Build for Linux amd64
make build-amd64
```

### Running Tests

```bash
# Run all tests
make test

# Run tests with HTML coverage
make test-coverage
```

### Running the Application

#### Centralized Mode

```bash
# Start etcd cluster for local development
make docker-up

# Run in centralized mode
./bin/conductor centralized config/config.centralized.yaml
```

#### Distributed Mode

```bash
# Start etcd cluster for local development
make docker-up

# Run multiple instances (one per cluster)
./bin/conductor distributed config/config.distributed.yaml
```

## Configuration

### Centralized Configuration

```yaml
kubeconfig_locations:
  - /path/to/kubeconfig1.yml
  - /path/to/kubeconfig2.yml

db:
  - http://localhost:2379

tasks:
  - name: "Scale down cluster 1 and delegate to cluster 2"
    when:
      - type: "endpoint_value"
        endpoint: "http://localhost:8000"
        value: 1
        operator: eq
      - type: "prometheus_metric"
        endpoint: "http://localhost:30092/metrics"
        metric_name: "ues_active"
        operator: "lt"
        value: 10
    then:
      - type: "k8s_scale_deployment"
        member: "https://cluster1:6443"
        deployment: "amf"
        namespace: "open5gs"
        replicas: 0
```

### Distributed Configuration

```yaml
name: instance-1

kubeconfig_locations:
  - /path/to/kubeconfig.yml

db:
  - http://localhost:2379

server:
  port: 8080

tasks:
  # Tasks definition same as centralized mode
```

## Task Definition

### Condition Types

| Type | Description |
|------|-------------|
| `endpoint_value` | Check HTTP endpoint response value |
| `endpoint_success` | Check if HTTP endpoint returns success |
| `prometheus_metric` | Check Prometheus metric value |
| `k8s_deployment_ready` | Check if Kubernetes deployment is ready |
| `always_true` | Always evaluates to true |

### Action Types

| Type | Description |
|------|-------------|
| `k8s_scale_deployment` | Scale a Kubernetes deployment |
| `k8s_restart_deployment` | Restart a Kubernetes deployment |
| `k8s_wait_deployment_rollout` | Wait for deployment rollout |
| `k8s_update_configmap` | Update a ConfigMap |
| `k8s_exec_deployment` | Execute command in deployment |
| `endpoint` | Make HTTP request |
| `delay` | Wait for specified duration |
| `config_value_sum` | Sum values across ConfigMaps |
| `echo` | Log a message |

## How It Works

### Task Execution Flow

1. **Condition Evaluation**: Conductor continuously monitors conditions
2. **Outbox Entry**: When conditions are met, task is added to outbox
3. **Idempotency Check**: Before each action, check if already executed
4. **Action Execution**: Execute action and track with unique ID
5. **Progress Tracking**: Update task progress in outbox
6. **Cleanup**: Remove task from outbox when complete

### Leader Election (Distributed Mode)

1. Instances campaign for leadership via etcd
2. Winner increments epoch number atomically
3. Leader executes tasks, followers accept commands
4. If leader fails, new leader is elected with new epoch
5. Zombie leaders are rejected due to epoch mismatch

### Idempotency

Each task action is assigned a unique UUID. Before execution:
1. Check if UUID already executed
2. If yes, skip (safe retry)
3. If no, execute and record UUID

## Development

### Project Structure

```
conductor/
├── cmd/
│   └── main.go              # Application entry point
├── internal/
│   ├── cluster/             # Cluster orchestration
│   │   ├── centralized/     # Centralized mode implementation
│   │   └── distributed/     # Distributed mode implementation
│   ├── task/                # Task execution
│   │   ├── common/          # Shared task types
│   │   ├── centralized/     # Centralized task handling
│   │   └── distributed/     # Distributed task handling
│   ├── k8s/                 # Kubernetes client
│   ├── server/              # HTTP server (distributed mode)
│   └── utils/               # Utilities
├── config/                  # Example configurations
├── deployment/              # Kubernetes manifests
└── docs/                    # Documentation
```

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make build` | Build the application |
| `make run` | Run the application |
| `make test` | Run tests |
| `make test-coverage` | Run tests with coverage report |
| `make fmt` | Format code |
| `make vet` | Run go vet |
| `make docker-up` | Start etcd cluster |
| `make docker-down` | Stop etcd cluster |

## Documentation

- [Centralized Architecture](docs/architecture-centralized.md)
- [Distributed Architecture](docs/architecture-distributed.md)
- [Centralized Outbox Flow](docs/outbox-centralized.md)
- [Distributed Outbox Flow](docs/outbox-distributed.md)
- [Task Execution Flow](docs/flow-distributed.md)
- [Zombie Leader Prevention](docs/zombi-leader.md)

## Use Cases

### Example: Load Balancing Across Clusters

Conductor can automatically scale services between clusters based on load:

1. Monitor UE (User Equipment) count on each cluster
2. When cluster 1 is underutilized (< 10 UEs) and cluster 2 has capacity (< 150 UEs)
3. Scale down services on cluster 1
4. Delegate load to cluster 2
5. Monitor and reverse when conditions change

### Example: Disaster Recovery

1. Detect primary cluster failure via endpoint checks
2. Automatically failover to secondary cluster
3. Update DNS/ConfigMaps to route traffic
4. Restore primary cluster when available

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make fmt` and `make test`
5. Submit a pull request

## License

This project is open source. See project files for license details.
