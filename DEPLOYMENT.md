# OpenCamunda - Deployment Guide

Zeebe Community Fork v8.5.25 — the last version under the Zeebe Community License 1.1.

## Quick Start

### Prerequisites

- Docker + Docker Compose (for local/Docker deployment)
- Helm 3 + Kubernetes cluster (for Kubernetes deployment)
- Java 21 + Maven (only if building from source)

---

## Option 1: Docker Compose (Local / Single-Server)

The simplest way to run OpenCamunda:

```bash
# Build the Docker image from source
docker compose build

# Start Zeebe + Elasticsearch
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f zeebe

# Stop
docker compose down

# Stop and remove all data
docker compose down -v
```

### With monitoring (Prometheus + Grafana):

```bash
docker compose --profile monitoring up -d
```

### Verify it's running:

```bash
# Health check
curl http://localhost:8080/actuator/health

# Topology (cluster info)
curl http://localhost:8080/actuator/cluster

# gRPC is on port 26500
# REST API is on port 8080
```

### Connect a client:

```bash
# Using zbctl (if available)
zbctl --insecure status

# Or use the Java/Go client with:
#   gRPC endpoint: localhost:26500
#   REST endpoint: http://localhost:8080
```

---

## Option 2: Pre-built Docker Image

If you want to build the image once and push it to a registry:

```bash
# Build
docker build -t your-registry.com/opencamunda/zeebe:8.5.25 .

# Push
docker push your-registry.com/opencamunda/zeebe:8.5.25

# Run standalone (no compose)
docker run -d \
  --name opencamunda \
  -p 26500:26500 \
  -p 8080:8080 \
  -e ZEEBE_BROKER_GATEWAY_ENABLE=true \
  -e ZEEBE_BROKER_NETWORK_HOST=0.0.0.0 \
  your-registry.com/opencamunda/zeebe:8.5.25
```

---

## Option 3: Kubernetes with Custom Helm Chart

A self-contained Helm chart is included in `helm/opencamunda/`:

```bash
# Install (single-node, good for dev/staging)
helm install opencamunda ./helm/opencamunda

# Install with custom values
helm install opencamunda ./helm/opencamunda \
  --set zeebe.clusterSize=3 \
  --set zeebe.partitionCount=3 \
  --set zeebe.replicationFactor=3 \
  --set zeebe.pvcSize=32Gi

# With a custom image registry
helm install opencamunda ./helm/opencamunda \
  --set zeebe.image.registry=your-registry.com \
  --set zeebe.image.repository=opencamunda/zeebe

# Port-forward to access locally
kubectl port-forward svc/opencamunda-zeebe-gateway 26500:26500 8080:8080

# Uninstall
helm uninstall opencamunda
```

### Production values example:

```yaml
# values-production.yaml
zeebe:
  clusterSize: 3
  partitionCount: 3
  replicationFactor: 3
  pvcSize: 64Gi
  resources:
    requests:
      cpu: 2
      memory: 4Gi
    limits:
      cpu: 4
      memory: 8Gi
  javaOpts: >-
    -Xms4g -Xmx4g
    -XX:+ExitOnOutOfMemoryError
    -XX:+HeapDumpOnOutOfMemoryError

elasticsearch:
  replicas: 3
  pvcSize: 100Gi
  heapSize: 2048m
  resources:
    requests:
      cpu: 2
      memory: 4Gi
    limits:
      cpu: 4
      memory: 8Gi

ingress:
  enabled: true
  host: zeebe.your-domain.com
  tls:
    enabled: true
    secretName: zeebe-tls
```

---

## Option 4: Use Official Camunda Helm Chart with Custom Image

You can use the [official Camunda Platform Helm chart](https://github.com/camunda/camunda-platform-helm)
and just swap the Zeebe image. This gives you the full Helm chart maturity
(pod disruption budgets, advanced affinity, etc.) while using your fork:

```bash
# Add Camunda Helm repo
helm repo add camunda https://helm.camunda.io
helm repo update

# Install using your custom image via the provided values override
helm install opencamunda camunda/camunda-platform \
  --version 10.x.x \
  -f helm/values-camunda-helm.yaml

# Or inline:
helm install opencamunda camunda/camunda-platform \
  --version 10.x.x \
  --set global.identity.auth.enabled=false \
  --set zeebe.image.repository=opencamunda/zeebe \
  --set zeebe.image.tag=8.5.25 \
  --set zeebeGateway.image.repository=opencamunda/zeebe \
  --set zeebeGateway.image.tag=8.5.25 \
  --set operate.enabled=false \
  --set tasklist.enabled=false \
  --set optimize.enabled=false \
  --set identity.enabled=false \
  --set identityKeycloak.enabled=false \
  --set connectors.enabled=false \
  --set webModeler.enabled=false \
  --set console.enabled=false
```

> **Note:** Operate, Tasklist, Optimize, Identity, Connectors, and WebModeler are
> closed-source Camunda products and are **not** included in this fork.
> Only Zeebe (broker + gateway) is open source under the community license.

---

## Ports Reference

| Port  | Protocol |            Description            |
|-------|----------|-----------------------------------|
| 8080  | HTTP     | REST API + Spring Actuator        |
| 26500 | gRPC     | Client Gateway (deploy, commands) |
| 26501 | Internal | Command API (broker-to-broker)    |
| 26502 | Internal | Internal API (cluster protocol)   |
| 9600  | HTTP     | Metrics (Actuator, Prometheus)    |

## Environment Variables

Key environment variables for configuration:

|                     Variable                     |        Description        |       Default       |
|--------------------------------------------------|---------------------------|---------------------|
| `ZEEBE_BROKER_GATEWAY_ENABLE`                    | Enable embedded gateway   | `true`              |
| `ZEEBE_BROKER_CLUSTER_CLUSTERSIZE`               | Number of brokers         | `1`                 |
| `ZEEBE_BROKER_CLUSTER_PARTITIONSCOUNT`           | Number of partitions      | `1`                 |
| `ZEEBE_BROKER_CLUSTER_REPLICATIONFACTOR`         | Replication factor        | `1`                 |
| `ZEEBE_BROKER_NETWORK_HOST`                      | Bind address              | `0.0.0.0`           |
| `ZEEBE_BROKER_DATA_SNAPSHOTPERIOD`               | Snapshot period           | `5m`                |
| `ZEEBE_STANDALONE_GATEWAY`                       | Run as standalone gateway | `false`             |
| `JAVA_TOOL_OPTIONS`                              | JVM arguments             | `-Xms512m -Xmx512m` |
| `ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_CLASSNAME` | ES exporter class         | -                   |
| `ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_URL`  | ES URL                    | -                   |

## Building from Source

```bash
# Full build (skip tests for speed)
mvn clean install -DskipTests

# Build only the dist (distribution tarball)
mvn -B -am -pl dist package -DskipChecks -DskipTests

# Build Docker image using the distball
docker build -t opencamunda/zeebe:8.5.25 .

# Build Docker image from source (no pre-built dist needed)
docker build -t opencamunda/zeebe:8.5.25 --build-arg DIST=build .
```

