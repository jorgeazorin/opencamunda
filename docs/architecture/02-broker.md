# 02 - Broker (Servidor Zeebe)

## Qué es el Broker

El Broker es el **proceso servidor** de Zeebe. Gestiona:
- **Particiones** del log de eventos
- **Consenso Raft** entre nodos del cluster
- **Stream processing** (engine + exporters)
- **Snapshots** para recovery rápido
- **Gateway embebido** (opcional)

Un cluster Zeebe tiene múltiples brokers, cada uno gestionando varias particiones.

## Ubicación del Código

```
zeebe/broker/src/main/java/io/camunda/zeebe/broker/
├── Broker.java                  ← Clase principal
├── bootstrap/                   ← Secuencia de arranque (11 pasos)
├── clustering/                  ← Configuración de cluster
├── engine/                      ← Integración con el engine
├── exporter/                    ← Gestión de exporters
├── jobstream/                   ← Job streaming a workers
├── logstreams/                  ← Adaptador: Raft → LogStream
├── partitioning/                ← Ciclo de vida de particiones
├── raft/                        ← Validación de entradas Raft
├── system/                      ← Monitorización y sistema
│   ├── configuration/           ← BrokerCfg y sub-configs
│   └── monitoring/              ← Health checks
└── transport/                   ← Capa de transporte de red
```

## Arranque del Broker (11 Pasos Secuenciales)

El `BrokerStartupProcess` orquesta el arranque en orden:

```
1. ClusterServicesStep
   └─ Arranca servicios de comunicación del cluster (Atomix)
   
2. ClusterTopologyManagerStep
   └─ Inicializa gestión de topología (quién es quién en el cluster)
   
3. DiskSpaceUsageMonitorStep
   └─ Monitor de espacio en disco (prevenir data corruption)
   
4. MonitoringServerStep
   └─ Arranca BrokerHealthCheckService como actor
   └─ Endpoints de health/readiness
   
5. ApiMessagingServiceStep
   └─ Configura el bus de mensajes para API interna
   
6. GatewayBrokerTransportStep
   └─ Inicializa transporte de red para gateway → broker
   
7. CommandApiServiceStep
   └─ Crea CommandApiServiceImpl
   └─ Configura backpressure (Netflix concurrency-limits)
   
8. EmbeddedGatewayServiceStep
   └─ Si está habilitado: arranca gateway gRPC dentro del broker
   └─ Crea JobStreamClientImpl para streaming de jobs
   
9. JobStreamServiceStep
   └─ Infraestructura para streaming de jobs a workers
   
10. PartitionManagerStep
    └─ Arranca PartitionManagerImpl
    └─ Crea particiones Raft
    └─ Inicia stream processors por partición
    
11. BrokerAdminServiceStep
    └─ API de administración (pause/resume exporters, take snapshot, etc.)
```

**Nota**: Cada paso tiene métricas de tiempo (`BrokerStepMetricDecorator`).

## Configuración del Broker

`BrokerCfg` contiene toda la configuración:

```
BrokerCfg
├── NetworkCfg           → host, ports, buffer sizes
├── ClusterCfg           → nodeId, clusterSize, partitionsCount, replicationFactor
├── ThreadsCfg           → cpuThreadCount, ioThreadCount
├── DataCfg              → dataDirectory, disk usage settings
├── ExporterCfg          → lista de exporters (className, args)
├── EmbeddedGatewayCfg   → enable/disable gateway embebido, network config
├── BackpressureCfg      → algoritmo de backpressure, límites
├── ProcessingCfg        → maxCommandsInBatch, etc.
├── ExperimentalCfg      → features experimentales
└── ...
```

## Particiones

### Concepto

- Un broker gestiona N **particiones**
- Cada partición tiene su propio log, estado (RocksDB), y stream processor
- Cada partición tiene un **líder** (read/write) y **seguidores** (read, replican)
- El líder se elige por **Raft consensus**

### Distribución

```
Cluster de 3 nodos, 3 particiones, replication factor 3:

Nodo 0: Partición 1 (LEADER)  | Partición 2 (FOLLOWER) | Partición 3 (FOLLOWER)
Nodo 1: Partición 1 (FOLLOWER)| Partición 2 (LEADER)   | Partición 3 (FOLLOWER)
Nodo 2: Partición 1 (FOLLOWER)| Partición 2 (FOLLOWER)  | Partición 3 (LEADER)
```

### Ciclo de Vida de una Partición

```
1. Raft Group se forma entre los nodos
2. Se elige un LEADER via Raft
3. El leader arranca:
   ├─ LogStream (adaptador sobre Raft log)
   ├─ RocksDB (estado)
   ├─ StreamProcessor (replay + processing)
   ├─ Exporters
   └─ Snapshot scheduler
4. Los followers replican el log
5. Si el leader cae, se elige nuevo leader → replay desde snapshot
```

### AtomixLogStorage - El puente Raft ↔ LogStream

```
zeebe/broker/logstreams/AtomixLogStorage.java
```

Adapta el log de Raft (Atomix) a la interfaz `LogStream` que usa el Stream Platform. Así el stream processor no sabe que debajo hay Raft.

## Health Checks

El broker expone estados de salud por partición:

```
HEALTHY     → Todo funciona
UNHEALTHY   → Problemas detectados (ej: disco lleno)
DEAD        → Sin recuperación posible
```

El `BrokerHealthCheckService` agrega el estado de todas las particiones.

## Embedded Gateway

Por defecto, el broker incluye un **gateway embebido** que expone gRPC/REST:
- Simplifica deploys de nodo único
- En producción, los gateways pueden ser separados (stateless, escalables)
- Configurable con `EmbeddedGatewayCfg`

## Backpressure

El `CommandApiServiceStep` configura backpressure usando **Netflix concurrency-limits**:
- Limita el número de comandos concurrentes
- Algoritmo adaptativo que se ajusta a la capacidad real
- Protege al broker de sobrecarga

## Exporters

Los exporters se configuran en `BrokerCfg.exporters`. Las implementaciones concretas
(Elasticsearch, OpenSearch, etc.) se encuentran en un repositorio separado.

```yaml
exporters:
  myexporter:
    className: com.example.MyExporter
    args:
      url: http://localhost:9200
```

El broker gestiona:
- Inicialización de exporters al arrancar
- Posición de export tracking (cada exporter tiene su posición)
- Pausa/resume vía admin API
- Retry automático en caso de fallo

## Job Streaming

El broker soporta streaming de jobs directamente a workers:
- `JobStreamServiceStep` arranca la infraestructura
- Los workers abren un stream bilateral
- El broker pushea jobs disponibles al worker sin esperar polling
- Más eficiente que long-polling para cargas altas

## Admin API

`BrokerAdminServiceStep` expone operaciones administrativas:
- Pausar/resumir procesamiento
- Pausar/resumir exporters
- Tomar snapshot manualmente
- Obtener estado de particiones
- Ban/unban de instancias de proceso

## Diagrama de un Nodo Broker

```
┌─────────────────────────────────────────────────────────────┐
│                     BROKER NODE                              │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              Embedded Gateway (opcional)                │  │
│  │  ┌──────┐  ┌──────────┐  ┌───────────────────────┐    │  │
│  │  │ gRPC │  │   REST   │  │    Interceptors       │    │  │
│  │  │:26500│  │  :8080   │  │  (auth, metrics)      │    │  │
│  │  └──┬───┘  └────┬─────┘  └───────────────────────┘    │  │
│  └─────┼────────────┼────────────────────────────────────┘  │
│        │            │                                        │
│  ┌─────▼────────────▼────────┐                              │
│  │     Command API           │  ← Backpressure              │
│  │  (CommandApiServiceImpl)  │                              │
│  └───────────┬───────────────┘                              │
│              │                                               │
│  ╔═══════════╧═══════════════════════════════════════════╗  │
│  ║              PARTICIÓN 1 (LEADER)                      ║  │
│  ║  ┌──────────┐  ┌──────────────┐  ┌───────────────┐   ║  │
│  ║  │   Raft   │  │  Log Stream  │  │   RocksDB     │   ║  │
│  ║  │ (Atomix) │─▶│  (Journal)   │  │   (Estado)    │   ║  │
│  ║  └──────────┘  └──────┬───────┘  └───────┬───────┘   ║  │
│  ║                       │                   │            ║  │
│  ║              ┌────────▼───────────────────▼────────┐   ║  │
│  ║              │         Stream Processor            │   ║  │
│  ║              │  ┌─────────┐  ┌──────────────────┐  │   ║  │
│  ║              │  │ Engine  │  │    Exporters     │  │   ║  │
│  ║              │  │         │  │ (ES, OpenSearch) │  │   ║  │
│  ║              │  └─────────┘  └──────────────────┘  │   ║  │
│  ║              └─────────────────────────────────────┘   ║  │
│  ║              ┌─────────────────────────────────────┐   ║  │
│  ║              │         Snapshot Store              │   ║  │
│  ║              └─────────────────────────────────────┘   ║  │
│  ╚════════════════════════════════════════════════════════╝  │
│                                                              │
│  ╔═══════════════════════════════════════════════════════╗  │
│  ║              PARTICIÓN 2 (FOLLOWER)                    ║  │
│  ║  (Solo replica, no procesa hasta ser elegido leader)   ║  │
│  ╚════════════════════════════════════════════════════════╝  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  Topology Manager │ Job Stream │ Admin API │ Health    │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Comunicación entre Nodos

| Puerto | Protocolo |                       Uso                       |
|--------|-----------|-------------------------------------------------|
| 26500  | gRPC      | API pública (clientes)                          |
| 26501  | Internal  | Command API (gateway → broker)                  |
| 26502  | Internal  | Cluster internal (Raft replication, membership) |

El transporte usa **Netty** con el framework **Atomix** para comunicación inter-nodo.
