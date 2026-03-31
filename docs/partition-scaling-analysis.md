# Análisis en Profundidad: Escalado Dinámico de Particiones en Zenda/Zeebe

## Resumen Ejecutivo

Actualmente el sistema permite escalar el número de **nodos** (brokers) en caliente mediante la API de topología (`scaleMembers`, `addMembers`, `removeMembers`), pero el **número de particiones es fijo desde el bootstrap del cluster** y no puede cambiarse en tiempo de ejecución. Este documento detalla la implementación de la **Estrategia A ("Solo Nuevos")** con **Routing Estable por Generaciones** para permitir escalar particiones dinámicamente sin romper las suscripciones de mensajes existentes.

---

## 1. Modelo de Routing por Generaciones

### 1.1 Concepto Fundamental

Cada vez que se escalan las particiones, se crea una **generación de routing** que registra el partition count en ese momento. Las suscripciones de mensajes existentes siguen usando el partition count de la generación bajo la que fueron creadas.

```
Generación 1: partitionCount=10  (cluster inicial)
Generación 2: partitionCount=15  (primer escalado)
Generación 3: partitionCount=30  (segundo escalado)  ← actual
```

### 1.2 ¿Cómo sabes cuándo ha terminado el período de transición?

**Una generación se retira cuando no quedan suscripciones de mensajes abiertas que fueron creadas bajo esa generación.**

Concretamente:

1. Cada `ProcessMessageSubscription` se crea con `subscriptionPartitionId = hash(correlationKey) % partitionCount_de_su_generacion + 1`
2. Esa suscripción vive en la partición calculada con el partition count de **su** generación
3. Cuando la suscripción se **cumple** (mensaje correlado), se **cancela**, o su **instancia de proceso completa/termina** → esa suscripción deja de existir
4. Cuando la última suscripción de la generación N se cierra → **la generación N se retira**

**Mecanismo de detección:**

```
Para cada partición, el engine trackea:
  - El número de suscripciones abiertas creadas con cada partition count histórico
  - Cuando un partition count llega a 0 suscripciones → notifica al coordinador

El coordinador agrega los reportes de TODAS las particiones:
  - Generación N se retira SOLO si TODAS las particiones reportan 0 suscripciones para esa generación
```

**En la práctica, esto sucede cuando:**
- Todos los procesos con message catch events creados antes del escalado han terminado
- Esto puede tardar desde minutos hasta semanas, dependiendo de la duración de los procesos

**Optimización:** Se puede forzar la retirada de una generación si se sabe que no hay procesos activos con esos eventos. El endpoint sería `POST /topology/routing/retire?generationId=1`.

### 1.3 ¿Qué pasa si se escala de 10 → 15 y luego de 15 → 30?

**Se acumulan generaciones activas.** Cada escalado crea una nueva generación:

```
Tras escalar 10 → 15:
  Generación 1: partitionCount=10 (puede tener suscripciones activas)
  Generación 2: partitionCount=15 ← actual

Meses después, escalar 15 → 30:
  Generación 1: partitionCount=10 (si aún tiene suscripciones activas)
  Generación 2: partitionCount=15 (puede tener suscripciones activas)
  Generación 3: partitionCount=30 ← actual
```

**Cuando un mensaje llega con correlationKey "order-123" (hash=12345):**

```java
Gen 1: 12345 % 10 + 1 = partición 6    // suscripción podría estar aquí
Gen 2: 12345 % 15 + 1 = partición 1    // o aquí
Gen 3: 12345 % 30 + 1 = partición 16   // o aquí

→ El mensaje se envía a particiones {1, 6, 16} (deduplicated)
→ Solo correlará donde haya una suscripción activa esperando
```

**Ejemplo timeline:**

```
T=0:  Cluster con 10 particiones. Proceso A se crea, suscripción en partition 6.
T=1:  Escalar a 15. Generación 2 creada.
T=2:  Proceso B se crea, suscripción en partition 1. (hash % 15)
T=3:  Mensaje "order-123" llega → se envía a {6, 1}
      → Correlaciona en partition 6 (Proceso A) ✓
      → Partition 1 no tiene suscripción → descarta (no-op) ✓
T=4:  Proceso A completa. Generación 1 se retira (0 suscripciones abiertas).
T=5:  Escalar a 30. Generación 3 creada.
T=6:  Proceso C se crea, suscripción en partition 16. (hash % 30)
T=7:  Mensaje "order-123" llega → se envía a {1, 16} (Gen 1 ya retirada)
      → Correlaciona en partition 1 (Proceso B) ✓
T=8:  Proceso B completa. Generación 2 se retira.
T=9:  Mensaje "order-123" llega → se envía solo a {16} (totalmente convergido)
```

**Coste del multi-envío:** El overhead escala con el número de generaciones activas (típicamente 1-2, máximo 3 si se hacen escalados muy seguidos).

---

## 1. Estado Actual: Por Qué las Particiones Son Fijas

### 1.1 Configuración Estática en Bootstrap

El número de particiones se configura en `ClusterCfg.java` y se fija al inicializar el cluster:

```java
// ClusterCfg.java
public static final int DEFAULT_PARTITIONS_COUNT = 1;
private int partitionsCount = DEFAULT_PARTITIONS_COUNT;

private void initPartitionIds() {
    partitionIds = IntStream.range(START_PARTITION_ID, START_PARTITION_ID + partitionsCount)
        .boxed().collect(Collectors.toList());
}
```

### 1.2 Partition Count Derivado, No Almacenado

En `ClusterTopology.java`, el número de particiones no es un campo explícito — se **deriva** de las particiones presentes en los miembros:

```java
public int partitionCount() {
    return (int) members.values().stream()
        .flatMap(m -> m.partitions().keySet().stream())
        .distinct().count();
}
```

Esto significa que hoy en día si no hay ningún miembro que tenga una partición X, esa partición "no existe" para la topología.

### 1.3 Operaciones de Topología Existentes

Las operaciones actuales permiten **mover particiones entre nodos** pero no **crear nuevas particiones**:

| Operación | Qué hace | Crea particiones nuevas |
|-----------|----------|------------------------|
| `MemberJoinOperation` | Añade un nodo al cluster | No |
| `MemberLeaveOperation` | Quita un nodo del cluster | No |
| `PartitionJoinOperation` | Añade una réplica de partición existente a un nodo | No |
| `PartitionLeaveOperation` | Quita una réplica de partición de un nodo | No |
| `PartitionReconfigurePriorityOperation` | Cambia prioridad de liderazgo | No |
| `PartitionForceReconfigureOperation` | Fuerza reconfiguración | No |

**No existe una `PartitionCreateOperation`** que cree una partición nueva con un ID que no existía previamente.

---

## 2. Componentes Afectados por un Cambio de Partition Count

### 2.1 🔴 Encoding de Keys (CRÍTICO — ROMPE COMPATIBILIDAD)

```java
// Protocol.java
public static final int PARTITION_BITS = 13;  // max 8192 particiones
public static final int KEY_BITS = 51;

public static long encodePartitionId(int partitionId, long key) {
    return ((long) partitionId << KEY_BITS) + key;
}
```

**Implicación**: Los keys de todas las entidades (process instances, jobs, messages, deployments, etc.) llevan el partition ID **embebido en los 13 bits altos**. Esto es **irreversible** — una entidad creada en la partición 3 siempre tendrá la partición 3 en su key.

**Impacto en escalado de particiones**: Las entidades existentes **no se pueden migrar** a nuevas particiones sin cambiar sus keys, lo cual rompería todas las referencias externas (IDs que los clientes ya conocen).

### 2.2 🔴 Correlación de Mensajes (CRÍTICO — CAMBIA ROUTING)

```java
// SubscriptionUtil.java
public static int getSubscriptionPartitionId(DirectBuffer correlationKey, int partitionCount) {
    int hashCode = getSubscriptionHashCode(correlationKey);
    return Math.abs(hashCode % partitionCount) + START_PARTITION_ID;
}
```

**Implicación**: La partición destino de un mensaje se calcula como `hash(correlationKey) % partitionCount`. Si el partition count cambia de 3 a 6, el mismo `correlationKey` puede mapear a una **partición diferente**.

**Ejemplo**:
- Con 3 particiones: `hash("order-123") % 3 + 1 = partición 2`
- Con 6 particiones: `hash("order-123") % 6 + 1 = partición 4`

Si hay una suscripción de mensaje esperando en partición 2, los nuevos mensajes irían a partición 4 → **correlación rota**.

### 2.3 🔴 Distribución de Deployments (CRÍTICO)

```java
// DeploymentCreateProcessor - DEPLOYMENT_PARTITION = 1 (siempre)
// CommandDistributionBehavior:
otherPartitions = IntStream.range(START_PARTITION_ID, START_PARTITION_ID + partitionsCount)
    .filter(partition -> partition != currentPartition)
    .boxed().toList();
```

**Implicación**: La lista de "otras particiones" a las que distribuir deployments se calcula **una vez en la construcción** del `CommandDistributionBehavior`. Las nuevas particiones no recibirían los deployments existentes automáticamente.

**Impacto**: Una nueva partición arrancaría sin ningún proceso/decisión desplegada → no podría crear instancias de procesos ya desplegados.

### 2.4 🟡 Gateway Routing (MEDIO)

```java
// RequestRetryHandler:
partitionIdIteratorForType(topology.getPartitionsCount())

// PublishMessageDispatchStrategy:
SubscriptionUtil.getSubscriptionPartitionId(correlationKey, partitionsCount)
```

El gateway lee `partitionsCount` desde la topología del cluster (vía gossip). Si la topología reporta nuevas particiones, el gateway **podría adaptarse automáticamente** gracias a que es dinámico. Sin embargo, el cambio en routing de mensajes rompe la correlación como se explica en 2.2.

### 2.5 🟡 Backup/Restore (MEDIO)

```java
// BackupDescriptor:
int numberOfPartitions();

// ValidatePartitionCount (en RestoreManager):
if (descriptor.numberOfPartitions() != expectedPartitionCount) {
    throw new BackupNotValidException(...);
}
```

**Implicación**: El backup almacena el `numberOfPartitions` en el descriptor. Un restore **valida** que el partition count actual coincida con el del backup. Si se añaden particiones, los backups anteriores no se podrán restaurar directamente.

### 2.6 🟡 Raft Groups por Partición (MEDIO)

Cada partición es un grupo Raft independiente (`RaftPartition`). Crear una nueva partición implica:
1. Crear un nuevo `RaftPartition` con su propio log (Journal)
2. Bootstrap del Raft group con el subconjunto de nodos asignados
3. Crear el `ZeebePartition` con su RocksDB (estado), StreamProcessor, y Exporters
4. El `PartitionManagerImpl` gestiona el lifecycle — soporta `joinPartition()` pero siempre para particiones que ya existían en la distribución

### 2.7 🟢 Topología / Gossip (BAJO IMPACTO)

El protocolo gossip y `ClusterTopology` podrían adaptarse relativamente fácil ya que el `partitionCount()` es derivado dinámicamente de los miembros.

### 2.8 🟢 Exporters (BAJO IMPACTO)

Los exporters (Elasticsearch/OpenSearch) operan por partición de forma independiente. Una nueva partición lanzaría nuevos exporters sin impactar las existentes.

---

## 3. Arquitectura de Keys y la Imposibilidad de Migrar Datos

### 3.1 El Problema Fundamental

```
Key de 64 bits:
┌─────────────┬─────────────────────────────────────────────────────┐
│ 13 bits     │ 51 bits                                             │
│ Partition ID│ Local Key (secuencial por partición)                │
└─────────────┴─────────────────────────────────────────────────────┘
```

Esto significa que:
- **NO se pueden migrar entidades entre particiones** sin cambiar su key
- Los keys son **referencias públicas** (los clientes los usan para cancel, resolve, complete, etc.)
- El estado en RocksDB está indexado por estos keys
- Los registros en el Journal tienen estos keys embebidos

### 3.2 Consecuencia

A diferencia de Kafka (donde se pueden re-particionar topics moviendo datos), en Zeebe **los datos son inmóviles** una vez creados en una partición.

---

## 4. Implementación Realizada — Estrategia A con Routing Estable

### 4.1 Nuevos Ficheros Creados

| Fichero | Propósito |
|---------|-----------|
| `topology/state/MessageRoutingState.java` | Modelo de generaciones de routing |
| `topology/changes/PartitionBootstrapApplier.java` | Applier para crear nueva partición desde cero |
| `topology/api/AddPartitionsRequestTransformer.java` | Transforma AddPartitionsRequest → operaciones |
| `broker-client/api/MultiPartitionDispatchStrategy.java` | Interfaz para rutear a múltiples particiones |
| `engine/deployment/distribute/NewPartitionDeploymentDistributor.java` | Detecta y distribuye deployments a nuevas particiones |

### 4.2 Ficheros Modificados

| Fichero | Cambio |
|---------|--------|
| `topology/state/TopologyChangeOperation.java` | +`PartitionBootstrapOperation` (nuevo sealed record) |
| `topology/state/ClusterTopology.java` | +`MessageRoutingState` field, +routing generation methods |
| `topology/state/PartitionState.java` | +`BOOTSTRAPPING` state, +`bootstrapping()` factory |
| `topology/changes/TopologyChangeAppliersImpl.java` | Registrar `PartitionBootstrapApplier` |
| `topology/changes/PartitionChangeExecutor.java` | +`bootstrap()` method |
| `topology/api/TopologyManagementRequest.java` | +`AddPartitionsRequest` record |
| `topology/api/TopologyManagementApi.java` | +`addPartitions()` method |
| `topology/api/TopologyManagementRequestsHandler.java` | +`addPartitions()` handler |
| `protocol-impl/SubscriptionUtil.java` | +`getSubscriptionPartitionIds()` multi-generation |
| `broker-client/api/BrokerClusterState.java` | +`getActiveRoutingPartitionCounts()` |
| `broker-client/impl/BrokerClusterStateImpl.java` | Implementar routing partition counts |
| `broker-client/impl/BrokerTopologyManagerImpl.java` | Propagar routing state al gateway |
| `gateway/impl/broker/PublishMessageDispatchStrategy.java` | +`determinePartitions()` multi-gen |
| `broker/partitioning/PartitionManagerImpl.java` | +`bootstrap()` para crear partición nueva |
| `engine/processing/common/CommandDistributionBehavior.java` | +`getOtherPartitionsForCount()` dinámico |
| `restore/RestoreManager.java` | Relajar validación: permitir backup con menos particiones |

### 4.3 Flujo Completo de Escalado

```
1. Admin llama: POST /topology/partitions
   { "newPartitionCount": 15, "members": ["0","1","2"], "replicationFactor": 3 }
   ↓
2. TopologyManagementRequestsHandler.addPartitions()
   ↓
3. AddPartitionsRequestTransformer:
   - Valida: 15 > 10 (actual) ✓
   - Genera PartitionBootstrapOperation por cada (miembro, partición nueva)
   - RoundRobinDistributor distribuye particiones 11-15 entre brokers
   ↓
4. TopologyChangeCoordinator ejecuta operaciones secuencialmente:
   For each PartitionBootstrapOperation:
     a. PartitionBootstrapApplier.initMemberState():
        - Valida: partición no existe, miembro ACTIVE
        - Marca partición como BOOTSTRAPPING en topología
     b. PartitionBootstrapApplier.applyOperation():
        - Llama PartitionChangeExecutor.bootstrap()
        - PartitionManagerImpl crea nuevo RaftPartition + ZeebePartition
        - Raft group se bootstrapa con log y RocksDB vacíos
        - Marca partición como ACTIVE
   ↓
5. ClusterTopology.addRoutingGeneration(15):
   - Generación 2 creada: {generationId=2, partitionCount=15}
   ↓
6. Gossip propaga nueva topología a todos los brokers y gateways
   ↓
7. Gateway ve activeRoutingPartitionCounts = {10, 15}
   - Mensajes ahora se envían a 2 particiones por correlationKey
   ↓
8. DeploymentRedistributor en partition 1 detecta nuevas particiones
   - Redistribuye todos los deployments a particiones 11-15
   ↓
9. Nuevas particiones reciben deployments y son plenamente funcionales
```

### 4.4 Diagrama de Estado Durante Transición

```
┌─────────────────────────────────────────────────────┐
│           ClusterTopology v50                       │
├─────────────────────────────────────────────────────┤
│ MessageRoutingState:                                │
│   Generation 1: partitionCount=10, retired=false    │
│   Generation 2: partitionCount=15, retired=false ←  │
├─────────────────────────────────────────────────────┤
│ Broker-0 (ACTIVE)                                   │
│   ├─ Partitions 1-5: ACTIVE                         │
│   ├─ Partition 11: ACTIVE (NEW)                     │
│   └─ Partition 14: ACTIVE (NEW)                     │
├─────────────────────────────────────────────────────┤
│ Broker-1 (ACTIVE)                                   │
│   ├─ Partitions 1-5: ACTIVE                         │
│   ├─ Partition 12: ACTIVE (NEW)                     │
│   └─ Partition 15: ACTIVE (NEW)                     │
├─────────────────────────────────────────────────────┤
│ Broker-2 (ACTIVE)                                   │
│   ├─ Partitions 1-5: ACTIVE                         │
│   └─ Partition 13: ACTIVE (NEW)                     │
└─────────────────────────────────────────────────────┘

Message "order-123" (hash=12345):
  Gen 1: 12345 % 10 + 1 = partition 6 ──→ Broker con P6
  Gen 2: 12345 % 15 + 1 = partition 1 ──→ Broker con P1
  → Se envía a ambas. Solo la que tenga suscripción activa correlacionará.
```

---

## 5. Respuestas a Preguntas Clave

### 5.1 ¿Cómo se sabe cuándo termina la transición?

**Mecánica precisa:**

```
                    ┌──────────────────────┐
                    │  Partición N         │
                    │                      │
                    │  Contador por Gen:   │
                    │  Gen1: 3 subscrips   │
                    │  Gen2: 7 subscrips   │
                    └───────┬──────────────┘
                            │ Periódicamente reporta al coordinador
                            ▼
                    ┌──────────────────────┐
                    │  Coordinador         │
                    │                      │
                    │  Gen 1 total: 15     │  ← Suma de todas las particiones
                    │  Gen 2 total: 42     │
                    └───────┬──────────────┘
                            │ Cuando Gen1 total = 0
                            ▼
                    ┌──────────────────────┐
                    │  retireGeneration(1) │
                    │                      │
                    │  → Gen 1 retired!    │
                    │  → Gossip update     │
                    │  → Gateway reduce    │
                    │    message fan-out   │
                    └──────────────────────┘
```

**Criterio concreto:** Generación N se retira cuando:
1. TODAS las particiones reportan 0 suscripciones de mensajes abiertas que usaron `hash % N.partitionCount`
2. O el admin fuerza la retirada con `POST /topology/routing/retire?generationId=N`

### 5.2 ¿Qué pasa si se escala 10→15 y meses después 15→30?

```
Estado tras 10→15→30:

MessageRoutingState:
  Gen 1: {id=1, partitionCount=10, retired=false}  ← procesos largos aún activos
  Gen 2: {id=2, partitionCount=15, retired=false}  ← procesos del mes pasado
  Gen 3: {id=3, partitionCount=30, retired=false}  ← actual

Mensaje con correlationKey "order-XYZ":
  Gen 1 target: hash % 10 + 1 = P3
  Gen 2 target: hash % 15 + 1 = P8
  Gen 3 target: hash % 30 + 1 = P23
  → Mensaje se envía a {3, 8, 23}

Conforme los procesos terminan:
  - Gen 1 todas las suscripciones cerradas → retire Gen 1
  - Ahora: mensaje se envía a {8, 23}
  - Gen 2 todas cerradas → retire Gen 2
  - Ahora: mensaje se envía a {23} (fully converged)
```

**Overhead:** Con 3 generaciones activas, cada publicación de mensaje se envía como máximo a 3 particiones (en lugar de 1). Este overhead es:
- **Aceptable** para cargas normales (el broker descarta rápidamente si no hay suscripción)
- **Temporal** — se reduce automáticamente conforme los procesos completan
- **Controlable** — el admin puede forzar retire si sabe que ya no hay procesos activos de una generación

---

## 6. Archivos Implementados — Resumen

```
NUEVOS:
zeebe/topology/src/main/java/.../state/MessageRoutingState.java
zeebe/topology/src/main/java/.../changes/PartitionBootstrapApplier.java
zeebe/topology/src/main/java/.../api/AddPartitionsRequestTransformer.java
zeebe/broker-client/src/main/java/.../api/MultiPartitionDispatchStrategy.java
zeebe/engine/src/main/java/.../distribute/NewPartitionDeploymentDistributor.java

MODIFICADOS:
zeebe/topology/src/main/java/.../state/TopologyChangeOperation.java
zeebe/topology/src/main/java/.../state/ClusterTopology.java
zeebe/topology/src/main/java/.../state/PartitionState.java
zeebe/topology/src/main/java/.../changes/TopologyChangeAppliersImpl.java
zeebe/topology/src/main/java/.../changes/PartitionChangeExecutor.java
zeebe/topology/src/main/java/.../api/TopologyManagementRequest.java
zeebe/topology/src/main/java/.../api/TopologyManagementApi.java
zeebe/topology/src/main/java/.../api/TopologyManagementRequestsHandler.java
zeebe/protocol-impl/src/main/java/.../SubscriptionUtil.java
zeebe/broker-client/src/main/java/.../api/BrokerClusterState.java
zeebe/broker-client/src/main/java/.../impl/BrokerClusterStateImpl.java
zeebe/broker-client/src/main/java/.../impl/BrokerTopologyManagerImpl.java
zeebe/gateway/src/main/java/.../broker/PublishMessageDispatchStrategy.java
zeebe/broker/src/main/java/.../partitioning/PartitionManagerImpl.java
zeebe/engine/src/main/java/.../common/CommandDistributionBehavior.java
zeebe/restore/src/main/java/.../RestoreManager.java
```

---

## 7. Trabajo Pendiente (no implementado)

| # | Item | Prioridad |
|---|------|-----------|
| 1 | Serialización de `MessageRoutingState` en Raft log (protobuf/custom) | Alta |
| 2 | Endpoint REST/gRPC para `addPartitions` (proto + controlador) | Alta |
| 3 | Mecanismo de conteo de suscripciones por generación en el engine | Alta |
| 4 | Endpoint para forzar retire de generación | Media |
| 5 | Redistribución automática de deployments a nuevas particiones | Alta |
| 6 | Tests de integración: escalado + message correlation | Alta |
| 7 | Tests de integración: escalado + backup/restore | Media |
| 8 | Métricas de Grafana para generaciones activas y overhead | Baja |
| 9 | Documentación para operadores (runbook) | Media |
