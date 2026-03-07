# 21 - Topología y Clustering

## Propósito

El módulo `topology` gestiona la distribución de particiones entre brokers, el descubrimiento de nodos, y el escalado dinámico del cluster.

## Estructura del Módulo

```
zeebe/topology/src/main/java/io/camunda/zeebe/topology/
├── state/
│   ├── ClusterTopology.java         ← Estado inmutable del cluster
│   ├── MemberState.java             ← Estado por nodo
│   ├── PartitionState.java          ← Estado por partición-por-nodo
│   ├── ClusterChangePlan.java       ← Plan de cambio en curso
│   └── CompletedChange.java         ← Historial de cambios
├── changes/
│   ├── TopologyChangeCoordinator.java   ← Orquesta cambios
│   ├── TopologyChangeAppliers.java      ← Aplica operaciones
│   ├── MemberJoinApplier.java           ← Añadir nodo
│   ├── MemberLeaveApplier.java          ← Quitar nodo
│   ├── PartitionJoinApplier.java        ← Añadir partición a nodo
│   ├── PartitionLeaveApplier.java       ← Quitar partición de nodo
│   └── PartitionReconfigurePriorityApplier.java ← Cambiar prioridad
├── gossip/
│   ├── ClusterTopologyGossiper.java     ← Protocolo gossip
│   └── ClusterTopologyGossipState.java  ← Estado del gossip
├── ClusterTopologyManager.java      ← API principal
├── TopologyInitializer.java         ← Inicialización al arrancar
├── PersistedClusterTopology.java    ← Persistencia en Raft
├── PartitionDistributor.java        ← Algoritmo de distribución
├── StaticConfiguration.java         ← Config estática (testing)
├── TopologyUpdateNotifier.java      ← Listeners de cambios
└── GatewayClusterTopologyService.java ← Expone topología al gateway
```

## Modelo de Datos

### ClusterTopology (Inmutable)

```java
public record ClusterTopology(
    long version,                              // Incrementa en cada cambio
    Map<MemberId, MemberState> members,        // Estado por nodo
    Optional<CompletedChange> lastChange,      // Último cambio completado
    Optional<ClusterChangePlan> pendingChanges // Cambio en curso
) {
    // Todas las mutaciones retornan NUEVA instancia
    ClusterTopology addMember(MemberId id, MemberState state);
    ClusterTopology updateMember(MemberId id, UnaryOperator<MemberState> updater);
    ClusterTopology removeMember(MemberId id);
    boolean isUninitialized();  // version == -1
}
```

### MemberState

```java
public class MemberState {
    MemberId id;                            // ID del nodo Atomix
    State state;                            // ACTIVE, JOINING, LEAVING
    long version;                           // Última versión de cambio
    Map<PartitionId, PartitionState> partitions; // Particiones en este nodo
}
```

### PartitionState

```java
public class PartitionState {
    PartitionId id;
    int priority;          // Prioridad de liderazgo (menor = preferido)
    State state;           // BOOTSTRAPPING, ACTIVE, LEAVING
}
```

## Distribución de Particiones

### Algoritmo Round-Robin

```java
public interface PartitionDistributor {
    Set<PartitionMetadata> distributePartitions(
        Set<MemberId> members,
        List<PartitionId> sortedPartitionIds,
        int replicationFactor
    );
}
// Implementación: RoundRobinPartitionDistributor
```

### Ejemplo: 3 brokers, 6 particiones, replication-factor=3

```
Broker-0: Partition 1(L), 2,    3,    4(L), 5,    6
Broker-1: Partition 1,    2(L), 3,    4,    5(L), 6
Broker-2: Partition 1,    2,    3(L), 4,    5,    6(L)

(L) = Líder preferido (priority más bajo)

Cada partición está en exactamente 3 nodos (replication-factor=3)
Round-robin asegura distribución balanceada
```

## Protocolo Gossip

### ClusterTopologyGossiper

Propagación **eventually-consistent** de la topología:

```
Integración:
├── ClusterCommunicationService  ← Envío/recepción de mensajes
└── ClusterMembershipService     ← Lifecycle de miembros

Topics:
├── "cluster-topology-sync"      ← Request: pedir topología completa
└── "cluster-topology-gossip"    ← Broadcast: propagar cambios

Estado:
└── ClusterTopologyGossipState   ← Versión conocida (evita re-gossip)
```

### Flujo del Gossip

```
Broker-0 tiene cambio de topología (version N+1)
   ↓
ClusterTopologyGossiper.gossip()
   ├─ Selecciona subset aleatorio de nodos
   └─ Envía en topic "cluster-topology-gossip"
   ↓
Broker-1 recibe gossip
   ├─ Compara version: si N+1 > local version N → actualizar
   └─ Propaga a sus vecinos
   ↓
Broker-2 recibe (transitivamente)
   └─ Eventualmente todos convergen
```

### Sync Periódico

```
scheduleSync():
  ├─ Periódicamente envía "cluster-topology-sync" a nodo aleatorio
  └─ El nodo responde con su topología completa
  └─ Si más reciente → actualizar local
```

## Cambios de Topología

### TopologyChangeCoordinator

```java
public interface TopologyChangeCoordinator {
    ActorFuture<ClusterTopology> getTopology();
    
    // Ejecutar cambio real
    ActorFuture<TopologyChangeResult> applyOperations(TopologyChangeRequest request);
    
    // Simular cambio (dry-run)
    ActorFuture<TopologyChangeResult> simulateOperations(TopologyChangeRequest request);
    
    // Cancelar cambio en curso
    ActorFuture<ClusterTopology> cancelChange(long changeId);
}

// Request funcional
interface TopologyChangeRequest {
    Either<Exception, List<TopologyChangeOperation>> operations(ClusterTopology current);
    boolean isForced();
}
```

### Appliers Disponibles

| Applier | Operación | Transiciones |
|---------|-----------|-------------|
| `MemberJoinApplier` | Añadir nodo | → JOINING → ACTIVE |
| `MemberLeaveApplier` | Quitar nodo | ACTIVE → LEAVING → removed |
| `PartitionJoinApplier` | Añadir partición a nodo | → BOOTSTRAPPING → ACTIVE |
| `PartitionLeaveApplier` | Quitar partición de nodo | ACTIVE → LEAVING → removed |
| `PartitionReconfigurePriorityApplier` | Cambiar prioridad líder | Actualiza priority |
| `PartitionForceReconfigureApplier` | Forzar reconfiguración | Ignora validaciones |

## Escalado Dinámico

### Scale Up (Añadir Broker)

```
1. Request: AddMembers { memberId: "broker-3" }
   ↓
2. TopologyChangeCoordinator.applyOperations()
   ↓
3. MemberJoinApplier:
   ├─ ClusterTopology.addMember("broker-3", JOINING)
   └─ version += 1
   ↓
4. Persistir en Raft log
   ↓
5. PartitionJoinApplier:
   ├─ Rebalancear particiones
   ├─ Asignar particiones al nuevo broker
   └─ Nuevas particiones: BOOTSTRAPPING
   ↓
6. Gossip → todos los brokers se enteran
   ↓
7. Broker-3 empieza a bootstrap particiones asignadas
   ├─ Recibe snapshots de los líderes
   ├─ Empieza replicación
   └─ Cuando alcanza: BOOTSTRAPPING → ACTIVE
   ↓
8. Otros brokers pueden liberar réplicas
   ↓
9. Cambio completado cuando todas las particiones ACTIVE
```

### Scale Down (Quitar Broker)

```
1. Request: RemoveMembers { memberId: "broker-2" }
   ↓
2. simulateOperations() → Validar que es posible
   (ej: no violar replication-factor)
   ↓
3. MemberLeaveApplier:
   ├─ MemberState → LEAVING
   └─ version += 1
   ↓
4. PartitionLeaveApplier × N particiones:
   ├─ Mover réplicas a otros brokers
   └─ Esperar que otros acepten liderazgo
   ↓
5. Broker-2 cierra particiones gracefully
   ↓
6. ClusterTopology.removeMember("broker-2")
   ↓
7. Gossip → todos actualizan topología
```

## Inicialización

### TopologyInitializer

Al arrancar un broker:

```
1. ¿Existe topología persistida?
   ├─ SÍ: Cargar de Raft log → usar
   └─ NO: ¿Primera vez?
       ├─ SÍ (bootstrap): Crear topología inicial con particiones
       └─ NO: Consultar quorum de nodos existentes
```

### Persistencia

```
PersistedClusterTopology:
  - Cada cambio de topología se escribe en Raft log
  - Garantiza consistencia entre nodos
  - Sobrevive a reinicios
```

## Gateway Integration

### GatewayClusterTopologyService

```java
// El Gateway usa la topología para rutear requests:
TopologyDto getTopology();  // Partition → Broker mapping

// El cliente usa esto para saber:
// - Qué broker es líder de cada partición
// - A dónde enviar cada request
// - Estado de salud del cluster
```

### Flujo de Routing

```
Client envía CreateProcessInstance
   ↓
Gateway recibe
   ↓
Gateway consulta topología:
   "¿Quién es líder de partition 1?"
   → Broker-0
   ↓
Gateway envía a Broker-0
   ↓
Si Broker-0 no está disponible:
   ↓
Gateway actualiza topología
   → retry con nuevo líder
```

## Diagrama de Estado Completo

```
Cluster con 3 brokers, 3 particiones:

┌─────────────────────────────────────────────┐
│          ClusterTopology v42                 │
├─────────────────────────────────────────────┤
│ Broker-0 (ACTIVE)                           │
│   ├─ Partition-1: ACTIVE, priority=1 ← LÍDER│
│   ├─ Partition-2: ACTIVE, priority=2        │
│   └─ Partition-3: ACTIVE, priority=3        │
├─────────────────────────────────────────────┤
│ Broker-1 (ACTIVE)                           │
│   ├─ Partition-1: ACTIVE, priority=2        │
│   ├─ Partition-2: ACTIVE, priority=1 ← LÍDER│
│   └─ Partition-3: ACTIVE, priority=2        │
├─────────────────────────────────────────────┤
│ Broker-2 (ACTIVE)                           │
│   ├─ Partition-1: ACTIVE, priority=3        │
│   ├─ Partition-2: ACTIVE, priority=3        │
│   └─ Partition-3: ACTIVE, priority=1 ← LÍDER│
├─────────────────────────────────────────────┤
│ pendingChanges: none                        │
│ lastChange: ScaleUp completed at v41        │
└─────────────────────────────────────────────┘
```
