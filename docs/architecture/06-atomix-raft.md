# 06 - Atomix Raft (Consenso Distribuido)

## Qué es Atomix en Zeebe

Zeebe incluye un **fork de Atomix** que implementa el protocolo de consenso **Raft**. Cada partición del broker tiene su propia instancia de Raft que garantiza:

1. **Elección de líder**: Un solo nodo escribe por partición
2. **Replicación de log**: Todos los cambios se replican a mayoría
3. **Consistencia**: Un record se considera committed cuando N/2+1 nodos lo tienen

## Estructura del Módulo

```
zeebe/atomix/
├── cluster/                     ← Implementación principal de Raft
│   └── src/main/java/io/atomix/raft/
│       ├── cluster/             ← Gestión de miembros (RaftCluster, RaftMember)
│       ├── impl/                ← RaftContext, DefaultRaftServer
│       ├── partition/           ← RaftPartition, RaftPartitionConfig
│       ├── protocol/            ← Mensajes: AppendRequest, VoteRequest, InstallRequest
│       ├── roles/               ← Máquinas de estado: LeaderRole, FollowerRole, CandidateRole
│       ├── snapshot/            ← Instalación de snapshots entre nodos
│       ├── storage/             ← RaftLog, MetaStore, RaftStorage
│       │   ├── log/             ← Log y readers
│       │   ├── log/entry/       ← ApplicationEntry, ConfigurationEntry
│       │   ├── serializer/      ← Serialización
│       │   └── system/          ← MetaStore, Configuration
│       ├── metrics/             ← Métricas de Raft
│       ├── utils/               ← ElectionTimer, VoteQuorum
│       └── zeebe/               ← Integración con Zeebe: ZeebeLogAppender, EntryValidator
└── utils/                       ← Utilidades generales
```

## Componentes Core

### RaftServer (Punto de Entrada)

```java
// Interface principal
public interface RaftServer {
    CompletableFuture<RaftServer> bootstrap(Collection<MemberId> members);
    CompletableFuture<RaftServer> join(MemberId memberId);
    Role getRole();
    RaftCluster cluster();
    void addRoleChangeListener(RaftRoleChangeListener listener);
    void addFailureListener(FailureListener listener);
}
```

### RaftContext (Contenedor de Estado)

El "cerebro" de cada nodo Raft. Mantiene todo el estado de la partición en un **single thread** (ThreadContext):

|          Campo           |           Tipo            |                Propósito                 |
|--------------------------|---------------------------|------------------------------------------|
| `role`                   | `volatile RaftRole`       | Rol actual (Follower, Candidate, Leader) |
| `leader`                 | `volatile MemberId`       | Quién es el líder actual                 |
| `term`                   | `volatile long`           | Término actual de Raft                   |
| `lastVotedFor`           | `MemberId`                | A quién votamos en este término          |
| `commitIndex`            | `long`                    | Último índice committed                  |
| `raftLog`                | `RaftLog`                 | Log local                                |
| `cluster`                | `RaftClusterContext`      | Miembros del cluster                     |
| `meta`                   | `MetaStore`               | Persiste term y voto (seguridad)         |
| `persistedSnapshotStore` | `ReceivableSnapshotStore` | Snapshots                                |

### RaftLog

```java
public class RaftLog {
    RaftLogReader openCommittedReader();     // Solo entries committed
    RaftLogReader openUncommittedReader();  // Todas las entries
    long getCommitIndex();
    void setCommitIndex(long index);
    IndexedRaftLogEntry getLastEntry();
    boolean deleteUntil(long index);        // Compactación del log
}
```

## Jerarquía de Roles

```
INACTIVE
  ├─→ FOLLOWER (ActiveRole)
  │     ├─→ CANDIDATE (ActiveRole)  ← Si timeout de elección
  │     └─→ LEADER (ActiveRole)     ← Si gana elección
  ├─→ PASSIVE (PassiveRole)          ← Non-voting replica
  └─→ PROMOTABLE (PassiveRole)      ← Elegible para promoción
```

### FollowerRole

- Mantiene un `ElectionTimer` que se resetea con cada heartbeat del líder
- Si el timer expira → transiciona a `CandidateRole`
- Procesa `AppendRequest` del líder (replicación de log)
- Procesa `VoteRequest` de candidatos

### CandidateRole

- Inicia elecciones enviando `VoteRequest` a todos los miembros
- Si recibe mayoría de votos → `LeaderRole`
- Si recibe heartbeat de nuevo líder → vuelve a `FollowerRole`
- Timeout → reinicia elección con nuevo `votingRound`

### LeaderRole

- Envía `AppendRequest` periódicos (heartbeat + replicación)
- Usa `LeaderAppender` para gestionar replicación por follower
- Trackea `matchIndex` por follower (hasta dónde han replicado)
- Avanza `commitIndex` cuando N/2+1 tienen el entry

## Elección de Líder

### Flujo Completo

```
1. ElectionTimer expira en Follower
   ↓
2. Pre-Vote (PollRequest) → Envía a todos los miembros
   ├─ Campos: term, candidate, lastLogIndex, lastLogTerm
   ├─ Propósito: Verificar que PUEDE ganar sin incrementar term
   └─ Cada nodo verifica que el candidato está "up-to-date"
   ↓
3. Si mayoría responde OK al poll:
   ↓
4. Vote Request → Envía VoteRequest incrementando term
   ├─ Condiciones para conceder voto:
   │   ├─ Término del candidato ≥ mi término
   │   ├─ No he votado por otro en este término
   │   └─ Log del candidato al menos tan actualizado como el mío
   └─ Persiste voto en MetaStore (durabilidad)
   ↓
5. Si mayoría concede voto:
   ↓
6. Transiciona a LEADER
   └─ appendInitialEntries() → Escribe entry inicial para confirmar liderazgo
```

### Quorum de Votos

|            Tipo            |       Uso       |                Lógica                 |
|----------------------------|-----------------|---------------------------------------|
| `SimpleVoteQuorum`         | Normal          | Requiere N/2+1 votos                  |
| `JointConsensusVoteQuorum` | Reconfiguración | Requiere mayoría en OLD Y NEW members |

### Configuración de Elección

```
electionTimeout: 2500ms             ← Cuánto espera un follower antes de candidatearse
heartbeatInterval: 250ms            ← Frecuencia de heartbeats del líder
priorityElectionEnabled: true       ← Elecciones con prioridad
```

Tipos de timer:
- `RandomizedElectionTimer` - Timeout aleatorio en rango [election, 2×election]
- `PriorityElectionTimer` - Respeta prioridades del nodo

## Replicación de Log

### Protocolo AppendRequest

```
Leader envía AppendRequest:
  ├─ term: término actual del líder
  ├─ leader: ID del líder
  ├─ prevLogIndex: índice antes de las nuevas entries
  ├─ prevLogTerm: término en prevLogIndex
  ├─ entries: lista de PersistedRaftRecord
  └─ commitIndex: último índice committed

Follower responde AppendResponse:
  ├─ status: OK / ERROR / PROTOCOL_ERROR
  ├─ succeeded: true/false
  ├─ lastLogIndex: último índice del follower
  └─ term: término del follower
```

### LeaderAppender (Gestión de Replicación)

```
Por cada follower, el líder mantiene:
  - matchIndex: último índice replicado confirmado
  - nextIndex: siguiente índice a enviar

Estrategia:
  1. Envía batch de entries (max 32KB por batch)
  2. Si éxito: matchIndex = response.lastIndex
  3. Si fallo: decrementa prevLogIndex y reintenta
  4. Commit: cuando N/2+1 tienen matchIndex ≥ N → commitIndex = N
```

### Diagrama de Replicación

```
          Leader (term=3)
          Log: [1][2][3][4][5]
                          ↑ commitIndex=3
                    ┌─────┼─────┐
                    ▼     ▼     ▼
               Follower1  F2    F3
               [1][2][3]  [1][2][3][4]  [1][2]
               match=3    match=4       match=2

Paso 1: Leader envía entry 4,5 a todos
Paso 2: F1 acepta → match=5, F2 ya tiene 4 acepta 5 → match=5
Paso 3: 3 de 4 nodos tienen index 5 (mayoría)
Paso 4: commitIndex = 5 → notifica followers en próximo heartbeat
```

## Snapshots

### Cuándo se Envían

Cuando un follower está muy atrasado (`matchIndex` muy por debajo del inicio del log):

```
Leader detecta: follower.matchIndex < log.firstIndex
  ↓
Envía InstallRequest por chunks:
  ├─ initial=true  (primer chunk)
  ├─ chunkId, nextChunkId
  ├─ data (ByteBuffer)
  ├─ checksum por chunk
  └─ complete=true (último chunk)
  ↓
Follower recibe chunks → reconstruye snapshot
  ↓
Aplica snapshot → resume replicación normal desde snapshot.index
```

### Compactación del Log

```java
// LogCompactor: elimina entries viejas después de snapshot
compact():
  - Calcula: compactableIndex = snapshot.index - replicationThreshold
  - replicationThreshold default: 100 entries
  - Llama raftLog.deleteUntil(compactableIndex)
```

## Gestión de Particiones

### RaftPartition

```java
public class RaftPartition {
    PartitionId partitionId;
    PartitionMetadata partitionMetadata;   // Miembros, réplicas
    Path dataDirectory;                     // Directorio de datos
    
    CompletableFuture<Void> bootstrap(...); // Arrancar partición
    CompletableFuture<Void> join(...);      // Unirse a partición existente
    CompletableFuture<Void> leave(timeout); // Abandonar partición
}
```

### Cambios de Configuración (Joint Consensus)

Para cambiar miembros del cluster de forma segura:

```
1. Fase "Joint" (old + new):
   ConfigurationEntry con oldMembers Y newMembers
   Requiere mayoría de AMBOS conjuntos (JointConsensusVoteQuorum)

2. Fase "New":
   ConfigurationEntry solo con newMembers
   Requiere mayoría del nuevo conjunto

Mensajes:
  - JoinRequest/Response: miembro se une
  - LeaveRequest/Response: miembro sale
  - ReconfigureRequest: pide cambio al líder
  - ForceConfigureRequest: fuerza configuración (emergencia)
```

## Integración con Zeebe

### ZeebeLogAppender (Punto de Conexión)

La interfaz que el broker usa para escribir en el log Raft:

```java
public interface ZeebeLogAppender {
    // Append con objeto ApplicationEntry
    void appendEntry(ApplicationEntry entry, AppendListener listener);
    
    // Append con posiciones y datos
    void appendEntry(long lowestPosition, long highestPosition, 
                     ByteBuffer data, AppendListener listener);
}

// Callbacks
interface AppendListener {
    void onWrite(IndexedRaftLogEntry indexed);  // Escrito en log local
    void onCommit(long index);                   // Committed por mayoría
    void onCommitError(Throwable error);         // Error
}
```

### ApplicationEntry (Datos de Zeebe)

```java
public interface ApplicationEntry {
    long lowestPosition();    // Posición más baja del record batch
    long highestPosition();   // Posición más alta del record batch
    BufferWriter dataWriter(); // Datos serializados (records)
}
```

### EntryValidator

Validación custom de entries antes de appendear:

```java
public interface EntryValidator {
    ValidationResult validateEntry(ApplicationEntry lastEntry, ApplicationEntry entry);
}
// Zeebe valida que las posiciones sean consecutivas y coherentes
```

## Configuración Completa

### RaftPartitionConfig

```
electionTimeout:               2500ms    ← Timeout de elección
heartbeatInterval:             250ms     ← Intervalo de heartbeat
maxAppendsPerFollower:         2         ← Appends paralelos por follower
maxAppendBatchSize:            32KB      ← Tamaño máximo de batch
priorityElectionEnabled:       true      ← Elecciones con prioridad
requestTimeout:                5s        ← Timeout de requests
snapshotRequestTimeout:        2500ms    ← Timeout de snapshot chunks
minStepDownFailureCount:       3         ← Fallos antes de step-down
maxQuorumResponseTimeout:      0 (2×election) ← Timeout de quorum
preferSnapshotReplicationThreshold: 100  ← Entries antes de snapshot
```

### RaftStorageConfig

```
segmentSize:            32MB     ← Tamaño de segmento del journal
freeDiskSpace:          1GB      ← Espacio libre mínimo
journalIndexDensity:    100      ← Densidad del índice (cada N entries)
preallocateSegmentFiles: true    ← Pre-alocar archivos
```

## Persistencia y Seguridad

### MetaStore

Persiste en disco (no volátil):
- **term**: Término actual (sobrevive a reinicios)
- **lastVotedFor**: A quién votó (evita votar dos veces)

### Threading

- **Single-threaded por partición**: Todo el procesamiento Raft ocurre en un `ThreadContext`
- **externalAccessLock**: Lock para acceso externo seguro (health checks, métricas)
- **ActorControl**: Los componentes Zeebe acceden vía el scheduler de actores

## Flujo Completo: Startup → Operación Normal

```
1. Broker arranca
   ↓
2. RaftServer.bootstrap(members)
   - Carga term/vote de MetaStore
   - Carga último snapshot
   - Inicializa RaftLog
   ↓
3. Transiciona a FOLLOWER
   - Inicia ElectionTimer
   ↓
4. Si no hay líder (ElectionTimer expira):
   - PollRequest → VoteRequest → LEADER
   ↓
5. Operación Normal (líder):
   - Client appends entry → ZeebeLogAppender.appendEntry()
   - Leader appendea a log local
   - Envía AppendRequest a followers
   - Followers appendean y confirman
   - Cuando N/2+1 confirman: commit
   - AppendListener.onCommit() → Zeebe procesa el record
   ↓
6. Periódicamente:
   - Heartbeats cada 250ms
   - Snapshot cuando log crece mucho
   - Compactación después de snapshot
```

