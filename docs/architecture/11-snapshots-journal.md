# 11 - Snapshots y Journal

## Propósito

- **Snapshots**: Capturan el estado completo (RocksDB) para recuperación rápida
- **Journal**: Log segmentado persistente donde viven los entries de Raft

Juntos permiten: **Snapshot + replay del journal desde snapshot.index** = estado completo restaurado.

## Módulos

```
zeebe/snapshot/     ← Interfaces y implementación de snapshots
zeebe/journal/      ← Journal segmentado (almacenamiento de Raft log)
```

## Snapshots

### Interfaces

```java
// Snapshot inmutable en disco
public interface PersistedSnapshot {
    int version();                 // Versión del formato
    long getIndex();               // Índice del state machine
    long getTerm();                // Término de Raft
    Path getPath();                // Directorio del snapshot
    Path getChecksumPath();        // Archivo de checksum
    String getId();                // ID: "index-term-timestamp"
    long getChecksum();            // Verificación de integridad
    long getCompactionBound();     // Posición para compactación del log
    SnapshotChunkReader newChunkReader(); // Para transferir a peers
}

// Snapshot en progreso (transient)
public interface TransientSnapshot extends PersistableSnapshot {
    ActorFuture<Void> take(Consumer<Path> writeConsumer); // Capturar estado
    TransientSnapshot withLastFollowupEventPosition(long pos);
}

// Persist (commit)
public interface PersistableSnapshot {
    ActorFuture<PersistedSnapshot> persist(); // Mover de pending/ a snapshots/
}

// Store que recibe snapshots de otros nodos
public interface ReceivableSnapshotStore extends PersistedSnapshotStore {
    ActorFuture<ReceivedSnapshot> newReceivedSnapshot(String snapshotId);
}

// Store base
public interface PersistedSnapshotStore {
    Optional<PersistedSnapshot> getLatestSnapshot();
    Set<PersistedSnapshot> getAllSnapshots();
    void addListener(PersistedSnapshotListener listener);
}
```

### Implementación: FileBasedSnapshotStore

Es un **Actor** que gestiona snapshots en disco:

```
Directorios:
  partition-X/
  ├── snapshots/          ← Snapshots finalizados
  │   └── 1500-3-1234-1400-1300/
  │       ├── state-file-1.sst    ← Archivos RocksDB
  │       ├── state-file-2.sst
  │       ├── zeebe.metadata       ← Metadatos
  │       └── snapshot.checksum    ← SFV checksums
  └── pending/            ← Snapshots temporales en curso
      └── 0001-1500-3-1234/
          └── ...
```

**Formato del ID**: `{index}-{term}-{timestamp}-{processedPosition}-{exportedPosition}`

### Flujo de Creación de Snapshot

```
1. Trigger: Engine decidió crear snapshot (ej: cada N records)
   ↓
2. store.newTransientSnapshot(index, term)
   ↓
3. FileBasedTransientSnapshot creado en pending/
   ↓
4. snapshot.take(dir -> {
       // Exportar RocksDB a dir
       rocksDb.createCheckpoint(dir);
   });
   ↓
5. Calcula checksums SFV (archivo por archivo)
   ↓
6. snapshot.persist()
   ├─ Mueve directorio: pending/ → snapshots/
   ├─ Notifica PersistedSnapshotListeners
   └─ Actualiza currentPersistedSnapshotRef
   ↓
7. LogCompactor puede compactar journal hasta snapshot.index
```

### Transferencia de Snapshot (Líder → Follower)

```
1. Líder detecta follower atrasado
   ↓
2. snapshot.newChunkReader() → SnapshotChunkReader
   ↓
3. Envía SnapshotChunks vía Raft InstallRequest:
   ├─ snapshotId
   ├─ totalCount (total de chunks)
   ├─ chunkName (identificador del chunk)
   ├─ checksum (CRC del chunk)
   ├─ snapshotChecksum (CRC del snapshot completo)
   └─ content (datos)
   ↓
4. Follower: store.newReceivedSnapshot(snapshotId)
   ↓
5. receivedSnapshot.writeChunk(chunk) × N
   ↓
6. Verificar checksums → persist()
   ↓
7. Resume replicación normal desde snapshot.index
```

### Restricciones
- **Serial**: Solo un snapshot a la vez (single Actor)
- **Copy-on-write**: RocksDB checkpoint es instantáneo gracias a copy-on-write
- **Integridad**: SFV checksums verifican cada archivo del snapshot

## Journal

### Interfaces

```java
public interface Journal extends AutoCloseable {
    // Escritura
    JournalRecord append(BufferWriter data);              // Sin ASQN
    JournalRecord append(long asqn, BufferWriter data);   // Con ASQN
    
    // Truncación/Compactación
    void deleteAfter(long index);      // Truncar después de index
    boolean deleteUntil(long index);   // Compactar hasta index
    void reset(long nextIndex);        // Limpiar todo
    
    // Consulta
    long getLastIndex();
    long getFirstIndex();
    
    // Lectura
    JournalReader openReader();
    JournalReader openReader(long index);  // Abrir en índice
}

public interface JournalRecord {
    long getIndex();           // Índice del record
    long getTerm();            // Término de Raft
    long getChecksum();        // CRC32 de los datos
    long getAsqn();            // Application Sequence Number
    UnsafeBuffer data();       // Payload
}

public interface JournalReader extends Iterator<JournalRecord> {
    boolean hasNext();
    JournalRecord next();
    long seek(long index);     // Buscar por índice
    long seekToFirst();
    long seekToLast();
}
```

### Implementación: SegmentedJournal

El journal está dividido en **segmentos** de tamaño fijo:

```
partition-X/journal/
├── journal-1.log       ← Segmento 1 (256MB)
├── journal-2.log       ← Segmento 2 (256MB)
├── journal-3.log       ← Segmento 3 (en escritura)
└── ...
```

### Componentes del Journal

```java
// Journal principal
public class SegmentedJournal implements Journal {
    SegmentsManager segments;        // Gestiona segmentos
    SegmentedJournalWriter writer;   // Un solo writer
    Set<SegmentedJournalReader> readers; // Múltiples readers
    JournalIndex journalIndex;       // Índice: record index → byte position
    StampedLock rwlock;              // Coordina reads/writes/deletes
}

// Segmento individual
public class Segment {
    SegmentFile file;               // Archivo en disco
    MappedByteBuffer buffer;        // Memory-mapped
    SegmentDescriptor descriptor;   // Metadatos (version, id, firstIndex)
    volatile boolean open;
    volatile boolean markedForDeletion;
}
```

### Layout de un Segmento

```
┌──────────────────────────────────────┐
│ SegmentDescriptor (header)           │
│ ├─ version                           │
│ ├─ segment id                        │
│ ├─ first index                       │
│ ├─ last index                        │
│ └─ last ASQN                         │
├──────────────────────────────────────┤
│ Record 1                             │
│ ├─ frame length                      │
│ ├─ data payload                      │
│ └─ CRC32 checksum                    │
├──────────────────────────────────────┤
│ Record 2                             │
│ ├─ ...                               │
├──────────────────────────────────────┤
│ ... más records ...                  │
├──────────────────────────────────────┤
│ [espacio libre / preallocated]       │
└──────────────────────────────────────┘
```

### Flujo de Escritura

```
1. journal.append(asqn, data)
   ↓
2. SegmentedJournalWriter.append()
   ├─ Intenta escribir en segmento actual
   ├─ Si lleno (SegmentFull) → createNewSegment()
   │   └─ Nuevo archivo journal-N.log (preallocated)
   ├─ Escribe: frame length + data + checksum
   └─ Actualiza JournalIndex
   ↓
3. Retorna JournalRecord con index, term, checksum
```

### Rotación de Segmentos

```
Segmento 3 (256MB) → LLENO
  ↓
createNewSegment():
  ├─ Crea journal-4.log
  ├─ Prealoca 256MB (si configurado)
  ├─ Escribe SegmentDescriptor
  └─ Cambia writer a nuevo segmento
  ↓
Segmento 4 ahora es el target de escritura
```

### Lectura Multi-Segmento

```java
// SegmentedJournalReader lee a través de segmentos
reader.seek(1500);
while (reader.hasNext()) {
    JournalRecord record = reader.next();
    // Si llega al final del segmento actual,
    // automáticamente avanza al siguiente segmento
}
```

### Concurrencia

- **StampedLock**: Coordina reads/writes/deletes
- **Un writer**: SegmentedJournalWriter (single-threaded)
- **Múltiples readers**: Cada uno con su SegmentReader
- **MappedByteBuffer**: Acceso eficiente vía memory-mapping

## Relación Snapshot ↔ Journal

### Flujo de Recuperación

```
Broker se reinicia:
   ↓
1. SnapshotStore carga último snapshot
   └─ snapshot: index=1500, term=3
   ↓
2. Restaura estado (RocksDB) desde snapshot
   ↓
3. Journal: lee desde index 1501 en adelante
   ↓
4. Replay: aplica cada entry al estado
   ├─ entry 1501: PROCESS_INSTANCE_CREATED
   ├─ entry 1502: JOB_CREATED
   ├─ ...
   └─ entry 1600: JOB_COMPLETED (último)
   ↓
5. Estado completamente recuperado hasta index 1600
```

### Compactación

```
1. Nuevo snapshot en index 2000
   ↓
2. LogCompactor.compact()
   ├─ compactableIndex = 2000 - replicationThreshold(100) = 1900
   ├─ journal.deleteUntil(1900)
   │   └─ Elimina segmentos cuyo lastIndex < 1900
   └─ Segmentos: journal-1.log ELIMINADO, journal-2.log ELIMINADO, journal-3.log CON entries ≥ 1900
```

### Diagrama Temporal

```
Time ──────────────────────────────────────────────────→

Journal entries:
[100..500] [501..1000] [1001..1500] [1501..2000] [2001..2500]
 segment-1   segment-2    segment-3    segment-4    segment-5

Snapshot @ 1500:
                              ▲
                              │
                     snapshot covers 1..1500

After compaction (threshold=100):
                    ┌─────────────────────────────────────┐
DELETED  DELETED    │  [1401..1500]  [1501..2000] [2001..] │
                    └─────────────────────────────────────┘
                    kept for slow followers
```

## Configuración

### Snapshot
| Parámetro | Descripción |
|-----------|-------------|
| Directorio por partición | Cada partición tiene su snapshot store |
| SFV checksums | Verificación de integridad archivo por archivo |
| Version = 1 | Formato actual |

### Journal
| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `maxSegmentSize` | 256MB | Tamaño por segmento |
| `journalIndexDensity` | 100 | Crear índice cada N records |
| `preallocateSegmentFiles` | true | Pre-alocar archivos |
| `directory` | Configurable | Directorio de almacenamiento |

## Layout Completo en Disco

```
data/
└── partition-1/
    ├── snapshots/
    │   ├── 5000-5-16938274-4800-4700/
    │   │   ├── 000001.sst
    │   │   ├── 000002.sst
    │   │   ├── MANIFEST-000001
    │   │   ├── zeebe.metadata
    │   │   └── snapshot.checksum
    │   └── 3000-4-16938200-2900-2800/    ← snapshot anterior (se puede borrar)
    ├── pending/                           ← vacío normalmente
    └── journal/
        ├── journal-8.log                  ← más antiguo (≥ snapshot - threshold)
        ├── journal-9.log
        └── journal-10.log                 ← más reciente (en escritura)
```
