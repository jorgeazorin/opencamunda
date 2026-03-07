# 07 - LogStreams

## Qué es LogStreams

El módulo `logstreams` proporciona una abstracción de **log append-only** sobre el almacenamiento de Raft. Es la capa entre el engine de Zeebe y el log de Atomix Raft.

```
Engine (lee/escribe records)
       ↕
   LogStreams (buffering, posicionamiento, sequencing)
       ↕
   LogStorage → Atomix Raft Log (replicación + persistencia)
```

## Estructura del Módulo

```
zeebe/logstreams/src/main/java/io/camunda/zeebe/logstreams/
├── log/
│   ├── LogStream.java              ← Interface principal
│   ├── LogStreamBuilder.java       ← Builder fluido
│   ├── LogStreamReader.java        ← Leer events
│   ├── LogStreamWriter.java        ← Escribir events
│   └── LogAppendEntry.java         ← Entry individual
├── impl/log/
│   ├── LogStreamImpl.java          ← Implementación (Actor)
│   ├── Sequencer.java              ← Asigna posiciones monotónicas
│   ├── LogStorageAppender.java     ← Consume sequencer, escribe a storage
│   └── LogStream*Impl.java         ← Implementaciones de reader/writer
└── storage/
    ├── LogStorage.java             ← Interface de storage
    └── LogStorageReader.java       ← Interface de lectura
```

## Interfaces Principales

### LogStream

```java
public interface LogStream extends AutoCloseable {
    int getPartitionId();
    String getLogName();
    
    // Crear reader/writer (async, retorna ActorFuture)
    ActorFuture<LogStreamReader> newLogStreamReader();
    ActorFuture<LogStreamWriter> newLogStreamWriter();
    
    // Notificación cuando hay datos disponibles
    void registerRecordAvailableListener(RecordAwaiter awaiter);
    void removeRecordAvailableListener(RecordAwaiter awaiter);
}
```

### LogStreamWriter

```java
public interface LogStreamWriter {
    // Verificar capacidad
    boolean canWriteEvents(int count, int batchSize);
    
    // Escribir batch atómico → retorna posición o error
    Either<WriteFailure, Long> tryWrite(List<LogAppendEntry> entries);
    Either<WriteFailure, Long> tryWrite(List<LogAppendEntry> entries, long sourcePosition);
}

enum WriteFailure {
    CLOSED,            // Log cerrado
    FULL,              // Buffer lleno (backpressure)
    INVALID_ARGUMENT   // Datos inválidos
}
```

### LogStreamReader

```java
public interface LogStreamReader extends Iterator<LoggedEvent> {
    boolean seekToNextEvent(long position);  // Buscar DESPUÉS de position
    boolean seek(long position);              // Buscar EN position
    void seekToFirstEvent();                  // Ir al inicio
    long seekToEnd();                         // Ir al final, retorna última position
    long getPosition();                       // Posición actual
    LoggedEvent peekNext();                   // Ver sin consumir
}
```

## Flujo de Escritura

```
1. LogStreamWriter.tryWrite(entries)
         ↓
2. Sequencer.tryWrite()
   ├─ Asigna posiciones monotónicas a cada entry
   ├─ Agrupa en SequencedBatch
   └─ Pone en ArrayBlockingQueue (capacidad: 128)
         ↓
3. ActorCondition señaliza LogStorageAppender
         ↓
4. LogStorageAppender.tryWriteBatch()
   ├─ Drena el Sequencer
   ├─ Agrupa entries en batch
   └─ Escribe a LogStorage
         ↓
5. LogStorage.append()  → Escritura física (Atomix Raft)
         ↓
6. LogStorage notifica CommitListeners
         ↓
7. LogStreamImpl notifica RecordAwaiters registrados
   └─ El engine sabe que hay nuevos records para procesar
```

## Componentes Internos

### Sequencer (Asignación de Posiciones)

El Sequencer es la cola **multiple-producer, single-consumer** que asigna posiciones:

```
Características:
- Capacidad: 128 entries (ArrayBlockingQueue<SequencedBatch>)
- Asigna posiciones monotónicas crecientes
- NO copia los datos, mantiene referencias hasta que el consumer las lee
- Soporta back-pointers (sourcePosition) para trazabilidad
- tryWrite() retorna Either<WriteFailure, Long> (posición asignada o error)
- tryRead() retorna SequencedBatch (non-blocking)
```

### LogStorageAppender (Consumidor del Sequencer)

Es un **Actor** que consume del Sequencer y escribe a storage:

```
- Drena el Sequencer en cada ciclo
- Agrupa entries en batches para escritura eficiente
- Usa AppenderFlowControl para backpressure
- Registra AppendListener con el LogStorage
- Métricas: tamaño de batch, throughput, latencia
```

### LogStreamImpl (Implementación Principal)

Extiende `Actor` para lifecycle management:

```
- Gestiona readers, writer, sequencer, appender
- Notifica recordAwaiters cuando hay commits en storage
- Lifecycle: start → crear componentes → esperar records → close
```

## Formato de Almacenamiento

### LogAppendEntry (Un Record)

```java
public interface LogAppendEntry {
    DirectBuffer data();      // Datos serializados del record
    long key();               // Clave del record
    Intent intent();          // Intención (ej: JOB_CREATED)
    RecordType recordType();  // COMMAND, EVENT, REJECTION
    ValueType valueType();    // Dominio (ej: JOB, PROCESS_INSTANCE)
}
```

### Formato Físico

```
┌─────────────────────────────────────────┐
│ DataFrameDescriptor                      │
├─────────────────────────────────────────┤
│ Frame Length (4 bytes)                   │
│ Frame Alignment Padding                 │
│ Entry Data (variable length)            │
│   ├─ key (8 bytes)                      │
│   ├─ intent                             │
│   ├─ recordType                         │
│   ├─ valueType                          │
│   └─ data payload                       │
└─────────────────────────────────────────┘
```

### Posiciones

- **64-bit monotónicas**: Cada entry tiene una posición única creciente
- **Asignadas por Sequencer**: No hay gaps
- **Back-pointer**: `sourcePosition` conecta entries derivadas con la entry original

## Relación con Atomix Raft

```
LogStream escribe a LogStorage
   ↓
LogStorage implementado por AtomixLogStorage
   ↓
AtomixLogStorage usa ZeebeLogAppender
   ↓
ZeebeLogAppender appendea ApplicationEntry al Raft log
   ↓
Raft replica a followers → commit cuando mayoría confirma
   ↓
CommitListener notifica → LogStream notifica RecordAwaiters
   ↓
Engine procesa el record
```

## Flujo de Lectura

```
1. LogStreamReader.seek(position)
   └─ Busca en el LogStorageReader subyacente
         ↓
2. reader.hasNext() → Verifica si hay más entries
         ↓
3. reader.next() → LoggedEvent
   ├─ Posición
   ├─ Key
   ├─ Intent
   ├─ RecordType
   ├─ ValueType
   └─ Data payload
         ↓
4. peekNext() permite ver sin consumir
```

### Batch Reading

```java
// LogStreamBatchReaderImpl: optimizado para leer múltiples records
// Usado por el stream processor durante replay
while (batchReader.hasNext()) {
    LoggedEvent event = batchReader.next();
    // procesar...
}
```

## Configuración

| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `maxFragmentSize` | 64KB (block-aligned) | Tamaño máximo de fragmento |
| Sequencer queue | 128 entries | Capacidad del buffer |
| `partitionId` | requerido | ID de la partición |
| `logName` | requerido | Nombre contextual |
| `logStorage` | requerido | Storage subyacente |

## Métricas

| Métrica | Componente | Qué mide |
|---------|-----------|----------|
| Batch length | Sequencer | Número de entries por batch |
| Batch size | Sequencer | Bytes por batch |
| Queue depth | Sequencer | Entries pendientes en cola |
| Append latency | Appender | Tiempo de escritura |
| Flow control | Appender | Presión de backpressure |

## Backpressure

Cuando el Sequencer está lleno (128 entries), `tryWrite()` retorna `WriteFailure.FULL`. El stream processor reacciona:

```
tryWrite() → FULL
  ↓
StreamProcessor espera
  ↓
LogStorageAppender drena el Sequencer
  ↓
Espacio disponible
  ↓
StreamProcessor reintenta
```
