# 08 - Stream Platform

## Qué es el Stream Platform

El Stream Platform es el **framework de procesamiento de eventos** de Zeebe. Se sitúa entre el log (logstreams) y el engine, orquestando:

1. **Replay**: Recuperación del estado a partir de eventos históricos
2. **Processing**: Procesamiento de nuevos comandos en tiempo real
3. **Writing**: Escritura atómica de resultados al log

## Ubicación del Código

```
zeebe/stream-platform/
├── src/main/java/io/camunda/zeebe/stream/
│   ├── api/           ← Interfaces públicas (contratos)
│   └── impl/          ← Implementación
│       ├── records/   ← TypedRecordImpl, RecordBatch
│       └── ...        ← StreamProcessor, state machines
```

## Interfaz Principal: RecordProcessor

El `RecordProcessor` es la interfaz que implementa el Engine para procesar records:

```java
public interface RecordProcessor {
    // Inicialización con contexto
    void init(RecordProcessorContext ctx);
    
    // ¿Acepta este tipo de record?
    boolean accepts(ValueType valueType);
    
    // Replay: reconstruir estado desde eventos
    void replay(TypedRecord record);
    
    // Process: procesar comandos nuevos
    ProcessingResult process(TypedRecord record, ProcessingResultBuilder builder);
    
    // Error: manejar errores de procesamiento
    ProcessingResult onProcessingError(
        Throwable ex, TypedRecord record, ProcessingResultBuilder builder);
}
```

### Contrato de `replay()`
- Solo recibe records de tipo **EVENT**
- **Puede** acceder a la DB (dentro de transacción)
- **NO puede** escribir al log
- **NO puede** ejecutar post-commit tasks
- **Propósito**: Reconstruir el estado determinísticamente desde el último snapshot

### Contrato de `process()`
- Solo recibe records de tipo **COMMAND**
- **Puede** acceder a la DB
- **Puede** generar follow-up events/commands vía `ProcessingResultBuilder`
- **Puede** añadir post-commit tasks
- Los follow-up events se aplican al estado en la misma transacción
- **Propósito**: Procesar comandos y generar resultados

### Contrato de `onProcessingError()`
- Se invoca si `process()` lanza excepción o falla el commit
- **Puede** generar rechazos
- El processor es responsable de logging

## TypedRecord

`TypedRecord` envuelve un record del log con metadatos deserializados:

```java
public interface TypedRecord<T extends UnifiedRecordValue> extends Record<T> {
    int getRequestStreamId();      // Stream del cliente
    long getRequestId();           // ID de correlación
    int getLength();               // Tamaño serializado
    
    // ¿Tiene petición de cliente asociada?
    default boolean hasRequestMetadata() { ... }
    
    // ¿Fue distribuido desde otra partición?
    default boolean isCommandDistributed() {
        return isCommand 
            && key != -1 
            && Protocol.decodePartitionId(key) != getPartitionId();
    }
}
```

## ProcessingResultBuilder

El builder construye el resultado de procesar un comando:

```java
public interface ProcessingResultBuilder {
    // Añadir record al batch de salida
    ProcessingResultBuilder appendRecord(long key, RecordValue val, RecordMetadata md);
    
    // Versión que devuelve Either (para overflow graceful)
    Either<RuntimeException, ProcessingResultBuilder> appendRecordReturnEither(
        long key, RecordValue val, RecordMetadata md);
    
    // Establecer respuesta al cliente
    ProcessingResultBuilder withResponse(
        RecordType type, long key, Intent intent, 
        UnpackedObject val, ValueType vt,
        RejectionType rejType, String rejMsg,
        long requestId, int requestStreamId);
    
    // Añadir tarea post-commit
    ProcessingResultBuilder appendPostCommitTask(PostCommitTask task);
    
    // Verificar espacio
    boolean canWriteEventOfLength(int len);
    
    // Construir resultado
    ProcessingResult build();
}
```

## ProcessingResult

El resultado final que se devuelve al stream processor:

```java
public interface ProcessingResult {
    ImmutableRecordBatch getRecordBatch();           // Records a escribir
    Optional<ProcessingResponse> getProcessingResponse(); // Respuesta al cliente
    boolean executePostCommitTasks();                // Tareas post-commit
    boolean isEmpty();                               // ¿Vacío? (skip)
}
```

## RecordProcessorContext

Contexto disponible para el processor:

```java
public interface RecordProcessorContext {
    int getPartitionId();
    ProcessingScheduleService getScheduleService();  // Programar tareas
    ZeebeDb getZeebeDb();                            // Base de datos
    TransactionContext getTransactionContext();        // Transacciones
    List<StreamProcessorLifecycleAware> getLifecycleListeners();
    InterPartitionCommandSender getPartitionCommandSender(); // Enviar a otra partición
    KeyGenerator getKeyGenerator();                   // Generar keys únicas
    MeterRegistry getMeterRegistry();                 // Métricas
}
```

## StreamProcessor - La Máquina Principal

El `StreamProcessor` es un **Actor** (single-threaded por partición) que orquesta todo:

```
Ciclo de Vida del StreamProcessor
═══════════════════════════════════

1. onActorStarting()
   └─ Crear LogStreamReader
   
2. onActorStarted()
   └─ Recuperar snapshot + inicializar processors
   
3. ReplayStateMachine.startRecover()
   └─ Leer todos los eventos desde el último snapshot
   └─ Para cada EVENT: llamar processor.replay(event)
   └─ Resultado: Estado reconstruido
   
4. ProcessingStateMachine.start()
   └─ Modo normal: procesar comandos entrantes
   └─ Para cada COMMAND:
      ├─ processor.process(command, builder)
      ├─ Escribir resultado al log (atómico)
      ├─ Commit de la transacción DB
      └─ Ejecutar post-commit tasks
   
5. onActorClosing() → onActorClosed()
   └─ Cleanup y shutdown
```

### Campos Clave
```java
LogStream logStream;                      // El log de la partición
int partitionId;                          // ID de la partición
ZeebeDb zeebeDb;                          // Estado persistente (RocksDB)
List<RecordProcessor> recordProcessors;   // Procesadores registrados
ReplayStateMachine replayStateMachine;    // Recovery
ProcessingStateMachine processingStateMachine; // Normal operation
```

## Flujo de Procesamiento Detallado

```
┌─────────────────────────────────────────────┐
│         PROCESSING STATE MACHINE             │
└─────────────────────────────────────────────┘

1. LogStreamReader.hasNext()
   └─ ¿Hay nuevo record en el log?
   
2. Leer record del log
   └─ Deserializar metadata + value
   
3. ¿Es COMMAND?
   │
   ├─ SÍ → 4. Iniciar transacción DB
   │        5. processor.process(command, builder)
   │        6. ¿Resultado?
   │        │
   │        ├─ ProcessingResult con eventos
   │        │  └─ 7. Escribir eventos al log
   │        │     8. Commit transacción DB
   │        │     9. Ejecutar post-commit tasks
   │        │    10. Enviar respuesta al cliente
   │        │
   │        └─ ProcessingResult vacío
   │           └─ Skip (no escribir nada)
   │
   └─ NO (EVENT) → Solo durante replay
                    └─ processor.replay(event)
```

## Dos Fases: Replay vs Processing

### Fase de Replay (Recuperación)
```
Snapshot (punto de control)
    ↓
Leer eventos desde snapshot position
    ↓
Para cada EVENT:
    processor.replay(event)  → Actualiza estado en DB
    ↓
Estado completamente reconstruido
    ↓
Pasar a fase de Processing
```

### Fase de Processing (Normal)
```
Nuevo COMMAND llega al log (vía Raft)
    ↓
processor.process(command, builder)
    ↓
Builder acumula:
  - Follow-up events
  - Follow-up commands
  - Respuesta al cliente
  - Post-commit tasks
    ↓
TODO se escribe atómicamente:
  - Eventos al log
  - Estado a RocksDB (misma transacción)
    ↓
Post-commit tasks ejecutadas
    ↓
Respuesta enviada al cliente
```

## Atomicidad

**TODO dentro de una sola transacción**:
- Los eventos generados por un comando
- Las actualizaciones de estado en RocksDB
- Si falla cualquier parte, se hace rollback completo

**Después del commit** (best-effort):
- Post-commit tasks (ej: enviar a exporters)
- Respuesta al cliente

Esto garantiza que el estado siempre es consistente con el log.

## PatternArquitectónicos Clave

1. **Single Writer per Partition**: Un solo stream processor por partición, sin locks
2. **Event Sourcing**: El log es la fuente de verdad, el estado se puede reconstruir
3. **Command-Query Separation**: Commands modifican, el estado se consulta
4. **Deterministic Replay**: Replay produce exactamente el mismo estado
5. **Batch Atomicity**: Todos los records de un resultado se escriben juntos
6. **Graceful Degradation**: `appendRecordReturnEither()` para manejar overflow sin excepciones
