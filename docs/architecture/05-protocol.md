# 05 - Protocolo Binario

## Qué es el Protocolo

El protocolo define el **formato de datos** que usa Zeebe internamente para representar todos los records (comandos, eventos, rechazos) que se escriben en el log y se procesan por el engine.

Usa **SBE (Simple Binary Encoding)** para serialización eficiente de bajo nivel, combinado con **MessagePack** para datos variables (variables de proceso, headers custom).

## Ubicación del Código

```
zeebe/protocol/                  ← Interfaces y enums (algunos generados por SBE)
zeebe/protocol-impl/             ← Implementación de serialización
zeebe/msgpack-core/              ← MessagePack core
zeebe/msgpack-value/             ← MessagePack value types
```

## Anatomía de un Record

Todo en Zeebe es un **Record**. Un record es la unidad atómica del log de eventos:

```
┌─────────────────────────────────────────────────────────┐
│                        RECORD                            │
├─────────────────────────────────────────────────────────┤
│  position: long          │ Posición en el log (única)    │
│  sourceRecordPosition: long│ Record que causó este (-1)  │
│  key: long               │ ID de la entidad              │
│  timestamp: long         │ Unix timestamp                │
│  partitionId: int        │ Partición                     │
│  recordType: RecordType  │ EVENT / COMMAND / REJECTION   │
│  valueType: ValueType    │ Tipo de dominio               │
│  intent: Intent          │ Acción/estado específico      │
│  rejectionType           │ Tipo de rechazo (si aplica)   │
│  rejectionReason         │ Razón del rechazo             │
│  brokerVersion           │ Versión del broker            │
│  recordVersion           │ Versión del schema            │
│  authorizations          │ Info de tenant                │
│  value: RecordValue      │ ← Datos del dominio           │
└─────────────────────────────────────────────────────────┘
```

## RecordType - Los 3 tipos de registro

```java
public enum RecordType {
    EVENT,              // Hecho consumado (resultado de procesamiento)
    COMMAND,            // Intención/instrucción del cliente
    COMMAND_REJECTION   // Comando rechazado con razón
}
```

**Flujo**: Un COMMAND entra → se procesa → genera EVENT(s) o COMMAND_REJECTION

## ValueType - Los ~40 dominios

Cada record pertenece a un dominio. Los principales:

```java
public enum ValueType {
    // Core
    JOB(0),                              // Jobs para workers
    DEPLOYMENT(4),                       // Deploy de recursos
    PROCESS_INSTANCE(5),                 // Instancias de proceso
    INCIDENT(6),                         // Incidentes
    
    // Messaging
    MESSAGE(10),                         // Mensajes publicados
    MESSAGE_SUBSCRIPTION(11),            // Suscripciones a mensajes
    PROCESS_MESSAGE_SUBSCRIPTION(12),    // Suscripciones en procesos
    
    // Jobs
    JOB_BATCH(14),                       // Activación batch de jobs
    
    // Scheduling
    TIMER(15),                           // Timers
    
    // Data
    VARIABLE(17),                        // Variables individuales
    VARIABLE_DOCUMENT(18),               // Documentos de variables
    
    // Process lifecycle
    PROCESS_INSTANCE_CREATION(19),       // Creación de instancias
    PROCESS_INSTANCE_RESULT(21),         // Resultado (con await)
    PROCESS(22),                         // Definiciones de proceso
    
    // Distribution
    DEPLOYMENT_DISTRIBUTION(23),         // Distribución de deploys
    COMMAND_DISTRIBUTION(33),            // Distribución de comandos
    
    // Decisions
    DECISION(25),                        // Definiciones de decisión
    DECISION_REQUIREMENTS(26),           // DRG (Decision Req Graph)
    DECISION_EVALUATION(27),             // Evaluación de decisiones
    
    // Advanced
    PROCESS_INSTANCE_MODIFICATION(28),   // Modificación de instancias
    ESCALATION(29),                      // Escalaciones
    SIGNAL_SUBSCRIPTION(30),             // Suscripciones a señales
    SIGNAL(31),                          // Señales
    RESOURCE_DELETION(32),               // Borrado de recursos
    FORM(36),                            // Formularios
    USER_TASK(37),                       // Tareas de usuario
    PROCESS_INSTANCE_MIGRATION(38),      // Migración de instancias
    COMPENSATION_SUBSCRIPTION(39),       // Compensaciones
    
    // System
    ERROR(20),                           // Errores
    CHECKPOINT(254),                     // Checkpoints
}
```

## Intent - La acción específica

Cada ValueType tiene su propio enum de intents. Ejemplo para **Job**:

```java
public enum JobIntent implements ProcessInstanceRelatedIntent {
    // Events (isEvent=true, resultado de procesamiento)
    CREATED(0),          // Job fue creado
    COMPLETED(2),        // Job fue completado
    TIMED_OUT(4),        // Job expiró por timeout
    FAILED(6),           // Job falló
    RETRIES_UPDATED(8),  // Retries actualizados
    CANCELED(10),        // Job cancelado
    ERROR_THROWN(11),    // Error lanzado por worker
    MIGRATED(19),        // Job migrado
    
    // Commands (isEvent=false, instrucciones del cliente)
    COMPLETE(1),         // Completar job
    TIME_OUT(3),         // Timeout del job (interno)
    FAIL(5),             // Fallar job
    UPDATE_RETRIES(7),   // Actualizar retries
    CANCEL(9),           // Cancelar job
    THROW_ERROR(12),     // Lanzar error BPMN
    RECUR_AFTER_BACKOFF(16), // Reintentar tras backoff
    YIELD(17),           // Ceder job
    UPDATE_TIMEOUT(18),  // Actualizar timeout
}
```

Para **ProcessInstance** (ciclo de vida de elementos BPMN):

```java
public enum ProcessInstanceIntent {
    // Lifecycle events
    ELEMENT_ACTIVATING(2),    // Elemento se está activando
    ELEMENT_ACTIVATED(3),     // Elemento activado
    ELEMENT_COMPLETING(4),    // Elemento se está completando
    ELEMENT_COMPLETED(5),     // Elemento completado
    ELEMENT_TERMINATING(6),   // Elemento se está terminando
    ELEMENT_TERMINATED(7),    // Elemento terminado
    SEQUENCE_FLOW_TAKEN(1),   // Flujo de secuencia tomado
    
    // Commands
    CANCEL(0),                // Cancelar instancia
    ACTIVATE_ELEMENT(8),      // Activar elemento
    COMPLETE_ELEMENT(9),      // Completar elemento
    TERMINATE_ELEMENT(10),    // Terminar elemento
    
    // Execution listeners
    COMPLETE_EXECUTION_LISTENER(11), // Completar listener
}
```

## Sistema de Keys

Las keys codifican la **partición** en los bits altos:

```java
// Protocol.java
// 13 bits para partition ID (max 8,192 particiones)
// 51 bits para key local (2^51 keys por partición)

long key = encodePartitionId(partitionId, localKey);
int partitionId = decodePartitionId(key);        // bits altos
long localKey = decodeKeyInPartition(key);        // bits bajos
```

**Ejemplo**: Key `2251799813685249`
- Partición: `1`
- Key local: `1`

Esto permite identificar a qué partición pertenece cualquier entidad solo por su key.

## Column Families (Estado en RocksDB)

El enum `ZbColumnFamilies` define ~100 column families. Las principales por dominio:

### Procesos

```
PROCESS_CACHE                        → Cache de definiciones de proceso
PROCESS_CACHE_BY_ID_AND_VERSION      → Índice por bpmnProcessId + versión
PROCESS_CACHE_DIGEST_BY_ID           → Digest por processId
PROCESS_VERSION                      → Última versión por processId
```

### Instancias de Proceso

```
ELEMENT_INSTANCE_KEY                 → Instancias de elementos activos
ELEMENT_INSTANCE_PARENT_CHILD        → Relación padre-hijo
```

### Jobs

```
JOBS                                 → Datos del job
JOB_STATES                           → Estado (activatable/activated/failed)
JOB_ACTIVATABLE                      → Índice de jobs activables
JOB_DEADLINES                        → Índice por deadline (timeouts)
JOB_BACKOFF                          → Índice por tiempo de backoff
```

### Variables

```
VARIABLES                            → Variables por scope
```

### Mensajes

```
MESSAGE_KEY                          → Mensajes publicados
MESSAGE_DEADLINES                    → Expiración de mensajes
MESSAGE_IDS                          → Deduplicación por ID
MESSAGE_CORRELATED                   → Mensajes ya correlacionados
MESSAGE_SUBSCRIPTION_BY_KEY          → Suscripciones activas
```

### Timers

```
TIMERS                               → Timers programados
TIMER_DUE_DATES                      → Índice por fecha de vencimiento
```

### Incidentes

```
INCIDENTS                            → Incidentes activos
INCIDENT_PROCESS_INSTANCES           → Índice por instancia de proceso
INCIDENT_JOBS                        → Índice por job
```

## Serialización SBE

Los schemas SBE están en `zeebe/protocol/src/main/resources/`:

- `protocol.xml` → Schema principal del protocolo
- `cluster-management-protocol.xml` → Protocolo de gestión del cluster

SBE genera código con estas características:
- **Sin allocaciones** (zero-copy, usa buffers directos)
- **Endianness**: Little Endian
- **Interfaces generadas**: Para interoperabilidad
- **Enums desconocidos**: Decodificación segura (forward compatibility)

## RecordMetadata (Cabecera SBE)

Cada record tiene una cabecera binaria codificada en SBE:

```java
public final class RecordMetadata {
    RecordType recordType;        // EVENT/COMMAND/REJECTION
    ValueType valueType;          // Dominio (JOB, PROCESS_INSTANCE, etc.)
    Intent intent;                // Acción específica
    long requestId;               // ID de request del cliente
    int requestStreamId;          // Stream del cliente
    RejectionType rejectionType;  // Tipo de rechazo
    String rejectionReason;       // Razón del rechazo
    int protocolVersion;          // Versión SBE
    VersionInfo brokerVersion;    // Versión del broker
    int recordVersion;            // Versión del record (para compat)
    AuthInfo authorization;       // Info de tenant/auth
}
```

## ValueTypeMapping

La clase `ValueTypeMapping` vincula cada `ValueType` con su clase Java de dominio y su enum de Intent:

```java
// ValueType.JOB → (JobRecordValue.class, JobIntent.class)
// ValueType.PROCESS_INSTANCE → (ProcessInstanceRecordValue.class, ProcessInstanceIntent.class)
// ValueType.MESSAGE → (MessageRecordValue.class, MessageIntent.class)
// ... ~40 mappings
```

Esto permite al engine saber cómo deserializar el valor de un record y qué intents son válidos.

## Cómo Añadir un Nuevo Tipo de Record

1. Añadir entrada en `ValueType` (en el schema SBE)
2. Crear el `*RecordValue` interface en `protocol/record/value/`
3. Crear el `*Intent` enum en `protocol/record/intent/`
4. Añadir mapping en `ValueTypeMapping`
5. Crear implementación en `protocol-impl/`
6. Regenerar código SBE: `mvn generate-sources -pl zeebe/protocol`

