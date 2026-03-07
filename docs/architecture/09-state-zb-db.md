# 09 - Estado y ZB-DB (RocksDB)

## Qué es ZB-DB

`zb-db` es una **capa de abstracción** typed sobre RocksDB que proporciona:
- Column families tipadas con `DbKey` y `DbValue` genéricos
- Transacciones optimistas (sin locks)
- Iteración por prefijo para consultas eficientes
- Serialización basada en buffers (zero-copy con Agrona)

## Ubicación del Código

```
zeebe/zb-db/                         ← Abstracción sobre RocksDB
├── src/main/java/io/camunda/zeebe/db/
│   ├── ZeebeDb.java                 ← Interfaz principal
│   ├── ColumnFamily.java            ← Column family tipada
│   ├── DbKey.java                   ← Interfaz para keys
│   ├── DbValue.java                 ← Interfaz para values
│   ├── TransactionContext.java      ← Transacciones
│   └── impl/                        ← Implementación RocksDB

zeebe/engine/src/main/java/.../state/ ← Estado del engine
│   ├── ProcessingDbState.java        ← Crea todos los estados
│   ├── instance/                     ← Estado de instancias
│   ├── deployment/                   ← Estado de deployments
│   ├── message/                      ← Estado de mensajes
│   └── ...
```

## ZeebeDb - Interfaz Principal

```java
public interface ZeebeDb<ColumnFamilyType extends Enum<?>> extends CloseableSilently {
    // Crear column family tipada
    <KeyType extends DbKey, ValueType extends DbValue>
    ColumnFamily<KeyType, ValueType> createColumnFamily(
        ColumnFamilyType columnFamily,
        TransactionContext context,
        KeyType key,
        ValueType value
    );
    
    // Crear contexto de transacciones
    TransactionContext createContext();
    
    // Crear snapshot
    Optional<String> createSnapshot(Path snapshotDir);
    
    // Propiedades de RocksDB
    Optional<String> getProperty(String propertyName);
    
    // Estado vacío
    boolean isEmpty(ColumnFamilyType column, TransactionContext context);
}
```

## ColumnFamily - Operaciones Tipadas

```java
public interface ColumnFamily<KeyType extends DbKey, ValueType extends DbValue> {
    // CRUD
    void insert(KeyType key, ValueType value);    // Insertar (falla si existe)
    void update(KeyType key, ValueType value);    // Actualizar (falla si no existe)
    void upsert(KeyType key, ValueType value);    // Insert o update
    void deleteExisting(KeyType key);             // Borrar (falla si no existe)
    void deleteIfExists(KeyType key);             // Borrar si existe
    
    // Lectura
    ValueType get(KeyType key);                   // Obtener valor (null si no existe)
    boolean exists(KeyType key);                  // ¿Existe?
    boolean isEmpty();                            // ¿Vacía?
    
    // Iteración completa
    void forEach(BiConsumer<KeyType, ValueType> consumer);
    
    // Iteración por prefijo (CLAVE para performance)
    void whileEqualPrefix(
        DbKey keyPrefix,
        BiConsumer<KeyType, ValueType> consumer
    );
    
    // Iteración por prefijo en orden inverso
    void whileEqualPrefix(
        DbKey keyPrefix,
        BiConsumer<KeyType, ValueType> consumer,
        boolean reversed
    );
    
    // Contar
    long count();
}
```

### Iteración por prefijo
La operación más importante para consultas. RocksDB ordena keys lexicográficamente, así que buscar por prefijo es O(n) donde n es el número de resultados, no el tamaño total.

## Tipos de DbKey y DbValue

### Keys primitivas
```java
DbLong          // long (8 bytes)
DbInt           // int (4 bytes)
DbString        // String (length-prefixed)
DbBytes         // byte[] raw
DbNil           // Vacío (para column families key-only)
```

### Keys compuestas
```java
DbCompositeKey<First, Second>   // Concatena dos keys
// Ejemplo: DbCompositeKey<DbString, DbLong> → "myType" + 12345
// Permite buscar por prefijo "myType" para obtener todos los de ese tipo

DbForeignKey<T extends DbKey>   // Key con referencia a otra CF
// Permite mantener consistencia referencial
```

### Values
Cualquier clase que implemente `DbValue` (que extiende `BufferReader` + `BufferWriter`):
- Serialización a buffer de Agrona
- Zero-copy: lee/escribe directamente en buffers de memoria

## TransactionContext

```java
public interface TransactionContext {
    // Ejecutar operación dentro de transacción
    void runInTransaction(TransactionOperation operation);
    
    // Obtener transacción actual (para nesting)
    ZeebeDbTransaction getCurrentTransaction();
}
```

Las transacciones son **optimistas** (RocksDB OptimisticTransactionDB):
- No hay locks durante la ejecución
- Conflict detection en commit time
- Soporta nesting automático

## Column Families del Engine

El estado del engine se organiza en ~100 column families (definidas en `ZbColumnFamilies`). Cada `Db*State` usa varias CFs para implementar su lógica.

### Ejemplo: DbJobState

```java
public class DbJobState {
    // CF principal: jobKey → JobRecord
    ColumnFamily<DbLong, JobRecordValue> jobsColumnFamily;
    
    // Índice por estado: (state, type, jobKey) → DbNil
    ColumnFamily<DbCompositeKey<DbCompositeKey<DbInt, DbString>, DbLong>, DbNil> 
        statesJobColumnFamily;
    
    // Índice de activables: (type+tenant, jobKey) → DbNil
    ColumnFamily<DbCompositeKey<DbString, DbLong>, DbNil> 
        activatableColumnFamily;
    
    // Índice por deadline: (deadline, jobKey) → DbNil
    ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> 
        deadlinesColumnFamily;
    
    // Índice por backoff: (backoffTime, jobKey) → DbNil
    ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> 
        backoffColumnFamily;
}
```

**Patrón**: Una CF para datos + múltiples CFs como índices secundarios.

Para encontrar jobs activables de tipo "payment":
```java
activatableColumnFamily.whileEqualPrefix(
    new DbString("payment"),  // prefijo
    (key, nil) -> {
        long jobKey = key.second().getValue();
        JobRecord job = jobsColumnFamily.get(new DbLong(jobKey));
        // usar job...
    }
);
```

### Ejemplo: DbElementInstanceState

```java
public class DbElementInstanceState {
    // Instancia por key
    ColumnFamily<DbLong, ElementInstance> elementInstanceColumnFamily;
    
    // Relación padre → hijos
    ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> parentChildColumnFamily;
    
    // Instancias de un key específico
    ElementInstance getInstance(long key);
    
    // Hijos de un scope
    List<ElementInstance> getChildren(long parentKey);
}
```

### ElementInstance (Value)
```java
public class ElementInstance implements DbValue {
    long key;
    long parentKey;
    int childCount;
    long jobKey;                    // Job asociado (si es task)
    int multiInstanceLoopCounter;
    int calledChildInstanceCount;
    int totalTokenCount;            // Tokens activos
    int activeSequenceFlows;        // Sequence flows pendientes
    ProcessInstanceIntent state;    // ACTIVATING, ACTIVATED, etc.
    // ... más campos
}
```

## ProcessingDbState - El Contenedor de Estado

Crea y mantiene todos los estados del engine:

```java
public class ProcessingDbState implements MutableProcessingState {
    MutableProcessState processState;           // Procesos desplegados
    MutableElementInstanceState elementInstanceState; // Elementos activos
    MutableJobState jobState;                   // Jobs
    MutableVariableState variableState;         // Variables
    MutableIncidentState incidentState;         // Incidentes
    MutableMessageState messageState;           // Mensajes
    MutableMessageSubscriptionState messageSubscriptionState;
    MutableTimerInstanceState timerInstanceState; // Timers
    MutableDeploymentState deploymentState;     // Deployments
    MutableDecisionState decisionState;         // Decisiones
    MutableFormState formState;                 // Formularios
    MutableSignalSubscriptionState signalSubscriptionState;
    MutableDistributionState distributionState;
    MutableUserTaskState userTaskState;         // Tareas de usuario
    MutableCompensationSubscriptionState compensationSubscriptionState;
    MutableMultiInstanceState multiInstanceState;
    MutableBannedInstanceState bannedInstanceState; // Instancias bloqueadas
    // ...
}
```

## Event Appliers

Cuando un evento se procesa (o se replaya), los **appliers** actualizan el estado:

```
engine/state/appliers/
├── EventAppliers.java           ← Registro de todos los appliers
├── JobCreatedApplier.java       ← JOB + CREATED → crear en jobState
├── JobCompletedApplier.java     ← JOB + COMPLETED → borrar de jobState
├── ElementInstanceActivatingApplier.java
├── ElementInstanceCompletedApplier.java
└── ... (uno por cada ValueType + Intent que modifica estado)
```

El `EventApplier` usa `ValueType + Intent` para encontrar el applier correcto:

```java
eventApplier.applyState(key, intent, value, recordVersion);
// → Busca el TypedEventApplier para (ValueType, Intent)
// → Ejecuta applier.applyState(key, value)
// → El applier modifica las column families correspondientes
```

## Snapshots

El estado en RocksDB se puede "fotografiar" como snapshot:
1. El `StreamProcessor` crea snapshots periódicamente
2. Un snapshot captura todo el estado de RocksDB en un punto
3. Al recuperarse, se carga el snapshot y se replaean solo los eventos posteriores
4. Esto evita tener que replayear todo el log desde el inicio

```
Timeline del Log:
[───────────────────────────────────────────────────]
        ↑                        ↑            ↑
    Snapshot 1              Snapshot 2     Posición actual
    
Recovery:
  1. Cargar Snapshot 2 (restaurar RocksDB)
  2. Replay eventos desde Snapshot 2 hasta posición actual
  3. Listo para procesar nuevos comandos
```

## Patrones de Consulta Comunes

### Lookup por key
```java
// O(1) - hash lookup en RocksDB
JobRecord job = jobsColumnFamily.get(new DbLong(jobKey));
```

### Buscar por tipo (prefijo)
```java
// O(n) donde n = resultados, no tamaño total
activatableColumnFamily.whileEqualPrefix(
    typeKey,  // prefijo
    (key, value) -> { /* procesar */ }
);
```

### Iterar hijos
```java
// Buscar todos los hijos de un elemento usando CompositeKey
parentChildColumnFamily.whileEqualPrefix(
    new DbLong(parentKey),
    (compositeKey, nil) -> {
        long childKey = compositeKey.second().getValue();
        // ...
    }
);
```

### Verificar existencia
```java
boolean exists = columnFamily.exists(key);  // O(1)
```
