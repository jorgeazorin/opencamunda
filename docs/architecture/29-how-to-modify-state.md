# 29 - How To: Modificar el Estado del Engine

## Resumen

Guía paso a paso para añadir o modificar estado persistente en el engine de Zeebe. El estado se almacena en RocksDB vía la abstracción `ZeebeDb` con column families tipadas. Usamos **DbJobState** como ejemplo real.

## Concepto

```
Record procesado por Engine
  ↓
EventApplier / Processor
  ↓
MutableState.xxx() → ZeebeDb ColumnFamily → RocksDB
```

Todo el estado del engine es reconstruible desde el log (event sourcing). El estado en RocksDB es un **snapshot optimizado** para lecturas rápidas.

## Archivos a Crear/Modificar

| # | Archivo | Módulo |
|---|---------|--------|
| 1 | `ZbColumnFamilies.java` | protocol |
| 2 | `ImmutableXxxState.java` | engine/state/immutable |
| 3 | `MutableXxxState.java` | engine/state/mutable |
| 4 | `DbXxxState.java` | engine/state/instance |
| 5 | `MutableProcessingState.java` | engine/state/mutable |
| 6 | `DbState.java` o `ProcessingDbState.java` | engine/state |

## Paso 1: Definir Column Families

En `protocol/src/main/java/.../ZbColumnFamilies.java`:

```java
public enum ZbColumnFamilies implements EnumValue {
  // ... existentes (numerados secuencialmente) ...
  
  // Añadir tus column families al final
  MY_ENTITY(100),                    // key → entity value
  MY_ENTITY_BY_TYPE(101),            // [type, key] → nil (índice)
  MY_ENTITY_DEADLINES(102),          // [deadline, key] → nil (índice temporal)
  
  // ...
}
```

**Reglas**:
- IDs deben ser **únicos** y **nunca reutilizados** (ni de @Deprecated)
- Convención: agrupar por dominio
- Un entity puede tener múltiples column families (datos + índices)

**Ejemplo real** (Jobs tiene 4 column families):

```java
JOBS(16),               // key → JobRecord (datos)
JOB_STATES(17),         // key → state byte (estado)
JOB_DEADLINES(18),      // [deadline, key] → nil (índice)
JOB_ACTIVATABLE(76),    // [[type, key], tenant] → nil (índice)
JOB_BACKOFF(42),        // [backoff, key] → nil (índice)
```

## Paso 2: Definir Interface Inmutable (lectura)

En `engine/src/main/java/.../state/immutable/`:

```java
public interface MyEntityState {
  
  boolean exists(long entityKey);
  
  MyEntityRecord getEntity(long entityKey);
  
  void forEach(BiFunction<Long, MyEntityRecord, Boolean> callback);
  
  // Queries por índice
  void forEachByType(
      String type, 
      BiFunction<Long, MyEntityRecord, Boolean> callback);
}
```

**Ejemplo real** (`JobState`):

```java
public interface JobState {
  boolean exists(long jobKey);
  State getState(long key);
  boolean isInState(long key, State state);
  JobRecord getJob(long key);
  void forEachActivatableJobs(
      DirectBuffer type, List<String> tenantIds,
      BiFunction<Long, JobRecord, Boolean> callback);
  
  enum State {
    ACTIVATABLE((byte) 0),
    ACTIVATED((byte) 1),
    FAILED((byte) 2),
    NOT_FOUND((byte) 3),
    ERROR_THROWN((byte) 4);
  }
}
```

## Paso 3: Definir Interface Mutable (escritura)

En `engine/src/main/java/.../state/mutable/`:

```java
public interface MutableMyEntityState extends MyEntityState {
  
  void create(long key, MyEntityRecord record);
  
  void update(long key, MyEntityRecord record);
  
  void delete(long key, MyEntityRecord record);
  
  // Operaciones de state machine
  void activate(long key, MyEntityRecord record);
  void complete(long key, MyEntityRecord record);
}
```

**Ejemplo real** (`MutableJobState`):

```java
public interface MutableJobState extends JobState {
  void create(long key, JobRecord record);
  void activate(long key, JobRecord record);
  void complete(long key, JobRecord record);
  void cancel(long key, JobRecord record);
  void fail(long key, JobRecord updatedValue);
  void timeout(long key, JobRecord record);
  void throwError(long key, JobRecord updatedValue);
  void delete(long key, JobRecord record);
  JobRecord updateJobRetries(long jobKey, int retries);
}
```

## Paso 4: Implementar DbState

En `engine/src/main/java/.../state/instance/`:

```java
public final class DbMyEntityState 
    implements MyEntityState, MutableMyEntityState {

  // --- Column Family: datos principales ---
  private final DbLong entityKey;
  private final MyEntityValue entityValue = new MyEntityValue();
  private final ColumnFamily<DbLong, MyEntityValue> entityColumnFamily;

  // --- Column Family: índice por tipo ---
  private final DbString typeKey;
  private final DbCompositeKey<DbString, DbForeignKey<DbLong>> typeEntityKey;
  private final ColumnFamily<DbCompositeKey<DbString, DbForeignKey<DbLong>>, DbNil> 
      byTypeColumnFamily;

  public DbMyEntityState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, 
      final TransactionContext transactionContext) {
    
    // Columna principal
    entityKey = new DbLong();
    entityColumnFamily = zeebeDb.createColumnFamily(
        ZbColumnFamilies.MY_ENTITY,
        transactionContext,
        entityKey,
        entityValue);

    // Índice por tipo
    typeKey = new DbString();
    final var fkEntity = new DbForeignKey<>(entityKey, ZbColumnFamilies.MY_ENTITY);
    typeEntityKey = new DbCompositeKey<>(typeKey, fkEntity);
    byTypeColumnFamily = zeebeDb.createColumnFamily(
        ZbColumnFamilies.MY_ENTITY_BY_TYPE,
        transactionContext,
        typeEntityKey,
        DbNil.INSTANCE);
  }

  // --- Lecturas ---
  
  @Override
  public boolean exists(final long key) {
    entityKey.wrapLong(key);
    return entityColumnFamily.exists(entityKey);
  }

  @Override
  public MyEntityRecord getEntity(final long key) {
    entityKey.wrapLong(key);
    final var value = entityColumnFamily.get(entityKey);
    return value != null ? value.getRecord() : null;
  }

  // --- Escrituras ---
  
  @Override
  public void create(final long key, final MyEntityRecord record) {
    entityKey.wrapLong(key);
    entityValue.setRecord(record);
    entityColumnFamily.upsert(entityKey, entityValue);

    // Actualizar índice
    typeKey.wrapString(record.getType());
    byTypeColumnFamily.upsert(typeEntityKey, DbNil.INSTANCE);
  }

  @Override
  public void delete(final long key, final MyEntityRecord record) {
    entityKey.wrapLong(key);
    entityColumnFamily.deleteExisting(entityKey);

    // Limpiar índice
    typeKey.wrapString(record.getType());
    byTypeColumnFamily.deleteExisting(typeEntityKey);
  }
}
```

## Paso 5: Tipos de Key y Value

### Keys disponibles (zeebe/zb-db)

| Clase | Uso |
|-------|-----|
| `DbLong` | Clave numérica (entity key) |
| `DbString` | Clave string (nombre, tipo) |
| `DbBytes` | Clave en bytes crudos |
| `DbCompositeKey<A, B>` | Clave compuesta (para índices) |
| `DbForeignKey<T>` | Referencia a otra column family |
| `DbTenantAwareKey<T>` | Clave con tenant ID |

### Values disponibles

| Clase | Uso |
|-------|-----|
| `DbNil` | Sin valor (para índices, solo la key importa) |
| Custom `DbValue` | Valor serializado (implementar `write`/`wrap`) |

### Ejemplo de Value personalizado

```java
public class MyEntityValue implements DbValue {
  private final MyEntityRecord record = new MyEntityRecord();
  
  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    record.wrap(buffer, offset, length);
  }
  
  @Override
  public int getLength() {
    return record.getLength();
  }
  
  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    record.write(buffer, offset);
  }
  
  public MyEntityRecord getRecord() { return record; }
  public void setRecord(final MyEntityRecord r) { /* copy data */ }
}
```

## Paso 6: Registrar en ProcessingState

### En MutableProcessingState

```java
public interface MutableProcessingState {
  // ... existentes ...
  MutableMyEntityState getMyEntityState();
}
```

### En ProcessingDbState (implementación)

```java
public class ProcessingDbState implements MutableProcessingState {
  private final DbMyEntityState myEntityState;
  
  public ProcessingDbState(ZeebeDb<ZbColumnFamilies> zeebeDb, ...) {
    // ... existentes ...
    myEntityState = new DbMyEntityState(zeebeDb, transactionContext);
  }
  
  @Override
  public MutableMyEntityState getMyEntityState() {
    return myEntityState;
  }
}
```

## Paso 7: Usar en Processors

```java
public class MyProcessor implements TypedRecordProcessor<MyRecord> {
  
  private final MutableMyEntityState entityState;
  
  public MyProcessor(final MutableProcessingState state) {
    this.entityState = state.getMyEntityState();
  }
  
  @Override
  public void processRecord(final TypedRecord<MyRecord> command, ...) {
    final var record = command.getValue();
    
    // Leer estado
    if (entityState.exists(command.getKey())) {
      // Ya existe
    }
    
    // Modificar estado
    entityState.create(command.getKey(), record);
    
    // Escribir evento
    stateWriter.appendFollowUpEvent(
        command.getKey(), MyIntent.CREATED, record);
  }
}
```

## Ejemplo Real: DbJobState (Simplificado)

```java
public final class DbJobState implements JobState, MutableJobState {

  // key → job record
  private final DbLong jobKey;
  private final ColumnFamily<DbLong, JobRecordValue> jobsColumnFamily;
  
  // key → state byte  
  private final ColumnFamily<DbForeignKey<DbLong>, JobStateValue> statesColumnFamily;
  
  // [[type, key], tenant] → nil (índice de activable jobs)
  private final ColumnFamily<
      DbTenantAwareKey<DbCompositeKey<DbString, DbForeignKey<DbLong>>>, DbNil> 
      activatableColumnFamily;
  
  // [deadline, key] → nil (índice de deadlines)
  private final ColumnFamily<DbCompositeKey<DbLong, DbForeignKey<DbLong>>, DbNil> 
      deadlinesColumnFamily;

  public DbJobState(ZeebeDb<ZbColumnFamilies> zeebeDb, TransactionContext ctx) {
    jobKey = new DbLong();
    jobsColumnFamily = zeebeDb.createColumnFamily(
        ZbColumnFamilies.JOBS, ctx, jobKey, jobRecordToRead);
    // ... crear otras column families ...
  }

  @Override
  public void create(final long key, final JobRecord record) {
    // 1. Guardar record
    jobKey.wrapLong(key);
    jobRecordToWrite.setRecordWithoutVariables(record);
    jobsColumnFamily.upsert(jobKey, jobRecordToWrite);
    
    // 2. Guardar estado
    jobState.setState(State.ACTIVATABLE);
    statesColumnFamily.upsert(fkJob, jobState);
    
    // 3. Actualizar índice de activable
    makeJobActivatable(key, record);
  }

  @Override  
  public void activate(final long key, final JobRecord record) {
    // 1. Actualizar record con worker info
    // 2. Cambiar estado a ACTIVATED
    // 3. Quitar de índice activatable
    // 4. Añadir a índice de deadlines
  }
}
```

## Patrones Comunes de Column Family

### Dato Principal (key → value)
```
CF: JOBS          key=42 → {type:"payment", retries:3, ...}
```

### Estado (key → enum byte)
```
CF: JOB_STATES    key=42 → ACTIVATABLE (byte 0)
```

### Índice por Propiedad ([prop, key] → nil)
```
CF: JOB_ACTIVATABLE  ["payment", 42] → nil
                     ["payment", 87] → nil  
                     ["email", 55] → nil
```

### Índice Temporal ([timestamp, key] → nil)
```
CF: JOB_DEADLINES  [1700000000, 42] → nil
                   [1700001000, 87] → nil
```

### Multi-tenant ([prop, key] + tenant → nil)
```
CF: JOB_ACTIVATABLE  [["payment", 42], "tenant-a"] → nil
                     [["payment", 87], "tenant-b"] → nil
```

## Reglas Importantes

1. **Event sourcing**: El estado se reconstruye replaying events. No guardes estado que no pueda derivarse de los records
2. **Transaccionalidad**: Todas las escrituras dentro de un `processRecord()` son atómicas (misma transacción RocksDB)
3. **Column family IDs son permanentes**: Nunca cambies el ID de una column family existente. Los deprecados se marcan `@Deprecated` pero no se borran
4. **Foreign keys**: Usa `DbForeignKey` para integridad referencial y cascading deletes
5. **Prefijo de búsqueda**: `DbCompositeKey` permite búsquedas por prefijo (primer componente) de forma eficiente
6. **No clonar records**: Usa `DbValue` wrappers que copian solo los datos necesarios

## Checklist

- [ ] Column families definidas en `ZbColumnFamilies` con IDs únicos
- [ ] Interface inmutable (lectura) definida
- [ ] Interface mutable (escritura) definida
- [ ] Implementación `DbXxxState` con column families
- [ ] Value wrappers creados (si necesario)
- [ ] State registrado en `ProcessingDbState`
- [ ] State accesible desde `MutableProcessingState`
- [ ] Usado en processors/event appliers
- [ ] Tests unitarios del state
- [ ] Verificar que el estado es reconstruible desde events
