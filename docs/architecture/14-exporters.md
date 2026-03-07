# 14 - Exporters

## Qué son los Exporters

Los **Exporters** son el mecanismo para sacar datos de Zeebe hacia sistemas externos (Elasticsearch, OpenSearch, etc.). Cada record procesado por el engine pasa por los exporters configurados.

```
Engine procesa record
       ↓
LogStream contiene el record
       ↓
ExporterDirector lee del LogStream
       ↓
Exporter.export(record) → Sistema externo
       ↓
Controller.updateLastExportedRecordPosition()
```

## API del Exporter

### Interface Exporter

```java
public interface Exporter {
    // Configuración al arrancar (validar config)
    default void configure(Context context) throws Exception {}
    
    // Abrir recursos (conexiones, etc.)
    default void open(Controller controller) {}
    
    // Exportar un record (llamado por cada record)
    default void export(Record<?> record) {}
    
    // Cerrar recursos
    default void close() {}
}
// Requisito: Debe tener constructor sin argumentos
```

### Interface Controller

```java
public interface Controller {
    // Señalar que exportó hasta esta posición
    void updateLastExportedRecordPosition(long position);
    void updateLastExportedRecordPosition(long position, byte[] metadata);
    
    // Obtener última posición exportada
    long getLastExportedRecordPosition();
    
    // Tareas programadas
    ScheduledTask scheduleCancellableTask(Duration delay, Runnable task);
    
    // Leer metadata persistida
    byte[] readMetadata();
}
```

### Interface Context

```java
public interface Context {
    MeterRegistry getMeterRegistry();    // Métricas
    Logger getLogger();                   // Logger pre-configurado
    Configuration getConfiguration();     // Config del exporter
    int getPartitionId();                 // Partición (null en validación)
    void setFilter(RecordFilter filter);  // Filtrar records por tipo
}
```

## ExporterDirector

El **ExporterDirector** es el Actor que coordina la exportación. Hay uno **por partición**.

### Modos de Operación

|    Modo     |   Rol    |                     Comportamiento                      |
|-------------|----------|---------------------------------------------------------|
| **ACTIVE**  | Líder    | Lee LogStream, llama `export()`, actualiza posiciones   |
| **PASSIVE** | Follower | Recibe posiciones del líder cada 15s, guarda localmente |

### Lifecycle del Director

```
onActorStarting():
  └─ Crea LogStreamReader
  
onActorStarted():
  ├─ Recupera posiciones del snapshot
  ├─ Inicializa ExporterContainers
  └─ Empieza a leer records

readNextEvent():
  ├─ Lee siguiente record del LogStream
  ├─ Aplica filtros (RecordType, ValueType)
  └─ Si pasa filtros → exportEvent()

exportEvent():
  ├─ Wrappea en RecordExporter
  ├─ Llama exporter.export(record)
  ├─ Si falla → reintenta con BackOffRetryStrategy (10s)
  └─ Si éxito → avanza posición

onActorClosed():
  └─ Limpieza de recursos
```

### Métodos de Control

```java
ActorFuture<Void> pauseExporting();      // Pausa dura: sin actualizaciones
ActorFuture<Void> softPauseExporting();  // Export sin actualizar posición
ActorFuture<Void> resumeExporting();     // Reanudar
ActorFuture<ExporterPhase> getPhase();   // Estado actual
```

## Implementaciones de Exporters

### OpenSearch Exporter

```
zeebe/exporters/opensearch-exporter/
├── OpensearchExporter.java         ← Implementa Exporter
├── OpensearchClient.java           ← Cliente REST para OpenSearch
├── RecordIndexRouter.java          ← Rutea records a índices por tipo
└── template/                       ← Templates de índices
```

Características:
- **Bulk requests**: Agrupa records en lotes (max 100MB)
- **Index naming**: Records van a índices por tipo (ej: `zeebe-record-job`)
- **Templates**: Define mappings de OpenSearch por cada ValueType
- **Metadata**: Persiste posición en metadata del Controller

### Elasticsearch Exporter

Similar al de OpenSearch (también en `zeebe/exporters/`), adaptado para Elasticsearch.

## Flujo Completo: Record → Export

```
1. Engine procesa comando JOB_CREATED
   ↓
2. Record escrito al LogStream (posición 42500)
   ↓
3. ExporterDirector en modo ACTIVE lee posición 42500
   ↓
4. Aplica filtros:
   ├─ RecordType: EVENT ✓
   └─ ValueType: JOB ✓ (el exporter tiene filtro para JOBS)
   ↓
5. RecordExporter wrappea el LoggedEvent
   ↓
6. Para cada ExporterContainer:
   ↓
7. exporter.export(record)
   ├─ OpensearchExporter añade a bulk buffer
   ├─ Si buffer lleno o timeout: flush → bulk request a OpenSearch
   └─ exporter.controller.updateLastExportedRecordPosition(42500)
   ↓
8. ExporterDirector persiste posición 42500 en ExportersState (ZeebeDb)
   ↓
9. Posición incluida en próximo snapshot
```

## Configuración

En `application.yaml` del broker:

```yaml
zeebe:
  broker:
    exporters:
      opensearch:
        className: io.camunda.zeebe.exporter.opensearch.OpensearchExporter
        args:
          url: "http://localhost:9200"
          bulk:
            size: 100    # MB máximo por bulk
            delay: 5     # segundos entre flushes
          index:
            prefix: "zeebe-record"
```

### Filtrado de Records

```java
// En exporter.configure():
context.setFilter(new RecordFilter() {
    @Override
    public boolean acceptType(RecordType recordType) {
        return recordType == RecordType.EVENT;  // Solo events
    }
    
    @Override 
    public boolean acceptValue(ValueType valueType) {
        return valueType == ValueType.JOB 
            || valueType == ValueType.PROCESS_INSTANCE;
    }
});
```

## Posiciones y Recovery

### Persistencia

- Cada exporter tiene su posición independiente
- Posiciones guardadas en **ExportersState** (tabla ZeebeDb)
- Incluidas en snapshots para recovery

### Recovery al Arrancar

```
1. Cargar snapshot (incluye posiciones de exporters)
   ↓
2. ExporterDirector obtiene última posición por exporter
   ↓
3. La mínima posición entre todos los exporters = punto de inicio
   ↓
4. LogStreamReader.seek(mínima posición + 1)
   ↓
5. Reexportar desde ahí (at-least-once delivery)
```

### Implicaciones

- **At-least-once**: Un record puede exportarse más de una vez (crash entre export y update posición)
- **Ordered**: Records se exportan en orden de posición
- **Per-partition**: Cada partición tiene sus exporters independientes

## Diagrama de Componentes

```
                ┌────────────────────┐
                │    ExporterDirector │ (per partition, Actor)
                │    ┌──────────────┐│
                │    │LogStreamReader││ ← Lee records
                │    └──────┬───────┘│
                │           │        │
                │    ┌──────▼───────┐│
                │    │RecordExporter ││ ← Wrappea en Record<?>
                │    └──────┬───────┘│
                │           │        │
       ┌────────┼───────────┼────────┼──────────┐
       │        │           │        │          │
┌──────▼──────┐ │   ┌──────▼──────┐ │  ┌───────▼─────┐
│ExporterCont.│ │   │ExporterCont.│ │  │ExporterCont.│
│ OpenSearch  │ │   │ Elastic     │ │  │ Custom      │
│ ┌─────────┐ │ │   │ ┌─────────┐ │ │  │ ┌─────────┐ │
│ │Exporter │ │ │   │ │Exporter │ │ │  │ │Exporter │ │
│ └─────────┘ │ │   │ └─────────┘ │ │  │ └─────────┘ │
│ ┌──────────┐│ │   │ pos=42300   │ │  │ pos=42500   │
│ │Controller││ │   └─────────────┘ │  └─────────────┘
│ │pos=42100 ││ │                    │
│ └──────────┘│ │                    │
└─────────────┘ │                    │
                │  mínima pos = 42100│
                │  (punto de inicio) │
                └────────────────────┘
```

## Escribir un Exporter Custom

```java
public class MyExporter implements Exporter {
    private Controller controller;
    
    @Override
    public void configure(Context context) {
        // Validar configuración
        MyConfig config = context.getConfiguration().instantiate(MyConfig.class);
    }
    
    @Override
    public void open(Controller controller) {
        this.controller = controller;
        // Abrir conexión a sistema externo
    }
    
    @Override
    public void export(Record<?> record) {
        // Enviar record a sistema externo
        sendToExternalSystem(record);
        
        // Señalar éxito
        controller.updateLastExportedRecordPosition(record.getPosition());
    }
    
    @Override
    public void close() {
        // Cerrar conexión
    }
}
```

