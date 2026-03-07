# 28 - How To: Añadir un Exporter

## Resumen

Guía paso a paso para crear un nuevo exporter que recibe todos los records procesados por el engine y los envía a un sistema externo. Usamos **DebugLogExporter** como ejemplo real de referencia.

## Concepto

Un **Exporter** recibe cada record (evento/comando) procesado por el engine de forma secuencial. Es el mecanismo principal para integrar Zeebe con sistemas externos (bases de datos, motores de búsqueda, colas de mensajes, etc.).

```
Engine → LogStream → ExporterDirector → [Exporter1, Exporter2, ...]
                                              │           │
                                              ▼           ▼
                                         Elasticsearch  Tu Exporter
```

## Archivos a Crear/Modificar

| # |            Archivo             |             Módulo             |
|---|--------------------------------|--------------------------------|
| 1 | `MyExporter.java`              | tu módulo o exporter existente |
| 2 | `MyExporterConfiguration.java` | (opcional) configuración typed |
| 3 | Configuración YAML             | broker config                  |
| 4 | `pom.xml`                      | (si módulo separado)           |

## Paso 1: Implementar la Interfaz Exporter

### API del Exporter

```java
// zeebe/exporter-api/src/main/java/.../exporter/api/Exporter.java
public interface Exporter {
    default void configure(Context context) throws Exception {}
    default void open(Controller controller) {}
    default void close() {}
    void export(Record<?> record);  // único método obligatorio
}
```

### Implementación Mínima

```java
package com.mycompany.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;

public class MyExporter implements Exporter {

  private Controller controller;
  private MyExporterConfiguration config;
  
  @Override
  public void configure(final Context context) throws Exception {
    // 1. Leer configuración
    config = context.getConfiguration().instantiate(MyExporterConfiguration.class);
    
    // 2. (Opcional) Filtrar qué records recibir
    context.setFilter(new Context.RecordFilter() {
      @Override
      public boolean acceptType(final RecordType recordType) {
        return recordType == RecordType.EVENT;  // solo eventos
      }
      
      @Override
      public boolean acceptValue(final ValueType valueType) {
        return valueType == ValueType.JOB 
            || valueType == ValueType.PROCESS_INSTANCE;
      }
    });
  }

  @Override
  public void open(final Controller controller) {
    this.controller = controller;
    // Inicializar conexiones, clientes, etc.
  }

  @Override
  public void close() {
    // Cerrar conexiones, flush buffers
  }

  @Override
  public void export(final Record<?> record) {
    // Procesar el record
    final String json = record.toJson();
    
    // Enviar a sistema externo
    sendToExternalSystem(json);
    
    // IMPORTANTE: Actualizar posición después de export exitoso
    controller.updateLastExportedRecordPosition(record.getPosition());
  }
  
  private void sendToExternalSystem(final String data) {
    // Tu lógica de exportación
  }
}
```

## Paso 2: Clase de Configuración (Opcional)

```java
public class MyExporterConfiguration {
  // Estos campos se mapean desde el YAML
  public String endpoint = "http://localhost:9200";
  public int batchSize = 100;
  public boolean prettyPrint = false;
  
  // Zeebe usa Jackson para deserializar
}
```

## Paso 3: Contexto y Controller

### Context (disponible en `configure`)

```java
public interface Context {
  MeterRegistry getMeterRegistry();        // Métricas Micrometer
  Logger getLogger();                       // Logger pre-configurado
  Configuration getConfiguration();         // Config del exporter
  int getPartitionId();                     // ID de partición
  void setFilter(RecordFilter filter);      // Filtrar records
  
  interface RecordFilter {
    boolean acceptType(RecordType recordType);   // COMMAND, EVENT, REJECTION
    boolean acceptValue(ValueType valueType);    // JOB, PROCESS_INSTANCE, etc.
  }
}
```

### Controller (disponible en `open`)

```java
public interface Controller {
  // Marcar posición exportada (el broker no reenviará records anteriores)
  void updateLastExportedRecordPosition(long position);
  
  // Con metadata opcional (para guardar estado propio)
  void updateLastExportedRecordPosition(long position, byte[] metadata);
  
  // Leer última posición confirmada
  long getLastExportedRecordPosition();
  
  // Programar tarea periódica (ej: flush batch)
  ScheduledTask scheduleCancellableTask(Duration delay, Runnable task);
  
  // Leer metadata guardada previamente
  Optional<byte[]> readMetadata();
}
```

## Paso 4: El Objeto Record

```java
public interface Record<T extends RecordValue> {
  long getPosition();               // Posición en el log
  long getSourceRecordPosition();   // Posición del comando original
  long getKey();                    // Clave del entity
  long getTimestamp();              // Timestamp de escritura
  
  Intent getIntent();               // CREATED, COMPLETED, etc.
  int getPartitionId();             // Partición
  RecordType getRecordType();       // COMMAND, EVENT, REJECTION
  RejectionType getRejectionType(); // Si es rejection
  String getRejectionReason();
  ValueType getValueType();         // JOB, PROCESS_INSTANCE, etc.
  
  T getValue();                     // Datos específicos del record
  
  String toJson();                  // Serialización JSON completa
  Record<T> clone();                // Deep copy
}
```

**Importante**: El `Record` envuelve un buffer interno. Si necesitas almacenar múltiples records, usa `record.toJson()` o `record.clone()`.

## Paso 5: Configurar en el Broker

### Opción A: Exporter en classpath (módulo interno)

```yaml
# application.yaml o zeebe.broker.cfg.yaml
zeebe:
  broker:
    exporters:
      myExporter:
        className: com.mycompany.exporter.MyExporter
        args:
          endpoint: "http://localhost:9200"
          batchSize: 100
          prettyPrint: false
```

### Opción B: Exporter como JAR externo

```yaml
zeebe:
  broker:
    exporters:
      myExporter:
        jarPath: /path/to/my-exporter.jar
        className: com.mycompany.exporter.MyExporter
        args:
          endpoint: "http://localhost:9200"
```

### Cómo se carga

```java
// ExporterDescriptor.java
public class ExporterDescriptor {
  private final Class<? extends Exporter> exporterClass;
  private final ExporterConfiguration configuration;

  public Exporter newInstance() {
    return ReflectUtil.newInstance(exporterClass);  // Constructor sin args
  }
}
```

## Ejemplo Real: DebugLogExporter

```java
public class DebugLogExporter implements Exporter {
  private DebugExporterConfiguration configuration;
  private ObjectMapper objectMapper;
  private LogFunction logger;

  @Override
  public void configure(final Context context) {
    configuration = context.getConfiguration()
        .instantiate(DebugExporterConfiguration.class);
    logger = LogLevel.getLogger(configuration.getLogLevel(), context.getLogger());
  }

  @Override
  public void open(final Controller controller) {
    logger.log("Debug exporter opened");
    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    if (configuration.prettyPrint) {
      objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
  }

  @Override
  public void close() {
    logger.log("Debug exporter closed");
  }

  @Override
  public void export(final Record<?> record) {
    logger.log("{}", objectMapper.writeValueAsString(record));
  }
}
```

## Patrones Avanzados

### Batch Export (con flush periódico)

```java
public class BatchExporter implements Exporter {
  private final List<String> batch = new ArrayList<>();
  private Controller controller;
  private long lastPosition;
  
  @Override
  public void open(final Controller controller) {
    this.controller = controller;
    // Flush cada 5 segundos
    controller.scheduleCancellableTask(Duration.ofSeconds(5), this::flush);
  }
  
  @Override
  public void export(final Record<?> record) {
    batch.add(record.toJson());
    lastPosition = record.getPosition();
    
    if (batch.size() >= 100) {
      flush();
    }
  }
  
  private void flush() {
    if (batch.isEmpty()) return;
    
    sendBatch(batch);
    batch.clear();
    controller.updateLastExportedRecordPosition(lastPosition);
    
    // Re-programar el flush periódico
    controller.scheduleCancellableTask(Duration.ofSeconds(5), this::flush);
  }
}
```

### Guardar Estado con Metadata

```java
@Override
public void open(final Controller controller) {
  // Restaurar estado previo
  controller.readMetadata().ifPresent(bytes -> {
    lastCheckpoint = deserialize(bytes);
  });
}

@Override
public void export(final Record<?> record) {
  // ... export logic ...
  
  // Guardar estado junto con posición
  byte[] state = serialize(myCheckpoint);
  controller.updateLastExportedRecordPosition(
      record.getPosition(), state);
}
```

## Reglas Importantes

1. **Constructor vacío obligatorio**: Zeebe instancia el exporter con `newInstance()` (reflexión)
2. **`export()` es blocking**: Si falla con RuntimeException, Zeebe reintenta indefinidamente
3. **Actualizar posición**: Si no llamas `updateLastExportedRecordPosition`, el exporter recibirá los mismos records tras restart
4. **Un exporter por partición**: Cada partición tiene su propia instancia del exporter
5. **Orden garantizado**: Los records llegan en orden de posición del log
6. **No bloquear**: El exporter bloquea el procesamiento del log → batching recomendado
7. **Thread-safety**: Una sola instancia por partición, llamada desde un solo thread

## Dependencia Maven

```xml
<dependency>
  <groupId>io.camunda</groupId>
  <artifactId>zeebe-exporter-api</artifactId>
  <version>${zeebe.version}</version>
  <scope>provided</scope>  <!-- ya está en el classpath del broker -->
</dependency>
```

## Checklist

- [ ] Clase implementando `Exporter` con constructor vacío
- [ ] `configure()`: leer config y setear filtros
- [ ] `open()`: inicializar recursos
- [ ] `export()`: procesar record y actualizar posición
- [ ] `close()`: liberar recursos
- [ ] Clase de configuración (opcional)
- [ ] Configuración YAML en broker
- [ ] Tests unitarios con records mockeados
- [ ] Test de integración con `RecordingExporter`

