# 30 - Flujo de Desarrollo

## Setup Inicial

### Requisitos

- **JDK 21** (Eclipse Temurin recomendado)
- **Maven 3.x** (incluido como wrapper `./mvnw`)
- **Docker** (para tests de integración con Testcontainers)
- **Git** con commit-lint configurado

### Primera compilación

```bash
git clone <repo>
cd opencamunda
mvn clean install -Dquickly    # Build rápido inicial
```

### IDE Recomendado

- **IntelliJ IDEA** o **VS Code** con extensiones Java
- Importar como proyecto Maven
- Configurar Java 21 como SDK del proyecto
- Instalar plugin de Google Java Format (para formateo automático)

## Workflow Diario

### 1. Hacer cambios en un módulo

```bash
# Compilar solo el módulo afectado y sus dependencias
mvn clean install -pl zeebe/engine -am -DskipTests

# Si cambias protocol, recompilar todo lo que depende de él
mvn clean install -pl zeebe/protocol,zeebe/protocol-impl -am -DskipTests
```

### 2. Ejecutar tests

```bash
# Tests unitarios de un módulo
mvn test -pl zeebe/engine

# Un test específico
mvn test -pl zeebe/engine -Dtest="BpmnStreamProcessorTest"

# Tests de integración
mvn verify -pl zeebe/qa/integration-tests -DskipUTs
```

### 3. Formatear código antes de commit

```bash
mvn spotless:apply
```

### 4. Verificar calidad

```bash
mvn verify -pl zeebe/engine -DskipITs   # Tests + checks
mvn checkstyle:check -pl zeebe/engine    # Solo checkstyle
```

## Estructura de un Módulo Típico

```
zeebe/engine/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── io/camunda/zeebe/engine/
│   │   │       ├── Engine.java              ← Entry point
│   │   │       ├── processing/              ← Procesadores de comandos
│   │   │       │   ├── bpmn/                ← Procesadores BPMN
│   │   │       │   ├── job/                 ← Procesadores de Jobs
│   │   │       │   └── deployment/          ← Procesadores de Deploy
│   │   │       └── state/                   ← Estado persistente
│   │   │           ├── appliers/            ← Aplican eventos al estado
│   │   │           └── instance/            ← Estado de instancias
│   │   └── resources/
│   └── test/
│       ├── java/                            ← Tests
│       └── resources/                       ← Fixtures de test
└── target/
    ├── classes/
    ├── generated-sources/                   ← Código generado
    └── surefire-reports/                    ← Reportes de tests
```

## Convenciones de Código

### Nombrado

- **Packages**: `io.camunda.zeebe.<modulo>.<subdominio>`
- **Processors**: `<Entity><Intent>Processor` (e.g., `JobCompleteProcessor`)
- **State**: `Db<Entity>State` (e.g., `DbJobState`)
- **Appliers**: `<Entity><Intent>Applier` (e.g., `JobCompletedApplier`)
- **Records**: `<Entity>Record` (e.g., `JobRecord`)
- **Intents**: `<Entity>Intent` enum (e.g., `JobIntent.COMPLETE`)

### Patrones Recurrentes

#### Command Pattern (Procesamiento de comandos)

```java
// 1. TypedRecordProcessor procesa un comando
public class JobCompleteProcessor implements TypedRecordProcessor<JobRecord> {
    @Override
    public void processRecord(TypedRecord<JobRecord> command) {
        // Validar estado actual
        // Si válido → accept (escribe evento)
        // Si inválido → reject (escribe rechazo)
    }
}
```

#### Either Pattern (Manejo de errores)

```java
// No se usan excepciones para flujo normal, se usa Either<Failure, Result>
return variableMappingBehavior
    .applyInputMappings(context, element)          // Either<Failure, ?>
    .flatMap(ok -> jobBehavior.evaluateExpressions()) // Composición
    .ifLeft(failure -> createIncident(failure));       // Manejo de error
```

#### State Pattern (Acceso a estado)

```java
// Siempre dentro de TransactionContext
transactionContext.runInTransaction(() -> {
    final var job = jobState.getJob(jobKey);
    if (job != null) {
        job.setRetries(newRetries);
        jobState.updateJob(jobKey, job);
    }
});
```

#### Behavior Pattern (Lógica reutilizable)

```java
// Los behaviors encapsulan lógica compartida entre processors
// Se inyectan via BpmnBehaviors
private final BpmnVariableMappingBehavior variableMappingBehavior;
private final BpmnJobBehavior jobBehavior;
private final BpmnEventSubscriptionBehavior eventSubscriptionBehavior;
```

## Cómo Encontrar Código

### "¿Dónde se procesa el comando X?"

1. Identifica el `ValueType` (e.g., `JOB`)
2. Identifica el `Intent` (e.g., `COMPLETE`)
3. Busca en `*EventProcessors.java` el registro: `onCommand(ValueType.JOB, JobIntent.COMPLETE, ...)`
4. Ahí está el procesador: `JobCompleteProcessor`

### "¿Dónde se aplica el evento X al estado?"

1. Busca en `engine/state/appliers/` el applier correspondiente
2. El `EventApplier` mapea `ValueType + Intent` → `TypedEventApplier`

### "¿Cómo se ejecuta el elemento BPMN Y?"

1. Ve a `engine/processing/bpmn/`
2. Busca el `BpmnElementProcessor` para ese tipo en `BpmnElementProcessors.java`
3. El processor implementa `onActivate()`, `onComplete()`, `onTerminate()`

### "¿Qué column families usa la entidad Z?"

1. Ve a `protocol/ZbColumnFamilies.java` → lista todas las CFs
2. Ve a `engine/state/<dominio>/Db<Entity>State.java` → usa column families específicas

### "¿Cómo llega un request del cliente al engine?"

```
Cliente → gRPC (gateway.proto)
       → GatewayGrpcService
       → EndpointManager
       → RequestMapper → BrokerRequest
       → BrokerClient → Transport → Broker
       → Partition → StreamProcessor → Engine.process()
       → RecordProcessorMap → TypedRecordProcessor
       → Resultado → ResponseMapper → gRPC Response
```

## Depuración

### Logs

- Framework: Log4j 2 + SLF4J
- Loggers por clase: `private static final Logger LOG = LoggerFactory.getLogger(...)`
- Cambiar nivel: configurar en `log4j2.xml` o vía propiedades de sistema

### Métricas

- Framework: Micrometer
- Cada módulo registra métricas propias
- Dashboards Grafana en `monitor/grafana/`

### Tests de Integración

- Usan **Testcontainers** para levantar Elasticsearch, Zeebe, etc.
- El módulo `zeebe/qa/` contiene los tests end-to-end
- `zeebe/test-util/` tiene utilidades para tests

## Diagrama de Dependencias para Desarrollo

```
Si modificas...          Recompila...
─────────────           ─────────────
protocol/               → protocol-impl → engine → broker (casi todo)
stream-platform/        → engine → broker
zb-db/                  → engine → broker
logstreams/             → stream-platform → engine → broker
gateway-protocol/       → gateway-protocol-impl → gateway → gateway-rest
engine/                 → broker
gateway/                → gateway-rest
scheduler/              → logstreams, stream-platform, engine, broker
util/                   → Prácticamente todo
```

## Cheat Sheet de Comandos

```bash
# Build completo rápido
mvn clean install -Dquickly

# Build completo sin tests
mvn clean install -DskipTests

# Solo un módulo
mvn clean install -pl zeebe/engine -am -DskipTests

# Formatear código
mvn spotless:apply

# Verificar formateo
mvn spotless:check

# Ejecutar test específico
mvn test -pl zeebe/engine -Dtest="MyTest#myMethod"

# Build Docker
DOCKER_BUILDKIT=1 docker build -t opencamunda .

# Ejecutar Docker
docker run -p 26500:26500 -p 8080:8080 opencamunda

# Perfilar build
mvn install -DskipTests  # Ver tiempos en .profiler/
```

