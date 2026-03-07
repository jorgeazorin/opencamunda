# 24 - Testing y QA

## Estructura de Módulos de Testing

```
zeebe/
├── test-util/             ← Utilidades base para todos los tests
├── exporter-test/         ← Harness para probar exporters
├── protocol-test-util/    ← Testing de bajo nivel del protocolo
├── protocol-asserts/      ← Assertions AssertJ auto-generadas
└── qa/
    ├── integration-tests/ ← Suite principal de integración
    ├── update-tests/      ← Tests de upgrade con Docker
    └── util/              ← Infraestructura de clusters de test
```

## test-util: Utilidades Base

### RecordingExporter (el más usado)

Captura **todos** los records emitidos durante un test:

```java
// Obtener todos los jobs creados
List<Record<JobRecordValue>> jobs = RecordingExporter.jobRecords()
    .withType("my-task")
    .withIntent(JobIntent.CREATED)
    .limit(5)
    .collect(Collectors.toList());

// Esperar un process instance completado
Record<ProcessInstanceRecordValue> completed =
    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(key)
        .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETED)
        .withElementType(BpmnElementType.PROCESS)
        .getFirst();
```

**Record Streams disponibles (~40 tipos)**:
| Stream | Dominio |
|--------|---------|
| `ProcessInstanceRecordStream` | Instancias de proceso |
| `JobRecordStream` | Jobs |
| `JobBatchRecordStream` | Batch de jobs |
| `MessageRecordStream` | Mensajes |
| `DeploymentRecordStream` | Deployments |
| `TimerRecordStream` | Timers |
| `IncidentRecordStream` | Incidentes |
| `VariableRecordStream` | Variables |
| `MessageSubscriptionRecordStream` | Suscripciones de mensajes |

### TestUtil

```java
// Reintentar hasta éxito (max 100 intentos)
TestUtil.doRepeatedly(() -> findResult())
    .until(result -> result != null);

// Esperar condición
TestUtil.waitUntil(() -> recordingExporter.getCount() > 5);
```

### JUnit Extensions

| Extension | Propósito |
|-----------|----------|
| `AutoCloseResources` + `@AutoCloseResource` | Cleanup declarativo de recursos |
| `AutoCloseResourceExtension` | JUnit 5 extension para auto-close |
| `CachedTestResultsExtension` | Cachear resultados de tests |
| `JMHTestExtension` + `@JMHTest` | Micro-benchmarks JMH |
| `@RegressionTest` | Marcar tests de regresión |
| `@StraceTest` | Tests con strace del sistema |

### Logging de Records

```java
// Log detallado de todos los records
RecordLogger.logRecords();

// Log compacto (más legible)
CompactRecordLogger.log(records);
```

### Assertions Personalizadas

- `DirectoryAssert` - Verificar contenido de directorios
- `EitherAssert` - Verificar Either<L,R>
- `SslAssert` - Verificar certificados SSL
- `TopologyAssert` - Verificar topología del cluster
- `BufferAssert` - Verificar buffers

## exporter-test: Harness para Exporters

Para probar implementaciones de Exporter:

```java
// Mock del Controller
ExporterTestController controller = new ExporterTestController();

// Contexto de test
ExporterTestContext context = new ExporterTestContext()
    .setConfiguration(config);

// Inicializar exporter
myExporter.configure(context);
myExporter.open(controller);

// Exportar un record
myExporter.export(testRecord);

// Verificar posición
assertThat(controller.getPosition()).isEqualTo(expectedPos);

// Ejecutar tareas programadas
controller.runScheduledTasks();
```

**Características del Controller de test:**
- Thread-safe
- Las tareas programadas NO se ejecutan automáticamente
- `runScheduledTasks()` las ejecuta sincrónicamente
- Las tareas persisten en memoria para verificación
- `resetScheduledTasks()` para limpiar

## protocol-test-util: Testing de Bajo Nivel

Para probar el protocolo directamente sin gateway:

```java
@ClassRule
public static RuleChain ruleChain = RuleChain
    .outerRule(BROKER_RULE)
    .around(API_RULE);

@Test
void shouldPublishMessage() {
    API_RULE.createCmdRequest()
        .type(ValueType.MESSAGE, PUBLISH)
        .command()
            .put("name", "order-canceled")
            .put("correlationKey", "order-123")
        .done()
        .sendAndAwait();
}
```

**Clases principales:**
| Clase | Propósito |
|-------|----------|
| `CommandApiRule` | Maneja ActorScheduler, MsgPackHelper, partition clients |
| `ExecuteCommandRequestBuilder` | API fluida para construir comandos |
| `PartitionTestClient` | Cliente de bajo nivel para particiones |
| `MsgPackHelper` | Serialización MessagePack |
| `EnumRandomizer` | Valores enum aleatorios para testing |

## protocol-asserts: Assertions Auto-Generadas

Generadas automáticamente por AssertJ Assertions Generator:

```java
// Assertions tipadas para records
assertThat(jobRecord)
    .hasType("my-task")
    .hasRetries(3)
    .hasElementId("task-1");

assertThat(processInstance)
    .hasBpmnProcessId("my-process")
    .hasVersion(1);
```

**Generación:**
- Plugin: `assertj-assertions-generator-maven-plugin:2.2.0`
- Paquete: `io.camunda.zeebe.protocol.record`
- Planas (no jerárquicas)
- Genera assertions regulares y soft assertions

## qa/util: Infraestructura de Clusters de Test

### ZeebeIntegrationExtension (JUnit 5)

```java
@ExtendWith(ZeebeIntegrationExtension.class)
class MyIntegrationTest {

    @TestCluster
    static TestCluster cluster;

    @TestApplication  
    TestApplication app;

    @Test
    void shouldDoSomething() {
        // cluster está corriendo
    }
}
```

### Clases de infraestructura

| Clase | Propósito |
|-------|----------|
| `TestZeebe` | Nodo Zeebe de test completo |
| `TestGateway` | Gateway de test |
| `TestStandaloneBroker` | Broker standalone de test |
| `ClusterActuatorAssert` | Assertions para actuator del cluster |
| `JobStreamActuatorAssert` | Assertions para job streaming |

## qa/integration-tests: Suite de Integración

- Plugin: **Maven Failsafe** (no Surefire)
- Patterns: `**/IT*.java`, `**/*IT.java`, `**/*ITCase.java`
- JaCoCo deshabilitado para estos tests
- Tests contra clusters reales en memoria

## qa/update-tests: Tests de Upgrade

- Basados en **Testcontainers** (v1.19.8)
- Imagen Docker: `camunda/zeebe:current-test`
- Prueban compatibilidad entre versiones
- Soporte para remote debugger: `RemoteDebugger.configureContainer()`
- Captura automática de logs en fallos

## Patrones de Testing

### 1. Patrón RecordingExporter (el estándar)

```java
@AutoCloseResources
class MyEngineTest {
    @AutoCloseResource
    EngineRule engineRule = EngineRule.singlePartition();

    @BeforeEach
    void setup() {
        RecordingExporter.reset();
    }

    @Test
    void shouldCompleteServiceTask() {
        // Given
        engineRule.deployment()
            .withXmlResource(process)
            .deploy();

        // When
        long instanceKey = engineRule.processInstance()
            .ofBpmnProcessId("my-process")
            .create();

        // Then
        assertThat(
            RecordingExporter.jobRecords(JobIntent.CREATED)
                .withProcessInstanceKey(instanceKey)
                .getFirst()
        ).isNotNull();
    }
}
```

### 2. Patrón Architecture Test (ArchUnit)

```java
class ArchitectureTest {
    @ArchTest
    static final ArchRule engineShouldNotDependOnBroker =
        noClasses()
            .that().resideInAPackage("..engine..")
            .should().dependOnClassesThat()
            .resideInAPackage("..broker..");
}
```

### 3. Patrón JUnit 4 Legacy (ClassRule)

```java
@ClassRule
public static RuleChain ruleChain = RuleChain
    .outerRule(BROKER_RULE)
    .around(API_RULE);

@Rule
public BrokerClassRuleHelper helper = new BrokerClassRuleHelper();

@Test
public void shouldRejectIncompleteCommand() {
    // Usa BrokerClassRuleHelper para lifecycle
}
```

### 4. Control del Tiempo

```java
// Reloj determinístico para tests
ControlledActorClock clock = new ControlledActorClock();
clock.addTime(Duration.ofMinutes(5));
// Ahora los timers se disparan como si hubieran pasado 5 minutos
```

## Ejecutar Tests

```bash
# Unit tests (Surefire)
mvn test -pl zeebe/engine

# Integration tests (Failsafe)
mvn verify -pl zeebe/qa/integration-tests

# Un test específico
mvn test -pl zeebe/engine -Dtest="MyTest#myMethod"

# Tests con Docker (update tests)
mvn verify -pl zeebe/qa/update-tests

# Saltar tests
mvn install -DskipTests           # Salta todos
mvn install -DskipUTs              # Salta unit tests
mvn install -DskipITs              # Salta integration tests
mvn install -DskipChecks           # Salta quality checks + tests
```

## Diagrama: Capas de Testing

```
┌─────────────────────────────────────────────┐
│           Integration Tests (qa/)           │  Clusters completos
│  Docker containers, Testcontainers, upgrade │  Failsafe plugin
├─────────────────────────────────────────────┤
│         Engine Tests (engine/test)          │  Engine en memoria
│  RecordingExporter, EngineRule              │  Surefire plugin
├─────────────────────────────────────────────┤
│        Protocol Tests (protocol-test)       │  Comando directo
│  CommandApiRule, PartitionTestClient        │  Bajo nivel
├─────────────────────────────────────────────┤
│          Unit Tests (cada módulo)           │  Clases aisladas
│  Mocks, AssertJ, ArchUnit                  │  Sin I/O real
└─────────────────────────────────────────────┘
```
