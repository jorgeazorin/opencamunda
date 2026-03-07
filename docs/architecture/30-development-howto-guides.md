# Zeebe Development How-To Guides

Four step-by-step guides for common development tasks in the Zeebe codebase, with exact file paths, class names, and code patterns.

---

## Guide 1: How to Add a New gRPC Endpoint

This guide traces the full path using **ResolveIncident** as the reference example — one of the simplest RPCs in the system.

### Step 1: Define the RPC in the Proto File

**File:** `zeebe/gateway-protocol/src/main/proto/gateway.proto`

Add your request/response messages and the RPC to the `Gateway` service:

```protobuf
// 1a. Define request message
message MyNewCommandRequest {
  int64 resourceKey = 1;
  string someField = 2;
}

// 1b. Define response message
message MyNewCommandResponse {
}

// 1c. Add to the Gateway service block
service Gateway {
  // ... existing RPCs ...
  rpc MyNewCommand (MyNewCommandRequest) returns (MyNewCommandResponse) {
  }
}
```

**Reference:** The `ResolveIncident` messages are defined as:
```protobuf
message ResolveIncidentRequest {
  int64 incidentKey = 1;
}
message ResolveIncidentResponse {
}
```

After editing, run `mvn generate-sources` in the `gateway-protocol` module to regenerate Java stubs. The generated class `GatewayGrpc.GatewayImplBase` will gain an abstract method for your new RPC.

### Step 2: Add the gRPC Service Method

**File:** `zeebe/gateway/src/main/java/io/camunda/zeebe/gateway/GatewayGrpcService.java`  
**Package:** `io.camunda.zeebe.gateway`

This class extends `GatewayGrpc.GatewayImplBase` and delegates every call to `EndpointManager`. Add:

```java
@Override
public void myNewCommand(
    final MyNewCommandRequest request,
    final StreamObserver<MyNewCommandResponse> responseObserver) {
  endpointManager.myNewCommand(
      request, ErrorMappingStreamObserver.ofStreamObserver(responseObserver));
}
```

**Pattern from ResolveIncident:**
```java
@Override
public void resolveIncident(
    final ResolveIncidentRequest request,
    final StreamObserver<ResolveIncidentResponse> responseObserver) {
  endpointManager.resolveIncident(
      request, ErrorMappingStreamObserver.ofStreamObserver(responseObserver));
}
```

### Step 3: Add the Endpoint Manager Handler

**File:** `zeebe/gateway/src/main/java/io/camunda/zeebe/gateway/EndpointManager.java`  
**Package:** `io.camunda.zeebe.gateway`

The `EndpointManager` uses a generic `sendRequest` pattern that connects three pieces: a `RequestMapper` function, a `ResponseMapper` function, and the `BrokerClient`.

```java
public void myNewCommand(
    final MyNewCommandRequest request,
    final ServerStreamObserver<MyNewCommandResponse> responseObserver) {
  sendRequest(
      request,
      RequestMapper::toMyNewCommandRequest,
      ResponseMapper::toMyNewCommandResponse,
      responseObserver);
}
```

The private `sendRequest` method in `EndpointManager` (line ~422) does:
1. Calls `requestMapper.apply(grpcRequest)` → produces a `BrokerRequest<T>`
2. Calls `brokerClient.sendRequestWithRetry(brokerRequest, ...)`
3. On response, calls `responseMapper` to convert back to a gRPC response

### Step 4: Create the Request Mapper

**File:** `zeebe/gateway/src/main/java/io/camunda/zeebe/gateway/RequestMapper.java`  
**Package:** `io.camunda.zeebe.gateway`

Map gRPC request fields to a Broker request object:

```java
public static BrokerMyNewCommandRequest toMyNewCommandRequest(
    final MyNewCommandRequest grpcRequest) {
  return new BrokerMyNewCommandRequest(grpcRequest.getResourceKey());
}
```

**Pattern from ResolveIncident:**
```java
public static BrokerResolveIncidentRequest toResolveIncidentRequest(
    final ResolveIncidentRequest grpcRequest) {
  return new BrokerResolveIncidentRequest(grpcRequest.getIncidentKey());
}
```

### Step 5: Create the Broker Request Class

**File (new):** `zeebe/gateway/src/main/java/io/camunda/zeebe/gateway/impl/broker/request/BrokerMyNewCommandRequest.java`  
**Package:** `io.camunda.zeebe.gateway.impl.broker.request`

Extend `BrokerExecuteCommand<T>` with the appropriate `ValueType` and `Intent`:

```java
public final class BrokerMyNewCommandRequest extends BrokerExecuteCommand<MyNewCommandRecord> {

  private final MyNewCommandRecord requestDto = new MyNewCommandRecord();

  public BrokerMyNewCommandRequest(final long resourceKey) {
    super(ValueType.MY_NEW_COMMAND, MyNewCommandIntent.DO_SOMETHING);
    request.setKey(resourceKey);
  }

  @Override
  public MyNewCommandRecord getRequestWriter() {
    return requestDto;
  }

  @Override
  protected MyNewCommandRecord toResponseDto(final DirectBuffer buffer) {
    final MyNewCommandRecord responseDto = new MyNewCommandRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }
}
```

**Reference:** `BrokerResolveIncidentRequest` at `zeebe/gateway/src/main/java/io/camunda/zeebe/gateway/impl/broker/request/BrokerResolveIncidentRequest.java`:
```java
public final class BrokerResolveIncidentRequest extends BrokerExecuteCommand<IncidentRecord> {
  private final IncidentRecord requestDto = new IncidentRecord();

  public BrokerResolveIncidentRequest(final long incidentKey) {
    super(ValueType.INCIDENT, IncidentIntent.RESOLVE);
    request.setKey(incidentKey);
  }
  // ...
}
```

The `BrokerExecuteCommand` base class (at `zeebe/broker-client/src/main/java/io/camunda/zeebe/broker/client/api/dto/BrokerExecuteCommand.java`) sets `ValueType` + `Intent` on the internal `ExecuteCommandRequest`.

### Step 6: Create the Response Mapper

**File:** `zeebe/gateway/src/main/java/io/camunda/zeebe/gateway/ResponseMapper.java`  
**Package:** `io.camunda.zeebe.gateway`

Map broker response back to a gRPC response:

```java
public static MyNewCommandResponse toMyNewCommandResponse(
    final long key, final MyNewCommandRecord brokerResponse) {
  return MyNewCommandResponse.newBuilder().build();
}
```

**Pattern from ResolveIncident:**
```java
public static ResolveIncidentResponse toResolveIncidentResponse(
    final long key, final IncidentRecord brokerResponse) {
  return ResolveIncidentResponse.newBuilder().build();
}
```

### Step 7: Define ValueType and Intent (Protocol Layer)

If your command introduces a new record type:

**File:** `zeebe/protocol/src/main/java/io/camunda/zeebe/protocol/record/ValueType.java`  
Add a new enum constant (e.g., `MY_NEW_COMMAND`).

**File (new):** `zeebe/protocol/src/main/java/io/camunda/zeebe/protocol/record/intent/MyNewCommandIntent.java`  
**Package:** `io.camunda.zeebe.protocol.record.intent`

```java
public enum MyNewCommandIntent implements Intent {
  DO_SOMETHING((short) 0, false),
  DONE((short) 1);

  private final short value;
  private final boolean shouldBanInstance;

  MyNewCommandIntent(final short value) { this(value, true); }
  MyNewCommandIntent(final short value, final boolean shouldBanInstance) {
    this.value = value;
    this.shouldBanInstance = shouldBanInstance;
  }

  @Override
  public short value() { return value; }

  @Override
  public boolean isEvent() { return this == DONE; }
}
```

**Reference:** `IncidentIntent` at `zeebe/protocol/src/main/java/io/camunda/zeebe/protocol/record/intent/IncidentIntent.java`.

### Step 8: Register Engine Processor

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/EngineProcessors.java`  
**Package:** `io.camunda.zeebe.engine.processing`

Register your processor with `TypedRecordProcessors.onCommand(ValueType, Intent, processor)`:

```java
typedRecordProcessors.onCommand(
    ValueType.MY_NEW_COMMAND,
    MyNewCommandIntent.DO_SOMETHING,
    new MyNewCommandProcessor(processingState, writers));
```

**Pattern from Incident:**
```java
// In IncidentEventProcessors.addProcessors():
typedRecordProcessors.onCommand(
    ValueType.INCIDENT,
    IncidentIntent.RESOLVE,
    new IncidentResolveProcessor(processingState, bpmnStreamProcessor, writers, jobActivationBehavior));
```

### Step 9: Implement the Engine Processor

**File (new):** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/mynewcommand/MyNewCommandProcessor.java`

```java
public final class MyNewCommandProcessor implements TypedRecordProcessor<MyNewCommandRecord> {

  private final StateWriter stateWriter;
  private final TypedResponseWriter responseWriter;

  public MyNewCommandProcessor(final ProcessingState state, final Writers writers) {
    stateWriter = writers.state();
    responseWriter = writers.response();
  }

  @Override
  public void processRecord(final TypedRecord<MyNewCommandRecord> command) {
    // 1. Read state, validate
    // 2. Write event (state change):
    //    stateWriter.appendFollowUpEvent(key, MyNewCommandIntent.DONE, record);
    // 3. Write response:
    //    responseWriter.writeEventOnCommand(key, MyNewCommandIntent.DONE, record, command);
  }
}
```

### Step 10: Register Event Applier

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/state/appliers/EventAppliers.java`  
**Package:** `io.camunda.zeebe.engine.state.appliers`

```java
register(MyNewCommandIntent.DONE, new MyNewCommandDoneApplier(state));
```

### Step 11: Update `Engine.SUPPORTED_VALUETYPES`

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/Engine.java`

Ensure your new `ValueType` is included in the range or add it explicitly:
```java
private static final EnumSet<ValueType> SUPPORTED_VALUETYPES =
    EnumSet.range(ValueType.JOB, ValueType.MULTI_INSTANCE);
```

### Summary: Full Request Path

```
Client → gRPC stub
  → GatewayGrpcService.myNewCommand()
    → EndpointManager.myNewCommand()
      → RequestMapper.toMyNewCommandRequest()  →  BrokerMyNewCommandRequest(ValueType, Intent)
        → BrokerClient.sendRequestWithRetry()
          → [network to broker]
            → Engine.process()
              → RecordProcessorMap.get(COMMAND, MY_NEW_COMMAND, DO_SOMETHING)
                → MyNewCommandProcessor.processRecord()
                  → stateWriter.appendFollowUpEvent() → EventApplier → state update
          → [response back]
      → ResponseMapper.toMyNewCommandResponse()
    → StreamObserver.onNext()
```

### Files Changed Checklist
| # | File | Action |
|---|------|--------|
| 1 | `zeebe/gateway-protocol/src/main/proto/gateway.proto` | Add messages + RPC |
| 2 | `zeebe/gateway/.../GatewayGrpcService.java` | Override new method |
| 3 | `zeebe/gateway/.../EndpointManager.java` | Add handler method |
| 4 | `zeebe/gateway/.../RequestMapper.java` | Add mapping method |
| 5 | `zeebe/gateway/.../impl/broker/request/BrokerXxxRequest.java` | **New file** |
| 6 | `zeebe/gateway/.../ResponseMapper.java` | Add mapping method |
| 7 | `zeebe/protocol/.../ValueType.java` | Add enum (if new type) |
| 8 | `zeebe/protocol/.../intent/XxxIntent.java` | **New file** |
| 9 | `zeebe/protocol-impl/.../record/value/xxx/XxxRecord.java` | **New file** (record value) |
| 10 | `zeebe/engine/.../processing/xxx/XxxProcessor.java` | **New file** |
| 11 | `zeebe/engine/.../processing/EngineProcessors.java` | Register processor |
| 12 | `zeebe/engine/.../state/appliers/EventAppliers.java` | Register event applier |
| 13 | `zeebe/engine/.../state/appliers/XxxApplier.java` | **New file** |
| 14 | `zeebe/engine/.../Engine.java` | Ensure ValueType in supported set |

---

## Guide 2: How to Add a New BPMN Element

This guide uses **ManualTask** as the reference — the simplest element processor in the system.

### Step 1: Add the Element Type to the Enum

**File:** `zeebe/protocol/src/main/java/io/camunda/zeebe/protocol/record/value/BpmnElementType.java`  
**Package:** `io.camunda.zeebe.protocol.record.value`

```java
public enum BpmnElementType {
  // ... existing types ...
  MANUAL_TASK("manualTask"),   // ← reference example
  MY_NEW_ELEMENT("myNewElement"), // ← your new element
  // ...
}
```

The string argument is the BPMN XML element tag name. `null` means no direct XML mapping (used for synthetic types like `MULTI_INSTANCE_BODY`).

### Step 2: Create the Executable Model Element

The BPMN model is parsed into executable model elements. If your new element behaves like an activity (has input/output mappings, boundary events, etc.), extend `ExecutableActivity`.

**Directory:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/deployment/model/element/`

For a simple task, you can reuse `ExecutableActivity` directly (as ManualTask does). For specialized behavior, create a new class:

```java
public class ExecutableMyNewElement extends ExecutableActivity {
  // add custom model properties here
}
```

### Step 3: Implement the Element Processor

**Interface:** `BpmnElementProcessor<T extends ExecutableFlowElement>` at  
`zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/bpmn/BpmnElementProcessor.java`

The lifecycle methods are:

| Method | Purpose |
|--------|---------|
| `onActivate(T, BpmnElementContext)` | Initialize and activate the element |
| `finalizeActivation(T, BpmnElementContext)` | Called after START execution listeners |
| `onComplete(T, BpmnElementContext)` | Leave the element, take outgoing flows |
| `finalizeCompletion(T, BpmnElementContext)` | Called after END execution listeners |
| `onTerminate(T, BpmnElementContext)` | Clean up on termination |
| `getType()` | Return the executable model class |

**Simplest example — ManualTaskProcessor:**

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/bpmn/task/ManualTaskProcessor.java`

```java
public class ManualTaskProcessor extends UndefinedTaskProcessor {
  public ManualTaskProcessor(
      final BpmnBehaviors bpmnBehaviors,
      final BpmnStateTransitionBehavior stateTransitionBehavior) {
    super(bpmnBehaviors, stateTransitionBehavior);
  }
}
```

**UndefinedTaskProcessor** (the base class, full implementation):

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/bpmn/task/UndefinedTaskProcessor.java`

```java
public class UndefinedTaskProcessor implements BpmnElementProcessor<ExecutableActivity> {

  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnCompensationSubscriptionBehaviour compensationSubscriptionBehaviour;

  public UndefinedTaskProcessor(
      final BpmnBehaviors bpmnBehaviors,
      final BpmnStateTransitionBehavior stateTransitionBehavior) {
    this.stateTransitionBehavior = stateTransitionBehavior;
    incidentBehavior = bpmnBehaviors.incidentBehavior();
    compensationSubscriptionBehaviour = bpmnBehaviors.compensationSubscriptionBehaviour();
  }

  @Override
  public Class<ExecutableActivity> getType() {
    return ExecutableActivity.class;
  }

  @Override
  public Either<Failure, ?> onActivate(
      final ExecutableActivity element, final BpmnElementContext context) {
    final var activated =
        stateTransitionBehavior.transitionToActivated(context, element.getEventType());
    stateTransitionBehavior.completeElement(activated);
    return SUCCESS;
  }

  @Override
  public Either<Failure, ?> onComplete(
      final ExecutableActivity element, final BpmnElementContext context) {
    compensationSubscriptionBehaviour.createCompensationSubscription(element, context);
    return stateTransitionBehavior
        .transitionToCompleted(element, context)
        .thenDo(completed -> {
          compensationSubscriptionBehaviour.completeCompensationHandler(completed);
          stateTransitionBehavior.takeOutgoingSequenceFlows(element, completed);
        });
  }

  @Override
  public void onTerminate(final ExecutableActivity element, final BpmnElementContext context) {
    final var terminated =
        stateTransitionBehavior.transitionToTerminated(context, element.getEventType());
    incidentBehavior.resolveIncidents(context);
    stateTransitionBehavior.onElementTerminated(element, terminated);
  }
}
```

Key behaviors available through `BpmnBehaviors`:
- `stateTransitionBehavior` — transitions between lifecycle states (ACTIVATING → ACTIVATED → COMPLETING → COMPLETED)
- `incidentBehavior` — raise/resolve incidents
- `variableMappingBehavior` — apply input/output variable mappings
- `eventSubscriptionBehavior` — manage message/timer/signal subscriptions
- `compensationSubscriptionBehaviour` — manage compensation handlers

### Step 4: Register the Processor

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/bpmn/BpmnElementProcessors.java`  
**Package:** `io.camunda.zeebe.engine.processing.bpmn`

In the constructor, add a mapping from `BpmnElementType` to your processor:

```java
// In BpmnElementProcessors constructor:
processors.put(
    BpmnElementType.MY_NEW_ELEMENT,
    new MyNewElementProcessor(bpmnBehaviors, stateTransitionBehavior));
```

**Existing registrations for reference:**
```java
// tasks
processors.put(BpmnElementType.SERVICE_TASK,
    new JobWorkerTaskProcessor(bpmnBehaviors, stateTransitionBehavior));
processors.put(BpmnElementType.MANUAL_TASK,
    new ManualTaskProcessor(bpmnBehaviors, stateTransitionBehavior));
// gateways
processors.put(BpmnElementType.EXCLUSIVE_GATEWAY,
    new ExclusiveGatewayProcessor(bpmnBehaviors, stateTransitionBehavior));
// containers
processors.put(BpmnElementType.PROCESS,
    new ProcessProcessor(bpmnBehaviors, stateTransitionBehavior));
// events
processors.put(BpmnElementType.START_EVENT,
    new StartEventProcessor(bpmnBehaviors, stateTransitionBehavior));
```

### Step 5: Update the BPMN Model Parser (if needed)

If the element requires custom properties parsed from the BPMN XML, update the model transformer.

**Directory:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/deployment/model/transformer/`

The transformer maps BPMN model elements to executable elements. The `BpmnElementType.bpmnElementTypeFor(elementTypeName)` method will already resolve the XML tag to the enum value if you set the string correctly in Step 1.

### Step 6: Handle Element in BpmnStreamProcessor

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/bpmn/BpmnStreamProcessor.java`

The `BpmnStreamProcessor` uses `BpmnElementProcessors.getProcessor(bpmnElementType)` to look up the correct processor. As long as you registered in Step 4, this works automatically.

### Summary: Files Changed
| # | File | Action |
|---|------|--------|
| 1 | `zeebe/protocol/.../BpmnElementType.java` | Add enum value |
| 2 | `zeebe/engine/.../bpmn/task/MyNewElementProcessor.java` | **New file** |
| 3 | `zeebe/engine/.../bpmn/BpmnElementProcessors.java` | Register processor |
| 4 | `zeebe/engine/.../deployment/model/element/ExecutableMyElement.java` | **New file** (if custom model needed) |
| 5 | `zeebe/engine/.../deployment/model/transformer/` | Update transformer (if custom parsing needed) |

### Element Processor Directory Structure
```
zeebe/engine/src/main/java/io/camunda/zeebe/engine/processing/bpmn/
├── BpmnElementProcessor.java          ← Interface
├── BpmnElementContainerProcessor.java ← Extended interface for containers
├── BpmnElementProcessors.java         ← Registry
├── BpmnStreamProcessor.java           ← Dispatches to processors
├── container/
│   ├── CallActivityProcessor.java
│   ├── ProcessProcessor.java
│   ├── SubProcessProcessor.java
│   └── ...
├── event/
│   ├── StartEventProcessor.java
│   ├── EndEventProcessor.java
│   └── ...
├── gateway/
│   ├── ExclusiveGatewayProcessor.java
│   ├── ParallelGatewayProcessor.java
│   └── ...
└── task/
    ├── UndefinedTaskProcessor.java     ← Base for simple tasks
    ├── ManualTaskProcessor.java        ← Simplest example
    ├── JobWorkerTaskProcessor.java     ← Task with job worker
    ├── UserTaskProcessor.java
    ├── ScriptTaskProcessor.java
    └── ...
```

---

## Guide 3: How to Add a New Exporter

This guide uses **DebugLogExporter** as a reference — the simplest built-in exporter.

### Step 1: Understand the Exporter Interface

**File:** `zeebe/exporter-api/src/main/java/io/camunda/zeebe/exporter/api/Exporter.java`  
**Package:** `io.camunda.zeebe.exporter.api`

```java
public interface Exporter {
  /** Configure the exporter. Called at startup for validation and before open. */
  default void configure(final Context context) throws Exception {}

  /** Allocate resources. After this, records will be published. */
  default void open(final Controller controller) {}

  /** Tear down and free resources. */
  default void close() {}

  /** Called for every record. Must call controller.updateLastExportedRecordPosition()
   *  once the record is guaranteed to be persisted. */
  void export(Record<?> record);
}
```

### Step 2: Understand the Context and Controller

**Context interface:** `zeebe/exporter-api/src/main/java/io/camunda/zeebe/exporter/api/context/Context.java`
```java
public interface Context {
  MeterRegistry getMeterRegistry();
  Logger getLogger();
  Configuration getConfiguration();      // access exporter config (args from YAML)
  int getPartitionId();
  void setFilter(RecordFilter filter);   // limit which records are exported

  interface RecordFilter {
    boolean acceptType(RecordType recordType);     // COMMAND, EVENT, REJECTION
    boolean acceptValue(ValueType valueType);      // JOB, INCIDENT, etc.
  }
}
```

**Controller interface:** `zeebe/exporter-api/src/main/java/io/camunda/zeebe/exporter/api/context/Controller.java`
```java
public interface Controller {
  /** Signal that all records up to this position are exported. */
  void updateLastExportedRecordPosition(long position);

  /** Same but with metadata (stored and returned on reopen). */
  void updateLastExportedRecordPosition(long position, byte[] metadata);

  long getLastExportedRecordPosition();

  ScheduledTask scheduleCancellableTask(Duration delay, Runnable task);

  Optional<byte[]> readMetadata();
}
```

### Step 3: Implement Your Exporter

**Reference:** `zeebe/broker/src/main/java/io/camunda/zeebe/broker/exporter/debug/DebugLogExporter.java`

```java
package com.example.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import org.slf4j.Logger;

public class MyCustomExporter implements Exporter {

  private Logger logger;
  private Controller controller;
  private MyExporterConfiguration configuration;

  @Override
  public void configure(final Context context) throws Exception {
    logger = context.getLogger();

    // Instantiate typed configuration from the YAML args
    configuration = context.getConfiguration().instantiate(MyExporterConfiguration.class);

    // Optionally filter which records to receive
    context.setFilter(new Context.RecordFilter() {
      @Override
      public boolean acceptType(final RecordType recordType) {
        return recordType == RecordType.EVENT;
      }
      @Override
      public boolean acceptValue(final ValueType valueType) {
        return valueType == ValueType.JOB;
      }
    });
  }

  @Override
  public void open(final Controller controller) {
    this.controller = controller;
    logger.info("MyCustomExporter opened");
    // Restore metadata from previous run if needed:
    // Optional<byte[]> metadata = controller.readMetadata();
  }

  @Override
  public void close() {
    logger.info("MyCustomExporter closed");
  }

  @Override
  public void export(final Record<?> record) {
    // Process the record (send to external system, write to file, etc.)
    logger.debug("Exporting record: {}", record.toJson());

    // IMPORTANT: Signal that this record has been successfully exported
    controller.updateLastExportedRecordPosition(record.getPosition());
  }

  // Plain Java bean — fields map to YAML args keys
  public static class MyExporterConfiguration {
    private String targetUrl = "http://localhost:8080";
    private int batchSize = 100;

    // getters and setters...
    public String getTargetUrl() { return targetUrl; }
    public void setTargetUrl(String targetUrl) { this.targetUrl = targetUrl; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
  }
}
```

### Step 4: Configure the Exporter

**Config location:** `application.yaml` (or environment variables)

For **internal** exporters (classes on the broker classpath):
```yaml
zeebe:
  broker:
    exporters:
      myExporter:
        className: com.example.exporter.MyCustomExporter
        args:
          targetUrl: http://my-service:8080
          batchSize: 500
```

For **external** exporters (packaged as a JAR):
```yaml
zeebe:
  broker:
    exporters:
      myExporter:
        jarPath: /path/to/my-exporter.jar
        className: com.example.exporter.MyCustomExporter
        args:
          targetUrl: http://my-service:8080
```

**Config class:** `zeebe/broker/src/main/java/io/camunda/zeebe/broker/system/configuration/ExporterCfg.java`
```java
public final class ExporterCfg implements ConfigurationEntry {
  private String jarPath;      // optional — for external JARs
  private String className;    // fully qualified class name
  private Map<String, Object> args;  // maps to your configuration bean
}
```

### Step 5: Understand How Exporters Are Loaded

**File:** `zeebe/broker/src/main/java/io/camunda/zeebe/broker/exporter/repo/ExporterDescriptor.java`

```java
public class ExporterDescriptor {
  private final ExporterConfiguration configuration;
  private final Class<? extends Exporter> exporterClass;

  public ExporterDescriptor(
      final String id,
      final Class<? extends Exporter> exporterClass,
      final Map<String, Object> args) {
    this.exporterClass = exporterClass;
    configuration = new ExporterConfiguration(id, args);
  }

  public Exporter newInstance() throws ExporterInstantiationException {
    return ReflectUtil.newInstance(exporterClass);  // requires no-arg constructor
  }
}
```

The broker loads all exporters from configuration, creates `ExporterDescriptor` instances, and instantiates them per partition.

### Step 6: Position Tracking

The `controller.updateLastExportedRecordPosition(position)` call is crucial:
- It tells the broker which records have been successfully exported
- The broker uses this to manage the log compaction — records below the minimum exporter position across all exporters can be compacted
- If an exporter crashes, it will re-receive records from its last acknowledged position
- Call it only after the record is **durably persisted** in your target system

### Key Files Reference
| File | Purpose |
|------|---------|
| `zeebe/exporter-api/src/main/java/.../Exporter.java` | Core interface |
| `zeebe/exporter-api/src/main/java/.../context/Context.java` | Configuration + filtering context |
| `zeebe/exporter-api/src/main/java/.../context/Controller.java` | Position tracking + scheduling |
| `zeebe/broker/src/main/java/.../debug/DebugLogExporter.java` | Simplest reference implementation |
| `zeebe/broker/src/main/java/.../configuration/ExporterCfg.java` | YAML config model |
| `zeebe/broker/src/main/java/.../repo/ExporterDescriptor.java` | Exporter loading/instantiation |
| `zeebe/broker/src/main/java/.../context/ExporterContext.java` | Context implementation |

### Exporter Lifecycle

```
1. Broker starts
2. For each configured exporter:
   a. ExporterDescriptor.newInstance()    → calls no-arg constructor
   b. exporter.configure(context)         → validate config, fail-fast
   c. [discard this instance — was only for validation]
3. For each partition:
   a. ExporterDescriptor.newInstance()    → fresh instance
   b. exporter.configure(context)         → configure with partition-specific context
   c. exporter.open(controller)           → allocate resources
   d. [records flow] → exporter.export(record) repeatedly
   e. exporter.close()                    → on shutdown
```

---

## Guide 4: How to Modify Engine State

This guide uses **Job state** as the reference — a well-structured, non-trivial example demonstrating column families, keys, values, and state operations.

### Step 1: Understand the ZbColumnFamilies Enum

**File:** `zeebe/protocol/src/main/java/io/camunda/zeebe/protocol/ZbColumnFamilies.java`  
**Package:** `io.camunda.zeebe.protocol`

Every piece of state is stored in a named column family (think: a separate table). Each has a unique integer ID:

```java
public enum ZbColumnFamilies implements EnumValue {
  DEFAULT(0),
  KEY(1),
  // ...
  JOBS(16),
  JOB_STATES(17),
  JOB_DEADLINES(18),
  JOB_ACTIVATABLE(76),
  JOB_BACKOFF(42),
  // ...
  // Last entry at time of writing: ~111+
}
```

**To add a new column family:** append a new constant with the next available integer:
```java
  MY_NEW_STATE(112),
  MY_NEW_STATE_INDEX(113),
```

> **Important:** IDs are permanent. Never reuse or reorder IDs — they're stored in the database.

### Step 2: Understand DbKey and DbValue

**File:** `zeebe/zb-db/src/main/java/io/camunda/zeebe/db/DbKey.java`
```java
public interface DbKey extends BufferReader, BufferWriter {}
```

**File:** `zeebe/zb-db/src/main/java/io/camunda/zeebe/db/DbValue.java`
```java
public interface DbValue extends BufferWriter, BufferReader {}
```

Built-in key/value types in `zeebe/zb-db/src/main/java/io/camunda/zeebe/db/impl/`:

| Class | Purpose |
|-------|---------|
| `DbLong` | 64-bit long key/value |
| `DbString` | String key/value |
| `DbInt` | 32-bit int key/value |
| `DbByte` | Single byte |
| `DbBytes` | Byte array |
| `DbNil` | Empty value (for set-like column families) |
| `DbCompositeKey<A, B>` | Composite key of two sub-keys |
| `DbForeignKey<T>` | Key that references another column family (for integrity) |
| `DbTenantAwareKey<T>` | Key with tenant ID prefix/suffix |

### Step 3: Study the DbJobState Pattern

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/state/instance/DbJobState.java`  
**Package:** `io.camunda.zeebe.engine.state.instance`

This is the concrete implementation. Key patterns:

**Constructor — define keys, values, and column families:**
```java
public DbJobState(
    final ZeebeDb<ZbColumnFamilies> zeebeDb,
    final TransactionContext transactionContext) {

  // Primary key → value column family
  jobKey = new DbLong();
  fkJob = new DbForeignKey<>(jobKey, ZbColumnFamilies.JOBS);
  jobsColumnFamily = zeebeDb.createColumnFamily(
      ZbColumnFamilies.JOBS, transactionContext, jobKey, jobRecordToRead);

  // Index: key → state
  statesJobColumnFamily = zeebeDb.createColumnFamily(
      ZbColumnFamilies.JOB_STATES, transactionContext, fkJob, jobState);

  // Index: [type, key, tenant] → nil (for activatable job lookups)
  jobTypeKey = new DbString();
  tenantIdKey = new DbString();
  typeJobKey = new DbCompositeKey<>(jobTypeKey, fkJob);
  tenantAwareTypeJobKey = new DbTenantAwareKey<>(tenantIdKey, typeJobKey, PlacementType.SUFFIX);
  activatableColumnFamily = zeebeDb.createColumnFamily(
      ZbColumnFamilies.JOB_ACTIVATABLE, transactionContext, tenantAwareTypeJobKey, DbNil.INSTANCE);

  // Index: [deadline, key] → nil (for timeout checks)
  deadlineKey = new DbLong();
  deadlineJobKey = new DbCompositeKey<>(deadlineKey, fkJob);
  deadlinesColumnFamily = zeebeDb.createColumnFamily(
      ZbColumnFamilies.JOB_DEADLINES, transactionContext, deadlineJobKey, DbNil.INSTANCE);
}
```

**State modification operations:**
```java
@Override
public void create(final long key, final JobRecord record) {
  final DirectBuffer type = record.getTypeBuffer();
  createJob(key, record, type);
}

@Override
public void activate(final long key, final JobRecord record) {
  updateJobRecord(key, record);
  updateJobState(State.ACTIVATED);
  makeJobNotActivatable(type, tenantId);
  addJobDeadline(key, deadline);
}

@Override
public void complete(final long key, final JobRecord record) {
  delete(key, record);  // removes from all column families
}
```

### Step 4: Create a New State Class

Follow this pattern:

**4a. Define the immutable interface:**

**File (new):** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/state/immutable/MyNewState.java`
```java
package io.camunda.zeebe.engine.state.immutable;

public interface MyNewState {
  MyNewRecord get(long key);
  boolean exists(long key);
}
```

**4b. Define the mutable interface:**

**File (new):** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/state/mutable/MutableMyNewState.java`
```java
package io.camunda.zeebe.engine.state.mutable;

import io.camunda.zeebe.engine.state.immutable.MyNewState;

public interface MutableMyNewState extends MyNewState {
  void create(long key, MyNewRecord record);
  void update(long key, MyNewRecord record);
  void delete(long key);
}
```

**4c. Implement the Db-backed state:**

**File (new):** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/state/instance/DbMyNewState.java`
```java
package io.camunda.zeebe.engine.state.instance;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.engine.state.mutable.MutableMyNewState;
import io.camunda.zeebe.protocol.ZbColumnFamilies;

public final class DbMyNewState implements MutableMyNewState {

  private final DbLong myKey;
  private final MyNewRecordValue myValue;
  private final ColumnFamily<DbLong, MyNewRecordValue> myColumnFamily;

  public DbMyNewState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb,
      final TransactionContext transactionContext) {
    myKey = new DbLong();
    myValue = new MyNewRecordValue();
    myColumnFamily = zeebeDb.createColumnFamily(
        ZbColumnFamilies.MY_NEW_STATE, transactionContext, myKey, myValue);
  }

  @Override
  public void create(final long key, final MyNewRecord record) {
    myKey.wrapLong(key);
    myValue.setFrom(record);
    myColumnFamily.insert(myKey, myValue);
  }

  @Override
  public MyNewRecord get(final long key) {
    myKey.wrapLong(key);
    final var value = myColumnFamily.get(myKey);
    return value != null ? value.toRecord() : null;
  }

  @Override
  public boolean exists(final long key) {
    myKey.wrapLong(key);
    return myColumnFamily.get(myKey) != null;
  }

  @Override
  public void update(final long key, final MyNewRecord record) {
    myKey.wrapLong(key);
    myValue.setFrom(record);
    myColumnFamily.update(myKey, myValue);
  }

  @Override
  public void delete(final long key) {
    myKey.wrapLong(key);
    myColumnFamily.deleteExisting(myKey);
  }
}
```

### Step 5: Register in ProcessingState

**File:** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/state/mutable/MutableProcessingState.java`

Add accessor:
```java
@Override
MutableMyNewState getMyNewState();
```

Then update the concrete implementation to instantiate `DbMyNewState` in its constructor.

### Step 6: Use State in Processors and Event Appliers

**In a processor (command handler):**
```java
public class MyProcessor implements TypedRecordProcessor<MyRecord> {
  private final MutableMyNewState myNewState;

  public MyProcessor(final MutableProcessingState state) {
    myNewState = state.getMyNewState();
  }

  @Override
  public void processRecord(final TypedRecord<MyRecord> command) {
    // Read state
    if (myNewState.exists(command.getKey())) { /* reject */ }
    // Write event (state writer handles applying via EventApplier)
    stateWriter.appendFollowUpEvent(key, intent, record);
  }
}
```

**In an event applier (applies state changes on replay):**

**File (new):** `zeebe/engine/src/main/java/io/camunda/zeebe/engine/state/appliers/MyNewCreatedApplier.java`
```java
public class MyNewCreatedApplier implements TypedEventApplier<MyNewIntent, MyNewRecordValue> {
  private final MutableMyNewState state;

  public MyNewCreatedApplier(final MutableMyNewState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final MyNewRecordValue value) {
    state.create(key, value);
  }
}
```

Then register in `EventAppliers.registerEventAppliers()`:
```java
register(MyNewIntent.CREATED, new MyNewCreatedApplier(state.getMyNewState()));
```

### ColumnFamily API Quick Reference

**File:** `zeebe/zb-db/src/main/java/io/camunda/zeebe/db/ColumnFamily.java`

```java
public interface ColumnFamily<KeyType extends DbKey, ValueType extends DbValue> {
  void insert(KeyType key, ValueType value);    // throws if key exists
  void update(KeyType key, ValueType value);    // throws if key doesn't exist
  void upsert(KeyType key, ValueType value);    // insert or update
  ValueType get(KeyType key);                   // returns null if not found
  void forEach(BiConsumer<KeyType, ValueType> consumer);
  void whileTrue(KeyValuePairVisitor<KeyType, ValueType> visitor);  // iterate until false
  void deleteExisting(KeyType key);             // throws if key doesn't exist
  void deleteIfExists(KeyType key);
  boolean isEmpty();
  long count();
}
```

### Composite Key Example (Secondary Index Pattern)

Common pattern for secondary indexes using `DbCompositeKey`:

```java
// Index: [type, entityKey] → nil
// Allows looking up all entities of a given type
DbString typeKey = new DbString();
DbLong entityKey = new DbLong();
DbCompositeKey<DbString, DbLong> compositeKey = new DbCompositeKey<>(typeKey, entityKey);
ColumnFamily<DbCompositeKey<DbString, DbLong>, DbNil> indexFamily =
    zeebeDb.createColumnFamily(ZbColumnFamilies.MY_INDEX, ctx, compositeKey, DbNil.INSTANCE);

// Add to index
typeKey.wrapString("service-task");
entityKey.wrapLong(42);
indexFamily.insert(compositeKey, DbNil.INSTANCE);

// Iterate all entries of type "service-task"
typeKey.wrapString("service-task");
indexFamily.whileEqualPrefix(typeKey, (key, nil) -> {
    long foundKey = key.second().getValue();
    // process...
    return true; // continue iterating
});
```

### Files Changed Checklist
| # | File | Action |
|---|------|--------|
| 1 | `zeebe/protocol/.../ZbColumnFamilies.java` | Add new column family enum(s) |
| 2 | `zeebe/engine/.../state/immutable/MyNewState.java` | **New file** — read interface |
| 3 | `zeebe/engine/.../state/mutable/MutableMyNewState.java` | **New file** — write interface |
| 4 | `zeebe/engine/.../state/instance/DbMyNewState.java` | **New file** — implementation |
| 5 | `zeebe/engine/.../state/mutable/MutableProcessingState.java` | Add accessor |
| 6 | `zeebe/engine/.../state/immutable/ProcessingState.java` | Add accessor |
| 7 | Concrete ProcessingState implementation | Instantiate DbMyNewState |
| 8 | `zeebe/engine/.../state/appliers/EventAppliers.java` | Register applier |
| 9 | `zeebe/engine/.../state/appliers/MyNewCreatedApplier.java` | **New file** |

### State Architecture Summary

```
ColumnFamily (RocksDB)
  └── ZbColumnFamilies enum → unique ID per "table"
       ├── Key: DbKey (DbLong, DbString, DbCompositeKey, ...)
       └── Value: DbValue (custom record values, DbNil for indexes)

State interfaces:
  ProcessingState (immutable reads)
    └── MutableProcessingState (reads + writes)
         ├── MutableJobState → DbJobState
         ├── MutableIncidentState → DbIncidentState
         ├── MutableMyNewState → DbMyNewState
         └── ...

Data flow:
  Processor writes event → StateWriter → EventApplier → TypedEventApplier → MutableState → ColumnFamily
  Processor reads state ← ProcessingState ← DbXxxState ← ColumnFamily
```
