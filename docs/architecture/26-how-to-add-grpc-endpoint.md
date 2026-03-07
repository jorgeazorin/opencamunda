# 26 - How To: Añadir un Endpoint gRPC

## Resumen

Guía paso a paso para añadir una nueva operación gRPC al gateway de Zeebe. Usamos **BroadcastSignal** como ejemplo real de referencia.

## Archivos a Modificar/Crear

| #  |                                Archivo                                |            Módulo             |
|----|-----------------------------------------------------------------------|-------------------------------|
| 1  | `gateway-protocol/src/main/proto/gateway.proto`                       | gateway-protocol              |
| 2  | `gateway/src/main/java/.../impl/broker/request/BrokerXxxRequest.java` | gateway                       |
| 3  | `gateway/src/main/java/.../RequestMapper.java`                        | gateway                       |
| 4  | `gateway/src/main/java/.../ResponseMapper.java`                       | gateway                       |
| 5  | `gateway/src/main/java/.../EndpointManager.java`                      | gateway                       |
| 6  | `gateway/src/main/java/.../GatewayGrpcService.java`                   | gateway                       |
| 7  | `protocol/src/main/java/.../record/ValueType.java`                    | protocol (si nuevo ValueType) |
| 8  | `protocol/src/main/java/.../record/intent/XxxIntent.java`             | protocol (si nuevo Intent)    |
| 9  | `protocol-impl/src/main/java/.../record/value/XxxRecord.java`         | protocol-impl                 |
| 10 | `stream-platform/src/main/java/.../TypedEventRegistry.java`           | stream-platform               |
| 11 | `engine/src/main/java/.../processing/EngineProcessors.java`           | engine                        |
| 12 | `engine/src/main/java/.../processing/xxx/XxxProcessor.java`           | engine                        |

## Paso 1: Definir el Proto

En `gateway-protocol/src/main/proto/gateway.proto`:

```protobuf
// 1a. Definir mensajes de Request y Response
message MyNewOperationRequest {
  int64 someKey = 1;
  string someValue = 2;
  string tenantId = 3;
}

message MyNewOperationResponse {
  int64 resultKey = 1;
}

// 1b. Añadir el RPC al servicio Gateway
service Gateway {
  // ... RPCs existentes ...
  
  rpc MyNewOperation (MyNewOperationRequest) returns (MyNewOperationResponse) {
  }
}
```

**Compilar proto**: `mvn generate-sources -pl zeebe/gateway-protocol`

## Paso 2: Definir Protocol (si es nuevo ValueType)

### 2a. ValueType (si necesario)

En `protocol/src/main/java/.../record/ValueType.java`, añadir:

```java
MY_NEW_VALUE((short) XX, true, true)  // (id, isEvent, isCommand)
```

### 2b. Intent

Crear `protocol/src/main/java/.../record/intent/MyNewIntent.java`:

```java
public enum MyNewIntent implements Intent {
  EXECUTE((short) 0),   // comando
  EXECUTED((short) 1);  // evento resultado

  private final short value;
  // ... boilerplate
}
```

### 2c. Record Value (protocol-impl)

Crear `protocol-impl/src/main/java/.../record/value/MyNewRecord.java`:

```java
public class MyNewRecord extends UnifiedRecordValue implements MyNewRecordValue {
  // Propiedades como campos SBE
  private final StringProperty someProp = new StringProperty("someValue");
  
  // Constructor, getters, wrap/write
}
```

### 2d. Registrar en TypedEventRegistry

En `stream-platform/src/main/java/.../TypedEventRegistry.java`:

```java
registry.put(ValueType.MY_NEW_VALUE, MyNewRecord.class);
```

## Paso 3: Crear BrokerRequest

Crear `gateway/src/main/java/.../impl/broker/request/BrokerMyNewRequest.java`:

```java
public final class BrokerMyNewRequest extends BrokerExecuteCommand<MyNewRecord> {

  private final MyNewRecord requestDto = new MyNewRecord();

  public BrokerMyNewRequest(final long someKey) {
    super(ValueType.MY_NEW_VALUE, MyNewIntent.EXECUTE);
    request.setRequestWriter(requestDto);
    requestDto.setSomeKey(someKey);
  }

  // Setters para campos adicionales
  public BrokerMyNewRequest setSomeValue(final DirectBuffer value) {
    requestDto.setSomeValue(value);
    return this;
  }

  public BrokerMyNewRequest setTenantId(final String tenantId) {
    requestDto.setTenantId(tenantId);
    return this;
  }

  @Override
  public MyNewRecord getRequestWriter() {
    return requestDto;
  }

  @Override
  protected MyNewRecord toResponseDto(final DirectBuffer buffer) {
    final MyNewRecord responseDto = new MyNewRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }
}
```

**Ejemplo real** (`BrokerBroadcastSignalRequest`):

```java
public final class BrokerBroadcastSignalRequest extends BrokerExecuteCommand<SignalRecord> {
  private final SignalRecord requestDto = new SignalRecord();

  public BrokerBroadcastSignalRequest(final String signalName) {
    super(ValueType.SIGNAL, SignalIntent.BROADCAST);
    requestDto.setSignalName(signalName);
  }
}
```

## Paso 4: RequestMapper

En `gateway/src/main/java/.../RequestMapper.java`, añadir método estático:

```java
public static BrokerMyNewRequest toMyNewRequest(
    final MyNewOperationRequest grpcRequest) {
  return new BrokerMyNewRequest(grpcRequest.getSomeKey())
      .setSomeValue(ensureJsonSet(grpcRequest.getSomeValue()))
      .setTenantId(ensureTenantIdSet("MyNewOperation", grpcRequest.getTenantId()));
}
```

**Ejemplo real**:

```java
public static BrokerBroadcastSignalRequest toBroadcastSignalRequest(
    final BroadcastSignalRequest grpcRequest) {
  return new BrokerBroadcastSignalRequest(grpcRequest.getSignalName())
      .setVariables(ensureJsonSet(grpcRequest.getVariables()))
      .setTenantId(ensureTenantIdSet("BroadcastSignal", grpcRequest.getTenantId()));
}
```

## Paso 5: ResponseMapper

En `gateway/src/main/java/.../ResponseMapper.java`, añadir:

```java
public static MyNewOperationResponse toMyNewResponse(
    final long key, final MyNewRecord response) {
  return MyNewOperationResponse.newBuilder()
      .setResultKey(key)
      .build();
}
```

## Paso 6: EndpointManager

En `gateway/src/main/java/.../EndpointManager.java`, añadir:

```java
public void myNewOperation(
    final MyNewOperationRequest request,
    final ServerStreamObserver<MyNewOperationResponse> responseObserver) {
  sendRequest(
      request,
      RequestMapper::toMyNewRequest,
      ResponseMapper::toMyNewResponse,
      responseObserver);
}
```

**Patrón**: `sendRequest` (partición fija) vs `sendRequestWithRetryPartitions` (multipartición).

## Paso 7: GatewayGrpcService

En `gateway/src/main/java/.../GatewayGrpcService.java`, añadir override:

```java
@Override
public void myNewOperation(
    final MyNewOperationRequest request,
    final StreamObserver<MyNewOperationResponse> responseObserver) {
  endpointManager.myNewOperation(
      request, ErrorMappingStreamObserver.ofStreamObserver(responseObserver));
}
```

## Paso 8: Engine Processor

Crear `engine/src/main/java/.../processing/mynew/MyNewProcessor.java`:

```java
public final class MyNewProcessor implements TypedRecordProcessor<MyNewRecord> {

  @Override
  public void processRecord(
      final TypedRecord<MyNewRecord> command,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final ProcessingState state) {
    
    final var record = command.getValue();
    // Lógica de negocio aquí
    
    // Escribir evento resultado
    streamWriter.appendFollowUpEvent(
        command.getKey(), 
        MyNewIntent.EXECUTED, 
        record);
    
    // Enviar respuesta al gateway
    responseWriter.writeEventOnCommand(
        command.getKey(), 
        MyNewIntent.EXECUTED, 
        record, 
        command);
  }
}
```

## Paso 9: Registrar Processor en EngineProcessors

En `engine/src/main/java/.../processing/EngineProcessors.java`:

```java
// Añadir el import
import io.camunda.zeebe.engine.processing.mynew.MyNewProcessor;

// En el método init, registrar:
final var myNewProcessor = new MyNewProcessor(/* dependencias */);
typedRecordProcessors.onCommand(
    ValueType.MY_NEW_VALUE, MyNewIntent.EXECUTE, myNewProcessor);
```

**Ejemplo real** (Signal):

```java
final var signalBroadcastProcessor =
    new SignalBroadcastProcessor(
        signalSubscriptionState, stateWriter, keyGenerator, distributionBehavior, writers);
typedRecordProcessors.onCommand(
    ValueType.SIGNAL, SignalIntent.BROADCAST, signalBroadcastProcessor);
```

## Paso 10: Registros Adicionales

### CommandApiRequestReader

En `broker/src/main/java/.../commandapi/CommandApiRequestReader.java`:

```java
RECORDS_BY_TYPE.put(ValueType.MY_NEW_VALUE, MyNewRecord::new);
```

## Resumen del Flujo Completo

```
Cliente gRPC
  │ MyNewOperationRequest
  ▼
GatewayGrpcService.myNewOperation()
  │
  ▼
EndpointManager.myNewOperation()
  │ sendRequest()
  ▼
RequestMapper.toMyNewRequest()
  │ → BrokerMyNewRequest(ValueType.MY_NEW_VALUE, MyNewIntent.EXECUTE)
  ▼
BrokerClient → Transport → Broker
  │
  ▼
CommandApiRequestReader (deserializa)
  │
  ▼
EngineProcessors → MyNewProcessor.processRecord()
  │ - Lógica de negocio
  │ - streamWriter.appendFollowUpEvent(EXECUTED)
  │ - responseWriter.writeEventOnCommand(EXECUTED)
  ▼
Respuesta vía Transport → Gateway
  │
  ▼
ResponseMapper.toMyNewResponse()
  │
  ▼
Cliente gRPC ← MyNewOperationResponse
```

## Checklist

- [ ] Proto message Request/Response definidos
- [ ] RPC añadido al service Gateway
- [ ] ValueType creado (si necesario)
- [ ] Intent enum creado
- [ ] Record value implementado (protocol-impl)
- [ ] TypedEventRegistry actualizado
- [ ] BrokerXxxRequest creado
- [ ] RequestMapper método añadido
- [ ] ResponseMapper método añadido
- [ ] EndpointManager método añadido
- [ ] GatewayGrpcService override añadido
- [ ] Engine Processor implementado
- [ ] EngineProcessors registro añadido
- [ ] CommandApiRequestReader actualizado
- [ ] Tests escritos (gateway test + engine test)

