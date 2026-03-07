# 22 - Transport y Networking

## Propósito

El módulo `transport` gestiona la **comunicación entre nodos** del cluster Zeebe (broker-to-broker y gateway-to-broker).

## Estructura

```
zeebe/transport/src/main/java/io/camunda/zeebe/transport/
├── RequestType.java              ← Tipos de request
├── ServerTransport.java          ← Interface servidor
├── ClientRequest.java            ← Interface request cliente
├── ServerResponse.java           ← Interface response
├── RequestHandler.java           ← Handler de requests
├── ServerOutput.java             ← Envío de respuestas
├── impl/
│   └── AtomixServerTransport.java ← Implementación sobre Atomix
└── stream/
    └── api/
        ├── ClientStreamService.java    ← Streams bidireccionales
        ├── RemoteStreamService.java    ← Streams de servidor
        ├── ClientStream.java           ← Stream del cliente
        └── RemoteStream.java           ← Stream remoto
```

## Tipos de Request

```java
public enum RequestType {
    COMMAND,    // Comandos del engine (write)
    QUERY,      // Consultas (read)
    ADMIN,      // Operaciones administrativas
    BACKUP,     // Operaciones de backup
    UNKNOWN     // Desconocido
}
```

## Interfaces Core

### ServerTransport

```java
public interface ServerTransport extends AutoCloseable {
    // Registrar handler para una partición y tipo
    ActorFuture<Void> subscribe(
        int partitionId, 
        RequestType type, 
        RequestHandler handler
    );
    
    // Deregistrar handler
    ActorFuture<Void> unsubscribe(int partitionId, RequestType type);
}
```

### ClientRequest

```java
public interface ClientRequest {
    int getPartitionId();        // Partición destino
    RequestType getRequestType(); // Tipo de request
}
```

### RequestHandler

```java
@FunctionalInterface
public interface RequestHandler {
    void onRequest(
        ServerOutput serverOutput,   // Para enviar respuesta
        int partitionId,
        long requestId,              // ID único snowflake
        DirectBuffer buffer,         // Datos de la request
        int offset,
        int length
    );
}
```

## Implementación: AtomixServerTransport

Usa el **MessagingService de Atomix** para comunicación entre nodos:

### Topics

El topic se construye con el patrón: `{tipo}-api-{partitionId}`

```
command-api-0     ← Comandos para partición 0
command-api-1     ← Comandos para partición 1
query-api-0       ← Queries para partición 0
backup-api-1      ← Backup para partición 1
admin-api-0       ← Admin para partición 0
```

### Flujo de Request/Response

```
1. Client/Gateway envía ClientRequest
   ├─ partitionId: 1
   ├─ requestType: COMMAND
   └─ data: bytes serializados
         ↓
2. Atomix MessagingService rutea por topic "command-api-1"
         ↓
3. AtomixServerTransport recibe
   ├─ Genera requestId único (Snowflake ID)
   ├─ Crea CompletableFuture para tracking
   └─ Llama RequestHandler.onRequest()
         ↓
4. Handler procesa la request
   └─ Llama ServerOutput.sendResponse(response bytes)
         ↓
5. CompletableFuture completa
   └─ Response enviada de vuelta al caller
```

## Streaming API

Para comunicación continua (ej: job streaming):

### ClientStreamService

```java
public interface ClientStreamService<M> {
    // Abrir stream con metadatos
    ClientStream<M> openStream(
        String streamType,
        M metadata,
        Consumer<byte[]> payloadConsumer
    );
}
```

### RemoteStreamService

```java
public interface RemoteStreamService<M, P> {
    // Iterar sobre streams activos
    void forEach(Consumer<RemoteStream<M, P>> consumer);
    
    // Callbacks de lifecycle
    void onStreamJoined(Consumer<RemoteStream<M, P>> callback);
    void onStreamRemoved(Consumer<RemoteStream<M, P>> callback);
}
```

### Streams: ClientStream y RemoteStream

```java
// Stream del lado del cliente
public interface ClientStream<M> {
    M getMetadata();
    void close();
}

// Stream del lado del servidor (broker)
public interface RemoteStream<M, P> {
    M getMetadata();
    void push(P payload);           // Enviar dato al cliente
    boolean isConnected();
}
```

### Errores de Streaming

| Error | Cuándo |
|-------|--------|
| `StreamExhaustedException` | Stream sin capacidad |
| `NoSuchStreamException` | Stream no existe |
| `ClientStreamBlockedException` | Stream bloqueado |

## Uso en Zeebe

### Gateway → Broker (Comandos)

```
Gateway recibe gRPC request del cliente
  ↓
Gateway usa ClientRequest para enviar a broker:
  ├─ partitionId = determinado por routing
  ├─ requestType = COMMAND
  └─ data = comando serializado (SBE)
  ↓
Broker recibe vía AtomixServerTransport
  ↓
RequestHandler procesa comando
  ↓
Respuesta de vuelta al Gateway
  ↓
Gateway responde al cliente por gRPC
```

### Broker → Broker (Replicación Raft)

```
La replicación Raft usa Atomix directamente (no el módulo transport):
  ├─ AppendRequest/Response
  ├─ VoteRequest/Response
  ├─ InstallRequest (snapshots)
  └─ ConfigureRequest (membership)
```

### Job Streaming (Gateway ↔ Broker)

```
1. Gateway abre ClientStream al broker:
   ├─ streamType: "job-activation"
   ├─ metadata: {jobType: "payment", worker: "w1"}
   └─ payloadConsumer: recibe jobs activados

2. Broker mantiene RemoteStream:
   ├─ Cuando hay jobs new: push(activatedJob)
   └─ Gateway recibe y envía al cliente

3. Si stream se cierra:
   └─ Fallback a long-polling
```

## Protocolo de Red

```
Atomix Messaging Layer (Netty-based)
  ├─ TCP connections entre nodos
  ├─ Serialización: bytes crudos (cada capa serializa su propio formato)
  ├─ Topic-based routing
  ├─ Request/Response pattern con IDs snowflake
  └─ Streaming pattern con server-push
```

## Diagrama de Red

```
┌──────────────────┐
│    Gateway        │
│                   │─── gRPC (26500) ──→ Clients
│                   │
│ AtomixClientTransport │
└────────┬─────────┘
         │  TCP (topics: command-api-*, query-api-*)
         │
    ┌────┼────────────────────┐
    │    │                    │
┌───▼────▼──┐  ┌──────────┐  ┌──────────┐
│  Broker 0  │  │ Broker 1 │  │ Broker 2 │
│            │──│          │──│          │
│ AtomixSrv  │  │ AtomixSrv│  │ AtomixSrv│
│ Transport  │  │ Transport│  │ Transport│
│            │  │          │  │          │
│ Raft (26502)│  │          │  │          │
└────────────┘  └──────────┘  └──────────┘
    ↕ Atomix Raft (inter-broker)  ↕
    TCP: AppendReq, VoteReq, InstallReq

Puertos:
  26500: gRPC (gateway → clients)
  26501: Command API (gateway → broker)
  26502: Internal API (broker ↔ broker, Raft)
```
