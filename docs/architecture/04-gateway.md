# 04 - Gateway (API gRPC y REST)

## Qué es el Gateway

El Gateway es el **punto de entrada** para todos los clientes de Zeebe. Expone:
- **API gRPC** (puerto 26500) — API principal, alta performance
- **API REST** (puerto 8080) — API web, más accesible

El gateway es **stateless**: no almacena datos, solo rutea peticiones al broker correcto.

## Ubicación del Código

```
zeebe/gateway-protocol/              ← Definiciones .proto y rest-api.yaml
zeebe/gateway-protocol-impl/         ← Código generado (gRPC stubs, protobuf)
zeebe/gateway/                       ← Gateway gRPC (core)
zeebe/gateway-rest/                  ← Gateway REST (Spring controllers)
```

## Arquitectura del Gateway

```
                Clientes
                   │
      ┌────────────┼────────────┐
      │ gRPC :26500│REST :8080  │
      │            │            │
┌─────▼────┐  ┌────▼──────┐    │
│Interceptors│  │Spring    │    │
│(auth,     │  │Controllers│    │
│metrics)   │  │           │    │
└─────┬─────┘  └────┬──────┘    │
      │              │           │
┌─────▼──────────────▼──────┐   │
│     EndpointManager       │   │
│  (dispatch central)       │   │
└────────────┬──────────────┘   │
             │                   │
┌────────────▼──────────────┐   │
│     RequestMapper         │   │
│  Proto/REST → BrokerReq   │   │
└────────────┬──────────────┘   │
             │                   │
┌────────────▼──────────────┐   │
│      BrokerClient         │   │
│  (envía al broker)        │   │
└────────────┬──────────────┘   │
             │                   │
      Broker (partición correcta)
```

## API gRPC Completa

Definida en `zeebe/gateway-protocol/src/main/proto/gateway.proto`:

### Operaciones de Procesos

|                RPC                |                 Request                  |                 Response                  |        Descripción        |
|-----------------------------------|------------------------------------------|-------------------------------------------|---------------------------|
| `DeployResource`                  | `DeployResourceRequest`                  | `DeployResourceResponse`                  | Deploy BPMN/DMN/Form      |
| `CreateProcessInstance`           | `CreateProcessInstanceRequest`           | `CreateProcessInstanceResponse`           | Crear instancia           |
| `CreateProcessInstanceWithResult` | `CreateProcessInstanceWithResultRequest` | `CreateProcessInstanceWithResultResponse` | Crear y esperar resultado |
| `CancelProcessInstance`           | `CancelProcessInstanceRequest`           | `CancelProcessInstanceResponse`           | Cancelar instancia        |
| `ModifyProcessInstance`           | `ModifyProcessInstanceRequest`           | `ModifyProcessInstanceResponse`           | Modificar en vuelo        |
| `MigrateProcessInstance`          | `MigrateProcessInstanceRequest`          | `MigrateProcessInstanceResponse`          | Migrar a nueva versión    |

### Operaciones de Jobs

|          RPC          |           Request            |           Response            |           Descripción           |
|-----------------------|------------------------------|-------------------------------|---------------------------------|
| `ActivateJobs`        | `ActivateJobsRequest`        | stream `ActivateJobsResponse` | Activar jobs (server streaming) |
| `StreamActivatedJobs` | `StreamActivatedJobsRequest` | stream `ActivatedJob`         | Stream bidireccional            |
| `CompleteJob`         | `CompleteJobRequest`         | `CompleteJobResponse`         | Completar job                   |
| `FailJob`             | `FailJobRequest`             | `FailJobResponse`             | Fallar job                      |
| `ThrowError`          | `ThrowErrorRequest`          | `ThrowErrorResponse`          | Lanzar error BPMN               |
| `UpdateJobRetries`    | `UpdateJobRetriesRequest`    | `UpdateJobRetriesResponse`    | Actualizar retries              |
| `UpdateJobTimeout`    | `UpdateJobTimeoutRequest`    | `UpdateJobTimeoutResponse`    | Actualizar timeout              |

### Otras Operaciones

|        RPC         |          Request          |          Response          |      Descripción       |
|--------------------|---------------------------|----------------------------|------------------------|
| `SetVariables`     | `SetVariablesRequest`     | `SetVariablesResponse`     | Set variables en scope |
| `ResolveIncident`  | `ResolveIncidentRequest`  | `ResolveIncidentResponse`  | Resolver incidente     |
| `PublishMessage`   | `PublishMessageRequest`   | `PublishMessageResponse`   | Publicar mensaje       |
| `BroadcastSignal`  | `BroadcastSignalRequest`  | `BroadcastSignalResponse`  | Broadcast señal        |
| `EvaluateDecision` | `EvaluateDecisionRequest` | `EvaluateDecisionResponse` | Evaluar DMN            |
| `DeleteResource`   | `DeleteResourceRequest`   | `DeleteResourceResponse`   | Borrar recurso         |
| `Topology`         | `TopologyRequest`         | `TopologyResponse`         | Info del cluster       |

## Mensajes Protobuf Clave

### ActivateJobsRequest

```protobuf
message ActivateJobsRequest {
    string type = 1;                    // Tipo de job (ej: "payment")
    string worker = 2;                  // ID del worker
    int64 timeout = 3;                  // Timeout en ms
    int32 maxJobsToActivate = 4;        // Máximo de jobs
    repeated string fetchVariable = 5;  // Variables a incluir
    int64 requestTimeout = 6;           // Long-polling timeout
    repeated string tenantIds = 7;      // Tenant IDs
}
```

### ActivatedJob

```protobuf
message ActivatedJob {
    int64 key = 1;                     // Job key
    string type = 2;                   // Tipo
    int64 processInstanceKey = 3;      // Instancia de proceso
    string bpmnProcessId = 4;          // ID del proceso BPMN
    int32 processDefinitionVersion = 5;// Versión
    int64 processDefinitionKey = 6;    // Key del proceso
    string elementId = 7;             // ID del elemento BPMN
    int64 elementInstanceKey = 8;     // Key de la instancia del elemento
    string customHeaders = 9;         // Headers custom (JSON)
    string worker = 10;               // Worker asignado
    int32 retries = 11;              // Retries restantes
    int64 deadline = 12;             // Deadline en epoch ms
    string variables = 13;           // Variables (JSON)
    string tenantId = 14;           // Tenant
}
```

### CreateProcessInstanceRequest

```protobuf
message CreateProcessInstanceRequest {
    int64 processDefinitionKey = 1;    // Key del proceso (o usar bpmnProcessId)
    string bpmnProcessId = 2;         // ID BPMN (alternativa a key)
    int32 version = 3;                // Versión (-1 = última)
    string variables = 4;             // Variables iniciales (JSON)
    repeated ProcessInstanceCreationStartInstruction startInstructions = 5;
    string tenantId = 6;
}
```

## API REST

Definida en `zeebe/gateway-protocol/src/main/proto/rest-api.yaml` (OpenAPI 3.0.3):

**Base URL**: `{schema}://{host}:{port}/v1`

### Endpoints REST

|  Método  |                   Path                    |     Descripción      |
|----------|-------------------------------------------|----------------------|
| `GET`    | `/v1/topology`                            | Info del cluster     |
| `PATCH`  | `/v1/user-tasks/{userTaskKey}/completion` | Completar user task  |
| `PATCH`  | `/v1/user-tasks/{userTaskKey}/assignment` | Asignar user task    |
| `PATCH`  | `/v1/user-tasks/{userTaskKey}`            | Actualizar user task |
| `DELETE` | `/v1/user-tasks/{userTaskKey}/assignee`   | Desasignar user task |

### Controllers REST

```
TopologyController      → GET /topology
UserTaskController      → Operaciones de user tasks
ZeebeRestController     → Operaciones generales
```

**Nota**: La API REST es más limitada que la gRPC. Muchas operaciones solo están en gRPC.

## Flujo de una Petición

### gRPC

```
1. Cliente envía ActivateJobsRequest via gRPC
2. Interceptors procesan: auth, metrics, tenant
3. GatewayGrpcService.activateJobs() recibe la llamada
4. Delega a EndpointManager
5. EndpointManager usa ActivateJobsHandler:
   ├─ LongPollingActivateJobsHandler (default): espera si no hay jobs
   └─ RoundRobinActivateJobsHandler: devuelve inmediato
6. RequestMapper convierte proto → BrokerRequest
7. BrokerClient envía a la partición correcta
8. Broker procesa y devuelve respuesta
9. ResponseMapper convierte BrokerResponse → proto
10. Respuesta enviada al cliente via StreamObserver
```

### REST

```
1. Cliente envía HTTP request
2. Spring Controller recibe
3. RequestMapper (REST) convierte body → BrokerRequest
4. BrokerClient envía al broker
5. RestErrorMapper maneja errores
6. Respuesta JSON al cliente
```

## Interceptors

### IdentityInterceptor (Autenticación)

```java
// 1. Extrae token Bearer del header Authorization
// 2. Valida token con Identity service
// 3. Si multi-tenancy: obtiene tenants autorizados
// 4. Si falla: UNAUTHENTICATED
```

### MetricCollectingServerInterceptor

Registra métricas de cada llamada gRPC (latencia, errores, throughput).

### ContextInjectingInterceptor

Inyecta contexto adicional en las llamadas (tenant, auth info).

## Job Activation Strategies

### Long Polling (Default)

```
1. Worker pide N jobs con requestTimeout=30s
2. Si hay jobs disponibles → devolver inmediato
3. Si NO hay jobs → mantener request abierto
4. Cuando llegan nuevos jobs → completar la request pendiente
5. Si pasa requestTimeout → devolver respuesta vacía
```

Más eficiente: menos requests al broker.

### Round Robin

```
1. Worker pide N jobs
2. Gateway envía request a cada partición en round-robin
3. Devuelve lo que sea que haya disponible
4. No espera
```

### Job Streaming (Moderno)

```
1. Worker abre StreamActivatedJobs (bidireccional)
2. Gateway registra con ClientStreamer
3. Broker pushea jobs disponibles en tiempo real
4. Stream permanece abierto hasta cancelación
```

Más eficiente que long-polling para alto throughput.

## Gateway Configuration

```java
GatewayCfg {
    NetworkCfg network;           // host, port, TLS
    SecurityCfg security;         // TLS config
    ClusterCfg cluster;           // contactPoint, requestTimeout
    ThreadsCfg threads;           // managementThreads
    LongPollingCfg longPolling;   // enabled, timeout
    InterceptorsCfg interceptors; // custom interceptors
}
```

## Escalabilidad

- Los gateways son **stateless** → se pueden escalar horizontalmente
- Cada gateway conoce la topología del cluster
- Rutea al broker correcto según la partición del record
- Load balancing entre gateways via DNS/LB externo

