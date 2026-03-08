# CreateProcessInstance con resultado síncrono

## Visión general

El cliente Java de Zeebe permite iniciar un proceso y **esperar de forma síncrona a que termine**, obteniendo las variables resultantes. La llamada gRPC subyacente es `CreateProcessInstanceWithResult`.

## Uso básico

```java
ProcessInstanceResult result = client
    .newCreateInstanceCommand()
    .bpmnProcessId("mi-proceso")
    .latestVersion()
    .variables(Map.of("input", 42))
    .withResult()                              // activa el modo síncrono
    .requestTimeout(Duration.ofSeconds(30))    // timeout de la petición
    .fetchVariables("output", "status")        // opcional: solo estas variables
    .send()
    .join();                                   // bloquea hasta completar

String variables = result.getVariables();            // JSON
Map<String, Object> map = result.getVariablesAsMap();
long piKey = result.getProcessInstanceKey();
```

## Cómo funciona por dentro

### 1. Cliente

Envía un `CreateProcessInstanceWithResultRequest` por gRPC con un `requestTimeout` (en ms). El deadline gRPC se establece en `requestTimeout + 10s` de margen (`DEADLINE_OFFSET` en `CreateProcessInstanceWithResultCommandImpl`).

### 2. Gateway

Traduce la petición gRPC a un comando del broker con intent `CREATE_WITH_AWAITING_RESULT` (en lugar del `CREATE` normal).

### 3. Engine — creación

El `ProcessInstanceCreationCreateWithResultProcessor` crea la instancia pero **no responde inmediatamente**. En su lugar guarda metadatos de espera (`AwaitProcessInstanceResultMetadata`) en el state del `ElementInstanceState`:

- `requestId` y `requestStreamId` — identifican la conexión gRPC abierta
- `fetchVariables` — qué variables recoger al completar

### 4. Engine — completación

Cuando el proceso **completa** (el elemento `PROCESS` llega a `ELEMENT_COMPLETED`), el behavior `BpmnProcessResultSenderBehavior.sendResult()` comprueba si hay metadata de "await". Si la hay:

1. Recopila las variables del root scope (todas, o solo las de `fetchVariables`)
2. Construye un `ProcessInstanceResultRecord` con intent `COMPLETED`
3. Envía la respuesta de vuelta al gateway usando el `requestId`/`requestStreamId` guardados

### 5. Gateway → Cliente

El gateway mapea la respuesta a `CreateProcessInstanceWithResultResponse` y completa la llamada gRPC abierta. El `Future` en el cliente se resuelve.

## Flujo de datos

```
Cliente Java                    Gateway                         Broker/Engine
     |                            |                                  |
     |-- CreateProcessInstance -->|                                  |
     |   WithResultRequest        |-- CREATE_WITH_AWAITING_RESULT -->|
     |   (requestTimeout=30s)     |                                  |
     |                            |   [guarda AwaitResultMetadata]    |
     |                            |   [crea instancia, NO responde]  |
     |                            |                                  |
     |   (conexión gRPC abierta)  |   ... proceso ejecutándose ...   |
     |                            |                                  |
     |                            |   [proceso ELEMENT_COMPLETED]    |
     |                            |   [BpmnProcessResultSender]      |
     |                            |<-- ProcessInstanceResultRecord --|
     |<-- WithResultResponse -----|                                  |
     |   (variables, piKey, etc)  |                                  |
```

## Procesos largos, timers y user tasks

La **conexión gRPC se mantiene abierta** todo el tiempo que dure el proceso, hasta el `requestTimeout`. Si el proceso no termina antes del timeout:

- El **deadline gRPC expira** → el cliente recibe una excepción `DEADLINE_EXCEEDED`
- La **instancia de proceso sigue ejecutándose** en el broker normalmente (no se cancela)
- Simplemente ya no hay nadie esperando la respuesta

Por tanto, **no es práctico** para procesos con:

| Escenario | Problema |
|---|---|
| **Timers** de larga duración | El timeout expirará antes de que el timer dispare |
| **User tasks** | Esperan interacción humana, tiempo impredecible |
| **Message catch events** con esperas largas | Dependen de eventos externos |
| **Subprocesos complejos** | Pueden tardar más que el timeout |

### Caso de uso ideal

Procesos **cortos tipo request-response**: recibir entrada → ejecutar service tasks automáticas → devolver resultado. Ejemplos típicos:

- Orquestación de llamadas a microservicios
- Cálculos o validaciones complejas modeladas como proceso
- Transformación de datos con lógica de decisión (DMN)

## Comportamiento ante incidentes

Si se produce un **incidente** durante la ejecución (fallo en un job, error de expresión, etc.):

- El proceso **se queda parado** en el incidente
- La conexión gRPC sigue abierta esperando
- Si el incidente **se resuelve antes del timeout** y el proceso completa → se devuelve el resultado normalmente
- Si **expira el timeout** → `DEADLINE_EXCEEDED` en el cliente, pero la instancia sigue viva con el incidente pendiente

**El engine no envía notificación de incidente por este canal.** Solo responde cuando el proceso **completa exitosamente** su end event. Cualquier otro escenario (incidente, timer largo, user task) resulta en timeout del lado del cliente.

## Archivos clave en el código

| Archivo | Rol |
|---|---|
| `zeebe/clients/java/.../CreateProcessInstanceCommandStep1.java` | API fluent del cliente (`.withResult()`) |
| `zeebe/clients/java/.../CreateProcessInstanceWithResultCommandImpl.java` | Implementación del comando en el cliente |
| `zeebe/gateway-protocol/src/main/proto/gateway.proto` | Definición protobuf del request/response |
| `zeebe/gateway/.../BrokerCreateProcessInstanceWithResultRequest.java` | Traducción gateway → broker |
| `zeebe/engine/.../ProcessInstanceCreationCreateWithResultProcessor.java` | Procesador en el engine (guarda metadata) |
| `zeebe/engine/.../AwaitProcessInstanceResultMetadata.java` | Metadata de espera persistida en el state |
| `zeebe/engine/.../BpmnProcessResultSenderBehavior.java` | Envía resultado cuando el proceso completa |
