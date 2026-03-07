# 15 - Sistema de Jobs y Workers

## Qué son los Jobs

Un **Job** es una unidad de trabajo que Zeebe delega a un **worker externo**. Es el mecanismo principal para ejecutar lógica de negocio.

Cuando el engine activa un **Service Task** (u otro task con job worker), crea un Job con:
- **type**: Tipo de trabajo (ej: `"payment-processing"`)
- **variables**: Datos necesarios para ejecutar el trabajo
- **retries**: Número de reintentos permitidos
- **timeout**: Tiempo máximo para completar

Un worker se suscribe a un tipo de job, lo procesa, y reporta el resultado al broker.

## Ciclo de Vida del Job

```
                    ┌──────────────┐
                    │   CREATED    │ ← Engine crea el job (Service Task activated)
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  ACTIVATABLE │ ← Job disponible para workers
                    └──────┬───────┘
                           │ Worker llama ActivateJobs
                    ┌──────▼───────┐
                    │  ACTIVATED   │ ← Asignado a un worker
                    └──────┬───────┘
                           │
              ┌────────────┼─────────────┬──────────────┐
              │            │             │              │
       ┌──────▼───┐  ┌────▼─────┐  ┌───▼────────┐  ┌──▼──────────┐
       │COMPLETED │  │  FAILED  │  │TIMED_OUT   │  │ERROR_THROWN │
       └──────────┘  └────┬─────┘  └───┬────────┘  └─────────────┘
                          │            │
                    ┌─────▼────────────▼────┐
                    │   ACTIVATABLE         │ ← Reintento (si retries > 0)
                    │   (o FAILED si        │
                    │    retries = 0)       │
                    └───────────────────────┘
```

## Cómo se Crean los Jobs

El engine crea un job cuando se activa un Service Task:

```
1. COMMAND: ACTIVATE_ELEMENT (Service Task)
2. Engine ejecuta JobWorkerTaskProcessor.finalizeActivation():
   a. Evalúa expresiones del job (type, retries, timeout)
   b. Crea el JobRecord
   c. Escribe EVENT: JOB_CREATED
3. JobCreatedApplier actualiza estado:
   a. Guarda job en JOBS column family
   b. Marca como ACTIVATABLE en JOB_ACTIVATABLE
   c. El Service Task queda en estado ELEMENT_ACTIVATED esperando
```

## Cómo los Workers Obtienen Jobs

### Mecanismo 1: Long Polling (Default)

```
Worker → Gateway: ActivateJobs(type="payment", maxJobs=10, timeout=30s)
                                │
Gateway → Broker(s):           │
  1. Envía request a particiones
  2. Si hay jobs:
     └─ Broker activa N jobs
     └─ Marca deadlines
     └─ Devuelve ActivatedJobs
  3. Si NO hay jobs:
     └─ Mantiene request abierta (long polling)
     └─ Cuando llegan nuevos jobs → completa la request
     └─ Si pasa requestTimeout → devuelve vacío
```

### Mecanismo 2: Job Streaming (Moderno)

```
Worker → Gateway: StreamActivatedJobs(type="payment")
                                │
Gateway registra stream con ClientStreamer
                                │
Cuando el Broker crea un job del tipo "payment":
  └─ Pushea directamente al worker via stream
  └─ Sin delay de polling
  └─ Más eficiente para alto throughput
```

## Procesamiento en el Broker

### JobBatchActivateProcessor
Procesa `ActivateJobs` del worker:

```
1. Buscar jobs ACTIVATABLE del tipo solicitado
   └─ activatableColumnFamily.whileEqualPrefix(type+tenant, ...)
2. Para cada job encontrado (hasta maxJobsToActivate):
   a. Verificar que no está en backoff
   b. Marcar como ACTIVATED
   c. Establecer deadline = ahora + timeout
   d. Asignar worker
   e. EVENT: JOB_ACTIVATED
3. Devolver batch de ActivatedJobs
```

### JobCompleteProcessor
Procesa `CompleteJob` del worker:

```
1. Verificar que el job existe y está ACTIVATED
2. Verificar que el worker coincide (seguridad)
3. EVENT: JOB_COMPLETED
4. Generar COMMAND: COMPLETE_ELEMENT (para continuar el proceso)
5. Aplicar variables de resultado
```

### JobFailProcessor
Procesa `FailJob` del worker:

```
1. Verificar que el job existe y está ACTIVATED
2. Decrementar retries
3. Si retries > 0:
   a. EVENT: JOB_FAILED
   b. Job vuelve a ACTIVATABLE (con backoff si especificado)
4. Si retries = 0:
   a. EVENT: JOB_FAILED
   b. Crear INCIDENTE (proceso queda bloqueado)
```

### JobTimeOutProcessor
Se ejecuta periódicamente (scheduled task):

```
1. Buscar jobs con deadline vencido
   └─ deadlinesColumnFamily.whileEqualPrefix(...)
2. Para cada job expirado:
   a. EVENT: JOB_TIMED_OUT
   b. Decrementar retries
   c. Devolver a ACTIVATABLE
```

### JobThrowErrorProcessor
Worker lanza un error BPMN:

```
1. Verificar que el job existe y está ACTIVATED
2. EVENT: JOB_ERROR_THROWN
3. Buscar Boundary Error Event en el proceso
4. Si existe: activar el boundary event (flujo alternativo)
5. Si no existe: propagar error al scope padre
6. Si nadie maneja: crear INCIDENTE
```

## Estado del Job en RocksDB

```java
// DbJobState usa 6 column families:

// 1. Datos del job: jobKey → JobRecord
ColumnFamily<DbLong, JobRecordValue> jobsColumnFamily;

// 2. Estado: (state, type, jobKey) → nil
ColumnFamily<...> statesJobColumnFamily;

// 3. Índice de activables: (type+tenant, jobKey) → nil
ColumnFamily<...> activatableColumnFamily;

// 4. Índice por deadline: (deadline, jobKey) → nil
ColumnFamily<...> deadlinesColumnFamily;

// 5. Índice por backoff: (backoffTime, jobKey) → nil
ColumnFamily<...> backoffColumnFamily;
```

### Consultas típicas
- **Jobs activables por tipo**: `activatableColumnFamily.whileEqualPrefix("payment")`
- **Jobs con timeout**: `deadlinesColumnFamily.whileEqualPrefix(...)` donde deadline < ahora
- **Jobs en backoff**: `backoffColumnFamily.whileEqualPrefix(...)` donde backoff > ahora

## Timeouts y Backoff

### Job Timeout
- Se establece cuando el worker activa el job
- Si el worker no completa/falla antes del deadline → `TIME_OUT`
- El `JobTimeoutCheckerScheduler` verifica periódicamente
- Configurable: `EngineConfiguration.jobsTimeoutCheckerPollingInterval`

### Retry Backoff
- Cuando un job falla, el worker puede especificar `retryBackoff`
- El job no será activable durante ese tiempo
- Algoritmo por defecto: exponential backoff
- El `JobRecurProcessor` devuelve el job a ACTIVATABLE después del backoff

## Custom Headers

Los Service Tasks pueden definir headers custom en BPMN:
```xml
<zeebe:taskHeaders>
    <zeebe:header key="url" value="https://api.payment.com" />
    <zeebe:header key="method" value="POST" />
</zeebe:taskHeaders>
```

Estos headers se incluyen en el `ActivatedJob.customHeaders` y son inmutables.

## Métricas de Jobs

```
JobProcessingMetrics
├── jobsCreated          → Counter: jobs creados
├── jobsActivated        → Counter: jobs activados por workers  
├── jobsCompleted        → Counter: jobs completados
├── jobsFailed           → Counter: jobs fallados
├── jobsTimedOut         → Counter: jobs expirados
├── jobsErrorThrown      → Counter: errores BPMN lanzados
└── jobActivationTime    → Histogram: latencia de activación
```

## Ejemplo Completo: Flujo de un Service Task

```
Tiempo →

1. Engine activa Service Task "Send Email"
   └─ JOB_CREATED (type="send-email", retries=3, timeout=5min)
   └─ ELEMENT_ACTIVATED (Service Task en espera)

2. Worker pide jobs
   └─ ActivateJobs(type="send-email", maxJobs=10)
   └─ JOB_ACTIVATED (worker="email-worker-1", deadline=now+5min)

3. Worker procesa
   └─ Envía email...
   └─ ¡Éxito!

4. Worker completa
   └─ CompleteJob(jobKey, variables={emailSent: true})
   └─ JOB_COMPLETED
   └─ COMPLETE_ELEMENT (Service Task)
   └─ ELEMENT_COMPLETING → ELEMENT_COMPLETED
   └─ SEQUENCE_FLOW_TAKEN → siguiente elemento

--- Si hubiera fallado: ---

4b. Worker falla
    └─ FailJob(jobKey, retries=2, errorMessage="SMTP error")
    └─ JOB_FAILED (retries=2)
    └─ Job vuelve a ACTIVATABLE
    └─ Otro worker (o el mismo) puede recogerlo

4c. Si retries llegan a 0
    └─ FailJob(jobKey, retries=0)
    └─ JOB_FAILED (retries=0)
    └─ INCIDENTE creado
    └─ Proceso BLOQUEADO hasta que se resuelva el incidente
```
