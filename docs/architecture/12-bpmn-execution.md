# 12 - Ejecución BPMN

## Cómo Zeebe Ejecuta BPMN

Zeebe ejecuta procesos **BPMN 2.0** siguiendo un modelo de event sourcing. Cada elemento BPMN pasa por un ciclo de vida definido y tiene un procesador dedicado.

## Elementos BPMN Soportados

### Tasks
| Elemento | BpmnElementType | Procesador | Comportamiento |
|----------|----------------|-----------|---------------|
| Service Task | `SERVICE_TASK` | `JobWorkerTaskProcessor` | Crea job, espera worker |
| User Task | `USER_TASK` | `UserTaskProcessor` | Crea user task, espera completado |
| Business Rule Task | `BUSINESS_RULE_TASK` | `BusinessRuleTaskProcessor` | Evalúa DMN o crea job |
| Script Task | `SCRIPT_TASK` | `ScriptTaskProcessor` | Evalúa FEEL o crea job |
| Send Task | `SEND_TASK` | `SendTaskProcessor` | Publica mensaje o crea job |
| Receive Task | `RECEIVE_TASK` | `ReceiveTaskProcessor` | Espera mensaje correlacionado |
| Manual Task | `MANUAL_TASK` | `ManualTaskProcessor` | Completa automáticamente |

### Gateways
| Elemento | BpmnElementType | Procesador | Comportamiento |
|----------|----------------|-----------|---------------|
| Exclusive (XOR) | `EXCLUSIVE_GATEWAY` | `ExclusiveGatewayProcessor` | Evalúa condiciones, toma UNA ruta |
| Parallel (AND) | `PARALLEL_GATEWAY` | `ParallelGatewayProcessor` | Fork: activa todas las rutas / Join: espera todas |
| Inclusive (OR) | `INCLUSIVE_GATEWAY` | `InclusiveGatewayProcessor` | Fork: activa rutas con condición true / Join: espera activas |
| Event-Based | `EVENT_BASED_GATEWAY` | `EventBasedGatewayProcessor` | Espera primer evento de N posibles |

### Events
| Elemento | BpmnElementType | Procesador | Comportamiento |
|----------|----------------|-----------|---------------|
| Start Event | `START_EVENT` | `StartEventProcessor` | Inicia el proceso |
| End Event | `END_EVENT` | `EndEventProcessor` | Termina el flujo |
| Intermediate Catch | `INTERMEDIATE_CATCH_EVENT` | `IntermediateCatchEventProcessor` | Espera evento |
| Intermediate Throw | `INTERMEDIATE_THROW_EVENT` | `IntermediateThrowEventProcessor` | Lanza evento |
| Boundary Event | `BOUNDARY_EVENT` | `BoundaryEventProcessor` | Evento en borde de tarea |

### Containers
| Elemento | BpmnElementType | Procesador | Comportamiento |
|----------|----------------|-----------|---------------|
| Process | `PROCESS` | `ProcessProcessor` | Proceso raíz |
| Sub-Process | `SUB_PROCESS` | `SubProcessProcessor` | Subproceso embedded |
| Event Sub-Process | `EVENT_SUB_PROCESS` | `EventSubProcessProcessor` | Activado por evento |
| Multi-Instance | `MULTI_INSTANCE_BODY` | `MultiInstanceBodyProcessor` | Loop paralelo/secuencial |
| Call Activity | `CALL_ACTIVITY` | `CallActivityProcessor` | Llama otro proceso |

### Otros
| Elemento | BpmnElementType | Descripción |
|----------|----------------|-------------|
| Sequence Flow | `SEQUENCE_FLOW` | Conexión entre elementos |

## Ciclo de Vida de un Elemento BPMN

Todo elemento pasa por estas fases:

```
ACTIVATE_ELEMENT (command)
       │
       ▼
ELEMENT_ACTIVATING (event)     ← processor.onActivate()
       │
       ├─ [Execution Listeners START] (si hay)
       │
       ▼
ELEMENT_ACTIVATED (event)      ← processor.finalizeActivation()
       │
       │  ... Elemento ejecutándose ...
       │  (ej: esperando job, mensaje, timer)
       │
       ▼
COMPLETE_ELEMENT (command)
       │
       ▼
ELEMENT_COMPLETING (event)     ← processor.onComplete()
       │
       ├─ [Execution Listeners END] (si hay)
       │
       ▼
ELEMENT_COMPLETED (event)      ← processor.finalizeCompletion()
       │
       ▼
SEQUENCE_FLOW_TAKEN (event)    ← avanza al siguiente elemento
       │
       ▼
ACTIVATE_ELEMENT (siguiente)   ← cascada continúa
```

### Terminación (alternativa a completado)
```
TERMINATE_ELEMENT (command)
       │
       ▼
ELEMENT_TERMINATING (event)    ← processor.onTerminate()
       │                          (cleanup: cancelar jobs, timers, etc.)
       ▼
ELEMENT_TERMINATED (event)
```

## Ejecución Detallada por Tipo

### Service Task (el más común)
```
1. ACTIVATE_ELEMENT
   └─ onActivate(): mapear variables de entrada (input mappings)
   └─ finalizeActivation():
      ├─ Evaluar expresiones (type, retries del job)
      ├─ Suscribir boundary events (si hay)
      ├─ Crear Job → JOB_CREATED
      └─ ELEMENT_ACTIVATED

2. [Worker procesa el job]

3. JOB_COMPLETED
   └─ Genera COMPLETE_ELEMENT

4. COMPLETE_ELEMENT
   └─ onComplete(): mapear variables de salida (output mappings)
   └─ finalizeCompletion():
      ├─ Crear compensación (si hay)
      ├─ ELEMENT_COMPLETED
      └─ Tomar sequence flows → ACTIVATE siguiente
```

### Exclusive Gateway (XOR)
```
Activación:
1. ACTIVATE_ELEMENT
   └─ onActivate(): evaluar condiciones de cada sequence flow saliente
   └─ Tomar la PRIMERA ruta cuya condición es true
   └─ Si ninguna: tomar default flow (o crear incidente)
   └─ ELEMENT_COMPLETED (los gateways se completan inmediatamente)
   └─ SEQUENCE_FLOW_TAKEN

Join:
1. No necesita join explícito (solo una ruta llega)
```

### Parallel Gateway (AND)
```
Fork:
1. ACTIVATE_ELEMENT
   └─ ELEMENT_ACTIVATED
   └─ Tomar TODAS las rutas salientes
   └─ SEQUENCE_FLOW_TAKEN × N

Join:
1. ACTIVATE_ELEMENT (cada flujo que llega)
   └─ Incrementar token count
   └─ Si tokens == número de rutas entrantes:
      └─ ELEMENT_COMPLETED → continuar
   └─ Si faltan tokens:
      └─ Esperar (no completar todavía)
```

### Timer Event
```
Intermediate Catch Timer:
1. ACTIVATE_ELEMENT
   └─ Evaluar expresión de timer (duration/date/cycle)
   └─ Crear TimerInstance con due date
   └─ ELEMENT_ACTIVATED (esperando)

2. [Timer checker detecta que due date pasó]
   └─ TIMER_TRIGGERED
   └─ COMPLETE_ELEMENT → continuar proceso

Boundary Timer (non-interrupting):
1. Se crea al activar el task padre
2. Si el timer se dispara:
   └─ TIMER_TRIGGERED
   └─ Activar ruta del boundary event
   └─ El task padre CONTINÚA ejecutándose

Boundary Timer (interrupting):
1. Se crea al activar el task padre
2. Si el timer se dispara:
   └─ TIMER_TRIGGERED
   └─ TERMINATE el task padre
   └─ Activar ruta del boundary event
```

### Message Catch Event
```
1. ACTIVATE_ELEMENT
   └─ Crear MessageSubscription (messageName + correlationKey)
   └─ ELEMENT_ACTIVATED (esperando)

2. Alguien publica mensaje con correlationKey coincidente
   └─ MESSAGE_CORRELATED
   └─ COMPLETE_ELEMENT → continuar proceso
```

### Sub-Process
```
1. ACTIVATE_ELEMENT (SubProcess)
   └─ ELEMENT_ACTIVATING
   └─ Activar Start Event del subproceso
   └─ ELEMENT_ACTIVATED

2. Los elementos internos se ejecutan normalmente

3. Cuando el End Event del subproceso se completa:
   └─ SubProcessProcessor.afterExecutionPathCompleted()
   └─ COMPLETE_ELEMENT (SubProcess)
   └─ ELEMENT_COMPLETED → continuar proceso padre
```

### Multi-Instance
```
Paralelo:
1. ACTIVATE_ELEMENT (Multi-Instance Body)
   └─ Evaluar inputCollection
   └─ Para cada elemento de la colección:
      └─ Crear scope hijo
      └─ Asignar inputElement variable
      └─ ACTIVATE_ELEMENT (inner activity)
   └─ ELEMENT_ACTIVATED

2. Cada instancia se ejecuta independientemente

3. Cuando todas las instancias completan:
   └─ Recoger outputCollection
   └─ ELEMENT_COMPLETED → continuar

Secuencial:
   Similar pero activa una instancia a la vez
```

### Call Activity
```
1. ACTIVATE_ELEMENT (Call Activity)
   └─ Buscar proceso hijo por bpmnProcessId
   └─ Crear instancia del proceso hijo
   └─ Mapear variables de entrada
   └─ ELEMENT_ACTIVATED

2. El proceso hijo se ejecuta completamente

3. Cuando el proceso hijo termina:
   └─ Mapear variables de salida
   └─ COMPLETE_ELEMENT (Call Activity)
   └─ ELEMENT_COMPLETED → continuar proceso padre
```

## Variable Mappings

### Input Mappings (al activar)
```
Definidos en BPMN:
<zeebe:ioMapping>
    <zeebe:input source="=customer.name" target="customerName" />
    <zeebe:input source="=order.total" target="amount" />
</zeebe:ioMapping>

Comportamiento:
- Se evalúan expresiones FEEL (`=customer.name`)
- Se crean variables en el scope del elemento
- Si una expresión falla → INCIDENTE
```

### Output Mappings (al completar)
```
<zeebe:ioMapping>
    <zeebe:output source="=result.status" target="paymentStatus" />
</zeebe:ioMapping>

Comportamiento:
- Se evalúan expresiones FEEL sobre las variables del job/task
- Se escriben en el scope padre
- Si una expresión falla → INCIDENTE
```

## Incidentes

Un **incidente** se crea cuando el engine no puede continuar:

```
Causas comunes:
- Expresión FEEL no evalúa (variable no existe)
- Input/output mapping falla
- Job con 0 retries
- Condición de gateway no evalúa
- Proceso hijo no encontrado (Call Activity)
- Mensaje no encuentra suscripción

Efecto:
- El elemento queda BLOQUEADO
- Se crea un record INCIDENT con información del error
- El proceso NO avanza hasta que se resuelva

Resolución:
1. Arreglar la causa (ej: set variable faltante)
2. ResolveIncident(incidentKey)
3. El engine reintenta la operación
```

## Scope de Variables

Las variables siguen una jerarquía de scopes:

```
Process Instance (scope raíz)
├── orderId = "123"
├── customer = {name: "John"}
│
├── Sub-Process (nuevo scope)
│   ├── internalVar = "abc"      ← Solo visible aquí
│   │
│   └── Service Task (hereda + puede tener locales)
│       └── taskInput = "xyz"    ← Solo visible en el task
│
└── Service Task (hereda del proceso)
    └── Ve: orderId, customer
```

- Las variables se buscan **hacia arriba** en la jerarquía
- Se escriben en el scope actual por defecto
- `SetVariables(local=false)` escribe en el scope padre

## Flujo Completo de Ejemplo

```
Proceso: Order Fulfillment
  Start → Validate Order → [XOR] → Payment → Ship → End
                             │
                      [Invalid] → Reject → End

1. CreateProcessInstance("order-fulfillment", {orderId: "123", amount: 50})
   └─ PROCESS_INSTANCE_CREATED
   └─ ACTIVATE Start Event → COMPLETED
   └─ SEQUENCE_FLOW_TAKEN
   └─ ACTIVATE "Validate Order" (Service Task)
   └─ JOB_CREATED(type="validate-order")

2. Worker completes "validate-order" job → {valid: true}
   └─ JOB_COMPLETED
   └─ COMPLETE "Validate Order"
   └─ SEQUENCE_FLOW_TAKEN
   └─ ACTIVATE XOR Gateway
   └─ Evalúa: =valid → true → toma ruta "Payment"
   └─ COMPLETED XOR Gateway
   └─ SEQUENCE_FLOW_TAKEN
   └─ ACTIVATE "Payment" (Service Task)
   └─ JOB_CREATED(type="process-payment")

3. Worker completes "process-payment" → {paid: true}
   └─ JOB_COMPLETED
   └─ COMPLETE "Payment"
   └─ SEQUENCE_FLOW_TAKEN
   └─ ACTIVATE "Ship" (Service Task)
   └─ JOB_CREATED(type="ship-order")

4. Worker completes "ship-order" → {trackingId: "ABC"}
   └─ JOB_COMPLETED
   └─ COMPLETE "Ship"
   └─ SEQUENCE_FLOW_TAKEN
   └─ ACTIVATE End Event → COMPLETED
   └─ COMPLETE Process → PROCESS_INSTANCE_COMPLETED
```
