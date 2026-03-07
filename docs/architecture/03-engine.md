# 03 - Engine (Motor de Ejecución)

## Qué es el Engine

El Engine (`zeebe-workflow-engine`) es el componente que procesa comandos y ejecuta la lógica BPMN. Es el **cerebro** de Zeebe.

Implementa la interfaz `RecordProcessor` del Stream Platform, lo que significa que:
- Recibe `TypedRecord` (comando o evento)
- Devuelve `ProcessingResult` (eventos generados, respuestas, post-commit tasks)

## Ubicación del Código

```
zeebe/engine/src/main/java/io/camunda/zeebe/engine/
├── Engine.java                    ← Entry point (implementa RecordProcessor)
├── EngineConfiguration.java       ← Configuración del engine
├── processing/                    ← TODA la lógica de procesamiento
│   ├── bpmn/                      ← Ejecución de elementos BPMN
│   │   ├── BpmnStreamProcessor.java     ← Procesador central BPMN
│   │   ├── BpmnElementProcessors.java   ← Registro de procesadores
│   │   ├── behavior/              ← Behaviors compartidos
│   │   ├── container/             ← Process, SubProcess, MultiInstance
│   │   ├── event/                 ← Start/End/Intermediate events
│   │   ├── gateway/               ← Exclusive, Parallel, Inclusive, EventBased
│   │   └── task/                  ← ServiceTask, UserTask, etc.
│   ├── deployment/                ← Deploy de procesos y recursos
│   ├── job/                       ← Ciclo de vida de jobs
│   ├── message/                   ← Correlación de mensajes
│   ├── processinstance/           ← Creación/cancelación de instancias
│   ├── timer/                     ← Timers y delays
│   ├── usertask/                  ← Tareas de usuario
│   ├── variable/                  ← Variables de proceso
│   ├── incident/                  ← Gestión de incidentes
│   ├── signal/                    ← Señales
│   ├── dmn/                       ← Evaluación de decisiones
│   ├── distribution/              ← Distribución entre particiones
│   ├── resource/                  ← Gestión de recursos
│   ├── scheduled/                 ← Tareas programadas
│   ├── common/                    ← Lógica común
│   └── streamprocessor/           ← Procesadores de stream
├── state/                         ← Estado persistente
│   ├── ProcessingDbState.java     ← Contenedor de todos los estados
│   ├── appliers/                  ← Event appliers
│   ├── instance/                  ← Estado de instancias
│   ├── deployment/                ← Estado de deployments
│   ├── message/                   ← Estado de mensajes
│   ├── variable/                  ← Estado de variables
│   └── ...
└── metrics/                       ← Métricas del engine
```

## Engine.java - El Entry Point

```java
public class Engine implements RecordProcessor {

    // 1. PROCESS: Recibe un comando y lo procesa
    public ProcessingResult process(TypedRecord record, ProcessingResultBuilder builder) {
        // a) Buscar procesador por (RecordType, ValueType, Intent)
        TypedRecordProcessor processor = recordProcessorMap.get(
            record.getRecordType(),
            record.getValueType(),
            record.getIntent().value()
        );
        
        // b) Verificar que la instancia no esté baneada
        if (shouldProcessCommand(record)) {
            processor.processRecord(record);
        }
        
        return builder.build();
    }

    // 2. REPLAY: Reconstruir estado desde eventos
    public void replay(TypedRecord event) {
        eventApplier.applyState(
            event.getKey(), 
            event.getIntent(), 
            event.getValue(), 
            event.getRecordVersion()
        );
    }

    // 3. ERROR: Manejar errores de procesamiento
    public ProcessingResult onProcessingError(Throwable ex, TypedRecord record, ...) {
        // Intentar que el procesador maneje el error
        // Si no puede, generar incidente o loguear
    }
}
```

## RecordProcessorMap - Lookup O(1)

El engine registra procesadores en un mapa 3D: `RecordType × ValueType × Intent → Processor`

```
Ejemplos de mapeo:
COMMAND + JOB + COMPLETE           → JobCompleteProcessor
COMMAND + JOB + FAIL               → JobFailProcessor
COMMAND + JOB_BATCH + ACTIVATE     → JobBatchActivateProcessor
COMMAND + PROCESS_INSTANCE + CANCEL → ProcessInstanceCancelProcessor
COMMAND + DEPLOYMENT + CREATE      → DeploymentCreateProcessor
COMMAND + MESSAGE + PUBLISH        → MessagePublishProcessor
...
```

El lookup es O(1), no hay reflexión ni búsqueda.

## BpmnStreamProcessor - El Procesador BPMN Central

Procesa todos los commands de tipo `PROCESS_INSTANCE`:

```java
public final class BpmnStreamProcessor 
    implements TypedRecordProcessor<ProcessInstanceRecord> {

    public void processRecord(TypedRecord<ProcessInstanceRecord> record) {
        ProcessInstanceIntent intent = record.getIntent();
        BpmnElementType elementType = record.getValue().getBpmnElementType();
        
        // 1. Buscar procesador para este tipo de elemento BPMN
        BpmnElementProcessor processor = processors.getProcessor(elementType);
        
        // 2. Validar transición de estado
        stateTransitionGuard.isValidStateTransition(context, element)
            .ifRightOrLeft(
                ok -> processEvent(intent, processor, element),
                violation -> reject(record, violation)
            );
    }
    
    private void processEvent(intent, processor, element) {
        switch (intent) {
            case ACTIVATE_ELEMENT:
                // Estado: → ELEMENT_ACTIVATING
                // Ejecutar: processor.onActivate()
                // Ejecutar: execution listeners (START)
                // Ejecutar: processor.finalizeActivation()
                // Estado: → ELEMENT_ACTIVATED
                break;
                
            case COMPLETE_ELEMENT:
                // Estado: → ELEMENT_COMPLETING
                // Ejecutar: processor.onComplete()
                // Ejecutar: execution listeners (END)
                // Ejecutar: processor.finalizeCompletion()
                // Estado: → ELEMENT_COMPLETED
                break;
                
            case TERMINATE_ELEMENT:
                // Estado: → ELEMENT_TERMINATING
                // Ejecutar: processor.onTerminate()
                // Estado: → ELEMENT_TERMINATED
                break;
        }
    }
}
```

## BpmnElementProcessor - Interfaz por Elemento

Cada tipo de elemento BPMN tiene su procesador:

```java
public interface BpmnElementProcessor<T extends ExecutableFlowElement> {
    Class<T> getType();
    
    // Fase ACTIVATE: Preparar el elemento
    Either<Failure, ?> onActivate(T element, BpmnElementContext context);
    
    // Fase ACTIVATE (post-listeners): Finalizar activación
    Either<Failure, ?> finalizeActivation(T element, BpmnElementContext context);
    
    // Fase COMPLETE: Completar el elemento
    Either<Failure, ?> onComplete(T element, BpmnElementContext context);
    
    // Fase COMPLETE (post-listeners): Finalizar completado
    Either<Failure, ?> finalizeCompletion(T element, BpmnElementContext context);
    
    // Fase TERMINATE: Limpiar y terminar
    void onTerminate(T element, BpmnElementContext context);
}
```

**Nota**: Devuelve `Either<Failure, ?>`. Un `Left(Failure)` crea un incidente automáticamente.

## Registro de Procesadores BPMN

`BpmnElementProcessors` mapea cada `BpmnElementType` a su procesador:

### Tasks (Tareas)

|         Tipo         |         Procesador          |          Qué hace           |
|----------------------|-----------------------------|-----------------------------|
| `SERVICE_TASK`       | `JobWorkerTaskProcessor`    | Crea job, espera worker     |
| `USER_TASK`          | `UserTaskProcessor`         | Crea user task              |
| `BUSINESS_RULE_TASK` | `BusinessRuleTaskProcessor` | Evalúa DMN o crea job       |
| `SCRIPT_TASK`        | `ScriptTaskProcessor`       | Evalúa FEEL o crea job      |
| `SEND_TASK`          | `SendTaskProcessor`         | Publica mensaje o crea job  |
| `RECEIVE_TASK`       | `ReceiveTaskProcessor`      | Espera mensaje              |
| `MANUAL_TASK`        | `ManualTaskProcessor`       | Se completa automáticamente |

### Gateways

|         Tipo          |          Procesador          |             Qué hace              |
|-----------------------|------------------------------|-----------------------------------|
| `EXCLUSIVE_GATEWAY`   | `ExclusiveGatewayProcessor`  | Evalúa condiciones, toma una ruta |
| `PARALLEL_GATEWAY`    | `ParallelGatewayProcessor`   | Fork/Join de flujos paralelos     |
| `INCLUSIVE_GATEWAY`   | `InclusiveGatewayProcessor`  | Fork/Join inclusivo               |
| `EVENT_BASED_GATEWAY` | `EventBasedGatewayProcessor` | Espera primer evento              |

### Events

|            Tipo            |            Procesador             |         Qué hace         |
|----------------------------|-----------------------------------|--------------------------|
| `START_EVENT`              | `StartEventProcessor`             | Inicia el proceso        |
| `END_EVENT`                | `EndEventProcessor`               | Termina el flujo         |
| `INTERMEDIATE_CATCH_EVENT` | `IntermediateCatchEventProcessor` | Espera evento            |
| `INTERMEDIATE_THROW_EVENT` | `IntermediateThrowEventProcessor` | Lanza evento             |
| `BOUNDARY_EVENT`           | `BoundaryEventProcessor`          | Evento en borde de tarea |

### Containers (SubProcesos)

|         Tipo          |          Procesador          |       Qué hace        |
|-----------------------|------------------------------|-----------------------|
| `PROCESS`             | `ProcessProcessor`           | Proceso raíz          |
| `SUB_PROCESS`         | `SubProcessProcessor`        | Subproceso embedded   |
| `EVENT_SUB_PROCESS`   | `EventSubProcessProcessor`   | Subproceso por evento |
| `MULTI_INSTANCE_BODY` | `MultiInstanceBodyProcessor` | Loop multi-instancia  |
| `CALL_ACTIVITY`       | `CallActivityProcessor`      | Llama otro proceso    |

## Ejemplo: Ciclo de Vida de un Service Task

```
1. ACTIVATE_ELEMENT (Service Task)
   │
   ├─ transitionToActivating()      → EVENT: ELEMENT_ACTIVATING
   │
   ├─ processor.onActivate()
   │  └─ applyInputMappings()        → Mapear variables de entrada
   │
   ├─ execution listeners (START)    → Si hay listeners, crear job
   │
   ├─ processor.finalizeActivation()
   │  ├─ evaluateJobExpressions()    → Evaluar type, retries, etc.
   │  ├─ subscribeToEvents()         → Suscribir boundary events
   │  ├─ createNewJob()              → EVENT: JOB_CREATED
   │  └─ transitionToActivated()     → EVENT: ELEMENT_ACTIVATED
   │
   └─ Job queda esperando...
   
2. Worker completa el job → COMMAND: JOB_COMPLETE
   │
   ├─ JobCompleteProcessor:
   │  ├─ Valida que el job existe y está activated
   │  ├─ EVENT: JOB_COMPLETED
   │  └─ Genera COMMAND: COMPLETE_ELEMENT (para el Service Task)
   
3. COMPLETE_ELEMENT (Service Task)
   │
   ├─ transitionToCompleting()      → EVENT: ELEMENT_COMPLETING
   │
   ├─ processor.onComplete()
   │  ├─ applyOutputMappings()      → Mapear variables de salida
   │  └─ unsubscribeFromEvents()    → Desuscribir boundary events
   │
   ├─ execution listeners (END)     → Si hay listeners, crear job
   │
   ├─ processor.finalizeCompletion()
   │  ├─ createCompensationSubscription()
   │  ├─ transitionToCompleted()    → EVENT: ELEMENT_COMPLETED
   │  └─ takeOutgoingSequenceFlows() → Avanzar al siguiente elemento
   │
   └─ EVENT: SEQUENCE_FLOW_TAKEN
      └─ COMMAND: ACTIVATE_ELEMENT (siguiente elemento)
```

## Behaviors - Lógica Reutilizable

Los procesadores no implementan toda la lógica directamente. Delegan en **behaviors**:

```
BpmnBehaviors (interfaz contenedora de todos los behaviors)
├── BpmnVariableMappingBehavior   → Input/output mappings
├── BpmnJobBehavior               → Crear/cancelar jobs
├── BpmnEventSubscriptionBehavior → Suscribirse/desuscribirse de eventos
├── BpmnStateTransitionBehavior   → Transiciones de estado
├── BpmnIncidentBehavior          → Crear/resolver incidentes
├── BpmnSignalBehavior            → Broadcasting de señales
├── BpmnCompensationBehavior      → Compensaciones
├── BpmnBufferedMessageStartEventBehavior
├── BpmnEventPublicationBehavior
└── ...
```

## Procesadores de Otros Dominios (No-BPMN)

### Jobs (`processing/job/`)

```
JobBatchActivateProcessor   → Activar batch de jobs (del worker)
JobCompleteProcessor        → Worker completa job
JobFailProcessor            → Worker falla job
JobThrowErrorProcessor      → Worker lanza error BPMN
JobTimeOutProcessor         → Job expira por timeout
JobCancelProcessor          → Cancelar job
JobUpdateRetriesProcessor   → Actualizar retries
JobUpdateTimeoutProcessor   → Actualizar timeout
JobYieldProcessor           → Worker cede job
JobRecurProcessor           → Reintentar tras backoff
```

### Deployment (`processing/deployment/`)

```
DeploymentCreateProcessor        → Desplegar recursos (BPMN, DMN, Forms)
DeploymentDistributeProcessor    → Distribuir deploy a otras particiones
```

### Messages (`processing/message/`)

```
MessagePublishProcessor              → Publicar mensaje
MessageCorrelateProcessor            → Correlacionar mensaje con proceso
MessageExpireProcessor               → Expirar mensajes viejos
ProcessMessageSubscriptionCorrelate  → Correlación en suscripciones
```

### Timers (`processing/timer/`)

```
TimerTriggerProcessor       → Timer se dispara
TimerCancelProcessor        → Cancelar timer
```

### Incidents (`processing/incident/`)

```
IncidentResolveProcessor    → Resolver incidente y reintentar
```

### Process Instance (`processing/processinstance/`)

```
ProcessInstanceCreationCreateProcessor  → Crear instancia de proceso
ProcessInstanceCancelProcessor          → Cancelar instancia
ModifyProcessInstanceProcessor          → Modificar instancia en vuelo
MigrateProcessInstanceProcessor         → Migrar a nueva versión
```

## Manejo de Errores: Either + Incidentes

El engine usa `Either<Failure, Result>` en lugar de excepciones:

```java
// Composición de operaciones que pueden fallar
return variableMappingBehavior.applyInputMappings(context, element)
    .flatMap(ok -> jobBehavior.evaluateJobExpressions(element, context))
    .flatMap(job -> eventSubscriptionBehavior.subscribeToEvents(element, context))
    .thenDo(ok -> {
        jobBehavior.createNewJob(context, element, jobProperties);
        stateTransitionBehavior.transitionToActivated(context);
    });

// Si cualquier paso devuelve Left(Failure):
// → Se crea un INCIDENTE automáticamente
// → El proceso queda bloqueado en ese punto
// → El usuario puede resolver el incidente y reintentar
```

### Instancias Baneadas

Si una instancia de proceso causa errores repetidos, se **banea**:
- `BannedInstanceState` marca la instancia
- El engine rechaza todos los comandos para esa instancia
- Previene loops infinitos de errores

## Distribución entre Particiones

Algunos comandos necesitan distribuirse a todas las particiones:
- **Deployments**: Se crean en partición 1 y se distribuyen
- **Signals**: Se broadcastean a todas las particiones
- **Comando distribution**: Framework genérico para distribución

```java
// DistributedTypedRecordProcessor distingue entre:
void processNewCommand(TypedRecord command);        // Comando original
void processDistributedCommand(TypedRecord command); // Comando recibido de otra partición
```

## Event Appliers

Los appliers actualizan el estado cuando se procesa un evento:

```
EventAppliers registra:
  (ValueType.JOB, JobIntent.CREATED)     → JobCreatedApplier
  (ValueType.JOB, JobIntent.COMPLETED)   → JobCompletedApplier
  (ValueType.JOB, JobIntent.ACTIVATED)   → JobActivatedApplier
  (ValueType.JOB, JobIntent.FAILED)      → JobFailedApplier
  ...
  (ValueType.PROCESS_INSTANCE, ELEMENT_ACTIVATING) → ElementInstanceActivatingApplier
  (ValueType.PROCESS_INSTANCE, ELEMENT_ACTIVATED)  → ElementInstanceActivatedApplier
  ...
```

Cada applier actualiza las column families correspondientes en RocksDB.

## Cómo Encontrar el Código de una Funcionalidad

|          Quiero entender...           |                             Buscar en...                             |
|---------------------------------------|----------------------------------------------------------------------|
| Cómo se ejecuta un Service Task       | `processing/bpmn/task/JobWorkerTaskProcessor`                        |
| Cómo se crea un job                   | `processing/bpmn/behavior/BpmnJobBehavior`                           |
| Cómo se activan jobs para workers     | `processing/job/JobBatchActivateProcessor`                           |
| Cómo se despliega un proceso          | `processing/deployment/DeploymentCreateProcessor`                    |
| Cómo se crea una instancia            | `processing/processinstance/ProcessInstanceCreationCreateProcessor`  |
| Cómo se correlaciona un mensaje       | `processing/message/MessageCorrelateProcessor`                       |
| Cómo se evalúa una expresión FEEL     | La usa `BpmnVariableMappingBehavior` → delega a `ExpressionLanguage` |
| Cómo se ejecuta una decisión DMN      | `processing/dmn/` → usa `DecisionEngine`                             |
| Cómo se maneja un timer               | `processing/timer/TimerTriggerProcessor`                             |
| Cómo se resuelve un incidente         | `processing/incident/IncidentResolveProcessor`                       |
| Cómo se actualiza el estado de un job | `state/appliers/Job*Applier` → modifica `DbJobState`                 |

