# 27 - How To: Añadir un Elemento BPMN

## Resumen

Guía paso a paso para añadir un nuevo tipo de elemento BPMN al engine de Zeebe. Usamos **ManualTask** y **UndefinedTaskProcessor** como ejemplos reales.

## Archivos a Modificar/Crear

| # | Archivo | Módulo | Acción |
|---|---------|--------|--------|
| 1 | `BpmnElementType.java` | protocol | Añadir enum value |
| 2 | `ExecutableXxx.java` | engine (deployment/model) | Crear modelo ejecutable |
| 3 | `XxxTransformer.java` | engine (deployment/transform) | Parser BPMN → modelo |
| 4 | `XxxProcessor.java` | engine (processing/bpmn) | Lógica de ejecución |
| 5 | `BpmnElementProcessors.java` | engine | Registrar processor |
| 6 | `BpmnStreamProcessor.java` | engine | (normalmente no necesita cambio) |

## Paso 1: Definir el BpmnElementType

En `protocol/src/main/java/.../record/value/BpmnElementType.java`:

```java
public enum BpmnElementType {
  // ... existentes ...
  
  // Añadir tu nuevo tipo
  MY_NEW_ELEMENT("myNewElement"),
  
  // ... 
}
```

El string `"myNewElement"` debe coincidir con el nombre del elemento en XML BPMN.

**Tipos existentes de referencia**:

```java
// Tasks
SERVICE_TASK("serviceTask"),
BUSINESS_RULE_TASK("businessRuleTask"),
SCRIPT_TASK("scriptTask"),
SEND_TASK("sendTask"),
USER_TASK("userTask"),
RECEIVE_TASK("receiveTask"),
MANUAL_TASK("manualTask"),
TASK("task"),

// Gateways
EXCLUSIVE_GATEWAY("exclusiveGateway"),
PARALLEL_GATEWAY("parallelGateway"),
EVENT_BASED_GATEWAY("eventBasedGateway"),
INCLUSIVE_GATEWAY("inclusiveGateway"),

// Containers
PROCESS("process"),
SUB_PROCESS("subProcess"),
EVENT_SUB_PROCESS(null),
MULTI_INSTANCE_BODY(null),
CALL_ACTIVITY("callActivity"),

// Events
START_EVENT("startEvent"),
INTERMEDIATE_CATCH_EVENT("intermediateCatchEvent"),
INTERMEDIATE_THROW_EVENT("intermediateThrowEvent"),
BOUNDARY_EVENT("boundaryEvent"),
END_EVENT("endEvent"),
```

## Paso 2: Crear el Modelo Ejecutable

En `engine/src/main/java/.../deployment/model/element/`:

```java
public class ExecutableMyNewElement extends ExecutableFlowElement
    implements ExecutableActivity {
  
  // Propiedades específicas de tu elemento
  private String myCustomProperty;
  
  public ExecutableMyNewElement(final String id) {
    super(id);
  }
  
  // Getters y setters
}
```

**Jerarquía de clases modelo:**

```
ExecutableFlowElement          ← base de todo
├── ExecutableActivity         ← tareas (tiene input/output mappings)
│   ├── ExecutableServiceTask
│   ├── ExecutableUserTask
│   └── ExecutableReceiveTask
├── ExecutableFlowNode         ← nodos que participan en flujo
│   ├── ExecutableCatchEventElement
│   └── ExecutableGateway
└── ExecutableCatchEventElement ← elementos que reciben eventos
```

Si tu elemento es una tarea simple, extiende `ExecutableActivity`.

## Paso 3: Crear el Transformer

En `engine/src/main/java/.../deployment/transform/`:

```java
public final class MyNewElementTransformer 
    implements BpmnElementTransformer<AbstractFlowElement> {

  @Override
  public Class<AbstractFlowElement> getType() {
    return AbstractFlowElement.class;
  }

  @Override
  public void transform(
      final AbstractFlowElement element,
      final TransformContext context) {
    
    final var executableElement = new ExecutableMyNewElement(element.getId());
    
    // Configurar propiedades desde el XML BPMN
    // executableElement.setMyProperty(element.getAttributeValue("myProp"));
    
    context.addFlowElement(executableElement);
  }
}
```

Registrar en `BpmnTransformer` si es necesario.

## Paso 4: Implementar el Processor

En `engine/src/main/java/.../processing/bpmn/task/` (o subcarpeta apropiada):

### Opción A: Task Simple (sin espera)

Si tu elemento se completa inmediatamente (como ManualTask):

```java
public class MyNewElementProcessor 
    implements BpmnElementProcessor<ExecutableActivity> {

  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnCompensationSubscriptionBehaviour compensationBehaviour;

  public MyNewElementProcessor(
      final BpmnBehaviors bpmnBehaviors,
      final BpmnStateTransitionBehavior stateTransitionBehavior) {
    this.stateTransitionBehavior = stateTransitionBehavior;
    incidentBehavior = bpmnBehaviors.incidentBehavior();
    compensationBehaviour = bpmnBehaviors.compensationSubscriptionBehaviour();
  }

  @Override
  public Class<ExecutableActivity> getType() {
    return ExecutableActivity.class;
  }

  @Override
  public Either<Failure, ?> onActivate(
      final ExecutableActivity element, 
      final BpmnElementContext context) {
    // Transiciona a ACTIVATED y luego inmediatamente completa
    final var activated = stateTransitionBehavior
        .transitionToActivated(context, element.getEventType());
    stateTransitionBehavior.completeElement(activated);
    return SUCCESS;
  }

  @Override
  public Either<Failure, ?> onComplete(
      final ExecutableActivity element, 
      final BpmnElementContext context) {
    // Crear compensación si necesario
    compensationBehaviour.createCompensationSubscription(element, context);
    return stateTransitionBehavior
        .transitionToCompleted(element, context)
        .thenDo(completed -> {
          compensationBehaviour.completeCompensationHandler(completed);
          stateTransitionBehavior.takeOutgoingSequenceFlows(element, completed);
        });
  }

  @Override
  public void onTerminate(
      final ExecutableActivity element, 
      final BpmnElementContext context) {
    final var terminated = stateTransitionBehavior
        .transitionToTerminated(context, element.getEventType());
    incidentBehavior.resolveIncidents(context);
    stateTransitionBehavior.onElementTerminated(element, terminated);
  }
}
```

### Opción B: Task con Job Worker (espera externa)

Si tu elemento necesita un worker externo (como ServiceTask), extiende `JobWorkerTaskProcessor`.

### Opción C: Task con Espera de Evento

Si tu elemento espera un evento (como ReceiveTask), necesitas abrir subscripciones de evento en `onActivate`.

## Paso 5: Registrar en BpmnElementProcessors

En `engine/src/main/java/.../processing/bpmn/BpmnElementProcessors.java`:

```java
public BpmnElementProcessors(
    final BpmnBehaviors bpmnBehaviors,
    final BpmnStateTransitionBehavior stateTransitionBehavior,
    final EngineConfiguration config) {
  
  // ... registros existentes ...
  
  // Añadir tu nuevo processor
  processors.put(
      BpmnElementType.MY_NEW_ELEMENT,
      new MyNewElementProcessor(bpmnBehaviors, stateTransitionBehavior));
}
```

**Ejemplo real** (ManualTask):

```java
processors.put(
    BpmnElementType.MANUAL_TASK,
    new ManualTaskProcessor(bpmnBehaviors, stateTransitionBehavior));
```

## Lifecycle del BpmnElementProcessor

```
                    ┌─────────────────────┐
                    │    ELEMENT_ACTIVATING │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   onActivate()       │ ← inicializar, input mappings
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │    ELEMENT_ACTIVATED  │
                    └──────────┬──────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │ (si no wait-state) │                     │ (si wait-state)
          │                    │                     │ esperar evento/job
          │               ┌────▼─────┐               │
          │               │ COMPLETING│               │
          │               └────┬─────┘               │
          │                    │                     │
          │         ┌──────────▼──────────┐          │
          │         │   onComplete()      │          │
          │         └──────────┬──────────┘          │
          │                    │                     │
          │         ┌──────────▼──────────┐          │
          │         │  ELEMENT_COMPLETED   │          │
          │         └─────────────────────┘          │
          │                                          │
          │ (terminación)                  ┌─────────▼────────┐
          └───────────────────────────────►│  onTerminate()   │
                                          └─────────┬────────┘
                                                    │
                                          ┌─────────▼────────┐
                                          │ ELEMENT_TERMINATED│
                                          └──────────────────┘
```

## Behaviors Disponibles (BpmnBehaviors)

| Behavior | Uso |
|----------|-----|
| `stateTransitionBehavior` | Transiciones de estado (activate, complete, terminate) |
| `incidentBehavior` | Crear/resolver incidentes |
| `variableMappingBehavior` | Input/output mappings de variables |
| `eventSubscriptionBehavior` | Abrir/cerrar subscripciones a eventos |
| `eventPublicationBehavior` | Publicar eventos |
| `compensationSubscriptionBehaviour` | Gestionar compensaciones |
| `bufferedMessageStartEventBehavior` | Mensajes de start event |

## Ejemplo Real Completo: ManualTask

`ManualTaskProcessor.java`:
```java
public class ManualTaskProcessor extends UndefinedTaskProcessor {
  public ManualTaskProcessor(
      final BpmnBehaviors bpmnBehaviors,
      final BpmnStateTransitionBehavior stateTransitionBehavior) {
    super(bpmnBehaviors, stateTransitionBehavior);
  }
}
```

Hereda de `UndefinedTaskProcessor` que:
1. **onActivate**: transiciona a ACTIVATED → inmediatamente completa
2. **onComplete**: crea compensación → transiciona a COMPLETED → toma sequence flows  
3. **onTerminate**: transiciona a TERMINATED → resuelve incidentes

## Checklist

- [ ] `BpmnElementType` enum añadido
- [ ] Modelo ejecutable creado (`ExecutableXxx`)
- [ ] Transformer BPMN→modelo implementado
- [ ] Processor implementado con `onActivate`, `onComplete`, `onTerminate`
- [ ] Processor registrado en `BpmnElementProcessors`
- [ ] Tests unitarios (processor) escritos
- [ ] Tests de integración (BPMN process completo) escritos
- [ ] BPMN model XML de test creado
