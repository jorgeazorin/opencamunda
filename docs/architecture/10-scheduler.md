# 10 - Scheduler (Modelo de Actores)

## Qué es el Scheduler

Zeebe usa un **modelo de actores** para concurrencia. Cada componente (broker, engine, log, etc.) es un Actor con garantía de single-thread. El scheduler gestiona la asignación de actores a threads.

## Estructura del Módulo

```
zeebe/scheduler/src/main/java/io/camunda/zeebe/scheduler/
├── Actor.java                    ← Clase base para todos los actores
├── ActorControl.java             ← API de auto-scheduling del actor
├── ActorTask.java                ← Unidad de trabajo planificada
├── ActorScheduler.java           ← Punto de entrada principal
├── ActorExecutor.java            ← Rutea tareas a thread pools
├── ActorThreadGroup.java         ← Pool de threads
├── ActorThread.java              ← Thread worker individual
├── ActorJob.java                 ← Unidad ejecutable (runnable/callable)
├── ActorCondition.java           ← Trigger de eventos reutilizable
├── ActorTimerQueue.java          ← Cola de prioridad para timers
├── ScheduledTimer.java           ← Handle de timer cancelable
├── SchedulingHints.java          ← CPU-bound vs I/O-bound
├── ConcurrencyControl.java       ← Interface para scheduling
├── WorkStealingGroup.java        ← Work-stealing entre threads
└── future/
    ├── ActorFuture.java          ← Future non-blocking
    └── CompletableActorFuture.java ← Future completable
```

## Modelo de Threads

```
┌─────────────────────────────────────────────┐
│              ActorScheduler                  │
├──────────────────────┬──────────────────────┤
│   CPU-Bound Pool     │   I/O-Bound Pool     │
│   (cores/2 threads)  │   (cores×2 threads)  │
│                      │                      │
│ ┌─────────────────┐  │ ┌─────────────────┐  │
│ │ ActorThread-0   │  │ │ ActorThread-0   │  │
│ │ ┌────────────┐  │  │ │ ┌────────────┐  │  │
│ │ │ Actor A    │  │  │ │ │ Actor D    │  │  │
│ │ │ Actor B    │  │  │ │ │ Actor E    │  │  │
│ │ └────────────┘  │  │ │ └────────────┘  │  │
│ └─────────────────┘  │ └─────────────────┘  │
│ ┌─────────────────┐  │ ┌─────────────────┐  │
│ │ ActorThread-1   │  │ │ ActorThread-1   │  │
│ │ ┌────────────┐  │  │ │ ...             │  │
│ │ │ Actor C    │  │  │ │                 │  │
│ │ └────────────┘  │  │ │                 │  │
│ └─────────────────┘  │ └─────────────────┘  │
└──────────────────────┴──────────────────────┘
        ↕ work stealing ↕
```

### Asignación de Threads

|   Pool    | Threads Default |         Propósito         |         Ejemplo         |
|-----------|-----------------|---------------------------|-------------------------|
| CPU-Bound | cores/2         | Actors sin I/O bloqueante | Engine, StreamProcessor |
| I/O-Bound | cores×2         | Actors con I/O            | Exporters, LogStorage   |

## Ciclo de Vida de un Actor

### Clase Base Actor

```java
public abstract class Actor {
    protected final ActorControl actor;  // API para self-scheduling
    
    // Callbacks de lifecycle
    protected void onActorStarting() {}   // Fase de setup
    protected void onActorStarted() {}    // Ejecución principal
    protected void onActorClosing() {}    // Teardown
    protected void onActorClosed() {}     // Cleanup final
    protected void onActorCloseRequested() {} // Pre-cierre
    
    // Manejo de errores
    protected void handleFailure(Throwable failure) {}
    
    // Control
    public ActorFuture<Void> closeAsync();  // Cierre non-blocking
    public String getName();                 // Nombre del actor
}
```

### Fases del Lifecycle

```
CREATED
   ↓ scheduler.submitActor(actor)
STARTING
   ↓ onActorStarting() completa
STARTED
   ↓ ejecuta jobs, timers, conditions
   ↓ actor.closeAsync()
CLOSE_REQUESTED
   ↓ onActorCloseRequested()
CLOSING
   ↓ onActorClosing() completa
CLOSED
   ↓ onActorClosed()
```

## ActorControl: API del Actor

Un actor usa `ActorControl` (campo `actor`) para programar tareas:

### Scheduling de Tareas

```java
// Dentro de un Actor:

// Ejecutar tarea inmediatamente (en el próximo ciclo)
actor.run(() -> processNextRecord());

// Ejecutar con resultado
ActorFuture<Result> future = actor.call(() -> computeResult());

// Timer one-shot
ScheduledTimer timer = actor.schedule(Duration.ofSeconds(5), () -> checkTimeout());

// Timer periódico
actor.runAtFixedRate(Duration.ofMillis(250), () -> sendHeartbeat());

// Timer absoluto
actor.runAt(timestamp, () -> triggerEvent());
```

### Conditions (Event-Driven)

```java
// Registrar condición reutilizable
ActorCondition condition = actor.onCondition("new-data", () -> processData());

// Desde otro actor/thread → señalizar
condition.signal();  // Enqueue job en el actor
```

### Futures

```java
// Esperar resultado de otro actor
ActorFuture<Data> future = otherActor.computeAsync();
actor.runOnCompletion(future, (data, error) -> {
    if (error == null) {
        handleResult(data);
    }
});
```

## Ejecución Interna

### ActorThread (Loop Principal)

Cada ActorThread ejecuta un loop tight:

```
while (running) {
    1. Drain external callbacks
       (submittedCallbacks: ManyToManyConcurrentArrayQueue)
    
    2. Update clock
       (DefaultActorClock o ControlledActorClock)
    
    3. Process expired timers
       (timerJobQueue: ActorTimerQueue, cola de prioridad por deadline)
    
    4. Get next ActorTask from work-stealing queue
       (WorkStealingGroup → puede robar de otros threads)
    
    5. Execute ActorTask
       - Procesa jobs hasta completar o yield
       - Jobs internos (fast-lane) → ejecución inmediata
       - Jobs externos → queue → procesar en orden
    
    6. If no work: idle strategy
       (spin → yield → LockSupport.parkNanos())
}
```

### ActorTask (Unidad de Trabajo)

```java
public class ActorTask {
    Actor actor;
    ActorJob currentJob;
    boolean shouldYield;
    List<ActorSubscription> subscriptions;  // Conditions, timers
    
    // Lifecycle
    ActorFuture<Void> onTaskScheduled(threadGroup);
    boolean execute(ActorThread runner);    // Ejecuta siguiente job
    ActorJob poll();                         // Obtiene job de la cola
    void submit(ActorJob job);              // Submission externa
}
```

### ActorJob (Tarea Individual)

```java
public class ActorJob {
    Runnable runnable;
    Callable<T> callable;
    ActorSubscription subscription;   // Si viene de condition/timer
    ActorFuture resultFuture;          // Para callable
    // Lifecycle: QUEUED → EXECUTING → TERMINATED
}
```

## Work Stealing

```
Thread-0 tiene 5 tasks pendientes
Thread-1 tiene 0 tasks (idle)

Thread-1 "roba" una task de Thread-0:
  WorkStealingGroup → each thread has local queue
  Idle thread can dequeue from busy thread's queue
  Ensures balanced utilization across threads
```

## Garantías de Concurrencia

### Single-Threaded por Actor

```
Actor A                    Actor B
┌───────────────┐         ┌───────────────┐
│ Thread-0      │         │ Thread-1      │
│ job1 → job2 → │         │ job1 → job2 → │
│ job3 → ...    │         │ ...           │
└───────────────┘         └───────────────┘
     ↑ NUNCA se ejecutan jobs de un
       mismo actor en paralelo
```

### Fast-Lane vs External Queue

```
Dentro del Actor (fast-lane):
  actor.run(() -> step2());  ← Se ejecuta inmediatamente
  │ usa ArrayDeque (single-thread, rápido)

Desde fuera del Actor (external):
  actorControl.run(() -> process());  ← Se encola
  │ usa ManyToOneConcurrentLinkedQueue (lock-free)
```

## ActorFuture

```java
public interface ActorFuture<V> {
    void onComplete(BiConsumer<V, Throwable> callback);
    V join();                           // Blocking wait
    V join(long timeout, TimeUnit unit); // Con timeout (max 300s)
    <U> ActorFuture<U> map(Function<V, U> mapper);
}

// Crear
CompletableActorFuture<V> future = new CompletableActorFuture<>();
future.complete(value);
future.completeExceptionally(error);
```

## Idle Strategy (Backoff)

Cuando un thread no tiene trabajo:

```
1. Spin (vueltas rápidas comprobando)
   ↓ Si no hay trabajo después de N spins
2. Thread.yield() 
   ↓ Si sigue sin trabajo
3. LockSupport.parkNanos()
   ↓ Hasta que hintWorkAvailable() lo despierte
```

## Configuración

```java
ActorScheduler.newActorScheduler()
    .setCpuBoundActorThreadCount(4)     // Default: cores/2
    .setIoBoundActorThreadCount(8)       // Default: cores×2
    .setActorClock(clock)                // DefaultActorClock o controlled
    .build();
```

## Ejemplo: Cómo el Engine Usa el Scheduler

```
1. Broker crea ActorScheduler
   ↓
2. Submite StreamProcessor como Actor CPU-bound
   scheduler.submitActor(streamProcessor, SchedulingHints.cpuBound());
   ↓
3. StreamProcessor.onActorStarting()
   - Abre LogStreamReader
   - Abre LogStreamWriter
   - Registra condition para nuevos records
   ↓
4. StreamProcessor.onActorStarted()
   - Registra ActorCondition: "new-record-available"
   ↓
5. Cuando llega un record:
   - LogStream señaliza condition
   - condition.signal() → enqueue job en StreamProcessor
   - StreamProcessor.processRecord() se ejecuta en single-thread
   ↓
6. StreamProcessor planifica timer para checkpoints:
   actor.runAtFixedRate(Duration.ofMinutes(1), () -> takeSnapshot());
```

## Métricas

- **Per-actor**: task count, execution time, scheduling latency
- **Global**: thread pool utilization, job queue depth
- **Scoped**: Las métricas se etiquetan por nombre de actor

