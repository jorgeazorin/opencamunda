# 18 - Cliente Java

## Qué es el Cliente Java

El cliente Java (`zeebe-client-java`) es la librería principal para interactuar con Zeebe desde aplicaciones Java. Usa **gRPC** como transporte y ofrece una API fluent.

## Ubicación del Código

```
zeebe/clients/java/src/main/java/io/camunda/zeebe/client/
├── ZeebeClient.java                ← Interfaz principal
├── ZeebeClientBuilder.java         ← Builder
├── ZeebeClientConfiguration.java   ← Configuración
├── CredentialsProvider.java        ← Autenticación
├── api/
│   ├── command/                    ← Comandos (24+ tipos)
│   ├── response/                   ← Tipos de respuesta
│   └── worker/                     ← Job workers
└── impl/                           ← Implementación interna
```

**Java mínimo**: JDK 8 (para máxima compatibilidad)

## Crear un Cliente

```java
// Conexión básica
ZeebeClient client = ZeebeClient.newClientBuilder()
    .gatewayAddress("localhost:26500")
    .usePlaintext()
    .build();

// Con autenticación OAuth
ZeebeClient client = ZeebeClient.newClientBuilder()
    .gatewayAddress("localhost:26500")
    .credentialsProvider(new OAuthCredentialsProvider.Builder()
        .clientId("my-client-id")
        .clientSecret("my-secret")
        .audience("zeebe.camunda.io")
        .build())
    .build();

// Camunda Cloud
ZeebeClient client = ZeebeClient.newCloudClientBuilder()
    .withClusterId("cluster-id")
    .withClientId("client-id")
    .withClientSecret("secret")
    .withRegion("bru-2")
    .build();
```

## ZeebeClient - API Completa

### Topología

```java
Topology topology = client.newTopologyRequest().send().join();
// → brokers, partitions, clusterSize, replicationFactor
```

### Deploy de Recursos

```java
DeploymentEvent result = client.newDeployResourceCommand()
    .addResourceFromClasspath("process.bpmn")       // BPMN desde classpath
    .addResourceFile("/path/to/decision.dmn")        // DMN desde fichero
    .addResourceBytes(formBytes, "form.form")        // Form desde bytes
    .tenantId("my-tenant")
    .send()
    .join();

// Resultado: processDefinitionKey, version, processId, etc.
```

### Crear Instancia de Proceso

```java
// Por bpmnProcessId
ProcessInstanceEvent instance = client.newCreateInstanceCommand()
    .bpmnProcessId("order-process")
    .latestVersion()
    .variables(Map.of("orderId", "12345", "amount", 99.99))
    .send()
    .join();

// Por processDefinitionKey
ProcessInstanceEvent instance = client.newCreateInstanceCommand()
    .processDefinitionKey(2251799813685249L)
    .variables("{\"orderId\": \"12345\"}")  // JSON string también funciona
    .send()
    .join();

// Crear y esperar resultado
ProcessInstanceResult result = client.newCreateInstanceCommand()
    .bpmnProcessId("order-process")
    .latestVersion()
    .variables(Map.of("orderId", "12345"))
    .withResult()                    // ← Espera a que termine el proceso
    .requestTimeout(Duration.ofMinutes(5))
    .send()
    .join();
String resultVars = result.getVariables(); // Variables finales
```

### Cancelar Instancia

```java
client.newCancelInstanceCommand(processInstanceKey)
    .send()
    .join();
```

### Modificar Instancia (en vuelo)

```java
client.newModifyProcessInstanceCommand(processInstanceKey)
    .activateElement("task-B")                // Activar elemento
    .withVariables(Map.of("x", 1), "task-B") // Con variables
    .terminateElement(elementInstanceKey)      // Terminar elemento
    .send()
    .join();
```

### Migrar Instancia

```java
client.newMigrateProcessInstanceCommand(processInstanceKey)
    .migrationPlan(targetProcessDefinitionKey)
    .addMappingInstruction("old-task", "new-task")  // Mapeo de elementos
    .send()
    .join();
```

### Publicar Mensaje

```java
client.newPublishMessageCommand()
    .messageName("payment-received")
    .correlationKey("order-12345")     // Clave de correlación
    .variables(Map.of("paid", true))
    .timeToLive(Duration.ofMinutes(10))
    .messageId("unique-msg-id")        // Para deduplicación
    .tenantId("my-tenant")
    .send()
    .join();
```

### Broadcast Signal

```java
client.newBroadcastSignalCommand()
    .signalName("order-shipped")
    .variables(Map.of("trackingId", "ABC123"))
    .send()
    .join();
```

### Evaluar Decisión DMN

```java
EvaluateDecisionResponse result = client.newEvaluateDecisionCommand()
    .decisionId("risk-assessment")
    .variables(Map.of("age", 25, "income", 50000))
    .send()
    .join();
String output = result.getDecisionOutput();
```

### Set Variables

```java
client.newSetVariablesCommand(elementInstanceKey)
    .variables(Map.of("status", "approved"))
    .local(false)   // false = propagar al scope padre
    .send()
    .join();
```

### Resolver Incidente

```java
client.newResolveIncidentCommand(incidentKey)
    .send()
    .join();
```

### Borrar Recurso

```java
client.newDeleteResourceCommand(resourceKey)
    .send()
    .join();
```

## Job Workers

### Patrón Básico

```java
// Abrir worker para un tipo de job
JobWorker worker = client.newWorker()
    .jobType("payment-processing")
    .handler((jobClient, job) -> {
        // 1. Obtener variables del job
        Map<String, Object> variables = job.getVariablesAsMap();
        String orderId = (String) variables.get("orderId");
        
        // 2. Ejecutar lógica de negocio
        boolean success = processPayment(orderId);
        
        // 3. Completar o fallar el job
        if (success) {
            jobClient.newCompleteCommand(job)
                .variables(Map.of("paymentStatus", "completed"))
                .send()
                .join();
        } else {
            jobClient.newFailCommand(job)
                .retries(job.getRetries() - 1)
                .errorMessage("Payment failed")
                .retryBackoff(Duration.ofSeconds(30))
                .send()
                .join();
        }
    })
    .name("payment-worker-1")
    .timeout(Duration.ofMinutes(5))
    .maxJobsActive(32)                    // Jobs concurrentes
    .pollInterval(Duration.ofSeconds(1))   // Intervalo de polling
    .requestTimeout(Duration.ofSeconds(30))
    .fetchVariables("orderId", "amount")   // Solo traer estas variables
    .tenantIds("tenant-1", "tenant-2")
    .open();

// El worker se ejecuta en background
// Para cerrar:
worker.close();
```

### Job Streaming (Moderno)

```java
// Más eficiente que polling - el broker pushea jobs
JobWorker worker = client.newWorker()
    .jobType("payment-processing")
    .handler(handler)
    .streamEnabled(true)           // ← Habilitar streaming
    .streamTimeout(Duration.ofHours(1))
    .open();
```

### Lanzar Error BPMN

```java
// Desde un worker, lanzar un error BPMN (para boundary error events)
jobClient.newThrowErrorCommand(job)
    .errorCode("PAYMENT_DECLINED")
    .errorMessage("Card declined")
    .variables(Map.of("reason", "insufficient funds"))
    .send()
    .join();
```

### Actualizar Job

```java
// Actualizar retries
client.newUpdateRetriesCommand(jobKey)
    .retries(5)
    .send()
    .join();

// Actualizar timeout
client.newUpdateTimeoutCommand(jobKey)
    .timeout(Duration.ofMinutes(10))
    .send()
    .join();
```

## ActivatedJob - Datos del Job

```java
// Toda la info disponible en un job activado
job.getKey();                      // long: Key única del job
job.getType();                     // String: Tipo (ej: "payment")
job.getProcessInstanceKey();       // long: Instancia de proceso
job.getBpmnProcessId();            // String: ID del proceso BPMN
job.getProcessDefinitionVersion(); // int: Versión del proceso
job.getProcessDefinitionKey();     // long: Key del proceso
job.getElementId();                // String: ID del elemento BPMN
job.getElementInstanceKey();       // long: Key de la instancia del elemento
job.getCustomHeaders();            // Map: Headers definidos en BPMN
job.getWorker();                   // String: Worker asignado
job.getRetries();                  // int: Retries restantes
job.getDeadline();                 // long: Deadline en epoch ms
job.getVariables();                // String: Variables (JSON)
job.getVariablesAsMap();           // Map<String, Object>: Variables
job.getVariablesAsType(MyClass.class); // Deserialización typada
job.getTenantId();                 // String: Tenant
```

## Configuración del Cliente

```java
ZeebeClientBuilder builder = ZeebeClient.newClientBuilder()
    // Conexión
    .gatewayAddress("localhost:26500")          // Host:Port del gateway
    .usePlaintext()                             // Sin TLS
    .keepAlive(Duration.ofSeconds(45))          // Keep-alive gRPC
    
    // Timeouts
    .defaultRequestTimeout(Duration.ofSeconds(20))  // Timeout por defecto
    .defaultJobTimeout(Duration.ofMinutes(5))       // Timeout de jobs
    .defaultJobWorkerMaxJobsActive(32)              // Max jobs concurrentes
    .defaultJobPollInterval(Duration.ofMillis(100)) // Intervalo de poll
    
    // Serialización
    .defaultJobWorkerStreamEnabled(false)           // Streaming por defecto
    .withJsonMapper(customMapper)                   // Custom JSON mapper
    
    // Seguridad
    .caCertificatePath("/path/to/ca.cert.pem")     // CA certificate
    .credentialsProvider(provider)                   // Auth provider
    
    // Multi-tenancy
    .defaultJobWorkerTenantIds("t1", "t2")          // Tenants por defecto
    .defaultTenantId("default");                     // Tenant por defecto
```

## Patrón Async

Todas las operaciones devuelven `ZeebeFuture<T>`:

```java
// Síncrono (bloquea)
ProcessInstanceEvent result = client.newCreateInstanceCommand()
    .bpmnProcessId("my-process")
    .latestVersion()
    .send()
    .join();

// Asíncrono (no bloquea)
ZeebeFuture<ProcessInstanceEvent> future = client.newCreateInstanceCommand()
    .bpmnProcessId("my-process")
    .latestVersion()
    .send();

// Hacer otras cosas...

// Obtener resultado cuando esté listo
future.whenComplete((result, error) -> {
    if (error != null) {
        handleError(error);
    } else {
        handleSuccess(result);
    }
});
```

## Dependencias del Cliente

Solo necesitas:

```xml
<dependency>
    <groupId>io.camunda</groupId>
    <artifactId>zeebe-client-java</artifactId>
    <version>8.5.25</version>
</dependency>
```

Trae transitivamente: gRPC, Protobuf, Jackson, zeebe-bpmn-model.
