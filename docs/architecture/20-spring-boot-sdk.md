# 20 - Spring Boot SDK

## Propósito

El módulo `spring-boot-starter-camunda-sdk` proporciona integración de Zeebe con Spring Boot mediante:
- Auto-configuración del cliente
- Anotación `@JobWorker` para crear workers declarativamente
- Health indicators y métricas
- Soporte para Camunda Cloud y self-managed

## Estructura

```
spring-boot-starter-camunda-sdk/src/main/java/io/camunda/zeebe/spring/client/
├── CamundaAutoConfiguration.java    ← Auto-config principal
├── properties/
│   └── ZeebeClientConfigurationProperties.java  ← Propiedades
├── annotation/
│   └── (en zeebe-client-spring)
│       └── JobWorker.java           ← Anotación @JobWorker
├── jobhandling/                     ← Procesamiento de @JobWorker
├── actuator/                        ← Health/Metrics
└── configuration/                   ← JsonMapper, métricas
```

## @JobWorker: Crear Workers Declarativamente

### Anotación

```java
@JobWorker(
    type = "payment",              // Tipo del job (obligatorio o por nombre del método)
    name = "payment-worker",       // Nombre del worker
    timeout = 30000,               // Timeout en ms
    maxJobsActive = 10,            // Jobs activos máximos
    pollInterval = 100,            // Intervalo de polling en ms
    requestTimeout = 30,           // Timeout de request en segundos
    fetchVariables = {"amount", "orderId"},  // Variables a traer
    fetchAllVariables = false,     // Traer todas las variables
    autoComplete = true,           // Completar job automáticamente
    enabled = true,                // Habilitado
    tenantIds = {}                 // Tenants autorizados
)
```

### Uso Simple

```java
@Component
public class PaymentWorker {

    @JobWorker(type = "process-payment")
    public void handlePayment(final ActivatedJob job) {
        Map<String, Object> variables = job.getVariablesAsMap();
        double amount = (double) variables.get("amount");
        
        // Procesar pago...
        
        // Si autoComplete=true, el job se completa automáticamente
        // al retornar sin excepción
    }
}
```

### Uso con Variables de Salida

```java
@JobWorker(type = "calculate-discount")
public Map<String, Object> calculateDiscount(final ActivatedJob job) {
    double total = (double) job.getVariablesAsMap().get("total");
    double discount = total > 100 ? 0.1 : 0.0;
    
    // Retornar variables = se completa con estas variables
    return Map.of("discount", discount, "finalAmount", total * (1 - discount));
}
```

### Uso con Control Manual

```java
@JobWorker(type = "manual-task", autoComplete = false)
public void manualTask(final JobClient client, final ActivatedJob job) {
    try {
        // Procesar...
        client.newCompleteCommand(job.getKey())
            .variables(Map.of("result", "ok"))
            .send()
            .join();
    } catch (Exception e) {
        client.newFailCommand(job.getKey())
            .retries(job.getRetries() - 1)
            .errorMessage(e.getMessage())
            .send()
            .join();
    }
}
```

## Configuración

### application.yaml

```yaml
zeebe:
  client:
    # Modo de conexión
    connection-mode: ADDRESS        # ADDRESS | CLOUD
    
    # Conexión directa
    broker:
      gateway-address: localhost:26500
      keep-alive: PT45S
    
    # Camunda Cloud
    cloud:
      cluster-id: "xxx-xxx-xxx"
      region: "bru-2"
      client-id: "my-client"
      client-secret: "secret"
    
    # Workers
    worker:
      max-jobs-active: 32
      default-name: "spring-worker"
      default-type: ""
    
    # Seguridad
    security:
      plaintext: true              # Desactivar TLS (dev)
      cert-path: ""                # Path a certificado
    
    # Multi-tenancy
    default-tenant-id: "<default>"
    
    # Feature flags
    enabled: true                   # Habilitar Spring starter
```

### Camunda Cloud

```yaml
zeebe:
  client:
    connection-mode: CLOUD
    cloud:
      cluster-id: "abc-123-def"
      region: "bru-2"
      client-id: "my-app"
      client-secret: "s3cr3t"
```

## Auto-Configuración

### CamundaAutoConfiguration

```java
@AutoConfiguration
@Import({
    JsonMapperConfiguration.class,       // Jackson integration
    ZeebeActuatorConfiguration.class,    // Health indicators
    MetricsDefaultConfiguration.class    // Micrometer metrics
})
public class CamundaAutoConfiguration {
    // Registra ZeebeLifecycleEventProducer
    // Publica eventos Spring para lifecycle del client
}
```

### Beans Registrados

| Bean | Propósito |
|------|----------|
| `ZeebeClient` | Cliente Zeebe configurado |
| `JsonMapper` | Serialización Jackson |
| `ZeebeLifecycleEventProducer` | Eventos de lifecycle |
| `HealthIndicator` | Health check del cluster |
| `MeterRegistry` | Métricas Micrometer |

## Lifecycle con Spring

```
1. Spring Boot arranca
   ↓
2. CamundaAutoConfiguration carga
   ↓
3. ZeebeClient se crea con propiedades de config
   ↓
4. Scanner encuentra métodos con @JobWorker
   ├─ Crea JobWorkerBuilder por cada @JobWorker
   └─ Registra handlers
   ↓
5. Workers se abren y empiezan a poll/stream
   ↓
6. Application running...
   ↓
7. Spring shutdown → workers se cierran → client se cierra
```

## Health Indicator

```
GET /actuator/health

{
  "status": "UP",
  "components": {
    "zeebe": {
      "status": "UP",
      "details": {
        "clusterSize": 3,
        "partitionsCount": 3,
        "replicationFactor": 3,
        "gatewayVersion": "8.5.25"
      }
    }
  }
}
```

## Métricas

Integración con Micrometer:
- `zeebe.client.worker.job.completed` - Jobs completados
- `zeebe.client.worker.job.failed` - Jobs fallidos
- `zeebe.client.worker.job.activated` - Jobs activados
- Etiquetas: `job.type`, `worker.name`

## Ejemplo Completo

```java
@SpringBootApplication
public class MyApp {
    public static void main(String[] args) {
        SpringApplication.run(MyApp.class, args);
    }
}

@Component
public class OrderWorkers {

    @JobWorker(type = "validate-order", fetchVariables = {"orderId", "items"})
    public Map<String, Object> validateOrder(final ActivatedJob job) {
        String orderId = (String) job.getVariablesAsMap().get("orderId");
        List<?> items = (List<?>) job.getVariablesAsMap().get("items");
        
        boolean valid = items != null && !items.isEmpty();
        return Map.of("valid", valid);
    }

    @JobWorker(type = "process-payment")
    public Map<String, Object> processPayment(final ActivatedJob job) {
        double amount = (double) job.getVariablesAsMap().get("amount");
        // Llamar API de pagos...
        return Map.of("paid", true, "transactionId", "TX-" + System.currentTimeMillis());
    }

    @JobWorker(type = "ship-order")
    public void shipOrder(final ActivatedJob job) {
        String orderId = (String) job.getVariablesAsMap().get("orderId");
        // Llamar API de envío...
        // autoComplete = true por defecto
    }
}
```
