# 31 - Gateway: Embedded vs Standalone

## Resumen

Zeebe soporta dos modos de despliegue del Gateway:

1. **Embedded**: El gateway se arranca **dentro del proceso del broker** como un servicio más
2. **Standalone**: El gateway es un **proceso separado** que se conecta al cluster de brokers por red

En ambos casos se usa **exactamente la misma clase `Gateway`** — la diferencia está en cómo se configura, cómo se arranca, y cómo el `BrokerClient` se conecta al broker.

## Arquitectura General

```
┌─────────────────────── MODO EMBEDDED ───────────────────────┐
│                                                              │
│  Proceso Único (StandaloneBroker)                           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Broker                                               │   │
│  │  ├─ Particiones, Engine, LogStreams...                │   │
│  │  ├─ GatewayBrokerTransport (AtomixServerTransport)   │   │
│  │  │                                                    │   │
│  │  ├─ EmbeddedGatewayService ─────────────────────┐    │   │
│  │  │  ├─ Gateway (gRPC server :26500)              │    │   │
│  │  │  ├─ BrokerClient ──→ Atomix MessagingService  │    │   │
│  │  │  └─ JobStreamClient                           │    │   │
│  │  │                                                │    │   │
│  │  └─ REST API (Spring :8080) ──→ BrokerClient     │    │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Clientes ──→ gRPC :26500                                   │
│  Clientes ──→ REST :8080                                    │
└──────────────────────────────────────────────────────────────┘

┌─────────────────────── MODO STANDALONE ─────────────────────┐
│                                                              │
│  Proceso 1 (StandaloneBroker)           Proceso 2 (StandaloneGateway) │
│  ┌─────────────────────────────┐   ┌──────────────────────┐ │
│  │  Broker                      │   │  Gateway (gRPC :26500)│ │
│  │  ├─ Particiones, Engine...   │   │  ├─ BrokerClient     │ │
│  │  ├─ GatewayBrokerTransport   │←──┤  ├─ JobStreamClient  │ │
│  │  │                           │red│  │  ├─ AtomixCluster  │ │
│  │  └─ gateway.enable=false     │   │  └─ REST API (:8080) │ │
│  └─────────────────────────────┘   └──────────────────────┘ │
│                                                              │
│  Clientes ──→ gRPC :26500 (gateway)                         │
│  Clientes ──→ REST :8080 (gateway)                          │
└──────────────────────────────────────────────────────────────┘
```

## Clases Clave

|            Clase             |        Módulo         |                                 Rol                                  |
|------------------------------|-----------------------|----------------------------------------------------------------------|
| `Gateway`                    | `zeebe/gateway`       | Clase core del gateway (compartida por ambos modos)                  |
| `EmbeddedGatewayService`     | `zeebe/broker`        | Wrapper que gestiona el lifecycle del gateway dentro del broker      |
| `EmbeddedGatewayServiceStep` | `zeebe/broker`        | Step del bootstrap del broker que instancia el gateway embebido      |
| `EmbeddedGatewayCfg`         | `zeebe/broker`        | Configuración del gateway embebido (`extends GatewayCfg` + `enable`) |
| `StandaloneGateway`          | `dist`                | Aplicación Spring Boot para gateway separado                         |
| `StandaloneBroker`           | `dist`                | Aplicación Spring Boot para broker (con o sin gateway embebido)      |
| `BrokerClientImpl`           | `zeebe/broker-client` | Cliente que envía requests al broker vía Atomix MessagingService     |
| `GatewayBrokerTransportStep` | `zeebe/broker`        | Crea el transporte que recibe comandos del gateway (siempre activo)  |
| `BrokerStartupProcess`       | `zeebe/broker`        | Orquesta el arranque del broker, decide si incluir el gateway        |
| `SpringBrokerBridge`         | `zeebe/broker`        | Puente para exponer servicios del broker a beans Spring              |
| `SpringGatewayBridge`        | `zeebe/gateway`       | Puente para exponer servicios del gateway a beans Spring             |

## Ubicación del Código

```
dist/src/main/java/io/camunda/zeebe/
├── broker/
│   ├── StandaloneBroker.java              ← Entry point del broker
│   ├── BrokerClientConfiguration.java     ← Crea BrokerClient para el broker
│   └── shared/
│       └── BrokerConfiguration.java       ← Config Spring + disable REST si gateway off
├── gateway/
│   ├── StandaloneGateway.java             ← Entry point del gateway standalone
│   ├── GatewayConfiguration.java          ← Config Spring del gateway
│   ├── GatewayClusterConfiguration.java   ← Crea AtomixCluster para gateway
│   ├── BrokerClientComponent.java         ← Crea BrokerClient para gateway standalone
│   ├── JobStreamComponent.java            ← Crea JobStreamClient
│   └── TopologyServices.java              ← Servicios de topología

zeebe/broker/src/main/java/io/camunda/zeebe/broker/
├── system/
│   ├── EmbeddedGatewayService.java        ← Servicio que envuelve Gateway
│   └── configuration/
│       ├── BrokerCfg.java                 ← Config del broker (contiene EmbeddedGatewayCfg)
│       └── EmbeddedGatewayCfg.java        ← Config del gateway embebido
├── bootstrap/
│   ├── BrokerStartupProcess.java          ← Secuencia de arranque
│   ├── EmbeddedGatewayServiceStep.java    ← Step que arranca el gateway
│   └── GatewayBrokerTransportStep.java    ← Step que crea transporte gateway↔broker
└── SpringBrokerBridge.java                ← Puente broker → Spring

zeebe/gateway/src/main/java/io/camunda/zeebe/gateway/
├── Gateway.java                           ← Clase principal del gateway (ambos modos)
└── impl/
    └── SpringGatewayBridge.java           ← Puente gateway → Spring

zeebe/broker-client/src/main/java/io/camunda/zeebe/broker/client/
├── api/BrokerClient.java                  ← Interfaz
└── impl/BrokerClientImpl.java             ← Implementación con Atomix
```

## Modo Embedded: Cómo Funciona

### 1. Configuración

La configuración vive dentro de `BrokerCfg` como una propiedad `gateway` de tipo `EmbeddedGatewayCfg`:

```java
// BrokerCfg.java
public class BrokerCfg {
    private EmbeddedGatewayCfg gateway = new EmbeddedGatewayCfg();
    // ...
}
```

`EmbeddedGatewayCfg` extiende `GatewayCfg` (la config base del gateway) y añade un flag `enable`:

```java
// EmbeddedGatewayCfg.java
public final class EmbeddedGatewayCfg extends GatewayCfg implements ConfigurationEntry {
    private boolean enable = true;  // ← HABILITADO POR DEFECTO

    @Override
    public void init(final BrokerCfg globalConfig, final String brokerBase) {
        final NetworkCfg networkCfg = globalConfig.getNetwork();

        // Usa el host del broker para el gateway
        init(networkCfg.getHost());

        // Configura el contact point al broker LOCAL
        getCluster().setInitialContactPoints(
            Collections.singletonList(
                NetUtil.toSocketAddressString(networkCfg.getInternalApi().getAddress())));

        // Ajusta el puerto con el offset del broker
        getNetwork().setPort(getNetwork().getPort() + (networkCfg.getPortOffset() * 10));
    }
}
```

**Punto clave**: `EmbeddedGatewayCfg.init()` configura automáticamente el gateway para apuntar al broker local. No necesitas configurar contact points manualmente.

### 2. Decisión de Arranque

En `BrokerStartupProcess`, la secuencia de startup del broker incluye condicionalmente el step del gateway:

```java
// BrokerStartupProcess.java
private List<StartupStep<BrokerStartupContext>> buildStartupSteps(final BrokerCfg config) {
    final var result = new ArrayList<>();

    result.add(new ClusterServicesStep());
    result.add(new ClusterTopologyManagerStep());
    result.add(new DiskSpaceUsageMonitorStep());
    result.add(new MonitoringServerStep());
    result.add(new ApiMessagingServiceStep());
    result.add(new GatewayBrokerTransportStep());    // ← SIEMPRE se ejecuta
    result.add(new CommandApiServiceStep());

    if (config.getGateway().isEnable()) {            // ← CONDICIONAL
        result.add(new EmbeddedGatewayServiceStep());
    }

    result.add(new JobStreamServiceStep());
    result.add(new PartitionManagerStep());
    result.add(new BrokerAdminServiceStep());

    return result;
}
```

**Nota importante**: `GatewayBrokerTransportStep` se ejecuta **siempre**, incluso si el gateway embebido está deshabilitado. Esto es porque el transporte es necesario para que gateways standalone puedan conectarse al broker.

### 3. Arranque del Gateway Embebido

`EmbeddedGatewayServiceStep` crea los componentes necesarios:

```java
// EmbeddedGatewayServiceStep.java
void startupInternal(...) {
    // 1. Crea JobStreamClient usando los servicios del cluster del broker
    final var jobStreamClient = new JobStreamClientImpl(
        scheduler,
        clusterServices.getCommunicationService(),
        brokerStartupContext.getMeterRegistry());

    // 2. Crea EmbeddedGatewayService, que internamente crea Gateway
    final var embeddedGatewayService = new EmbeddedGatewayService(
        shutdownTimeout,
        configuration,            // BrokerCfg (contiene EmbeddedGatewayCfg)
        identityConfiguration,
        scheduler,
        concurrencyControl,
        jobStreamClient,
        brokerClient,             // ← BrokerClient ya creado por Spring
        meterRegistry);

    // 3. Arranca el gateway
    embeddedGatewayService.start();
}
```

`EmbeddedGatewayService.start()` hace dos cosas:

```java
// EmbeddedGatewayService.java
public ActorFuture<Gateway> start() {
    // 1. Inicia el JobStreamClient y lo registra como listener de topología
    concurrencyControl.runOnCompletion(
        jobStreamClient.start(),
        (ok, error) -> brokerClient.getTopologyManager()
                                   .addTopologyListener(jobStreamClient));

    // 2. Arranca el Gateway (server gRPC)
    return gateway.start();
}
```

### 4. REST API en Modo Embedded

El `StandaloneBroker` escanea el paquete `io.camunda.zeebe.gateway.rest`:

```java
@SpringBootApplication(
    scanBasePackages = {
        "io.camunda.zeebe.broker",
        "io.camunda.zeebe.shared",
        "io.camunda.zeebe.gateway.rest"   // ← Incluye REST controllers
    })
public class StandaloneBroker { ... }
```

Los controllers REST usan `@ConditionalOnRestGatewayEnabled`, que se desactiva cuando `zeebe.broker.gateway.enable=false`:

```java
// BrokerConfiguration.java
@ConditionalOnProperty(prefix = "zeebe.broker.gateway", name = "enable", havingValue = "false")
@Bean
public RestGatewayDisabled disableRestGateway() {
    return new RestGatewayDisabled();  // Este bean hace que @ConditionalOnRestGatewayEnabled = false
}
```

Es decir: si el gateway embebido está deshabilitado, la REST API del broker **también se deshabilita automáticamente**.

## Modo Standalone: Cómo Funciona

### 1. Entry Point

`StandaloneGateway` es una aplicación Spring Boot independiente:

```java
@SpringBootApplication(
    scanBasePackages = {
        "io.camunda.zeebe.gateway",
        "io.camunda.zeebe.shared",
        "io.camunda.zeebe.util.liveness"
    })
public class StandaloneGateway implements CommandLineRunner {

    public static void main(final String[] args) {
        final var application = MainSupport.createDefaultApplicationBuilder()
            .sources(StandaloneGateway.class)
            .profiles(Profile.GATEWAY.getId())  // ← Perfil "gateway"
            .build(args);
        application.run();
    }
}
```

### 2. Cluster Propio

A diferencia del embebido (que reutiliza el cluster Atomix del broker), el gateway standalone **crea su propio `AtomixCluster`** para unirse al cluster de brokers:

```java
// GatewayClusterConfiguration.java
@Bean(destroyMethod = "stop")
public AtomixCluster atomixCluster(final ClusterConfig config) {
    return new AtomixCluster(config, Version.from(VersionUtil.getVersion()), meterRegistry);
}
```

El cluster se configura con `initialContactPoints` que apuntan a los brokers:

```yaml
# gateway.yaml.template
zeebe:
  gateway:
    cluster:
      initialContactPoints: [127.0.0.1:26502]  # Puerto interno del broker
      memberId: gateway
      clusterName: zeebe-cluster
```

### 3. BrokerClient Propio

El gateway standalone crea su propio `BrokerClient` que se comunica por red:

```java
// BrokerClientComponent.java
@Bean(destroyMethod = "close")
public BrokerClient brokerClient() {
    return new BrokerClientImpl(
        config.getCluster().getRequestTimeout(),
        atomixCluster.getMessagingService(),  // ← Red (Atomix messaging)
        atomixCluster.getEventService(),
        actorScheduler,
        topologyManager,
        metrics);
}
```

### 4. Secuencia de Arranque

```java
// StandaloneGateway.run()
@Override
public void run(final String... args) throws Exception {
    // 1. Arranca el cluster Atomix (se une al cluster de brokers)
    atomixCluster.start();

    // 2. Arranca el job stream client
    jobStreamClient.start().join();

    // 3. Registra listener de topología
    brokerClient.getTopologyManager().addTopologyListener(jobStreamClient);

    // 4. Crea y arranca el Gateway (misma clase que el embebido)
    gateway = new Gateway(
        configuration.shutdownTimeout(),
        configuration.config(),
        identityConfiguration,
        brokerClient,
        actorScheduler,
        jobStreamClient.streamer(),
        meterRegistry);

    // 5. Registra bridges para Spring
    springGatewayBridge.registerGatewayStatusSupplier(gateway::getStatus);
    springGatewayBridge.registerClusterStateSupplier(...);
    springGatewayBridge.registerJobStreamClient(() -> jobStreamClient);

    // 6. Arranca el server gRPC
    gateway.start().join(30, TimeUnit.SECONDS);
}
```

## Comparación Detallada

|           Aspecto           |                               Embedded                                |                  Standalone                   |
|-----------------------------|-----------------------------------------------------------------------|-----------------------------------------------|
| **Proceso**                 | Mismo proceso que el broker                                           | Proceso separado                              |
| **Entry point**             | `StandaloneBroker`                                                    | `StandaloneGateway`                           |
| **Spring Profile**          | `broker`                                                              | `gateway`                                     |
| **Clase Gateway**           | `Gateway` (misma)                                                     | `Gateway` (misma)                             |
| **Config prefix**           | `zeebe.broker.gateway.*`                                              | `zeebe.gateway.*`                             |
| **Enable/Disable**          | `zeebe.broker.gateway.enable=true/false`                              | N/A (si ejecutas el proceso, está activo)     |
| **Default**                 | Habilitado (`enable=true`)                                            | N/A                                           |
| **AtomixCluster**           | Reutiliza el del broker                                               | Crea el suyo propio                           |
| **BrokerClient**            | `BrokerClientImpl` (Spring bean del broker)                           | `BrokerClientImpl` (Spring bean del gateway)  |
| **Comunicación con broker** | Atomix MessagingService (in-process, pero igualmente pasa por Atomix) | Atomix MessagingService (por red)             |
| **REST API**                | Incluida (via `@ComponentScan` de `gateway.rest`)                     | Incluida (en el mismo proceso)                |
| **Deshabilitar REST**       | Automático si `gateway.enable=false`                                  | No aplica                                     |
| **Contact points**          | Auto-configurados al broker local                                     | Manual: `initialContactPoints`                |
| **Port gRPC**               | 26500 (ajustado por `portOffset`)                                     | 26500                                         |
| **JobStreamClient**         | Creado en `EmbeddedGatewayServiceStep`                                | Creado via Spring `@Bean`                     |
| **Tolerancia a fallos**     | Si el broker cae, el gateway cae                                      | Si un broker cae, el gateway redirige a otros |
| **Escalado independiente**  | No es posible                                                         | Sí, puedes tener N gateways para M brokers    |

## Detalle Importante: La Comunicación NO es "Directa"

Aunque el gateway embebido vive en el mismo proceso que el broker, la comunicación **no es una llamada directa en memoria**. El `BrokerClient` del gateway embebido usa igualmente `BrokerClientImpl` con `AtomixClientTransportAdapter`, que envía mensajes a través del `MessagingService` de Atomix.

```
Gateway (embebido) → BrokerClientImpl 
                      → AtomixClientTransportAdapter 
                        → Atomix MessagingService
                          → (in-process, no sale a la red)
                            → AtomixServerTransport (GatewayBrokerTransportStep)
                              → CommandApiService → Engine
```

La diferencia es que en modo embebido, el `MessagingService` detecta que el destino es el mismo nodo (mismo `MemberId`) y hace la entrega **in-process sin pasar por la red TCP**. En modo standalone, el mensaje viaja por red TCP.

Esto significa que:
- El overhead del embebido es **menor** (evita serialización de red), pero **no es cero** (aún serializa/deserializa mensajes Atomix)
- El modelo es **uniforme**: el código del gateway no sabe ni le importa si está embebido o no

## Flujo de un Request de Cliente

```
                                 EMBEDDED                          STANDALONE
                                 ========                          ==========

1. Cliente gRPC          → Gateway :26500                   → Gateway :26500
                           (mismo proceso del broker)         (proceso separado)

2. GatewayGrpcService    → EndpointManager                 → EndpointManager

3. EndpointManager       → BrokerClient.sendRequest()      → BrokerClient.sendRequest()

4. BrokerRequestManager  → Busca líder en topología        → Busca líder en topología

5. Transport             → AtomixClientTransportAdapter    → AtomixClientTransportAdapter
                           → MessagingService               → MessagingService
                           → IN-PROCESS delivery            → TCP delivery a broker remoto

6. Broker recibe         → AtomixServerTransport           → AtomixServerTransport
                           → CommandApiService              → CommandApiService
                           → Engine procesa                 → Engine procesa

7. Respuesta regresa     → Misma ruta en reversa           → Misma ruta en reversa
```

## Configuración YAML

### Broker con Gateway Embebido (default)

```yaml
zeebe:
  broker:
    gateway:
      enable: true                    # Default, se puede omitir
      network:
        host: 0.0.0.0
        port: 26500                   # Puerto gRPC del gateway
      cluster:
        requestTimeout: 15s
      security:
        enabled: false
      longPolling:
        enabled: true
    network:
      host: 0.0.0.0
      port: 26501                     # Puerto de comando del broker
```

**Variables de entorno**:
- `ZEEBE_BROKER_GATEWAY_ENABLE=true`
- `ZEEBE_BROKER_GATEWAY_NETWORK_PORT=26500`

### Broker sin Gateway (para usar gateway standalone)

```yaml
zeebe:
  broker:
    gateway:
      enable: false                   # ← Deshabilita el gateway embebido y la REST API
    network:
      host: 0.0.0.0
```

### Gateway Standalone

```yaml
zeebe:
  gateway:
    network:
      host: 0.0.0.0
      port: 26500
    cluster:
      initialContactPoints: [broker1:26502, broker2:26502]  # Puerto interno de los brokers
      memberId: gateway
      clusterName: zeebe-cluster
      requestTimeout: 15s
    security:
      enabled: false
    longPolling:
      enabled: true
```

**Variables de entorno**:
- `ZEEBE_GATEWAY_NETWORK_PORT=26500`
- `ZEEBE_GATEWAY_CLUSTER_INITIALCONTACTPOINTS=broker1:26502,broker2:26502`

## ¿Cuándo Usar Cada Modo?

### Embedded (default) — Para desarrollo y clusters pequeños

- Setup más simple: un solo proceso
- Menor latencia (comunicación in-process)
- Menos componentes que gestionar
- Útil para desarrollo local, testing, y producción simple

### Standalone — Para producción a escala

- **Escalado independiente**: puedes tener 3 brokers y 5 gateways (o al revés)
- **Aislamiento de fallos**: si un broker cae, los gateways redirigen a otros
- **Balanceo de carga**: múltiples gateways detrás de un load balancer
- **Recursos dedicados**: los brokers usan toda su CPU/memoria para procesamiento

## Diagrama de Secuencia de Arranque

### Modo Embedded

```
StandaloneBroker.run()
  │
  └→ new Broker(systemContext, springBrokerBridge)
      │
      └→ BrokerStartupProcess.start()
          │
          ├→ ClusterServicesStep         → Arranca Atomix cluster
          ├→ ClusterTopologyManagerStep  → Manager de topología
          ├→ DiskSpaceUsageMonitorStep   → Monitor de disco
          ├→ MonitoringServerStep        → Métricas
          ├→ ApiMessagingServiceStep     → Servicio de mensajería
          ├→ GatewayBrokerTransportStep  → AtomixServerTransport (recibe de gateways)
          ├→ CommandApiServiceStep       → Servicio de comandos
          │
          ├→ [SI gateway.enable=true]
          │   └→ EmbeddedGatewayServiceStep
          │       ├→ new JobStreamClientImpl(...)
          │       ├→ new EmbeddedGatewayService(...)
          │       │   └→ new Gateway(config, brokerClient, ...)
          │       └→ embeddedGatewayService.start()
          │           ├→ jobStreamClient.start()
          │           └→ gateway.start()
          │               ├→ Crea ActivateJobsHandler
          │               ├→ Crea StreamJobsHandler
          │               ├→ Crea gRPC Server en :26500
          │               └→ server.start()
          │
          ├→ JobStreamServiceStep        → Servicio de job streaming
          ├→ PartitionManagerStep        → Gestiona particiones
          └→ BrokerAdminServiceStep      → API de administración
```

### Modo Standalone

```
StandaloneGateway.run()
  │
  ├→ atomixCluster.start()               → Se une al cluster de brokers
  │
  ├→ jobStreamClient.start()             → Inicia streaming de jobs
  │
  ├→ topologyManager.addTopologyListener(jobStreamClient)
  │
  ├→ new Gateway(config, brokerClient, ...)
  │
  ├→ springGatewayBridge.register*(...)   → Registra suppliers
  │
  └→ gateway.start()
      ├→ Crea ActivateJobsHandler
      ├→ Crea StreamJobsHandler
      ├→ Crea gRPC Server en :26500
      └→ server.start()
```

