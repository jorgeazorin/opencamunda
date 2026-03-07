# 01 - Arquitectura General

## Diagrama de Alto Nivel

```
┌─────────────────────────────────────────────────────────────────────┐
│                           CLIENTES                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │
│  │ Java SDK │  │  Go SDK  │  │  zbctl   │  │ Spring Boot SDK  │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬─────────┘   │
└───────┼──────────────┼──────────────┼─────────────────┼─────────────┘
        │              │              │                  │
        │         gRPC (26500) / REST (8080)             │
        │              │              │                  │
┌───────┼──────────────┼──────────────┼─────────────────┼─────────────┐
│       ▼              ▼              ▼                  ▼             │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │                    GATEWAY                               │        │
│  │  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │        │
│  │  │  gRPC   │  │   REST   │  │Intercept.│  │  Auth   │ │        │
│  │  │ Service │  │  Control │  │          │  │         │ │        │
│  │  └────┬────┘  └────┬─────┘  └──────────┘  └─────────┘ │        │
│  └───────┼─────────────┼──────────────────────────────────┘        │
│          │             │                                            │
│          ▼             ▼                                            │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │              BROKER (Servidor Zeebe)                     │        │
│  │                                                          │        │
│  │  ┌─────────────────────────────────────────────────┐    │        │
│  │  │              PARTICIÓN (x N)                      │    │        │
│  │  │                                                   │    │        │
│  │  │  ┌───────────┐     ┌──────────────────┐          │    │        │
│  │  │  │   Raft    │────▶│  Log (Journal)   │          │    │        │
│  │  │  │ Consensus │     │  append-only     │          │    │        │
│  │  │  └───────────┘     └────────┬─────────┘          │    │        │
│  │  │                             │                     │    │        │
│  │  │                   ┌─────────▼──────────┐          │    │        │
│  │  │                   │  Stream Processor  │          │    │        │
│  │  │                   │  (Lee comandos,    │          │    │        │
│  │  │                   │   genera eventos)  │          │    │        │
│  │  │                   └─────────┬──────────┘          │    │        │
│  │  │                             │                     │    │        │
│  │  │              ┌──────────────┼──────────────┐      │    │        │
│  │  │              ▼              ▼               ▼      │    │        │
│  │  │  ┌──────────────┐  ┌──────────────┐  ┌────────┐  │    │        │
│  │  │  │    ENGINE     │  │    STATE     │  │EXPORTER│  │    │        │
│  │  │  │  (Procesa    │  │  (RocksDB)   │  │        │  │    │        │
│  │  │  │   BPMN)      │  │              │  │  ES/OS │  │    │        │
│  │  │  └──────────────┘  └──────────────┘  └────────┘  │    │        │
│  │  │                                                   │    │        │
│  │  │  ┌──────────────────────────────────────────┐     │    │        │
│  │  │  │            SNAPSHOT STORE                  │     │    │        │
│  │  │  │  (Checkpoint periódico del estado)         │     │    │        │
│  │  │  └──────────────────────────────────────────┘     │    │        │
│  │  └───────────────────────────────────────────────┘    │        │
│  │                                                          │        │
│  │  ┌──────────┐  ┌────────────┐  ┌───────────────┐       │        │
│  │  │ Topology │  │  Backup    │  │   Transport   │       │        │
│  │  │ Manager  │  │  Manager   │  │   (Netty)     │       │        │
│  │  └──────────┘  └────────────┘  └───────────────┘       │        │
│  └─────────────────────────────────────────────────────────┘        │
│                           NODO ZEEBE                                 │
└─────────────────────────────────────────────────────────────────────┘

         │                    │                      │
         ▼                    ▼                      ▼
    ┌─────────┐       ┌──────────────┐        ┌──────────┐
    │  Nodo 2 │       │   Nodo 3     │        │ Elastic/ │
    │ (Raft   │       │   (Raft      │        │ OpenSrch │
    │ follower│       │    follower)  │        │          │
    └─────────┘       └──────────────┘        └──────────┘
```

## Patrón Fundamental: Command-Event Sourcing

Zeebe usa un patrón de **Command Sourcing** donde:

1. **Todo es un record** en un log append-only
2. Un **Command** es una intención (ej: "crear instancia de proceso")
3. Un **Event** es un hecho consumado (ej: "instancia de proceso creada")
4. Un **Rejection** es un comando rechazado (ej: "proceso no existe")

### Flujo de un Comando

```
1. Cliente envía comando via gRPC
   │
2. Gateway recibe y lo reenvía al Broker líder de la partición correcta
   │
3. Broker escribe el COMMAND en el log
   │
4. Raft replica el command a los followers
   │
5. Stream Processor lee el command del log
   │
6. Engine procesa el command:
   │  ├─ Valida contra el estado actual (RocksDB)
   │  ├─ Si es válido → genera EVENT(s)  
   │  └─ Si es inválido → genera REJECTION
   │
7. Los events se escriben en el log
   │
8. Los events actualizan el estado en RocksDB (via State Appliers)
   │
9. Los exporters envían los events a Elasticsearch/OpenSearch
   │
10. La respuesta vuelve al cliente via Gateway
```

### Tipos de Records

|  Intent   |     Descripción     |                Ejemplo                 |
|-----------|---------------------|----------------------------------------|
| COMMAND   | Intención de cambio | `CREATE ProcessInstance`               |
| EVENT     | Hecho consumado     | `CREATED ProcessInstance`              |
| REJECTION | Comando rechazado   | `REJECTED ProcessInstance (not found)` |

## Componentes Principales

### 1. Gateway (`zeebe/gateway/`, `zeebe/gateway-rest/`)

**Responsabilidad**: Punto de entrada para clientes

- Expone API **gRPC** (puerto 26500) y **REST** (puerto 8080)
- Traduce peticiones de clientes a comandos internos
- Rutea comandos a la partición correcta
- Gestiona autenticación e interceptores
- NO procesa lógica de negocio

**Archivos clave**:
- `zeebe/gateway-protocol/src/main/proto/` → definiciones .proto
- `zeebe/gateway/src/main/java/.../grpc/` → servicios gRPC
- `zeebe/gateway-rest/src/main/java/.../rest/` → controladores REST

### 2. Broker (`zeebe/broker/`)

**Responsabilidad**: Servidor que gestiona particiones

- Gestiona el ciclo de vida de **particiones**
- Coordina el **consenso Raft** entre nodos
- Orquesta el **stream processing** (lectura del log → procesamiento → escritura)
- Gestiona **snapshots** y **backups**
- Expone endpoints de **admin** y **health**

**Subsistemas del Broker**:

```
broker/
├── bootstrap/     → Secuencia de arranque (pasos ordenados)
├── clustering/    → Configuración del cluster
├── engine/        → Integración con el motor de workflow
├── exporter/      → Gestión de exporters
├── jobstream/     → Streaming de jobs a workers
├── logstreams/    → Adaptador para el log (AtomixLogStorage)
├── partitioning/  → Ciclo de vida de particiones
├── raft/          → Validación de entradas Raft
├── system/        → Monitorización y sistema
└── transport/     → Capa de transporte
```

### 3. Engine (`zeebe/engine/`) — `zeebe-workflow-engine`

**Responsabilidad**: Motor de ejecución BPMN

- Procesa **comandos** y genera **eventos**
- Ejecuta la lógica de cada **elemento BPMN**
- Gestiona **estado** de instancias de proceso, jobs, mensajes, timers
- Evalúa **expresiones** FEEL
- Ejecuta **decisiones** DMN

**Dos subsistemas principales**:

#### Processing (`engine/processing/`)

Contiene los **procesadores de comandos** organizados por dominio:

```
processing/
├── bpmn/           → Ejecución de elementos BPMN
├── deployment/     → Despliegue de procesos/decisiones
├── job/            → Ciclo de vida de jobs
├── message/        → Correlación de mensajes
├── processinstance/ → Ciclo de vida de instancias
├── timer/          → Timers y delays
├── usertask/       → Tareas de usuario
├── variable/       → Variables de proceso
├── incident/       → Gestión de incidentes
├── signal/         → Señales
├── dmn/            → Decisiones
└── streamprocessor/ → Framework de procesamiento
```

#### State (`engine/state/`)

Gestiona el **estado persistente** en RocksDB:

```
state/
├── appliers/      → Aplican eventos al estado (actualizan RocksDB)
├── instance/      → Estado de instancias de proceso
├── deployment/    → Estado de deployments
├── message/       → Estado de mensajes y correlaciones
├── variable/      → Estado de variables
├── migration/     → Migraciones de esquema
└── mutable/       → Interfaces de estado mutable
```

### 4. Protocolo (`zeebe/protocol/`, `zeebe/protocol-impl/`)

**Responsabilidad**: Formato binario de serialización

- Define los **tipos de records** (ProcessInstance, Job, Message, etc.)
- Usa **SBE** (Simple Binary Encoding) para serialización eficiente
- Define las **column families** de RocksDB (`ZbColumnFamilies`)
- Define las **intents** de cada tipo de record

### 5. Log y Streaming (`zeebe/logstreams/`, `zeebe/stream-platform/`)

**Responsabilidad**: Almacenamiento y procesamiento del log de eventos

- **Logstreams**: Abstracción append-only sobre el journal/Raft
- **Stream Platform**: Framework que lee records del log, los procesa, y escribe resultados

### 6. Almacenamiento (`zeebe/zb-db/`, `zeebe/snapshot/`, `zeebe/journal/`)

**Responsabilidad**: Persistencia

- **zb-db**: Abstracción sobre RocksDB con typed column families
- **snapshot**: Snapshots periódicos del estado para recovery rápido
- **journal**: Write-ahead log para durabilidad

### 7. Clustering (`zeebe/atomix/`, `zeebe/topology/`)

**Responsabilidad**: Distribución y consenso

- **Atomix**: Implementación de Raft + SWIM failure detection
- **Topology**: Gestión de la topología del cluster y distribución de particiones

## Flujo de Datos Detallado

### Deploy de un Proceso

```
1. Cliente: deployProcess("myProcess.bpmn")
2. Gateway: → Broker (partición 1, deployment partition)
3. Log: COMMAND DeploymentCreate
4. Engine: Parsea BPMN, valida, asigna keys
5. Log: EVENT DeploymentCreated
6. Engine: Distribuye a otras particiones
7. Estado: Guarda proceso en DeploymentState
8. Exporter: → Elasticsearch
9. Gateway: ← Respuesta al cliente con processDefinitionKey
```

### Crear Instancia de Proceso

```
1. Cliente: createProcessInstance("myProcess", variables)
2. Gateway: → Broker (partición por hash de key)
3. Log: COMMAND ProcessInstanceCreate
4. Engine: 
   - Busca proceso en estado
   - Crea instancia
   - Activa start event
   - Activa siguiente(s) elemento(s)
5. Log: 
   - EVENT ProcessInstanceCreated
   - EVENT ElementActivating (start event)
   - EVENT ElementCompleted (start event)  
   - EVENT ElementActivating (next element)
   - ... (cascade de eventos)
6. Estado: Actualiza instancias, elementos activos, variables
```

### Ejecutar un Service Task (Job)

```
1. Engine activa un Service Task
2. Log: EVENT JobCreated (con type, retries, variables)
3. Estado: Job guardado en JobState

4. Worker: activateJobs(type="myTask", maxJobsToActivate=10)
5. Gateway: → Broker
6. Engine: Busca jobs disponibles del tipo solicitado
7. Log: EVENT JobActivated
8. Respuesta: → Worker recibe el job con variables

9. Worker ejecuta lógica de negocio

10. Worker: completeJob(jobKey, resultVariables)
11. Gateway: → Broker
12. Log: COMMAND JobComplete
13. Engine: Completa job, avanza proceso al siguiente elemento
14. Log: EVENT JobCompleted, EVENT ElementCompleted, ...
```

## Modelo de Datos

### Keys y Particiones

- Cada entidad tiene una **key** de 64 bits
- Los primeros bits codifican la **partición**
- Ejemplo: key `2251799813685249` → partición 1, posición 1

### Column Families (RocksDB)

El estado se organiza en **column families** (definidas en `ZbColumnFamilies`):
- `PROCESS_CACHE` → Definiciones de procesos desplegados
- `ELEMENT_INSTANCE_KEY` → Instancias de elementos activos
- `JOBS` → Jobs pendientes/activos
- `MESSAGE_KEY` → Mensajes publicados
- `VARIABLES` → Variables de proceso
- `TIMERS` → Timers programados
- `INCIDENTS` → Incidentes activos
- ... y muchas más (~50 column families)

## Mapa de Dependencias entre Módulos

```
                    ┌─────────┐
                    │  util   │ ← Base de todo
                    └────┬────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         ┌─────────┐ ┌───────┐ ┌──────────┐
         │scheduler│ │protocol│ │msgpack-* │
         └────┬────┘ └───┬───┘ └────┬─────┘
              │          │          │
              ▼          ▼          ▼
         ┌─────────┐ ┌──────────────────┐
         │ journal │ │  protocol-impl   │
         └────┬────┘ └────────┬─────────┘
              │               │
              ▼               │
         ┌──────────┐        │
         │logstreams│◄───────┘
         └────┬─────┘
              │
    ┌─────────┼──────────┐
    ▼         ▼          ▼
┌──────┐ ┌────────┐ ┌───────────────┐
│zb-db │ │snapshot│ │stream-platform│
└──┬───┘ └───┬────┘ └──────┬────────┘
   │         │             │
   └─────────┼─────────────┘
             ▼
        ┌─────────┐
        │ engine  │
        └────┬────┘
             │
        ┌────┼────────────────┐
        ▼    ▼                ▼
   ┌────────┐ ┌─────────┐ ┌────────┐
   │ broker │ │ gateway │ │exporter│
   └────────┘ └─────────┘ └────────┘
                   │
              ┌────┼────┐
              ▼         ▼
        ┌──────────┐ ┌──────────┐
        │gateway-  │ │gateway-  │
        │protocol  │ │rest      │
        └──────────┘ └──────────┘
```

## Siguiente Documento

→ [02-broker.md](02-broker.md) - El Broker en detalle: arranque, particiones, ciclo de vida
