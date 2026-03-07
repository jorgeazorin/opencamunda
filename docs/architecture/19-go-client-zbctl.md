# 19 - Go Client y zbctl

## Estructura del Módulo

```
clients/go/
├── pkg/
│   ├── worker/           ← Job workers
│   │   └── jobWorker.go
│   ├── commands/         ← Todos los comandos
│   └── zbc/              ← Client principal
│       └── client.go
├── cmd/
│   └── zbctl/            ← CLI tool
│       └── main.go
├── internal/             ← Código interno
├── vendor/               ← Dependencias vendored
└── test/                 ← Tests de integración
```

## Client Go

### Crear Cliente

```go
client, err := zbc.NewClient(&zbc.ClientConfig{
    GatewayAddress:         "localhost:26500",
    UsePlaintextConnection: true,
})
defer client.Close()
```

### Comandos Disponibles (~25)

| Categoría | Comando | Método |
|-----------|---------|--------|
| **Deploy** | Deploy resource | `client.NewDeployResourceCommand()` |
| **Procesos** | Create instance | `client.NewCreateInstanceCommand()` |
| | Create with result | `client.NewCreateInstanceCommand()...WithResult()` |
| | Cancel instance | `client.NewCancelInstanceCommand()` |
| | Set variables | `client.NewSetVariablesCommand()` |
| | Modify instance | `client.NewModifyProcessInstanceCommand()` |
| | Migrate instance | `client.NewMigrateProcessInstanceCommand()` |
| **Jobs** | Activate jobs | `client.NewActivateJobsCommand()` |
| | Complete job | `client.NewCompleteJobCommand()` |
| | Fail job | `client.NewFailJobCommand()` |
| | Throw error | `client.NewThrowErrorCommand()` |
| | Update retries | `client.NewUpdateJobRetriesCommand()` |
| | Update timeout | `client.NewUpdateJobTimeoutCommand()` |
| **Mensajes** | Publish message | `client.NewPublishMessageCommand()` |
| | Broadcast signal | `client.NewBroadcastSignalCommand()` |
| **Decisiones** | Evaluate decision | `client.NewEvaluateDecisionCommand()` |
| **Recursos** | Delete resource | `client.NewDeleteResourceCommand()` |
| **Incidentes** | Resolve incident | `client.NewResolveIncidentCommand()` |
| **Cluster** | Topology | `client.NewTopologyCommand()` |

### Ejemplo: Deploy y Crear Instancia

```go
// Deploy
resp, err := client.NewDeployResourceCommand().
    AddResourceFile("process.bpmn").
    Send(ctx)

// Crear instancia
result, err := client.NewCreateInstanceCommand().
    BPMNProcessId("my-process").
    LatestVersion().
    VariablesFromString(`{"orderId": "123"}`).
    Send(ctx)

fmt.Println(result.ProcessInstanceKey)
```

## Job Workers en Go

### Configuración

```go
worker := client.NewJobWorker().
    JobType("payment").
    Handler(handlePayment).
    Name("payment-worker").
    MaxJobsActive(32).       // Máx jobs activos simultáneos
    Concurrency(4).          // Goroutines de procesamiento
    PollInterval(100 * time.Millisecond).
    StreamEnabled(true).     // Activar streaming
    Open()

defer worker.Close()
worker.AwaitClose()
```

### Handler

```go
func handlePayment(client worker.JobClient, job entities.Job) {
    // Leer variables
    vars, _ := job.GetVariablesAsMap()
    amount := vars["amount"].(float64)
    
    // Procesar...
    
    // Completar
    _, err := client.NewCompleteJobCommand().
        JobKey(job.Key).
        VariablesFromMap(map[string]interface{}{
            "paid": true,
        }).
        Send(context.Background())
    
    if err != nil {
        // Fallar el job
        client.NewFailJobCommand().
            JobKey(job.Key).
            Retries(job.Retries - 1).
            ErrorMessage(err.Error()).
            Send(context.Background())
    }
}
```

### Defaults del Worker

| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `MaxJobsActive` | 32 | Jobs activos máximos |
| `Concurrency` | 4 | Goroutines paralelas |
| `PollInterval` | 100ms | Intervalo de polling |
| `Threshold` | 0.3 | Poll cuando 30% libre |
| `StreamEnabled` | false | Usar streaming |

## zbctl: CLI Tool

### Uso Básico

```bash
# Estado del cluster
zbctl status

# Deploy
zbctl deploy process.bpmn

# Crear instancia
zbctl create instance my-process --variables '{"orderId": "123"}'

# Completar job
zbctl complete job <key> --variables '{"result": "ok"}'

# Publicar mensaje
zbctl publish message "payment" --correlationKey "order-123"

# Resolver incidente
zbctl resolve incident <key>

# Activar jobs
zbctl activate jobs my-task-type
```

### Configuración por Variables de Entorno

```bash
export ZEEBE_ADDRESS="localhost:26500"
export ZEEBE_CLIENT_ID="my-client"
export ZEEBE_CLIENT_SECRET="secret"
export ZEEBE_INSECURE_CONNECTION="true"
```

### OAuth

```bash
# Con OAuth
zbctl status \
  --clientId my-client \
  --clientSecret secret \
  --authzUrl https://auth.example.com/oauth/token

# El token se cachea automáticamente
```

### Health Check

```bash
zbctl status
# Muestra: cluster size, partitions, leaders, broker versions
```
