# 16 - Messaging (Correlación de Mensajes)

## Qué es el Sistema de Mensajes

El sistema de mensajes permite **comunicación asíncrona** entre:
- Sistemas externos y procesos Zeebe
- Diferentes instancias de proceso

Casos de uso:
- Recibir notificaciones externas (pagos, aprobaciones)
- Iniciar procesos basado en eventos
- Comunicar entre sub-procesos

## Conceptos Clave

### Mensaje
```
Un mensaje tiene:
  ├─ messageName: "payment-received"        ← Tipo de mensaje
  ├─ correlationKey: "order-123"            ← Clave para matching
  ├─ variables: {amount: 150, status: "ok"} ← Datos del mensaje
  ├─ timeToLive: PT10M                      ← Cuánto tiempo esperar matching
  └─ messageId: "msg-abc-123"               ← Para deduplicación (opcional)
```

### Suscripción
```
Una suscripción espera mensajes:
  ├─ messageName: "payment-received"        ← Tipo esperado
  ├─ correlationKey: "=orderId"             ← Expresión FEEL evaluada
  ├─ processInstanceKey: 12345              ← Instancia que espera
  └─ elementInstanceKey: 67890              ← Elemento BPMN específico
```

## Tipos de Suscripciones

| Tipo | Cuándo se crea | Propósito |
|------|---------------|----------|
| **MessageSubscription** | Catch Event activado | Espera mensaje en catch event o receive task |
| **ProcessMessageSubscription** | A nivel de instancia | Tracking a nivel de instancia de proceso |
| **MessageStartEventSubscription** | Deploy de proceso | Inicia nueva instancia al recibir mensaje |

## Flujo de Publicación de Mensaje

### MessagePublishProcessor

```
1. Client: publishMessage("payment-received", "order-123", {amount: 150})
   ↓
2. MessagePublishProcessor recibe comando PUBLISH
   ↓
3. Deduplicación:
   ├─ ¿Existe mensaje con mismo (name, correlationKey, messageId)?
   ├─ SÍ → REJECTION: ALREADY_EXISTS
   └─ NO → continuar
   ↓
4. Almacenar mensaje en MessageState:
   ├─ messageKey → StoredMessage
   ├─ Crear índice: (name, correlationKey) → messageKey
   ├─ Crear índice de deadline para TTL
   └─ Crear índice de messageId para dedup
   ↓
5. Buscar suscripciones que matcheen:
   ├─ MessageSubscriptions locales (catch events)
   │   ├─ Buscar por (tenantId, messageName, correlationKey)
   │   └─ Si match → CORRELATING event
   ├─ MessageStartEventSubscriptions
   │   ├─ Buscar por messageName
   │   └─ Si match → crear nueva instancia de proceso
   └─ Suscripciones remotas (otras particiones)
       └─ Enviar comando de correlación cross-partition
   ↓
6. MESSAGE_PUBLISHED event
```

## Correlación de Mensajes

### MessageCorrelator

La correlación conecta un mensaje publicado con una suscripción que espera:

```java
public boolean correlateNextMessage(
    long subscriptionKey, 
    MessageSubscriptionRecord subscriptionRecord
) {
    // 1. Buscar mensajes que matcheen (name + correlationKey)
    // 2. Para cada mensaje candidato:
    //    - ¿Deadline > currentTime? (no expirado)
    //    - ¿No ya correlacionado con este proceso?
    // 3. Si match válido: escribir CORRELATING event
    // 4. FIFO: toma el primer match válido
}
```

### Flujo Completo

```
                    PUBLISH MESSAGE
                    name="payment"
                    correlationKey="order-123"
                    variables={amount: 150}
                          │
                          ▼
┌─────────────────────────────────────────────┐
│              MessageState                    │
│                                             │
│  Mensajes almacenados:                      │
│  [key=1] payment/order-123 → {amount: 150}  │
│  [key=2] payment/order-456 → {amount: 200}  │
│                                             │
│  Suscripciones activas:                     │
│  [sub=A] payment/order-123 → PI:12345       │
│  [sub=B] payment/order-789 → PI:67890       │
└──────────────────────┬──────────────────────┘
                       │
                       │ Match: mensaje key=1 ↔ sub=A
                       │ (name="payment", correlationKey="order-123")
                       ▼
              MESSAGE_CORRELATED
              ├─ Variables {amount: 150} copiadas al scope
              ├─ Catch event COMPLETA
              └─ Proceso continúa
```

## Estado del Mensaje (DbMessageState)

### Column Families

| Column Family | Key | Value | Propósito |
|--------------|-----|-------|----------|
| `message` | messageKey | StoredMessage | Datos del mensaje |
| `nameCorrelationMessage` | (tenant, name, corrKey, msgKey) | nil | Búsqueda por name+key |
| `deadline` | (deadline, msgKey) | nil | Expiración por TTL |
| `messageId` | (tenant, name, corrKey, msgId) | nil | Deduplicación |
| `messageCorrelation` | (msgKey, bpmnProcessId) | nil | Tracking de qué proceso ya recibió |

### Operaciones Clave

```java
// Almacenar mensaje
void putMessage(long messageKey, StoredMessage message);

// Buscar mensajes para correlación
void visitMessages(tenantId, messageName, correlationKey, visitor);

// Verificar deduplicación
boolean exist(name, correlationKey, messageId, tenantId);

// Marcar como correlacionado con un proceso
void putMessageCorrelation(messageKey, bpmnProcessId);

// Buscar mensajes expirados
void visitMessagesWithDeadlineBeforeTimestamp(timestamp, lastIndex, visitor);
```

## TTL y Expiración

### MessageTimeToLiveChecker

Tarea periódica que limpia mensajes expirados:

```
Cada N segundos:
  ├─ Buscar mensajes con deadline < now()
  ├─ Para cada mensaje expirado:
  │   └─ Crear comando EXPIRE en MessageBatchRecord
  ├─ Batch limitado para no saturar el LogStream
  └─ Continúa desde último índice en próxima iteración
```

### Configuración
- `executionInterval`: Intervalo entre ejecuciones
- `batchLimit`: Máximo de EXPIRE por ejecución
- `enableMessageTtlCheckerAsync`: Ejecución async

## Suscripciones de Mensaje

### Creación de Suscripción

Cuando un Intermediate Catch Event (mensaje) se activa:

```
1. Element activating (ej: Intermediate Catch Event)
   ↓
2. Engine crea MessageSubscription:
   ├─ messageName: del BPMN
   ├─ correlationKey: evalúa expresión FEEL
   ├─ processInstanceKey
   └─ elementInstanceKey
   ↓
3. Si hay mensaje pendiente que matchee:
   └─ Correlación inmediata → catch event completa
   
4. Si no hay mensaje:
   └─ Suscripción queda esperando (OPENED)
   └─ Cuando llegue un mensaje → correlación
```

### Estados de Suscripción

```
ProcessMessageSubscription:
  OPENING → OPENED → CLOSING
  
  OPENING: Creándose (comando enviado)
  OPENED: Activa, esperando mensaje
  CLOSING: Cancelándose (elemento terminado)
```

## Message Start Events

Los Message Start Events inician nuevas instancias de proceso:

```
1. Deploy de proceso con Message Start Event
   ├─ messageName: "new-order"
   └─ Se crea MessageStartEventSubscription
   
2. Publicar mensaje "new-order":
   ├─ Buscar MessageStartEventSubscription por messageName
   ├─ MATCH → Crear nueva instancia de proceso
   ├─ Variables del mensaje → variables iniciales del proceso
   └─ PROCESS_INSTANCE_CREATED
```

## Cross-Partition Messaging

Las suscripciones pueden estar en una partición diferente al mensaje:

```
Partition 1: Tiene la suscripción (catch event en PI:12345)
Partition 3: Recibe el mensaje publicado

Flujo:
1. Mensaje publicado en Partition 3
   ↓
2. Partition 3 busca suscripciones locales → no match
   ↓
3. SubscriptionCommandSender envía comando a Partition 1:
   ├─ openProcessMessageSubscription()
   └─ correlateProcessMessageSubscription()
   ↓
4. Partition 1 recibe y procesa correlación
   ↓
5. Catch event en PI:12345 completa
```

### Distribución de Particiones

- Los mensajes se publican en una partición basada en `correlationKey` (hash)
- Las suscripciones se crean en la partición del process instance
- El sistema coordina entre particiones vía PartitionMessagingService

## Ejemplo Completo

### BPMN
```
Start → Service Task → [Receive Task: "payment-received"] → End
                         correlationKey: "=orderId"
```

### Ejecución
```
1. CreateProcessInstance({orderId: "ORD-123"})
   └─ PI creada en Partition 1

2. Service Task completa → Receive Task activado
   └─ MessageSubscription creada:
      name="payment-received", corrKey="ORD-123"

3. Sistema externo: publishMessage("payment-received", "ORD-123", {paid: true})
   └─ Mensaje en Partition (hash de "ORD-123")

4. Correlación:
   ├─ Si mismo partition: correlación inmediata
   └─ Si diferente partition: cross-partition RPC

5. Resultado:
   ├─ Variables {paid: true} copiadas al scope
   ├─ Receive Task completa
   └─ Proceso continúa al End Event
```

## Diagrama de Estado

```
        publishMessage()              Catch Event activado
             │                              │
             ▼                              ▼
      ┌─────────────┐              ┌──────────────────┐
      │   Mensaje    │   match?    │   Suscripción     │
      │ almacenado   │◄──────────►│   esperando        │
      │ (con TTL)    │             │                    │
      └──────┬───────┘             └────────┬───────────┘
             │                               │
             │     ┌─────────────────────┐   │
             └────►│  MESSAGE_CORRELATED │◄──┘
                   │  Variables copiadas │
                   │  Catch event done   │
                   └─────────────────────┘
                   
Si no match:
  Mensaje → espera hasta TTL expira → EXPIRED
  Suscripción → espera hasta catch event se cancela
```
