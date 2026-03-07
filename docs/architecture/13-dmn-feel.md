# 13 - DMN y FEEL

## Qué es DMN

**DMN** (Decision Model and Notation) es un estándar para modelar decisiones de negocio. Zeebe soporta:

- **Decision Tables**: Tablas con reglas (input → output)
- **Decision Requirement Graphs (DRG)**: Grafos de decisiones que dependen unas de otras
- **Literal Expressions**: Expresiones FEEL simples

## Qué es FEEL

**FEEL** (Friendly Enough Expression Language) es el lenguaje de expresiones usado en:
- Condiciones de gateways XOR/OR
- Input/Output mappings
- Decision tables
- Timer expressions
- Cualquier expresión evaluable en el proceso

## Módulos

```
zeebe/dmn/   ← Motor de decisiones DMN
zeebe/feel/  ← Implementación FEEL (Scala)
```

## Motor DMN

### DecisionEngine

```java
public interface DecisionEngine {
    // Parsear un recurso DMN
    ParsedDecisionRequirementsGraph parse(InputStream dmnResource);
    
    // Evaluar una decisión
    DecisionEvaluationResult evaluateDecisionById(
        ParsedDecisionRequirementsGraph drg,
        String decisionId,
        DecisionContext context
    );
}
```

### Factory

```java
public static DecisionEngine createDecisionEngine() {
    return new DmnScalaDecisionEngine();  // Implementación basada en Scala
}
```

### Flujo de Evaluación

```
1. Deploy de recurso DMN
   ├─ DecisionEngine.parse(dmnInputStream)
   ├─ Valida: isValid() → true/false
   └─ Almacena ParsedDecisionRequirementsGraph en estado

2. Evaluación (desde proceso o API directa)
   ├─ Busca decisión por ID o key
   ├─ Obtiene DRG que contiene la decisión
   └─ DecisionEngine.evaluateDecisionById(drg, decisionId, variables)

3. Resultado
   ├─ isFailure() → true: error de evaluación
   ├─ getOutput() → MessagePack encoded result
   └─ getEvaluatedDecisions() → lista de decisiones evaluadas
```

### Resultado de Evaluación

```java
public interface DecisionEvaluationResult {
    boolean isFailure();
    String getFailureMessage();
    String getFailedDecisionId();
    DirectBuffer getOutput();                    // MessagePack encoded
    List<EvaluatedDecision> getEvaluatedDecisions();
}

public interface EvaluatedDecision {
    String decisionId();
    String decisionName();
    DecisionType decisionType();
    DirectBuffer decisionOutput();               // MessagePack
    List<EvaluatedInput> evaluatedInputs();      // Para decision tables
    List<MatchedRule> matchedRules();             // Reglas que matched
}
```

## Integración con el Engine

### Processor de Evaluación

```java
public class DecisionEvaluationEvaluteProcessor 
    implements TypedRecordProcessor<DecisionEvaluationRecord> {
    
    // Procesa comando EVALUATE_DECISION
    // 1. Busca decisión por ID/key + tenant
    // 2. Obtiene DRG
    // 3. Llama DecisionBehavior.evaluateDecisionInDrg()
    // 4. Escribe evento EVALUATED o FAILED
}
```

### DecisionBehavior

```java
public class DecisionBehavior {
    // Evaluar decisión dentro de un DRG
    DecisionEvaluationResult evaluateDecisionInDrg(
        ParsedDecisionRequirementsGraph drg,
        String decisionId,
        DirectBuffer variables   // MessagePack → Map
    );
    
    // Crear evento de resultado
    Tuple<DecisionEvaluationIntent, DecisionEvaluationRecord> 
        createDecisionEvaluationEvent(
            PersistedDecision decision,
            DecisionEvaluationResult result
        );
}
```

### Business Rule Task

Cuando un proceso tiene un Business Rule Task:

```
1. Service Task con tipo "dmn" (o Business Rule Task BPMN)
   ↓
2. Engine evalúa la decisión referenciada
   ├─ decisionId del task definition
   ├─ Variables del scope como input
   └─ DecisionEngine.evaluateDecisionById()
   ↓
3. Resultado se asigna como variables de salida
   ├─ Output mapping del task definition
   └─ Variable "result" con el output de la decisión
   ↓
4. Si falla → Incidente
```

## FEEL: Lenguaje de Expresiones

### Implementación

Basada en **Scala** (Camunda FEEL parsing library):

```
zeebe/feel/src/main/scala/io/camunda/zeebe/feel/impl/
├── FeelFunctionProvider.scala    ← Funciones custom
└── ...
```

### Tipos de Valores

|    Tipo FEEL    |                Ejemplo                 |
|-----------------|----------------------------------------|
| `number`        | `42`, `3.14`                           |
| `string`        | `"hello"`                              |
| `boolean`       | `true`, `false`                        |
| `date`          | `date("2024-01-15")`                   |
| `time`          | `time("14:30:00")`                     |
| `date and time` | `date and time("2024-01-15T14:30:00")` |
| `duration`      | `duration("PT2H30M")`                  |
| `list`          | `[1, 2, 3]`                            |
| `context`       | `{name: "John", age: 30}`              |
| `null`          | `null`                                 |

### Operaciones

```
Aritméticas:   +, -, *, /, %
Comparación:   =, !=, <, >, <=, >=
Booleanas:     and, or, not
String:        + (concatenación)
```

### Funciones Estándar FEEL

|   Categoría    |                                                                                                          Funciones                                                                                                           |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Agregación** | `count()`, `sum()`, `min()`, `max()`, `mean()`                                                                                                                                                                               |
| **String**     | `concatenate()`, `substring()`, `string length()`, `upper case()`, `lower case()`, `contains()`, `starts with()`, `ends with()`, `matches()`, `replace()`, `split()`                                                         |
| **Listas**     | `list contains()`, `count()`, `min()`, `max()`, `sum()`, `mean()`, `sublist()`, `append()`, `concatenate()`, `insert before()`, `remove()`, `reverse()`, `index of()`, `union()`, `distinct values()`, `flatten()`, `sort()` |
| **Contexto**   | `get value()`, `get entries()`, `context put()`, `context merge()`                                                                                                                                                           |
| **Conversión** | `string()`, `number()`, `date()`, `time()`, `duration()`                                                                                                                                                                     |
| **Temporal**   | `now()`, `today()`, `day of week()`, `month of year()`                                                                                                                                                                       |
| **Rango**      | `before()`, `after()`, `meets()`, `met by()`, `overlaps()`, `during()`, `includes()`                                                                                                                                         |

### Funciones Custom de Zeebe

```scala
// FeelFunctionProvider.scala
"cycle" → cycle(repetitions, interval)
// Crea patrón ISO 8601 repeat: R3/PT10M

// Ejemplo: timer con 3 repeticiones cada 10 minutos
cycle(3, duration("PT10M"))  → "R3/PT10M"
cycle(duration("PT1H"))       → "R/PT1H" (infinito)
```

### Dónde se usa FEEL en Zeebe

|             Contexto              |             Ejemplo             |
|-----------------------------------|---------------------------------|
| **Gateway XOR/OR conditions**     | `= order.total > 100`           |
| **Input mappings**                | `source="=customer.name"`       |
| **Output mappings**               | `source="=result.status"`       |
| **Timer duration**                | `= duration("PT5M")`            |
| **Timer date**                    | `= now() + duration("P1D")`     |
| **Timer cycle**                   | `= cycle(3, duration("PT10M"))` |
| **Message correlation key**       | `= order.orderId`               |
| **Multi-instance collection**     | `= items`                       |
| **Multi-instance element**        | `= item`                        |
| **Job type**                      | `= "process-" + order.type`     |
| **Script task**                   | `= a + b`                       |
| **Decision table inputs/outputs** | Cualquier expresión FEEL        |

### Evaluación de Expresiones

```
Todas las expresiones en Zeebe empiezan con "="
  ├─ "=customer.name"  → evalúa como FEEL
  ├─ "=42 > 10"        → true (boolean)
  ├─ "static-string"   → NO es FEEL, es string literal
  └─ Si falla → INCIDENTE con mensaje de error
```

## Decision Table Ejemplo

```
┌──────────────────┬───────────────┬───────────────┐
│ Input: amount    │ Input: risk   │ Output: rate  │
├──────────────────┼───────────────┼───────────────┤
│ < 10000          │ "low"         │ 0.05          │
│ < 10000          │ "medium"      │ 0.07          │
│ < 10000          │ "high"        │ 0.10          │
│ >= 10000         │ "low"         │ 0.04          │
│ >= 10000         │ "medium"      │ 0.06          │
│ >= 10000         │ "high"        │ 0.09          │
└──────────────────┴───────────────┴───────────────┘
Hit Policy: UNIQUE (exactamente una regla debe match)
```

Variables de entrada: `{amount: 5000, risk: "medium"}`
Resultado: `{rate: 0.07}`

## Hit Policies Soportadas

|    Policy     | Símbolo |               Comportamiento                |
|---------------|---------|---------------------------------------------|
| Unique        | U       | Exactamente una regla matchea               |
| First         | F       | Primera regla que matchea                   |
| Priority      | P       | Regla con más prioridad                     |
| Any           | A       | Cualquier regla (todas dan mismo resultado) |
| Collect       | C       | Todas las reglas que matchean (lista)       |
| Collect Sum   | C+      | Suma de outputs                             |
| Collect Min   | C<      | Mínimo de outputs                           |
| Collect Max   | C>      | Máximo de outputs                           |
| Collect Count | C#      | Cuenta de matches                           |
| Rule Order    | R       | Todas en orden de regla                     |
| Output Order  | O       | Todas ordenadas por output                  |

