# 23 - Autenticación e Identity

## Módulos

```
zeebe/auth/       ← JWT, tenant authorization
zeebe/gateway/    ← Interceptors (Identity, métricas)
```

## Modelo de Autenticación

```
Client con JWT Token
       │
       ▼
Gateway (gRPC/REST)
  ├─ IdentityInterceptor: verifica JWT
  ├─ Extrae authorized_tenants del token
  └─ Inyecta en gRPC Context
       │
       ▼
RequestMapper
  ├─ Lee authorized_tenants del Context
  └─ Añade tenantId al comando
       │
       ▼
Engine/Broker
  └─ Procesa comando con tenantId
```

## JWT: Interfaces y Clases

### JwtAuthorizationBuilder

```java
public interface JwtAuthorizationBuilder<T, A, U> {
    T withSubject(String subject);       // Default: "zeebe-client"
    T withIssuer(String issuer);         // Default: "zeebe-gateway"
    T withAudience(String audience);     // Default: "zeebe-broker"
    T withSigningAlgorithm(A algorithm);
    U build();
}
```

### AuthorizationDecoder

```java
public interface AuthorizationDecoder<T> {
    T decode();  // Retorna las autorizaciones del usuario
}
```

### TenantAuthorizationChecker

```java
public interface TenantAuthorizationChecker {
    Boolean isAuthorized(String tenantId);
    Boolean isFullyAuthorized(List<String> tenantIds);
}
```

### Implementación JWT

```java
// JwtAuthorizationDecoder
public class JwtAuthorizationDecoder 
    implements JwtAuthorizationBuilder<...>, AuthorizationDecoder<Map<String, Object>> {
    
    // Configurar
    JwtAuthorizationDecoder withJwtToken(String token);
    JwtAuthorizationDecoder withClaim(String claimName);
    
    // Verificar firma
    DecodedJWT build();  // Verifica issuer, audience, subject, firma
    
    // Extraer claims
    Map<String, Object> decode();  // Extrae "authorized_tenants"
}
```

### Token JWT Esperado

```json
{
    "iss": "zeebe-gateway",
    "aud": "zeebe-broker",
    "sub": "zeebe-client",
    "authorized_tenants": ["tenant-1", "tenant-2"],
    "exp": 1700000000
}
```

### Verificación de Tenants

```java
// TenantAuthorizationCheckerImpl
public class TenantAuthorizationCheckerImpl implements TenantAuthorizationChecker {
    private final List<String> authorizedTenants;
    
    public Boolean isAuthorized(String tenantId) {
        return authorizedTenants.contains(tenantId);
    }
    
    public Boolean isFullyAuthorized(List<String> tenantIds) {
        return authorizedTenants.containsAll(tenantIds);
    }
    
    // Factory desde mapa de autorización
    static TenantAuthorizationChecker fromAuthorizationMap(Map<String, Object> authMap) {
        List<String> tenants = (List) authMap.getOrDefault(
            Authorization.AUTHORIZED_TENANTS, List.of());
        return new TenantAuthorizationCheckerImpl(tenants);
    }
}
```

## Gateway: Interceptors

### IdentityInterceptor

Interceptor gRPC que verifica autenticación:

```java
public class IdentityInterceptor implements ServerInterceptor {
    private Identity identity;
    private IdentityTenantService tenantService;
    private MultiTenancyCfg multiTenancy;
    
    @Override
    public ServerCall.Listener interceptCall(call, headers, next) {
        // 1. Extraer "Authorization: Bearer <JWT>" del header
        String token = extractBearerToken(headers);
        
        // 2. Verificar token con Identity
        identity.authentication().verifyToken(token);
        
        // 3. Si multi-tenancy habilitado:
        if (multiTenancy.isEnabled()) {
            // Obtener tenants autorizados
            List<String> tenants = tenantService.getTenantsForToken(token);
            // Inyectar en gRPC Context
            InterceptorUtil.setAuthorizedTenants(tenants);
        }
        
        // 4. Si falla → Status.UNAUTHENTICATED
    }
}
```

### InterceptorUtil

```java
public class InterceptorUtil {
    static Key<List<String>> AUTHORIZED_TENANTS_KEY =
        Context.key("io.camunda.zeebe:authorized_tenants");
    
    public static Context setAuthorizedTenants(List<String> tenantIds);
    public static Key<List<String>> getAuthorizedTenantsKey();
}
```

## Multi-Tenancy

### Concepto

Multi-tenancy permite que múltiples "tenants" (organizaciones) compartan un mismo cluster:

```
Cluster Zeebe
├── Tenant "acme-corp"
│   ├── Procesos propios
│   ├── Instancias propias
│   └── Jobs propios
├── Tenant "beta-inc"
│   ├── Procesos propios
│   └── ...
└── Tenant "<default>"
    └── Sin tenant explícito
```

### Cómo Funciona

1. **JWT contiene** `authorized_tenants: ["acme-corp"]`
2. **Gateway inyecta** tenant list en el Context
3. **RequestMapper** extrae tenantId del Context y lo añade al comando
4. **Engine** procesa con tenantId:
   - Deploy → recurso asociado a tenant
   - CreateProcessInstance → instancia con tenant
   - PublishMessage → mensaje con tenant
   - ActivateJobs → solo jobs del tenant autorizado
5. **Estado** indexado por tenant:
   - Column families incluyen tenantId en las keys
   - Queries filtran por tenant

### Configuración

```yaml
zeebe:
  gateway:
    security:
      authentication:
        mode: identity    # none | identity
    multiTenancy:
      enabled: true
```

## Flujo Completo con Auth

```
1. Client obtiene JWT de su Identity Provider
   ↓
2. Client envía gRPC request con header:
   Authorization: Bearer eyJhbGc...
   ↓
3. Gateway recibe request
   ↓
4. IdentityInterceptor:
   ├─ Extrae Bearer token
   ├─ Verifica firma con Identity Provider
   ├─ Obtiene tenants: ["acme-corp"]
   └─ Inyecta en Context
   ↓
5. RequestMapper:
   ├─ Lee authorized_tenants del Context
   └─ Crea comando con tenantId="acme-corp"
   ↓
6. Engine/Broker:
   ├─ Procesa comando
   ├─ Valida tenant en cada operación
   └─ Responde
   ↓
7. Gateway retorna respuesta al client
```

## Sin Autenticación

Cuando no hay autenticación configurada (`mode: none`):
- No se evalúa JWT
- No hay filtrado por tenant
- Todo va al tenant `<default>`
- **Modo de desarrollo** típico

## Configuración de Identity

```yaml
# Configuración del Identity Provider
identity:
  issuerBackendUrl: "https://identity.example.com"
  audience: "zeebe-api"
  type: KEYCLOAK    # GENERIC | KEYCLOAK
  baseUrl: "https://identity.example.com/auth/realms/camunda"
```

## Diagrama de Componentes

```
┌──────────┐     JWT      ┌────────────────────┐
│  Client  │──────────────►│     Gateway        │
│  (Java)  │              │  ┌───────────────┐  │
│          │              │  │IdentityInterc.│  │
│ obtiene  │              │  │               │  │
│ JWT de   │              │  │ verify token  │  │
│ Identity │              │  │ get tenants   │  │
│ Provider │              │  └───────┬───────┘  │
└──────────┘              │          │          │
                          │  ┌───────▼───────┐  │
     ┌────────────┐       │  │RequestMapper  │  │
     │  Identity  │◄──────┤  │ + tenantId    │  │
     │  Provider  │verify │  └───────┬───────┘  │
     │ (Keycloak) │       │          │          │
     └────────────┘       └──────────┼──────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │   Broker/Engine       │
                          │   procesa con tenant  │
                          └──────────────────────┘
```
