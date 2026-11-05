# 📦 **WASM UDF Support for Dozer Core**  
**Proyecto:** `dozer-wasm-udf`  
**Autor:** @mau16ro (Senior Developer)  
**Entrega:** 27 Oct 2026  
**Recompensa:** $600 USD  

---  

## 1️⃣ Resumen Ejecutivo  

| **Objetivo** | Permitir que los usuarios definan y ejecuten **User Defined Functions (UDF)** escritas en **WASM (AssemblyScript)** dentro de las consultas SQL de Dozer Core. |
|---|---|
| **Beneficios** | • Extensibilidad sin recompilar Dozer.<br>• Seguridad y aislamiento mediante sandbox WASM.<br>• Compatibilidad multi‑plataforma (Linux, macOS, Windows).<br>• Posibilidad de reutilizar lógica existente en JavaScript/TypeScript a través de AssemblyScript. |
| **Alcance** | - API pública para registrar UDFs.<br>- Motor de ejecución WASM integrado con **wasmtime**.<br>- Conversión automática de tipos entre Dozer y WASM (i64, f64, string).<br>- Manejo de errores y límites de recursos (memoria, tiempo). |
| **Entregables** | 1. Arquitectura y diagramas de componentes.<br>2. Implementación completa (Rust + ejemplos AssemblyScript).<br>3. Test suite con cobertura > 90 %.<br>4. Guía de instalación, compilación y uso. |

---  

## 2️⃣ Arquitectura de la Solución  

### 2.1 Visión General  

```
+-------------------+          +-------------------+          +-------------------+
|   Dozer SQL Engine|  <--->  |  WASM UDF Manager |  <--->  |   WASM Runtime (wasmtime) |
+-------------------+          +-------------------+          +-------------------+
        ^                                 ^                              ^
        |                                 |                              |
        |  register_udf(name, wasm_path)  |  invoke(name, args)          |
        |                                 |                              |
        |                                 |  sandbox (mem, cpu)          |
        |                                 |                              |
        +-------------------+   +-------------------+   +-------------------+
                              |   AssemblyScript  |   |   Rust Host API   |
                              +-------------------+   +-------------------+
```

### 2.2 Componentes Clave  

| Componente | Responsabilidad | Tecnologías |
|---|---|---|
| **WasmUdfManager** | - Registro y desregistro dinámico de UDFs.<br>- Carga perezosa del módulo WASM.<br>- Mantenimiento de un *cache* de instancias `Engine/Store`. | Rust, **wasmtime** (v21) |
| **WasmUdf** | Representa una función exportada dentro del módulo (`_start` o `invoke`). Gestiona la conversión de tipos y el *call* al runtime. | Rust, `wasmtime::Func` |
| **SQL Extension Point** | Hook dentro del parser/planificador de Dozer que reconoce `CALL wasm.<name>(args…)` y delega al `WasmUdfManager`. | Dozer Core (Rust) |
| **AssemblyScript SDK** | Pequeña librería (`dozer-wasm-sdk`) que expone helpers (`export function invoke(...): any`) y tipado para los usuarios de WASM. | AssemblyScript 0.27, `as-wasm` |
| **Sandboxing** | - Límite de memoria (default 8 MiB).<br>- Timeout mediante `wasmtime::Config::epoch_interruption`. | Wasmtime config |
| **Error Handling** | Convierte `Trap` o errores del host a `DozerError::UdfExecution`. | Rust `anyhow` + Dozer error enums |

### 2.3 Flujo de Ejecución  

1. **Registro** – El usuario llama a la API de Dozer:  

   ```rust
   dozer.register_wasm_udf("my_hash", "./udfs/hash.wasm");
   ```

2. **Parseo SQL** – El parser reconoce `CALL wasm.my_hash(arg1, arg2)` y crea un `LogicalPlan::WasmUdf`.  

3. **Planificación** – El planificador verifica tipos (solo `INT`, `FLOAT`, `STRING` permitidos).  

4. **Ejecución** – En tiempo de ejecución:  

   - `WasmUdfManager` recupera o carga la instancia del módulo.  
   - Convierte los argumentos a WASM (`i64`, `f64`, `i32` + memoria para strings).  
   - Llama a la función exportada `invoke`.  
   - Lee el valor de retorno y lo mapea de nuevo a `DozerValue`.  

5. **Resultado** – El valor retornado se utiliza como cualquier otro campo/expresión en la consulta.

### 2.4 Diagrama de Secuencia (simplificado)  

```
SQL Parser               Planner                Executor               WasmUdfManager           Wasmtime
   |                        |                       |                         |                     |
   |--- parse CALL -------->|                       |                         |                     |
   |                        |--- validate types -->|                         |                     |
   |                        |                       |--- get_or_load(module) ->|                     |
   |                        |                       |                         |--- instantiate --->|
   |                        |                       |--- marshal args -------->|                     |
   |                        |                       |                         |--- invoke -------->|
   |                        |                       |<--- result -------------|                     |
   |<--- push result -------|                       |                         |                     |
```

---  

## 3️⃣ Código Fuente (listo para producción)  

> **Estructura del repositorio**  

```
dozer-wasm-udf/
├─ Cargo.toml
├─ src/
│  ├─ lib.rs            # Public API
│  ├─ manager.rs        # WasmUdfManager
│  ├─ udf.rs            # WasmUdf wrapper
│  └─ error.rs          # Errores específicos
├─ examples/
│  ├─ assemblyscript/
│  │   ├─ src/
│  │   │   └─ index.ts
│  │   └─ asconfig.json
│  └─ rust_demo/
│       └─ main.rs
└─ tests/
   └─ integration.rs
```

### 3.1 `Cargo.toml`  

```toml
[package]
name = "dozer-wasm-udf"
version = "0.4.0"
edition = "2021"
license = "MIT"
description = "WASM based User Defined Functions for Dozer Core"
repository = "https://github.com/mau16ro/dozer-wasm-udf"
keywords = ["dozer", "wasm", "udf", "sql"]
categories = ["database", "wasm", "runtime"]

[dependencies]
wasmtime = { version = "21.0", features = ["async"] }
anyhow = "1.0"
thiserror = "1.0"
serde = { version = "1.0", features = ["derive"] }
log = "0.4"

# Optional: expose the SDK as a separate crate in the future
# [workspace]
# members = ["sdk"]
```

### 3.2 `src/lib.rs`  

```rust
//! Public API for registering and invoking WASM UDFs in Dozer.
//! --------------------------------------------------------
//! This crate is deliberately lightweight: the only heavy
//! dependency is **wasmtime**, which provides a fast, sandboxed
//! runtime for WebAssembly modules.

mod manager;
mod udf;
mod error;

pub use manager::{WasmUdfManager, WasmUdfConfig};
pub use error::WasmUdfError;
pub use udf::WasmUdfResult;

/// High‑level helper that can be plugged directly into Dozer's
/// `ExtensionRegistry` (or similar).
pub struct DozerWasmExtension {
    manager: WasmUdfManager,
}

impl DozerWasmExtension {
    /// Creates a new extension with the default configuration.
    pub fn new() -> Self {
        Self {
            manager: WasmUdfManager::default(),
        }
    }

    /// Registers a WASM UDF. `name` is the identifier used in SQL (`CALL wasm.<name>(…)`).
    /// `wasm_path` may be absolute or relative to the current working directory.
    pub fn register<U: Into<String>, P: Into<std::path::PathBuf>>(
        &mut self,
        name: U,
        wasm_path: P,
    ) -> Result<(), WasmUdfError> {
        self.manager.register(name, wasm_path)
    }

    /// Internal entry‑point used by the SQL executor.
    pub(crate) fn invoke(
        &self,
        name: &str,
        args: &[crate::dozer_types::types::Value],
    ) -> Result<WasmUdfResult, WasmUdfError> {
        self.manager.invoke(name, args)
    }
}
```

### 3.3 `src/error.rs`  

```rust
use thiserror::Error;

/// Errors that can happen while handling a WASM UDF.
#[derive(Debug, Error)]
pub enum WasmUdfError {
    #[error("UDF `{0}` not found")]
    NotFound(String),

    #[error("Failed to load WASM module from `{0}`: {1}")]
    LoadError(String, #[source] anyhow::Error),

    #[error("Invocation error in `{0}`: {1}")]
    InvocationError(String, #[source] anyhow::Error),

    #[error("Type mismatch for argument `{0}` (expected {1}, got {2})")]
    TypeMismatch(String, &'static str, String),

    #[error("Returned value could not be mapped to Dozer type")]
    ReturnMappingError,

    #[error("Resource limit exceeded (memory or time)")]
    ResourceLimit,
}
```

### 3.4 `src/udf.rs`  

```rust
use anyhow::Result;
use wasmtime::{Engine, Func, Instance, Linker, Store, TypedFunc, Val};
use crate::error::WasmUdfError;
use crate::dozer_types::types::Value;

/// Result type returned by a WASM UDF after conversion back to Dozer's `Value`.
pub type WasmUdfResult = Value;

/// Wrapper around a compiled WASM module exposing a single exported
/// function named `invoke`.  The signature must be:
///
/// ```text
/// (i64, i64, ...) -> i64   // i64 used as pointer to a UTF‑8 string in linear memory
/// ```
///
/// The helper automatically writes string arguments into the module's
/// memory and reads the result back.
pub struct WasmUdf {
    name: String,
    instance: Instance,
    store: Store<()>,
    // Cached typed function for fast invocation
    invoke_fn: TypedFunc<(i32, i32), i32>,
    // Memory is required for string marshaling
    memory: wasmtime::Memory,
}

impl WasmUdf {
    /// Constructs a `WasmUdf` from a compiled module.
    pub fn new(name: impl Into<String>, engine: &Engine, bytes: &[u8]) -> Result<Self, WasmUdfError> {
        let name = name.into();
        let mut store = Store::new(engine, ());
        // Enable epoch based interruption for timeouts
        store.set_epoch_deadline(1);

        let module = wasmtime::Module::from_binary(engine, bytes)
            .map_err(|e| WasmUdfError::LoadError(name.clone(), e.into()))?;

        let mut linker = Linker::new(engine);
        // Provide a very small `env` with `console_log` for debugging (optional)
        linker.func_wrap(
            "env",
            "console_log",
            |ptr: i32, len: i32, mut caller: wasmtime::Caller<'_, ()>| {
                let mem = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(m)) => m,
                    _ => return,
                };
                let data = mem
                    .data(&caller)
                    .get(ptr as usize..(ptr + len) as usize)
                    .unwrap_or(&[]);
                if let Ok(s) = std::str::from_utf8(data) {
                    log::debug!("[WASM {}] {}", name, s);
                }
            },
        )?;

        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|e| WasmUdfError::LoadError(name.clone(), e.into()))?;

        // Exported `invoke` function
        let invoke = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "invoke")
            .map_err(|e| WasmUdfError::LoadError(name.clone(), e.into()))?;

        // Exported memory
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| WasmUdfError::LoadError(name.clone(), anyhow::anyhow!("missing memory")))?;

        Ok(Self {
            name,
            instance,
            store,
            invoke_fn: invoke,
            memory,
        })
    }

    /// Marshal arguments, call the WASM function and convert the result.
    pub fn call(&mut self, args: &[Value]) -> Result<WasmU