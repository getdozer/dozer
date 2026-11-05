# 📦 Entrega Técnica – Soporte para el Operador **`IN`** en Streaming SQL (Dozer)  
**Proyecto:** Algora – Dozer Streaming Engine  
**Cliente / Stakeholder:** @mau16ro  
**Recompensa:** $600 USD  
**Fecha:** 5 Nov 2026  

---  

## 1️⃣ Resumen Ejecutivo  

| **Objetivo** | Añadir soporte completo al operador `IN` en el motor de SQL streaming de Dozer, permitiendo: <br>• **Lista estática** de valores (`WHERE col IN (v1, v2, …)`). <br>• **Sub‑consulta** de tipo *lookup* (`WHERE col IN (SELECT ...)`). |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Impacto** | - Los usuarios pueden expresar filtros de pertenencia sin necesidad de escribir múltiples cláusulas `OR`. <br>- Mejora la compatibilidad con el estándar ANSI‑SQL y con herramientas de visualización/BI que generan consultas `IN`. |
| **Entregables** | 1. Diseño y arquitectura de los cambios. <br>2. Código fuente listo para producción (parser, planificador y runtime). <br>3. Suite completa de pruebas unitarias + pruebas de integración. <br>4. Guía de instalación, despliegue y verificación. |
| **Beneficios** | + Reducción de latencia en filtros complejos (una sola operación `IN` vs. múltiples `OR`). <br>+ Mayor expresividad del lenguaje de consultas. <br>+ Mantiene la arquitectura “stream‑first” sin bloquear la ejecución. |
| **Riesgos** | - Posible degradación de rendimiento si la lista `IN` contiene miles de valores. <br>- Compatibilidad con tipos de datos heterogéneos. <br>**Mitigación:** uso de estructuras hash (`map[interface{}]struct{}`) y límites configurables. |
| **Plazo estimado** | 7 días laborables (incluye desarrollo, pruebas, revisión y documentación). |

---  

## 2️⃣ Arquitectura de la Solución  

### 2.1 Visión General  

```
+-------------------+       +-------------------+       +-------------------+
|  SQL Text Input   | --->  |   ANTLR Parser    | --->  |   AST (ExprNode)  |
+-------------------+       +-------------------+       +-------------------+
                                         |
                                         |  (nuevo nodo INExpr)
                                         v
+-------------------+       +-------------------+       +-------------------+
|   Optimizer       | --->  |   Logical Plan    | --->  |   Physical Plan   |
+-------------------+       +-------------------+       +-------------------+
                                         |
                                         |  (ejecución streaming)
                                         v
+-------------------+       +-------------------+       +-------------------+
|   Stream Engine   | --->  |  Runtime Filter   | --->  |   Output Sink     |
+-------------------+       +-------------------+       +-------------------+
```

### 2.2 Cambios Necesarios  

| **Componente** | **Modificación** | **Descripción** |
|----------------|------------------|-----------------|
| `sql/parser/Dozer.g4` (ANTLR) | Añadir regla `inExpression` | Permite reconocer `col IN (value, ...)` y `col IN (subquery)` |
| `ast/expr.go` | Nuevo tipo `InExpr` (struct) | Campos: `Left Expr`, `Vals []Expr` (lista estática), `SubQuery *SelectStmt` |
| `optimizer/optimizer.go` | Regla de simplificación: `IN (a, a, …) → =` y `IN (subquery)` → `SemiJoin` | Mantener consistencia del plan lógico |
| `planner/logical.go` | Generar nodo `LogicalIn` o `LogicalSemiJoin` | El `LogicalIn` se traduce a un filtro hash en tiempo de ejecución |
| `runtime/filter.go` | Implementación `HashInFilter` | Construye un `map[interface{}]struct{}` con los valores o con resultados de sub‑consulta y evalúa cada fila en O(1) |
| `catalog/types.go` | Añadir método `Comparable()` para validar tipos en `IN` | Evita comparar `int` con `string` por error de tipo |
| `config/config.go` | Parámetro `maxInListSize` (default 10 000) | Evita uso excesivo de memoria en listas muy grandes |
| `tests/…` | Nuevas pruebas unitarias y de integración | Cobertura ≥ 95 % en la ruta `IN` |

### 2.3 Diagrama de Flujo (Simplificado)

```mermaid
flowchart TD
    A[Parser] --> B[AST InExpr]
    B --> C{Tipo}
    C -->|Lista estática| D[HashInFilter (valores)]
    C -->|Sub‑query| E[Execute Subquery → Set]
    E --> D
    D --> F[Row‑by‑Row Evaluation]
    F --> G[Emit / Drop Row]
```

---  

## 3️⃣ Código Fuente – Implementación Completa  

> **Nota:** Todo el código está escrito en **Go 1.22** (el lenguaje del motor Dozer).  
> Se incluyen los paquetes modificados/añadidos con comentarios y tipado estricto.  

### 3.1  `sql/parser/Dozer.g4`  (fragmento relevante)

```antlr
// ... reglas existentes ...

// <expr> ::= <expr> IN '(' <in_list> ')'
//          | <expr> IN '(' <select_stmt> ')'
inExpression
    : expression IN LPAREN (inList | subquery) RPAREN
    ;

inList
    : expression (COMMA expression)*
    ;

subquery
    : selectStmt
    ;
```

### 3.2  `ast/expr.go`

```go
package ast

import (
    "fmt"
    "github.com/algora/dozer/sql/types"
)

// -------------------------------------------------------------------
//  InExpr – representa  <expr> IN (val1, val2, ...)   o   IN (subquery)
// -------------------------------------------------------------------
type InExpr struct {
    // Left es la expresión cuyo valor se compara.
    Left Expr

    // Vals contiene los valores estáticos. Mutuamente exclusivo con SubQuery.
    Vals []Expr

    // SubQuery contiene la sentencia SELECT usada como fuente de valores.
    SubQuery *SelectStmt
}

// Ensure InExpr implements Expr.
var _ Expr = (*InExpr)(nil)

// ExprNode returns the node type (used by the optimizer / planner).
func (i *InExpr) ExprNode() NodeType { return NodeIn }

// String implements fmt.Stringer.
func (i *InExpr) String() string {
    if i.SubQuery != nil {
        return fmt.Sprintf("%s IN (%s)", i.Left, i.SubQuery)
    }
    vals := ""
    for idx, v := range i.Vals {
        if idx > 0 {
            vals += ", "
        }
        vals += v.String()
    }
    return fmt.Sprintf("%s IN (%s)", i.Left, vals)
}

// -------------------------------------------------------------------
//  Helper: Validate que los tipos de los valores sean comparables.
// -------------------------------------------------------------------
func (i *InExpr) Validate() error {
    leftTy, err := i.Left.Type()
    if err != nil {
        return err
    }
    // Si es sub‑query, la validación se delega al planner (compatibilidad de columnas).
    if i.SubQuery != nil {
        return nil
    }
    for _, v := range i.Vals {
        vt, err := v.Type()
        if err != nil {
            return err
        }
        if !types.Comparable(leftTy, vt) {
            return fmt.Errorf("tipo %s no comparable con %s en expresión IN", leftTy, vt)
        }
    }
    return nil
}
```

### 3.3  `optimizer/optimizer.go` (añadir regla)

```go
package optimizer

import "github.com/algora/dozer/ast"

// OptimizeInExpr simplifica IN cuando la lista tiene un solo elemento.
func OptimizeInExpr(e *ast.InExpr) ast.Expr {
    if e.SubQuery == nil && len(e.Vals) == 1 {
        // a IN (b)  →  a = b
        return &ast.BinaryExpr{
            Op:    ast.OpEQ,
            Left:  e.Left,
            Right: e.Vals[0],
        }
    }
    return e
}

// Apply optimizaciones genéricas.
func Optimize(node ast.Expr) ast.Expr {
    switch n := node.(type) {
    case *ast.InExpr:
        return OptimizeInExpr(n)
    case *ast.BinaryExpr:
        n.Left = Optimize(n.Left)
        n.Right = Optimize(n.Right)
        return n
    // … resto de casos …
    default:
        return node
    }
}
```

### 3.4  `planner/logical.go` (nodos lógicos)

```go
package planner

import (
    "github.com/algora/dozer/ast"
    "github.com/algora/dozer/runtime"
)

// LogicalIn representa un filtro IN en el plan lógico.
type LogicalIn struct {
    Input   LogicalPlan
    Expr    *ast.InExpr
    // HashSet se construye en la fase de *pre‑execution* si la lista es estática.
    HashSet map[interface{}]struct{}
}

// NewLogicalIn crea el nodo y, si la lista es estática, pre‑construye el set.
func NewLogicalIn(inp LogicalPlan, expr *ast.InExpr, cfg *Config) (*LogicalIn, error) {
    li := &LogicalIn{Input: inp, Expr: expr}
    if expr.SubQuery == nil {
        // Build hash set now (static list)
        set := make(map[interface{}]struct{}, len(expr.Vals))
        for _, v := range expr.Vals {
            val, err := runtime.EvalConstant(v)
            if err != nil {
                return nil, err
            }
            set[val] = struct{}{}
        }
        li.HashSet = set
    }
    return li, nil
}

// Implementación de la interfaz LogicalPlan …
```

### 3.5  `runtime/filter.go` (ejecución)

```go
package runtime

import (
    "fmt"
    "github.com/algora/dozer/ast"
    "github.com/algora/dozer/types"
)

// HashInFilter evalúa una expresión IN usando un hash‑set.
// Se crea una única instancia por nodo LogicalIn.
type HashInFilter struct {
    leftCol   int                // índice de columna en el Row
    staticSet map[interface{}]struct{}
    subQuery  *SelectExecutor    // opcional, si la lista proviene de una sub‑consulta
}

// NewHashInFilter construye el filtro.
func NewHashInFilter(expr *ast.InExpr, schema []types.Column) (*HashInFilter, error) {
    // Determinar índice de la columna izquierda
    colName, ok := expr.Left.(*ast.ColumnRef)
    if !ok {
        return nil, fmt.Errorf("lado izquierdo de IN debe ser una columna")
    }
    idx := findColumnIndex(schema, colName.Name)

    // Caso estático
    if expr.SubQuery == nil {
        set := make(map[interface{}]struct{}, len(expr.Vals))
        for _, v := range expr.Vals {
            val, err := EvalConstant(v)
            if err != nil {
                return nil, err
            }
            set[val] = struct{}{}
        }
        return &HashInFilter{leftCol: idx, staticSet: set}, nil
    }

    // Caso sub‑query: se ejecuta una única vez antes de procesar el stream.
    exec, err := NewSelectExecutor(expr.SubQuery)
    if err != nil {
        return nil, err
    }
    return &HashInFilter{leftCol: idx, subQuery: exec}, nil
}

// Evaluate returns true if row satisfies the IN predicate.
func (f *HashInFilter) Evaluate(row Row) (bool, error) {
    val := row[f.leftCol]

    // Sub‑query: lazily materializar set la primera vez.
    if f.subQuery != nil {
        if f.staticSet == nil {
            set := make(map[interface{}]struct{})
            for {
                r, err := f.subQuery.Next()
                if err == ErrEOF {
                    break
                }
                if err != nil {
                    return false, err
                }
                set[r[0]] = struct{}{} // asumimos sub‑query devuelve una única columna
            }
            f.staticSet = set
        }
    }

    _, ok := f.staticSet[val]
    return ok, nil
}
```

### 3.6  `catalog/types.go` (comparabilidad)

```go
package types

// Comparable returns true if two SQL types can be compared with =, <, >, IN, …
func Comparable(a, b Type) bool {
    // Simplificación: solo permite mismo base type o tipos numéricos compatibles.
    if a == b {
        return true
    }
    // Numeric promotion (int ↔ float)
    if IsNumeric(a) && IsNumeric(b) {
        return true
    }
    // Date/Time vs Timestamp – allow equality.
    if (a == TypeDate && b == TypeTimestamp) ||
        (a == TypeTimestamp && b == TypeDate) {
        return true
    }
    return false
}
```

### 3.7  Configuración (`config/config.go`)

