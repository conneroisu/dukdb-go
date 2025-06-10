# SQL Parser Architecture Analysis for Pure-Go DuckDB Implementation

## Executive Summary

This document provides a comprehensive analysis of three SQL parser architectural approaches for implementing a pure-Go DuckDB-compatible database system. Based on extensive research of DuckDB's source code and existing Go SQL parsers, this analysis recommends the optimal architecture for handling DuckDB-specific SQL extensions while maintaining performance and extensibility.

## 1. DuckDB Parser Architecture Analysis

### Current DuckDB Implementation

DuckDB uses a **modified PostgreSQL parser** through the `libpg_query` library with the following characteristics:

**Parser Structure:**
- Located in `src/parser/` with modular organization
- Uses `third_party/libpg_query` containing modified PostgreSQL grammar
- Generates parser using Python scripts (`generate_grammar.py`, `generate_flex.py`)
- Grammar files are in Yacc/Bison format (`.y` files) with Flex lexer (`.l` files)

**Key Components:**
1. **Lexer (scan.l)** - Tokenizes SQL input
2. **Grammar (sql_statement.y)** - Defines SQL syntax rules
3. **Transformer** - Converts parse tree to DuckDB AST
4. **AST Nodes** - `SQLStatement`, `QueryNode`, `TableRef`, `ParsedExpression`

**Parser Characteristics:**
- **Catalog-agnostic** - No type checking or table validation during parsing
- **Modular design** - Separate handling for constraints, expressions, statements
- **PostgreSQL heritage** - Inherits robust SQL parsing capabilities
- **C++ namespace wrapping** - `duckdb_libpgquery` namespace isolation

### DuckDB-Specific SQL Extensions

DuckDB supports several analytical SQL extensions that must be handled:

1. **Advanced Aggregation Functions**
   - Window functions with complex frames
   - Statistical functions (percentile_cont, etc.)
   - Array aggregation functions

2. **Data Type Extensions**
   - ARRAY, MAP, STRUCT composite types
   - JSON type with path expressions
   - Interval arithmetic

3. **Analytical Query Features**
   - PIVOT/UNPIVOT operations
   - QUALIFY clause for window function filtering
   - EXCLUDE clause in window functions

4. **File Format Integration**
   - Parquet file reading with schema inference
   - CSV with type inference
   - JSON lines format support

## 2. Go SQL Parser Landscape Analysis

### Existing Go Parsers Comparison

| Parser | Approach | SQL Dialect | Strengths | Limitations |
|--------|----------|-------------|-----------|-------------|
| **xwb1989/sqlparser** | Yacc-generated (Vitess-derived) | MySQL | Simple, popular | Limited DDL, missing features |
| **vitess/sqlparser** | Yacc-generated | MySQL | Comprehensive DML | Partial DDL support |
| **blastrain/vitess-sqlparser** | Hybrid (Vitess + TiDB) | MySQL | Complete SQL+DDL | Complex integration |
| **pingcap/parser** | Goyacc-generated | MySQL | Highly compatible | MySQL-specific |

### Key Insights from Analysis

1. **Yacc/Goyacc dominance** - Most successful Go SQL parsers use generated parsers
2. **MySQL compatibility focus** - Limited PostgreSQL/DuckDB dialect support
3. **Modularity challenges** - Hand-written parsers struggle with SQL complexity
4. **Performance characteristics** - Generated parsers offer better performance for complex grammars

## 3. Architectural Approach Options

### Option 1: Hand-Written Recursive Descent Parser

**Architecture:**
```go
type Parser struct {
    lexer   *Lexer
    current Token
    peek    Token
}

// One function per grammar rule
func (p *Parser) parseSelectStatement() (*SelectStmt, error)
func (p *Parser) parseExpression() (Expression, error)
func (p *Parser) parseTableReference() (TableRef, error)
```

**Implementation Strategy:**
- Top-down parsing with recursive function calls
- Direct mapping of grammar rules to Go functions
- Manual AST construction and error handling

**DuckDB Compatibility Analysis:**
- ✅ **Pros:** Easy to add DuckDB-specific syntax extensions
- ✅ **Pros:** Fine-grained control over error messages
- ✅ **Pros:** No external grammar dependencies
- ❌ **Cons:** Complex SQL expressions difficult to handle
- ❌ **Cons:** Left-recursion requires grammar transformation
- ❌ **Cons:** Operator precedence handling becomes complex

**Performance Characteristics:**
- **Parsing Speed:** Moderate (recursive function calls overhead)
- **Memory Usage:** Higher (call stack depth for complex queries)
- **Startup Time:** Fast (no parser generation step)

**Maintenance Overhead:**
- **Grammar Changes:** High - manual code updates required
- **Testing:** High - extensive testing needed for each grammar rule
- **Debugging:** Moderate - direct code debugging possible

**Extension Capabilities:**
- **New SQL Features:** Easy to add with new parsing functions
- **Dialect Variations:** Requires separate parsing branches
- **Error Recovery:** Can be sophisticated with custom logic

**Recommendation Score: 6/10**

### Option 2: Generated Parser (Goyacc/ANTLR)

**Architecture:**
```go
//go:generate goyacc -o parser.go -p "duckdb" grammar.y

type Parser struct {
    lexer   DuckDBLexer
    result  SQLStatement
}

// Generated parser with custom actions
%token SELECT FROM WHERE
%type <stmt> select_statement
%type <expr> expression

select_statement: SELECT select_list FROM table_reference opt_where
```

**Implementation Strategy:**
- Grammar-driven parser generation
- Yacc/Bison grammar file with embedded Go actions
- Automatic AST construction with custom semantic actions

**DuckDB Compatibility Analysis:**
- ✅ **Pros:** Can handle complex SQL grammar completely
- ✅ **Pros:** Proven approach (matches DuckDB's current strategy)
- ✅ **Pros:** Handles operator precedence automatically
- ✅ **Pros:** Left-recursion support built-in
- ❌ **Cons:** Grammar file complexity for DuckDB extensions
- ❌ **Cons:** Debugging generated code is difficult

**Performance Characteristics:**
- **Parsing Speed:** Excellent (LR parser efficiency)
- **Memory Usage:** Low (table-driven parsing)
- **Startup Time:** Moderate (parse table initialization)

**Maintenance Overhead:**
- **Grammar Changes:** Low - modify grammar file and regenerate
- **Testing:** Moderate - test grammar rules systematically
- **Debugging:** High - generated code debugging challenges

**Extension Capabilities:**
- **New SQL Features:** Add grammar rules and semantic actions
- **Dialect Variations:** Conditional grammar rules possible
- **Error Recovery:** Built-in with custom error actions

**Recommendation Score: 8/10**

### Option 3: Hybrid Tokenizer + Parser Combinator

**Architecture:**
```go
type ParserCombinator func(TokenStream) (ASTNode, TokenStream, error)

// Combinator building blocks
func Sequence(parsers ...ParserCombinator) ParserCombinator
func Choice(parsers ...ParserCombinator) ParserCombinator
func Optional(parser ParserCombinator) ParserCombinator
func ZeroOrMore(parser ParserCombinator) ParserCombinator

// High-level parser composition
var selectParser = Sequence(
    Keyword("SELECT"),
    selectList,
    Keyword("FROM"),
    tableReference,
    Optional(whereClause),
)
```

**Implementation Strategy:**
- Functional parser composition using combinators
- Separate tokenization and parsing phases
- Composable parsing functions for complex constructs

**DuckDB Compatibility Analysis:**
- ✅ **Pros:** Highly composable for complex DuckDB syntax
- ✅ **Pros:** Easy to test individual parsing components
- ✅ **Pros:** Functional approach maps well to SQL structure
- ❌ **Cons:** Performance overhead from functional composition
- ❌ **Cons:** Left-recursion still requires careful handling
- ❌ **Cons:** Learning curve for parser combinator concepts

**Performance Characteristics:**
- **Parsing Speed:** Good (backtracking can be expensive)
- **Memory Usage:** Moderate (closure allocations)
- **Startup Time:** Fast (no generation phase)

**Maintenance Overhead:**
- **Grammar Changes:** Low - modify combinator composition
- **Testing:** Low - test individual combinators easily
- **Debugging:** Moderate - functional composition debugging

**Extension Capabilities:**
- **New SQL Features:** Excellent - compose new parsing logic
- **Dialect Variations:** Excellent - conditional combinator selection
- **Error Recovery:** Good - custom error handling combinators

**Recommendation Score: 7/10**

## 4. Detailed Architecture Recommendation

### Recommended Approach: Generated Parser with Goyacc

**Primary Recommendation: Generated Parser (Option 2)**

Based on comprehensive analysis, the **Generated Parser using Goyacc** approach is optimal for the pure-Go DuckDB implementation for the following reasons:

#### Technical Justification

1. **DuckDB Compatibility Alignment**
   - Mirrors DuckDB's current Yacc-based approach
   - Can handle the full complexity of DuckDB's SQL dialect
   - Proven capability with PostgreSQL-derived grammar

2. **Performance Superiority**
   - LR(1) parsing provides optimal performance for complex SQL
   - Table-driven parsing minimizes runtime overhead
   - Excellent memory efficiency for large queries

3. **Maintenance Efficiency**
   - Grammar modifications are centralized in `.y` files
   - Automatic code generation reduces manual maintenance
   - Systematic testing approach through grammar validation

4. **Extension Capability**
   - Grammar rules can be added incrementally for new features
   - Semantic actions provide flexibility for AST construction
   - Conditional compilation possible for dialect variations

#### Implementation Architecture

```go
// Core parser structure
type DuckDBParser struct {
    lexer  *DuckDBLexer
    errors []ParseError
    ast    SQLStatement
}

// Grammar file structure (duckdb.y)
%{
package parser

import "github.com/your-org/dukdb-go/ast"
%}

%union {
    stmt     ast.SQLStatement
    expr     ast.Expression
    tableRef ast.TableReference
    str      string
    num      int64
}

%token <str> IDENTIFIER STRING_LITERAL
%token <num> INTEGER_LITERAL
%token SELECT FROM WHERE AS

%type <stmt> sql_statement select_statement
%type <expr> expression column_reference
%type <tableRef> table_reference

%%

sql_statement:
    select_statement { $$ = $1 }
    | insert_statement { $$ = $1 }
    | update_statement { $$ = $1 }
    | delete_statement { $$ = $1 }
    
select_statement:
    SELECT select_list FROM table_reference opt_where_clause opt_group_by opt_having opt_order_by opt_limit
    {
        $$ = &ast.SelectStatement{
            SelectList: $2,
            From: $4,
            Where: $5,
            GroupBy: $6,
            Having: $7,
            OrderBy: $8,
            Limit: $9,
        }
    }
```

#### DuckDB-Specific Extensions Integration

**1. Advanced Data Types**
```yacc
data_type:
    simple_type { $$ = $1 }
    | array_type { $$ = $1 }
    | map_type { $$ = $1 }
    | struct_type { $$ = $1 }

array_type:
    data_type '[' ']' { $$ = &ast.ArrayType{ElementType: $1} }

map_type:
    MAP '(' data_type ',' data_type ')' { $$ = &ast.MapType{KeyType: $3, ValueType: $5} }

struct_type:
    STRUCT '(' struct_field_list ')' { $$ = &ast.StructType{Fields: $3} }
```

**2. Window Functions and Analytics**
```yacc
window_function:
    function_name '(' opt_expression_list ')' OVER window_specification
    {
        $$ = &ast.WindowFunction{
            Function: $1,
            Arguments: $3,
            Over: $5,
        }
    }

window_specification:
    '(' opt_partition_by opt_order_by opt_window_frame ')'
    {
        $$ = &ast.WindowSpec{
            PartitionBy: $2,
            OrderBy: $3,
            Frame: $4,
        }
    }
```

**3. PIVOT/UNPIVOT Operations**
```yacc
table_reference:
    simple_table_reference { $$ = $1 }
    | pivot_table_reference { $$ = $1 }
    | unpivot_table_reference { $$ = $1 }

pivot_table_reference:
    table_reference PIVOT '(' aggregate_function FOR column_name IN '(' pivot_values ')' ')'
    {
        $$ = &ast.PivotTable{
            Source: $1,
            AggregateFunction: $4,
            PivotColumn: $6,
            Values: $9,
        }
    }
```

#### Lexical Analysis Strategy

**Enhanced Lexer for DuckDB Extensions**
```go
type DuckDBLexer struct {
    input    string
    position int
    current  rune
    keywords map[string]TokenType
}

func NewDuckDBLexer(input string) *DuckDBLexer {
    keywords := map[string]TokenType{
        // Standard SQL keywords
        "SELECT": SELECT,
        "FROM":   FROM,
        "WHERE":  WHERE,
        
        // DuckDB-specific keywords
        "PIVOT":     PIVOT,
        "UNPIVOT":   UNPIVOT,
        "QUALIFY":   QUALIFY,
        "EXCLUDE":   EXCLUDE,
        "STRUCT":    STRUCT,
        "MAP":       MAP,
        "ARRAY":     ARRAY,
    }
    
    return &DuckDBLexer{
        input:    input,
        keywords: keywords,
    }
}
```

#### Error Handling and Recovery

**Comprehensive Error Reporting**
```go
type ParseError struct {
    Position Location
    Message  string
    Expected []string
    Found    string
    Context  string
}

func (p *DuckDBParser) Error(msg string) {
    p.errors = append(p.errors, ParseError{
        Position: p.lexer.GetPosition(),
        Message:  msg,
        Context:  p.lexer.GetContext(),
    })
}

// Grammar error recovery
%error-verbose

error_recovery:
    error ';' { yyerrok; }
    | error SELECT { yyerrok; }
```

### Secondary Recommendation: Hybrid Approach for Specific Use Cases

For certain components where maximum flexibility is needed (such as expression parsing with complex operator precedence), a **hybrid approach** combining generated parsing for statements with parser combinators for expressions could be optimal:

```go
// Generated parser handles statements
func (p *DuckDBParser) parseStatement() ast.SQLStatement

// Parser combinators handle complex expressions
func parseExpression(tokens TokenStream) (ast.Expression, error) {
    return Choice(
        parseLogicalExpression,
        parseArithmeticExpression,
        parseComparisonExpression,
        parseFunctionCall,
        parseWindowFunction,
    )(tokens)
}
```

## 5. Implementation Roadmap

### Phase 1: Core Infrastructure (Weeks 1-2)
1. **Lexer Implementation**
   - Basic tokenization for SQL keywords and operators
   - DuckDB-specific keyword recognition
   - String literal and numeric literal handling

2. **Basic Grammar Foundation**
   - Simple SELECT statements
   - Basic expressions and operators
   - Table references and joins

### Phase 2: Standard SQL Support (Weeks 3-6)
1. **DML Statements**
   - INSERT, UPDATE, DELETE statements
   - Subqueries and CTEs
   - UNION, INTERSECT, EXCEPT operations

2. **DDL Statements**
   - CREATE/DROP TABLE
   - ALTER TABLE operations
   - Index creation and management

### Phase 3: DuckDB Extensions (Weeks 7-10)
1. **Advanced Data Types**
   - ARRAY, MAP, STRUCT types
   - JSON type with path expressions
   - Composite type operations

2. **Analytical Features**
   - Window functions with all frame types
   - PIVOT/UNPIVOT operations
   - QUALIFY clause support

### Phase 4: Performance and Polish (Weeks 11-12)
1. **Optimization**
   - Parser performance tuning
   - Memory usage optimization
   - Error message improvement

2. **Testing and Validation**
   - Comprehensive test suite
   - DuckDB compatibility testing
   - Performance benchmarking

## 6. Conclusion

The **Generated Parser using Goyacc** approach provides the optimal balance of performance, maintainability, and DuckDB compatibility for the pure-Go DuckDB implementation. This architecture choice:

- **Mirrors proven success** of DuckDB's current parser approach
- **Provides excellent performance** through LR parsing techniques
- **Enables systematic extension** for DuckDB-specific SQL features
- **Maintains code quality** through grammar-driven development
- **Supports comprehensive testing** through systematic grammar validation

The recommended implementation roadmap provides a structured approach to building a production-ready SQL parser that can handle the full complexity of DuckDB's SQL dialect while maintaining the benefits of a pure-Go implementation.