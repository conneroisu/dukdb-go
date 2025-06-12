package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

// Statement represents a parsed SQL statement
type Statement interface {
	Type() StatementType
}

// StatementType represents the type of SQL statement
type StatementType int

const (
	StmtSelect StatementType = iota
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtCreateTable
	StmtDropTable
	StmtCreateIndex
	StmtDropIndex
	StmtBegin
	StmtCommit
	StmtRollback
	StmtUnion
)

// ParseSQL parses a SQL string into a statement AST
func ParseSQL(sql string) (Statement, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("empty SQL statement")
	}
	
	// Simple keyword-based parsing for now
	// In production, this would use a proper parser generator
	upperSQL := strings.ToUpper(sql)
	
	switch {
	case strings.HasPrefix(upperSQL, "SELECT"):
		return parseSelect(sql)
	case strings.HasPrefix(upperSQL, "INSERT"):
		return parseInsert(sql)
	case strings.HasPrefix(upperSQL, "UPDATE"):
		return parseUpdate(sql)
	case strings.HasPrefix(upperSQL, "DELETE"):
		return parseDelete(sql)
	case strings.HasPrefix(upperSQL, "CREATE TABLE"):
		return parseCreateTable(sql)
	case strings.HasPrefix(upperSQL, "DROP TABLE"):
		return parseDropTable(sql)
	case strings.HasPrefix(upperSQL, "BEGIN"):
		return &BeginStatement{}, nil
	case strings.HasPrefix(upperSQL, "COMMIT"):
		return &CommitStatement{}, nil
	case strings.HasPrefix(upperSQL, "ROLLBACK"):
		return &RollbackStatement{}, nil
	default:
		return nil, fmt.Errorf("unsupported SQL statement: %s", sql)
	}
}

// SelectStatement represents a SELECT query
type SelectStatement struct {
	Columns    []Expression
	From       *TableRef
	Joins      []JoinClause
	Where      Expression
	GroupBy    []Expression
	Having     Expression
	OrderBy    []OrderByExpr
	Limit      *int64
	Offset     *int64
}

// UnionStatement represents a UNION/UNION ALL query
type UnionStatement struct {
	Left     Statement  // First SELECT statement
	Right    Statement  // Second SELECT statement
	UnionAll bool       // true for UNION ALL, false for UNION (distinct)
	OrderBy  []OrderByExpr
	Limit    *int64
	Offset   *int64
}

func (s *UnionStatement) Type() StatementType { return StmtUnion }

// JoinClause represents a JOIN in a SELECT statement
type JoinClause struct {
	Type      JoinType
	Table     *TableRef
	Condition Expression
}

// JoinType represents the type of join
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

func (s *SelectStatement) Type() StatementType { return StmtSelect }

// InsertStatement represents an INSERT statement
type InsertStatement struct {
	Table   *TableRef
	Columns []string
	Values  [][]Expression
}

func (s *InsertStatement) Type() StatementType { return StmtInsert }

// UpdateStatement represents an UPDATE statement
type UpdateStatement struct {
	Table   *TableRef
	Sets    []SetClause
	Where   Expression
}

func (s *UpdateStatement) Type() StatementType { return StmtUpdate }

// SetClause represents a column = value assignment in UPDATE
type SetClause struct {
	Column string
	Value  Expression
}

// DeleteStatement represents a DELETE statement
type DeleteStatement struct {
	Table *TableRef
	Where Expression
}

func (s *DeleteStatement) Type() StatementType { return StmtDelete }

// CreateTableStatement represents a CREATE TABLE statement
type CreateTableStatement struct {
	Schema  string
	Table   string
	Columns []ColumnDefinition
}

func (s *CreateTableStatement) Type() StatementType { return StmtCreateTable }

// Transaction control statements
type BeginStatement struct{}
func (s *BeginStatement) Type() StatementType { return StmtBegin }

type CommitStatement struct{}
func (s *CommitStatement) Type() StatementType { return StmtCommit }

type RollbackStatement struct{}
func (s *RollbackStatement) Type() StatementType { return StmtRollback }

// DropTableStatement represents a DROP TABLE statement
type DropTableStatement struct {
	Schema   string
	Table    string
	IfExists bool
}

func (s *DropTableStatement) Type() StatementType { return StmtDropTable }

// Expression represents a SQL expression
type Expression interface {
	ExprType() ExpressionType
}

// ExpressionType represents the type of expression
type ExpressionType int

const (
	ExprColumn ExpressionType = iota
	ExprConstant
	ExprFunction
	ExprBinary
	ExprUnary
	ExprCase
	ExprSubquery
	ExprParameter
	ExprFieldAccess
)

// ColumnExpr represents a column reference
type ColumnExpr struct {
	Table  string
	Column string
}

func (e *ColumnExpr) ExprType() ExpressionType { return ExprColumn }

// ConstantExpr represents a constant value
type ConstantExpr struct {
	Value interface{}
}

func (e *ConstantExpr) ExprType() ExpressionType { return ExprConstant }

// ParameterExpr represents a parameter placeholder (?)
type ParameterExpr struct {
	Index int // 1-based parameter index
}

func (e *ParameterExpr) ExprType() ExpressionType { return ExprParameter }

// SubqueryExpr represents a subquery expression
type SubqueryExpr struct {
	Subquery Statement
	Type     SubqueryType // scalar, exists, etc.
}

func (e *SubqueryExpr) ExprType() ExpressionType { return ExprSubquery }

// SubqueryType represents the type of subquery
type SubqueryType int

const (
	SubqueryScalar SubqueryType = iota // Returns single value
	SubqueryExists                     // EXISTS predicate
	SubqueryIn                         // IN predicate
)

// FunctionExpr represents a function call
type FunctionExpr struct {
	Name  string
	Args  []Expression
	Alias string // Column alias (e.g., "count" from "COUNT(*) as count")
}

func (e *FunctionExpr) ExprType() ExpressionType { return ExprFunction }

// BinaryExpr represents a binary expression
type BinaryExpr struct {
	Left  Expression
	Op    BinaryOp
	Right Expression
}

func (e *BinaryExpr) ExprType() ExpressionType { return ExprBinary }

// BinaryOp represents a binary operator
type BinaryOp int

const (
	OpEq BinaryOp = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAnd
	OpOr
	OpPlus
	OpMinus
	OpMult
	OpDiv
	OpLike
	OpBetween
)

// UnaryExpr represents a unary expression
type UnaryExpr struct {
	Op   UnaryOp
	Expr Expression
}

func (e *UnaryExpr) ExprType() ExpressionType { return ExprUnary }

// UnaryOp represents a unary operator
type UnaryOp int

const (
	OpIsNull UnaryOp = iota
	OpIsNotNull
	OpNot
)

// FieldAccessExpr represents accessing a field of a struct or array element
type FieldAccessExpr struct {
	Expr  Expression // The struct/array expression
	Field string     // The field name or array index
}

func (e *FieldAccessExpr) ExprType() ExpressionType { return ExprFieldAccess }

// TableRef represents a table reference
type TableRef struct {
	Schema string
	Table  string
	Alias  string
}

// OrderByExpr represents an ORDER BY expression
type OrderByExpr struct {
	Expr Expression
	Desc bool
}

// Simple parser implementations
func parseSelect(sql string) (Statement, error) {
	// Check for UNION first by looking for UNION/UNION ALL at the top level
	// This needs to be done before cleaning up SQL as we need to preserve structure
	unionStmt := parseUnion(sql)
	if unionStmt != nil {
		return unionStmt, nil
	}
	
	// Clean up SQL formatting (remove extra whitespace, newlines)
	sql = strings.TrimSpace(sql)
	sql = strings.Join(strings.Fields(sql), " ")
	
	// Very basic SELECT parsing
	parts := strings.Fields(sql)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid SELECT statement")
	}
	
	// Handle SELECT constant (e.g., SELECT 1)
	if len(parts) == 2 {
		// Try to parse as number
		if num, err := parseNumber(parts[1]); err == nil {
			return &SelectStatement{
				Columns: []Expression{&ConstantExpr{Value: num}},
			}, nil
		}
		// Otherwise treat as string
		return &SelectStatement{
			Columns: []Expression{&ConstantExpr{Value: parts[1]}},
		}, nil
	}
	
	// Handle SELECT * FROM table
	if len(parts) >= 4 && parts[1] == "*" && strings.ToUpper(parts[2]) == "FROM" {
		tableName := parts[3]
		return &SelectStatement{
			Columns: []Expression{&ColumnExpr{Column: "*"}},
			From:    &TableRef{Table: tableName},
		}, nil
	}
	
	// Handle SELECT col1, col2 FROM table
	if strings.Contains(strings.ToUpper(sql), " FROM ") {
		fromIdx := findTopLevelFromClause(sql)
		if fromIdx == -1 {
			return nil, fmt.Errorf("FROM clause not found")
		}
		selectPart := sql[7:fromIdx] // Skip "SELECT "
		fromPart := sql[fromIdx+6:]   // Skip " FROM "
		
		// Parse columns
		columns, err := parseSelectColumns(selectPart)
		if err != nil {
			return nil, err
		}
		
		// Parse FROM clause (potentially with JOINs)
		fromClause, joins, remainder, err := parseFromClause(fromPart)
		if err != nil {
			return nil, err
		}
		
		// Parse remainder for WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, etc.
		where, groupBy, having, orderBy, limit, err := parseCompleteSelectRemainder(remainder)
		if err != nil {
			return nil, err
		}
		
		return &SelectStatement{
			Columns: columns,
			From:    fromClause,
			Joins:   joins,
			Where:   where,
			GroupBy: groupBy,
			Having:  having,
			OrderBy: orderBy,
			Limit:   limit,
		}, nil
	}
	
	// Handle column-only SELECT (e.g., SELECT 'High Value' as type, id, amount)
	// Check if this might have WHERE, ORDER BY, etc without FROM
	upperSQL := strings.ToUpper(sql)
	selectEnd := len(sql)
	
	// Look for clauses that might appear after the column list
	clauseKeywords := []string{" WHERE ", " ORDER BY ", " GROUP BY ", " HAVING ", " LIMIT "}
	for _, keyword := range clauseKeywords {
		if idx := strings.Index(upperSQL, keyword); idx != -1 {
			selectEnd = idx
			break
		}
	}
	
	selectPart := sql[7:selectEnd] // Skip "SELECT "
	remainder := ""
	if selectEnd < len(sql) {
		remainder = sql[selectEnd:]
	}
	
	columns, err := parseSelectColumns(selectPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SELECT columns: %w", err)
	}
	
	stmt := &SelectStatement{
		Columns: columns,
	}
	
	// Parse remainder for WHERE, ORDER BY, etc. if present
	if remainder != "" {
		where, groupBy, having, orderBy, limit, err := parseCompleteSelectRemainder(remainder)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SELECT remainder: %w", err)
		}
		stmt.Where = where
		stmt.GroupBy = groupBy
		stmt.Having = having
		stmt.OrderBy = orderBy
		stmt.Limit = limit
	}
	
	return stmt, nil
}

func parseInsert(sql string) (Statement, error) {
	// Very basic INSERT INTO table VALUES (...) parsing
	upperSQL := strings.ToUpper(sql)
	if !strings.Contains(upperSQL, "VALUES") {
		return nil, fmt.Errorf("INSERT must contain VALUES")
	}
	
	// Extract table name (assuming INSERT INTO table VALUES format)
	parts := strings.Fields(sql)
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid INSERT statement")
	}
	
	tableName := parts[2]
	
	// Parse VALUES clause
	valuesIdx := strings.Index(upperSQL, "VALUES")
	if valuesIdx == -1 {
		return nil, fmt.Errorf("VALUES not found")
	}
	
	valuesPart := sql[valuesIdx+6:] // Skip "VALUES"
	valuesPart = strings.TrimSpace(valuesPart)
	
	// Parse value tuples - very basic for now
	var values [][]Expression
	paramIndex := 1 // Global parameter index counter
	
	// Handle simple case: VALUES (1, 'Hello'), (2, 'World')
	if strings.HasPrefix(valuesPart, "(") {
		// Find all value tuples
		tuples := splitValueTuples(valuesPart)
		for _, tuple := range tuples {
			valueRow, newParamIndex, err := parseValueTupleWithIndex(tuple, paramIndex)
			if err != nil {
				return nil, err
			}
			paramIndex = newParamIndex
			values = append(values, valueRow)
		}
	}
	
	return &InsertStatement{
		Table:  &TableRef{Table: tableName},
		Values: values,
	}, nil
}

// splitValueTuples splits VALUES clause into individual tuples
func splitValueTuples(valuesPart string) []string {
	var tuples []string
	var current strings.Builder
	parenDepth := 0
	inString := false
	
	for i, ch := range valuesPart {
		switch ch {
		case '(':
			if !inString {
				parenDepth++
			}
			current.WriteRune(ch)
		case ')':
			if !inString {
				parenDepth--
			}
			current.WriteRune(ch)
			if parenDepth == 0 && current.Len() > 0 {
				tuples = append(tuples, current.String())
				current.Reset()
			}
		case '\'':
			inString = !inString
			current.WriteRune(ch)
		case ',':
			if parenDepth > 0 || inString {
				current.WriteRune(ch)
			}
		default:
			if parenDepth > 0 {
				current.WriteRune(ch)
			}
		}
		
		// Prevent infinite loops
		if i > 10000 {
			break
		}
	}
	
	return tuples
}


// parseValueTupleWithIndex parses a value tuple tracking parameter indices
func parseValueTupleWithIndex(tuple string, startParamIndex int) ([]Expression, int, error) {
	// Remove parentheses
	tuple = strings.TrimPrefix(tuple, "(")
	tuple = strings.TrimSuffix(tuple, ")")
	
	// Split by comma (handling strings properly)
	parts := splitByComma(tuple)
	
	var expressions []Expression
	paramIndex := startParamIndex
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		
		// Parse value
		if part == "?" {
			// Parameter placeholder - use global index
			expressions = append(expressions, &ParameterExpr{Index: paramIndex})
			paramIndex++
		} else if strings.HasPrefix(part, "{") && strings.HasSuffix(part, "}") {
			// Struct literal like {'a': 'b', 'c': 'd'}
			structExpr := parseStructLiteral(part)
			expressions = append(expressions, structExpr)
		} else if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			// Array literal like ['a', 'b', 'c']
			arrayExpr := parseArrayLiteral(part)
			expressions = append(expressions, arrayExpr)
		} else if strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'") {
			// String literal
			value := part[1 : len(part)-1]
			expressions = append(expressions, &ConstantExpr{Value: value})
		} else if strings.Contains(part, "(") && strings.Contains(part, ")") {
			// Function call like MAP(...) or other functions
			funcExpr := parseFunctionExpression(part)
			if funcExpr != nil {
				expressions = append(expressions, funcExpr)
			} else {
				// Fallback to constant
				expressions = append(expressions, &ConstantExpr{Value: part})
			}
		} else {
			// Try to parse as number
			if num, err := parseNumber(part); err == nil {
				expressions = append(expressions, &ConstantExpr{Value: num})
			} else {
				// Treat as string
				expressions = append(expressions, &ConstantExpr{Value: part})
			}
		}
	}
	
	return expressions, paramIndex, nil
}

// parseStructLiteral parses struct literals like {'key': 'value', 'num': 123}
func parseStructLiteral(structStr string) Expression {
	// Remove braces
	structStr = strings.TrimPrefix(structStr, "{")
	structStr = strings.TrimSuffix(structStr, "}")
	structStr = strings.TrimSpace(structStr)
	
	// Handle empty struct
	if structStr == "" {
		return &ConstantExpr{Value: map[string]interface{}{}}
	}
	
	// Split struct fields by comma, respecting quoted strings and nested structures
	fields := splitByComma(structStr)
	structMap := make(map[string]interface{})
	
	for _, field := range fields {
		field = strings.TrimSpace(field)
		
		// Find the colon separator
		colonIdx := strings.Index(field, ":")
		if colonIdx == -1 {
			continue // Skip invalid field
		}
		
		// Extract key and value
		keyPart := strings.TrimSpace(field[:colonIdx])
		valuePart := strings.TrimSpace(field[colonIdx+1:])
		
		// Parse key (should be quoted)
		var key string
		if strings.HasPrefix(keyPart, "'") && strings.HasSuffix(keyPart, "'") {
			key = keyPart[1 : len(keyPart)-1]
		} else {
			key = keyPart // Allow unquoted keys
		}
		
		// Parse value
		var value interface{}
		if strings.HasPrefix(valuePart, "'") && strings.HasSuffix(valuePart, "'") {
			// String literal
			value = valuePart[1 : len(valuePart)-1]
		} else if strings.HasPrefix(valuePart, "[") && strings.HasSuffix(valuePart, "]") {
			// Array literal - parse and extract the value
			arrayExpr := parseArrayLiteral(valuePart)
			if constExpr, ok := arrayExpr.(*ConstantExpr); ok {
				value = constExpr.Value
			} else {
				value = valuePart // Fallback
			}
		} else if strings.Contains(valuePart, "(") && strings.Contains(valuePart, ")") {
			// Function call - we need to store this as is for now, it will be evaluated during INSERT
			// We can't evaluate it here because we don't have the execution context
			value = valuePart
		} else if num, err := parseNumber(valuePart); err == nil {
			// Number
			value = num
		} else {
			// Treat as string (unquoted)
			value = valuePart
		}
		
		structMap[key] = value
	}
	
	return &ConstantExpr{Value: structMap}
}

// parseArrayLiteral parses array literals like ['a', 'b', 'c'] or [1, 2, 3]
func parseArrayLiteral(arrayStr string) Expression {
	// Remove brackets
	arrayStr = strings.TrimPrefix(arrayStr, "[")
	arrayStr = strings.TrimSuffix(arrayStr, "]")
	arrayStr = strings.TrimSpace(arrayStr)
	
	// Handle empty array
	if arrayStr == "" {
		return &ConstantExpr{Value: []interface{}{}}
	}
	
	// Split array elements by comma, respecting quoted strings
	elements := splitByComma(arrayStr)
	var values []interface{}
	
	for _, element := range elements {
		element = strings.TrimSpace(element)
		
		// Parse each element
		if strings.HasPrefix(element, "'") && strings.HasSuffix(element, "'") {
			// String literal
			value := element[1 : len(element)-1]
			values = append(values, value)
		} else if num, err := parseNumber(element); err == nil {
			// Number
			values = append(values, num)
		} else {
			// Treat as string (unquoted)
			values = append(values, element)
		}
	}
	
	return &ConstantExpr{Value: values}
}

// parseNumber attempts to parse a string as a number
func parseNumber(s string) (interface{}, error) {
	// Try integer first
	if i, err := strconv.ParseInt(s, 10, 32); err == nil {
		return int32(i), nil
	}
	
	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	
	return nil, fmt.Errorf("not a number")
}

// splitByComma splits a string by commas, respecting quoted strings, array brackets, struct braces, and parentheses
func splitByComma(s string) []string {
	var parts []string
	var current strings.Builder
	inString := false
	bracketDepth := 0
	braceDepth := 0
	parenDepth := 0
	
	for _, ch := range s {
		switch ch {
		case '\'':
			inString = !inString
			current.WriteRune(ch)
		case '[':
			if !inString {
				bracketDepth++
			}
			current.WriteRune(ch)
		case ']':
			if !inString {
				bracketDepth--
			}
			current.WriteRune(ch)
		case '{':
			if !inString {
				braceDepth++
			}
			current.WriteRune(ch)
		case '}':
			if !inString {
				braceDepth--
			}
			current.WriteRune(ch)
		case '(':
			if !inString {
				parenDepth++
			}
			current.WriteRune(ch)
		case ')':
			if !inString {
				parenDepth--
			}
			current.WriteRune(ch)
		case ',':
			if !inString && bracketDepth == 0 && braceDepth == 0 && parenDepth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}
	
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	
	return parts
}

// splitByCommaRespectingParens splits a string by commas, respecting parentheses for CREATE TABLE column definitions
func splitByCommaRespectingParens(s string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	
	for _, ch := range s {
		switch ch {
		case '(':
			parenDepth++
			current.WriteRune(ch)
		case ')':
			parenDepth--
			current.WriteRune(ch)
		case ',':
			if parenDepth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}
	
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	
	return parts
}

func parseCreateTable(sql string) (Statement, error) {
	// Very basic CREATE TABLE parsing
	// Expected format: CREATE TABLE [schema.]table (col1 type1, col2 type2, ...)
	
	// Remove CREATE TABLE prefix
	sql = sql[12:] // len("CREATE TABLE")
	sql = strings.TrimSpace(sql)
	
	// Find opening parenthesis
	parenIdx := strings.Index(sql, "(")
	if parenIdx == -1 {
		return nil, fmt.Errorf("CREATE TABLE missing column definitions")
	}
	
	// Extract table name
	tableName := strings.TrimSpace(sql[:parenIdx])
	schema := "main"
	
	// Check for schema.table format
	if idx := strings.Index(tableName, "."); idx != -1 {
		schema = tableName[:idx]
		tableName = tableName[idx+1:]
	}
	
	// Extract column definitions
	endParen := strings.LastIndex(sql, ")")
	if endParen == -1 {
		return nil, fmt.Errorf("CREATE TABLE missing closing parenthesis")
	}
	
	colDefs := sql[parenIdx+1 : endParen]
	columns := parseColumnDefinitions(colDefs)
	
	return &CreateTableStatement{
		Schema:  schema,
		Table:   tableName,
		Columns: columns,
	}, nil
}

func parseColumnDefinitions(colDefs string) []ColumnDefinition {
	// Simple parsing of column definitions
	var columns []ColumnDefinition
	
	// Split by comma, respecting parentheses for complex types
	parts := splitByCommaRespectingParens(colDefs)
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		
		// Split column definition into name and type
		// For complex types like STRUCT(...), we need to be more careful about parsing
		spaceIdx := strings.Index(part, " ")
		if spaceIdx == -1 {
			continue // No space found, invalid column definition
		}
		
		colName := strings.TrimSpace(part[:spaceIdx])
		colType := strings.TrimSpace(part[spaceIdx+1:])
		
		// Check for array syntax (e.g., VARCHAR[], INTEGER[])
		isArray := strings.HasSuffix(colType, "[]")
		if isArray {
			colType = strings.TrimSuffix(colType, "[]")
		}
		
		// Convert SQL type to storage type
		var logicalType storage.LogicalType
		if isArray {
			// For now, treat all arrays as generic LIST type
			// TODO: Parse element type and create typed list
			logicalType = storage.LogicalType{ID: storage.TypeList}
		} else {
			upperColType := strings.ToUpper(colType)
			switch {
			case upperColType == "INTEGER" || upperColType == "INT":
				logicalType = storage.LogicalType{ID: storage.TypeInteger}
			case upperColType == "BIGINT":
				logicalType = storage.LogicalType{ID: storage.TypeBigInt}
			case upperColType == "VARCHAR" || upperColType == "TEXT" || upperColType == "STRING":
				logicalType = storage.LogicalType{ID: storage.TypeVarchar}
			case upperColType == "DOUBLE" || upperColType == "REAL":
				logicalType = storage.LogicalType{ID: storage.TypeDouble}
			case upperColType == "BOOLEAN" || upperColType == "BOOL":
				logicalType = storage.LogicalType{ID: storage.TypeBoolean}
			case upperColType == "DECIMAL":
				logicalType = storage.LogicalType{ID: storage.TypeDecimal, Width: 18, Scale: 3}
			case upperColType == "DATE":
				logicalType = storage.LogicalType{ID: storage.TypeDate}
			case upperColType == "TIMESTAMP":
				logicalType = storage.LogicalType{ID: storage.TypeTimestamp}
			case upperColType == "UUID":
				logicalType = storage.LogicalType{ID: storage.TypeUUID}
			case upperColType == "LIST" || strings.HasPrefix(upperColType, "LIST("):
				// For now, treat LIST as a generic list type
				// TODO: Parse LIST(element_type) syntax
				logicalType = storage.LogicalType{ID: storage.TypeList}
			case upperColType == "STRUCT" || strings.HasPrefix(upperColType, "STRUCT("):
				// For now, treat STRUCT as a generic struct type
				// TODO: Parse STRUCT(field_name type, ...) syntax
				logicalType = storage.LogicalType{ID: storage.TypeStruct}
			case upperColType == "MAP" || strings.HasPrefix(upperColType, "MAP("):
				// For now, treat MAP as a generic map type
				// TODO: Parse MAP(key_type, value_type) syntax
				logicalType = storage.LogicalType{ID: storage.TypeMap}
			default:
				// Default to varchar
				logicalType = storage.LogicalType{ID: storage.TypeVarchar}
			}
		}
		
		columns = append(columns, ColumnDefinition{
			Name:     colName,
			Type:     logicalType,
			Nullable: true, // Default to nullable
		})
	}
	
	return columns
}

// parseWhereExpression parses a WHERE clause expression
func parseWhereExpression(where string) Expression {
	paramIdx := 1
	return parseWhereExpressionWithParams(where, &paramIdx)
}

// parseWhereExpressionWithParams parses a WHERE clause expression with parameter tracking
func parseWhereExpressionWithParams(where string, paramIndex *int) Expression {
	where = strings.TrimSpace(where)
	upperWhere := strings.ToUpper(where)
	
	// Handle EXISTS subqueries first
	if strings.HasPrefix(upperWhere, "EXISTS") {
		return parseSubqueryInWhere(where)
	}
	
	// Check for BETWEEN first (before AND/OR to avoid conflicts)
	if strings.Contains(upperWhere, " BETWEEN ") {
		betweenIdx := strings.Index(upperWhere, " BETWEEN ")
		andIdx := strings.Index(upperWhere[betweenIdx:], " AND ")
		if andIdx != -1 {
			andIdx += betweenIdx
			column := strings.TrimSpace(where[:betweenIdx])
			lowerBound := strings.TrimSpace(where[betweenIdx+9:andIdx]) // Skip " BETWEEN "
			upperBound := strings.TrimSpace(where[andIdx+5:]) // Skip " AND "
			
			// Create column >= lower AND column <= upper
			lowerExpr := &BinaryExpr{
				Left:  parseColumnExpression(column),
				Op:    OpGe,
				Right: parseValueExpressionWithParams(lowerBound, paramIndex),
			}
			
			upperExpr := &BinaryExpr{
				Left:  parseColumnExpression(column),
				Op:    OpLe,
				Right: parseValueExpressionWithParams(upperBound, paramIndex),
			}
			
			return &BinaryExpr{
				Left:  lowerExpr,
				Op:    OpAnd,
				Right: upperExpr,
			}
		}
	}
	
	// Handle AND expressions (higher precedence)
	if andIdx := strings.Index(upperWhere, " AND "); andIdx != -1 {
		left := strings.TrimSpace(where[:andIdx])
		right := strings.TrimSpace(where[andIdx+5:]) // Skip " AND "
		
		return &BinaryExpr{
			Left:  parseWhereExpressionWithParams(left, paramIndex),
			Op:    OpAnd,
			Right: parseWhereExpressionWithParams(right, paramIndex),
		}
	}
	
	// Handle OR expressions (lower precedence)
	if orIdx := strings.Index(upperWhere, " OR "); orIdx != -1 {
		left := strings.TrimSpace(where[:orIdx])
		right := strings.TrimSpace(where[orIdx+4:]) // Skip " OR "
		
		return &BinaryExpr{
			Left:  parseWhereExpressionWithParams(left, paramIndex),
			Op:    OpOr,
			Right: parseWhereExpressionWithParams(right, paramIndex),
		}
	}
	
	// Check for IS NULL / IS NOT NULL
	if strings.HasSuffix(upperWhere, " IS NULL") {
		column := strings.TrimSpace(where[:len(where)-8]) // Remove " IS NULL"
		return &UnaryExpr{
			Op:   OpIsNull,
			Expr: parseColumnExpression(column),
		}
	}
	
	if strings.HasSuffix(upperWhere, " IS NOT NULL") {
		column := strings.TrimSpace(where[:len(where)-12]) // Remove " IS NOT NULL"
		return &UnaryExpr{
			Op:   OpIsNotNull,
			Expr: parseColumnExpression(column),
		}
	}
	
	// Check for LIKE
	if strings.Contains(upperWhere, " LIKE ") {
		likeIdx := strings.Index(upperWhere, " LIKE ")
		column := strings.TrimSpace(where[:likeIdx])
		pattern := strings.TrimSpace(where[likeIdx+6:]) // Skip " LIKE "
		
		return &BinaryExpr{
			Left:  parseColumnExpression(column),
			Op:    OpLike,
			Right: parseValueExpressionWithParams(pattern, paramIndex),
		}
	}
	
	// Simple comparison parsing (e.g., "column = value")
	// Check for common operators
	operators := []string{">=", "<=", "!=", "<>", "=", ">", "<"}
	
	for _, op := range operators {
		if idx := strings.Index(where, op); idx != -1 {
			left := strings.TrimSpace(where[:idx])
			right := strings.TrimSpace(where[idx+len(op):])
			
			// Parse left side (column)
			var leftExpr Expression
			if isIdentifier(left) {
				leftExpr = parseColumnExpression(left)
			} else {
				// Try to parse as constant
				if val, err := parseValue(left); err == nil {
					leftExpr = &ConstantExpr{Value: val}
				} else {
					leftExpr = parseColumnExpression(left)
				}
			}
			
			// Parse right side (value)
			var rightExpr Expression
			if right == "?" {
				// Parameter
				rightExpr = &ParameterExpr{Index: *paramIndex}
				*paramIndex++
			} else if val, err := parseValue(right); err == nil {
				rightExpr = &ConstantExpr{Value: val}
			} else {
				rightExpr = &ColumnExpr{Column: right}
			}
			
			// Map operator to BinaryOp
			var binOp BinaryOp
			switch op {
			case "=":
				binOp = OpEq
			case "!=", "<>":
				binOp = OpNe
			case "<":
				binOp = OpLt
			case "<=":
				binOp = OpLe
			case ">":
				binOp = OpGt
			case ">=":
				binOp = OpGe
			}
			
			return &BinaryExpr{
				Left:  leftExpr,
				Op:    binOp,
				Right: rightExpr,
			}
		}
	}
	
	// Check for function calls (contains parentheses)
	if strings.Contains(where, "(") && strings.Contains(where, ")") {
		funcExpr := parseFunctionExpression(where)
		if funcExpr != nil {
			return funcExpr
		}
	}
	
	// If no operator found, try to parse as a simple column reference or value
	// This could be the case for expressions like "column" without operators
	if isIdentifier(where) {
		return &ColumnExpr{Column: where}
	}
	
	// Try to parse as a constant value
	if val, err := parseValue(where); err == nil {
		return &ConstantExpr{Value: val}
	}
	
	// If all else fails, return nil
	return nil
}

// parseValueExpression parses a value expression (constant, parameter, or column)

// parseValueExpressionWithParams parses a value expression with parameter tracking
func parseValueExpressionWithParams(s string, paramIndex *int) Expression {
	s = strings.TrimSpace(s)
	
	// Check for parameter
	if s == "?" {
		expr := &ParameterExpr{Index: *paramIndex}
		*paramIndex++
		return expr
	}
	
	// Check for string literal
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return &ConstantExpr{Value: s[1 : len(s)-1]}
	}
	
	// Try to parse as number
	if num, err := parseNumber(s); err == nil {
		return &ConstantExpr{Value: num}
	}
	
	// Otherwise, treat as column reference
	return &ColumnExpr{Column: s}
}

// parseValue attempts to parse a string as a value (number or string)
func parseValue(s string) (interface{}, error) {
	s = strings.TrimSpace(s)
	
	// Check for string literal
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return s[1 : len(s)-1], nil
	}
	
	// Try to parse as number
	if num, err := parseNumber(s); err == nil {
		return num, nil
	}
	
	return nil, fmt.Errorf("not a value")
}

// isIdentifier checks if a string is a valid identifier
func isIdentifier(s string) bool {
	if s == "" {
		return false
	}
	
	// Simple check - starts with letter or underscore
	first := s[0]
	return (first >= 'a' && first <= 'z') || 
	       (first >= 'A' && first <= 'Z') || 
	       first == '_'
}

// parseDropTable parses a DROP TABLE statement
func parseDropTable(sql string) (Statement, error) {
	// Remove DROP TABLE prefix
	upperSQL := strings.ToUpper(sql)
	if strings.HasPrefix(upperSQL, "DROP TABLE IF EXISTS") {
		sql = sql[20:] // len("DROP TABLE IF EXISTS")
		tableName := strings.TrimSpace(sql)
		schema := "main"
		
		// Check for schema.table format
		if idx := strings.Index(tableName, "."); idx != -1 {
			schema = tableName[:idx]
			tableName = tableName[idx+1:]
		}
		
		return &DropTableStatement{
			Schema:   schema,
			Table:    tableName,
			IfExists: true,
		}, nil
	} else if strings.HasPrefix(upperSQL, "DROP TABLE") {
		sql = sql[10:] // len("DROP TABLE")
		tableName := strings.TrimSpace(sql)
		schema := "main"
		
		// Check for schema.table format
		if idx := strings.Index(tableName, "."); idx != -1 {
			schema = tableName[:idx]
			tableName = tableName[idx+1:]
		}
		
		return &DropTableStatement{
			Schema:   schema,
			Table:    tableName,
			IfExists: false,
		}, nil
	}
	
	return nil, fmt.Errorf("invalid DROP TABLE statement")
}

// parseUpdate parses an UPDATE statement
func parseUpdate(sql string) (Statement, error) {
	// Very basic UPDATE table SET col = val WHERE condition parsing
	
	// Remove UPDATE keyword
	sql = sql[6:] // len("UPDATE")
	sql = strings.TrimSpace(sql)
	
	// Global parameter counter
	paramIndex := 1
	
	// Find SET keyword
	setIdx := strings.Index(strings.ToUpper(sql), " SET ")
	if setIdx == -1 {
		return nil, fmt.Errorf("UPDATE missing SET clause")
	}
	
	// Extract table name
	tableName := strings.TrimSpace(sql[:setIdx])
	sql = sql[setIdx+5:] // Skip " SET "
	
	// Find WHERE clause (optional)
	whereIdx := strings.Index(strings.ToUpper(sql), " WHERE ")
	setPart := sql
	var wherePart string
	
	if whereIdx != -1 {
		setPart = strings.TrimSpace(sql[:whereIdx])
		wherePart = strings.TrimSpace(sql[whereIdx+7:]) // Skip " WHERE "
	}
	
	// Parse SET clauses (very basic for now)
	sets := []SetClause{}
	setPairs := strings.Split(setPart, ",")
	
	for _, pair := range setPairs {
		pair = strings.TrimSpace(pair)
		eqIdx := strings.Index(pair, "=")
		if eqIdx == -1 {
			return nil, fmt.Errorf("invalid SET clause: %s", pair)
		}
		
		column := strings.TrimSpace(pair[:eqIdx])
		valuePart := strings.TrimSpace(pair[eqIdx+1:])
		
		// Parse value expression
		var valueExpr Expression
		if valuePart == "?" {
			// Parameter
			valueExpr = &ParameterExpr{Index: paramIndex}
			paramIndex++
		} else if strings.HasPrefix(valuePart, "'") && strings.HasSuffix(valuePart, "'") {
			// String literal
			valueExpr = &ConstantExpr{Value: valuePart[1:len(valuePart)-1]}
		} else if strings.Contains(valuePart, "*") || strings.Contains(valuePart, "+") || strings.Contains(valuePart, "-") {
			// Expression (very basic handling)
			valueExpr = parseMathExpression(valuePart)
		} else if num, err := parseNumber(valuePart); err == nil {
			// Number
			valueExpr = &ConstantExpr{Value: num}
		} else {
			// Treat as column reference or string
			valueExpr = &ConstantExpr{Value: valuePart}
		}
		
		sets = append(sets, SetClause{
			Column: column,
			Value:  valueExpr,
		})
	}
	
	// Parse WHERE clause
	var whereExpr Expression
	if wherePart != "" {
		whereExpr = parseWhereExpressionWithParams(wherePart, &paramIndex)
	}
	
	return &UpdateStatement{
		Table: &TableRef{Table: tableName},
		Sets:  sets,
		Where: whereExpr,
	}, nil
}

// parseDelete parses a DELETE statement
func parseDelete(sql string) (Statement, error) {
	// Very basic DELETE FROM table WHERE condition parsing
	upperSQL := strings.ToUpper(sql)
	
	// Remove DELETE keyword
	if strings.HasPrefix(upperSQL, "DELETE FROM ") {
		sql = sql[12:] // len("DELETE FROM ")
	} else if strings.HasPrefix(upperSQL, "DELETE ") {
		sql = sql[7:] // len("DELETE ")
		// Check for FROM
		if strings.HasPrefix(strings.ToUpper(sql), "FROM ") {
			sql = sql[5:] // len("FROM ")
		}
	} else {
		return nil, fmt.Errorf("invalid DELETE statement")
	}
	
	sql = strings.TrimSpace(sql)
	
	// Find WHERE clause (optional but usually present)
	whereIdx := strings.Index(strings.ToUpper(sql), " WHERE ")
	tableName := sql
	var wherePart string
	
	if whereIdx != -1 {
		tableName = strings.TrimSpace(sql[:whereIdx])
		wherePart = strings.TrimSpace(sql[whereIdx+7:]) // Skip " WHERE "
	}
	
	// Parse WHERE clause
	var whereExpr Expression
	if wherePart != "" {
		whereExpr = parseWhereExpression(wherePart)
	}
	
	return &DeleteStatement{
		Table: &TableRef{Table: tableName},
		Where: whereExpr,
	}, nil
}

// parseMathExpression parses simple math expressions like "value * 1.1"
func parseMathExpression(expr string) Expression {
	// Very basic math expression parsing
	// For now, just return as a constant expression
	// In a real implementation, this would parse into a proper expression tree
	
	// Handle simple cases like "value * 1.1"
	if strings.Contains(expr, "*") {
		parts := strings.Split(expr, "*")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])
			
			var leftExpr, rightExpr Expression
			
			// Parse left side
			if num, err := parseNumber(left); err == nil {
				leftExpr = &ConstantExpr{Value: num}
			} else {
				leftExpr = &ColumnExpr{Column: left}
			}
			
			// Parse right side
			if num, err := parseNumber(right); err == nil {
				rightExpr = &ConstantExpr{Value: num}
			} else {
				rightExpr = &ColumnExpr{Column: right}
			}
			
			return &BinaryExpr{
				Left:  leftExpr,
				Op:    OpMult,
				Right: rightExpr,
			}
		}
	}
	
	// For other cases, just return as column reference
	return &ColumnExpr{Column: expr}
}

// parseFunctionExpression parses a function call like COUNT(*) or SUM(column)
func parseFunctionExpression(funcCall string) Expression {
	funcCall = strings.TrimSpace(funcCall)
	
	// Find the opening parenthesis
	parenIdx := strings.Index(funcCall, "(")
	if parenIdx == -1 {
		return nil
	}
	
	// Extract function name and arguments
	funcName := strings.TrimSpace(funcCall[:parenIdx])
	argsStr := strings.TrimSpace(funcCall[parenIdx+1:])
	
	// Remove closing parenthesis
	if !strings.HasSuffix(argsStr, ")") {
		return nil
	}
	argsStr = argsStr[:len(argsStr)-1]
	
	// Parse arguments
	var args []Expression
	if argsStr != "" {
		if argsStr == "*" {
			// Special case for COUNT(*)
			args = []Expression{&ColumnExpr{Column: "*"}}
		} else {
			// Parse comma-separated arguments, respecting brackets and parentheses
			argParts := splitFunctionArguments(argsStr)
			for _, arg := range argParts {
				arg = strings.TrimSpace(arg)
				if arg == "*" {
					args = append(args, &ColumnExpr{Column: "*"})
				} else {
					// Check for array literal first
					if strings.HasPrefix(arg, "[") && strings.HasSuffix(arg, "]") {
						// Parse as array literal
						arrayExpr := parseArrayLiteral(arg)
						args = append(args, arrayExpr)
					} else {
						// Try to parse as constant or column, or complex expression (including nested functions)
						if val, err := parseValue(arg); err == nil {
							args = append(args, &ConstantExpr{Value: val})
						} else {
							args = append(args, parseComplexExpressionValue(arg))
						}
					}
				}
			}
		}
	}
	
	return &FunctionExpr{
		Name: strings.ToUpper(funcName),
		Args: args,
	}
}

// parseSelectColumns parses the column list in a SELECT statement
func parseSelectColumns(selectPart string) ([]Expression, error) {
	colParts := splitSelectColumns(selectPart)
	var columns []Expression
	
	for _, col := range colParts {
		col = strings.TrimSpace(col)
		if col == "*" {
			columns = append(columns, &ColumnExpr{Column: "*"})
		} else {
			// Check for alias (e.g., "COUNT(*) as count" or "amount AS total")
			var expr Expression
			
			// Split on "AS" or "as" (case insensitive)
			upperCol := strings.ToUpper(col)
			asIdx := strings.Index(upperCol, " AS ")
			if asIdx != -1 {
				exprPart := strings.TrimSpace(col[:asIdx])
				alias := strings.TrimSpace(col[asIdx+4:]) // Skip " AS "
				
				// Parse the expression part
				expr = parseComplexExpressionValue(exprPart)
				if funcExpr, ok := expr.(*FunctionExpr); ok {
					// Set the alias for function expressions
					funcExpr.Alias = alias
				}
			} else {
				// No alias, parse the entire column
				expr = parseComplexExpressionValue(col)
			}
			
			// For now, ignore the alias and just use the expression
			// In a full implementation, we'd store the alias information
			columns = append(columns, expr)
		}
	}
	
	return columns, nil
}

// parseColumnExpression parses a column expression (potentially qualified with table alias)
func parseColumnExpression(col string) Expression {
	col = strings.TrimSpace(col)
	
	// Check for array/map access with brackets first
	if strings.Contains(col, "[") {
		bracketIdx := strings.Index(col, "[")
		base := col[:bracketIdx]
		indexPart := col[bracketIdx+1:]
		
		// Remove closing bracket
		if strings.HasSuffix(indexPart, "]") {
			indexPart = indexPart[:len(indexPart)-1]
		}
		
		// Parse the base expression
		baseExpr := parseColumnExpression(base)
		
		// Parse the index/key
		var keyExpr Expression
		if strings.HasPrefix(indexPart, "'") && strings.HasSuffix(indexPart, "'") {
			// String key
			keyExpr = &ConstantExpr{Value: indexPart[1:len(indexPart)-1]}
		} else if num, err := parseNumber(indexPart); err == nil {
			// Numeric index
			keyExpr = &ConstantExpr{Value: num}
		} else {
			// Column reference or other expression
			keyExpr = parseColumnExpression(indexPart)
		}
		
		// Return a function expression for array/map access
		return &FunctionExpr{
			Name: "element_at",
			Args: []Expression{baseExpr, keyExpr},
		}
	}
	
	// Check for dots which could indicate table.column or struct field access
	if strings.Contains(col, ".") {
		parts := strings.Split(col, ".")
		
		// If we have multiple dots, it's likely struct field access
		// e.g., customer_info.addresses.billing.city
		if len(parts) > 2 {
			// Start with the first part as a column
			expr := Expression(&ColumnExpr{Column: parts[0]})
			
			// Chain field accesses
			for i := 1; i < len(parts); i++ {
				expr = &FieldAccessExpr{
					Expr:  expr,
					Field: strings.TrimSpace(parts[i]),
				}
			}
			return expr
		} else if len(parts) == 2 {
			// Could be table.column or column.field
			// For now, we'll assume it's table.column if the first part is a known table alias
			// Otherwise, treat it as field access
			// TODO: This needs context about table aliases to be more accurate
			
			// Common table aliases
			knownAliases := map[string]bool{
				"c": true, "o": true, "p": true, "s": true,
				"customers": true, "orders": true, "products": true,
			}
			
			firstPart := strings.TrimSpace(parts[0])
			if knownAliases[firstPart] {
				// Treat as table.column
				return &ColumnExpr{
					Table:  firstPart,
					Column: strings.TrimSpace(parts[1]),
				}
			} else {
				// Treat as column.field
				return &FieldAccessExpr{
					Expr:  &ColumnExpr{Column: firstPart},
					Field: strings.TrimSpace(parts[1]),
				}
			}
		}
	}
	
	return &ColumnExpr{Column: col}
}

// parseFromClause parses FROM table [alias] [JOIN ...] and returns the table, joins, and remainder
func parseFromClause(fromPart string) (*TableRef, []JoinClause, string, error) {
	fromPart = strings.TrimSpace(fromPart)
	
	// For now, implement simple parsing - just handle basic table with optional alias
	// Complex JOIN parsing would be more sophisticated
	
	// Find the first JOIN keyword (if any)
	upperFrom := strings.ToUpper(fromPart)
	joinIdx := -1
	joinKeywords := []string{" JOIN ", " INNER JOIN ", " LEFT JOIN ", " RIGHT JOIN "}
	
	for _, keyword := range joinKeywords {
		if idx := strings.Index(upperFrom, keyword); idx != -1 {
			if joinIdx == -1 || idx < joinIdx {
				joinIdx = idx
			}
		}
	}
	
	var mainTable *TableRef
	var joins []JoinClause
	var remainder string
	
	if joinIdx == -1 {
		// No JOINs, parse simple FROM table [alias] WHERE/LIMIT...
		parts := strings.Fields(fromPart)
		if len(parts) == 0 {
			return nil, nil, "", fmt.Errorf("empty FROM clause")
		}
		
		tableName := parts[0]
		alias := ""
		nextIdx := 1
		
		// Check if next part is an alias (not a keyword)
		if len(parts) > 1 {
			next := strings.ToUpper(parts[1])
			if next != "WHERE" && next != "JOIN" && next != "INNER" && next != "LEFT" && next != "RIGHT" && next != "ORDER" && next != "GROUP" && next != "LIMIT" {
				alias = parts[1]
				nextIdx = 2
			}
		}
		
		mainTable = &TableRef{Table: tableName, Alias: alias}
		
		// Everything else is remainder
		if nextIdx < len(parts) {
			remainder = strings.Join(parts[nextIdx:], " ")
		}
	} else {
		// Has JOINs - parse complex FROM clause with JOINs
		tablePart := fromPart[:joinIdx]
		joinPart := fromPart[joinIdx:]
		
		// Parse main table with optional alias
		parts := strings.Fields(tablePart)
		if len(parts) == 0 {
			return nil, nil, "", fmt.Errorf("empty FROM clause")
		}
		
		tableName := parts[0]
		alias := ""
		
		// Check if next part is an alias (not a keyword)
		if len(parts) > 1 {
			next := strings.ToUpper(parts[1])
			if next != "WHERE" && next != "JOIN" && next != "INNER" && next != "LEFT" && next != "RIGHT" && next != "ORDER" && next != "GROUP" && next != "LIMIT" {
				alias = parts[1]
			}
		}
		
		mainTable = &TableRef{Table: tableName, Alias: alias}
		
		// Parse JOINs
		var err error
		joins, remainder, err = parseJoins(joinPart)
		if err != nil {
			return nil, nil, "", err
		}
	}
	
	return mainTable, joins, remainder, nil
}

// parseSelectRemainder parses WHERE, GROUP BY, ORDER BY, LIMIT etc. from the remainder of a SELECT

// parseCompleteSelectRemainder parses WHERE, GROUP BY, HAVING, ORDER BY, LIMIT etc. from the remainder of a SELECT
func parseCompleteSelectRemainder(remainder string) (Expression, []Expression, Expression, []OrderByExpr, *int64, error) {
	remainder = strings.TrimSpace(remainder)
	if remainder == "" {
		return nil, nil, nil, nil, nil, nil
	}
	
	var where Expression
	var groupBy []Expression
	var having Expression
	var orderBy []OrderByExpr
	var limit *int64
	
	// Parse each clause in order
	parts := strings.Fields(remainder)
	if len(parts) == 0 {
		return nil, nil, nil, nil, nil, nil
	}
	
	i := 0
	for i < len(parts) {
		upperPart := strings.ToUpper(parts[i])
		
		switch upperPart {
		case "WHERE":
			// Find the end of the WHERE clause
			whereStart := i + 1
			whereEnd := findNextClauseStart(parts, whereStart, []string{"GROUP", "HAVING", "ORDER", "LIMIT"})
			if whereEnd == -1 {
				whereEnd = len(parts)
			}
			
			if whereStart < len(parts) {
				whereStr := strings.Join(parts[whereStart:whereEnd], " ")
				where = parseWhereExpression(whereStr)
			}
			i = whereEnd
			
		case "GROUP":
			// Expect "GROUP BY"
			if i+1 < len(parts) && strings.ToUpper(parts[i+1]) == "BY" {
				groupByStart := i + 2
				groupByEnd := findNextClauseStart(parts, groupByStart, []string{"HAVING", "ORDER", "LIMIT"})
				if groupByEnd == -1 {
					groupByEnd = len(parts)
				}
				
				if groupByStart < len(parts) {
					groupByStr := strings.Join(parts[groupByStart:groupByEnd], " ")
					groupBy = parseGroupByColumns(groupByStr)
				}
				i = groupByEnd
			} else {
				i++
			}
			
		case "HAVING":
			// Parse HAVING clause
			havingStart := i + 1
			havingEnd := findNextClauseStart(parts, havingStart, []string{"ORDER", "LIMIT"})
			if havingEnd == -1 {
				havingEnd = len(parts)
			}
			
			if havingStart < len(parts) {
				havingStr := strings.Join(parts[havingStart:havingEnd], " ")
				having = parseHavingExpression(havingStr)
			}
			i = havingEnd
			
		case "ORDER":
			// Expect "ORDER BY"
			if i+1 < len(parts) && strings.ToUpper(parts[i+1]) == "BY" {
				orderByStart := i + 2
				orderByEnd := findNextClauseStart(parts, orderByStart, []string{"LIMIT"})
				if orderByEnd == -1 {
					orderByEnd = len(parts)
				}
				
				if orderByStart < len(parts) {
					orderByStr := strings.Join(parts[orderByStart:orderByEnd], " ")
					orderBy = parseOrderByColumns(orderByStr)
				}
				i = orderByEnd
			} else {
				i++
			}
			
		case "LIMIT":
			// Parse LIMIT value
			if i+1 < len(parts) {
				if limitVal, err := strconv.ParseInt(parts[i+1], 10, 64); err == nil {
					limit = &limitVal
				}
				i += 2
			} else {
				i++
			}
			
		default:
			i++
		}
	}
	
	return where, groupBy, having, orderBy, limit, nil
}

// findNextClauseStart finds the next SQL clause keyword
func findNextClauseStart(parts []string, start int, clauses []string) int {
	for i := start; i < len(parts); i++ {
		upperPart := strings.ToUpper(parts[i])
		for _, clause := range clauses {
			if upperPart == clause {
				return i
			}
		}
	}
	return -1
}

// parseGroupByColumns parses GROUP BY column list
func parseGroupByColumns(groupByStr string) []Expression {
	var groupBy []Expression
	
	// Split by comma and parse each column
	colParts := strings.Split(groupByStr, ",")
	for _, col := range colParts {
		col = strings.TrimSpace(col)
		if col != "" {
			groupBy = append(groupBy, parseColumnExpression(col))
		}
	}
	
	return groupBy
}

// parseOrderByColumns parses ORDER BY column list
func parseOrderByColumns(orderByStr string) []OrderByExpr {
	var orderBy []OrderByExpr
	
	// Split by comma and parse each column
	colParts := strings.Split(orderByStr, ",")
	for _, col := range colParts {
		col = strings.TrimSpace(col)
		if col != "" {
			// Check for DESC
			desc := false
			if strings.HasSuffix(strings.ToUpper(col), " DESC") {
				desc = true
				col = strings.TrimSpace(col[:len(col)-5])
			} else if strings.HasSuffix(strings.ToUpper(col), " ASC") {
				col = strings.TrimSpace(col[:len(col)-4])
			}
			
			orderBy = append(orderBy, OrderByExpr{
				Expr: parseColumnExpression(col),
				Desc: desc,
			})
		}
	}
	
	return orderBy
}

// parseJoins parses JOIN clauses and returns joins and remainder
func parseJoins(joinPart string) ([]JoinClause, string, error) {
	joinPart = strings.TrimSpace(joinPart)
	var joins []JoinClause
	remainder := ""
	
	// Process JOIN clauses one by one
	for len(joinPart) > 0 {
		// Find the JOIN type
		upperJoinPart := strings.ToUpper(joinPart)
		
		var joinType JoinType
		var keywordLen int
		
		if strings.HasPrefix(upperJoinPart, "INNER JOIN ") {
			joinType = InnerJoin
			keywordLen = 11
		} else if strings.HasPrefix(upperJoinPart, "LEFT JOIN ") {
			joinType = LeftJoin
			keywordLen = 10
		} else if strings.HasPrefix(upperJoinPart, "RIGHT JOIN ") {
			joinType = RightJoin
			keywordLen = 11
		} else if strings.HasPrefix(upperJoinPart, "JOIN ") {
			joinType = InnerJoin
			keywordLen = 5
		} else {
			// Not a JOIN, this is the remainder
			remainder = joinPart
			break
		}
		
		// Skip the JOIN keyword
		afterJoin := strings.TrimSpace(joinPart[keywordLen:])
		
		// Find the ON keyword
		onIdx := strings.Index(strings.ToUpper(afterJoin), " ON ")
		if onIdx == -1 {
			return nil, "", fmt.Errorf("JOIN missing ON clause")
		}
		
		// Parse table name and alias
		tablePart := strings.TrimSpace(afterJoin[:onIdx])
		parts := strings.Fields(tablePart)
		if len(parts) == 0 {
			return nil, "", fmt.Errorf("empty table name in JOIN")
		}
		
		tableName := parts[0]
		alias := ""
		if len(parts) > 1 {
			alias = parts[1]
		}
		
		table := &TableRef{Table: tableName, Alias: alias}
		
		// Parse ON condition
		afterOn := strings.TrimSpace(afterJoin[onIdx+4:]) // Skip " ON "
		
		// Find the end of the ON condition - look for next JOIN or clause keywords
		conditionEnd := len(afterOn)
		nextJoinIdx := findNextJoin(afterOn)
		nextClauseIdx := findNextClause(afterOn)
		
		if nextJoinIdx != -1 && (nextClauseIdx == -1 || nextJoinIdx < nextClauseIdx) {
			conditionEnd = nextJoinIdx
		} else if nextClauseIdx != -1 {
			conditionEnd = nextClauseIdx
		}
		
		conditionPart := strings.TrimSpace(afterOn[:conditionEnd])
		condition := parseJoinCondition(conditionPart)
		
		// Create JOIN clause
		joins = append(joins, JoinClause{
			Type:      joinType,
			Table:     table,
			Condition: condition,
		})
		
		// Continue with the rest
		if conditionEnd < len(afterOn) {
			joinPart = strings.TrimSpace(afterOn[conditionEnd:])
		} else {
			joinPart = ""
		}
	}
	
	return joins, remainder, nil
}

// findNextJoin finds the index of the next JOIN keyword
func findNextJoin(s string) int {
	upper := strings.ToUpper(s)
	joinKeywords := []string{" INNER JOIN ", " LEFT JOIN ", " RIGHT JOIN ", " JOIN "}
	
	minIdx := -1
	for _, keyword := range joinKeywords {
		idx := strings.Index(upper, keyword)
		if idx != -1 && (minIdx == -1 || idx < minIdx) {
			minIdx = idx
		}
	}
	
	return minIdx
}

// findNextClause finds the index of the next SQL clause keyword
func findNextClause(s string) int {
	upper := strings.ToUpper(s)
	clauses := []string{" WHERE ", " GROUP ", " ORDER ", " HAVING ", " LIMIT "}
	
	minIdx := -1
	for _, clause := range clauses {
		idx := strings.Index(upper, clause)
		if idx != -1 && (minIdx == -1 || idx < minIdx) {
			minIdx = idx
		}
	}
	
	return minIdx
}

// parseJoinCondition parses a JOIN ON condition
func parseJoinCondition(condition string) Expression {
	// For now, parse simple equality conditions like "o.customer_id = c.id"
	// In a full implementation, this would be more sophisticated
	
	// Look for equality operator
	eqIdx := strings.Index(condition, "=")
	if eqIdx == -1 {
		// Fallback: return a simple column expression
		return &ColumnExpr{Column: condition}
	}
	
	left := strings.TrimSpace(condition[:eqIdx])
	right := strings.TrimSpace(condition[eqIdx+1:])
	
	// Parse left and right sides as column expressions
	leftExpr := parseJoinColumn(left)
	rightExpr := parseJoinColumn(right)
	
	return &BinaryExpr{
		Left:  leftExpr,
		Op:    OpEq,
		Right: rightExpr,
	}
}

// parseJoinColumn parses a column reference in a JOIN condition
func parseJoinColumn(col string) Expression {
	col = strings.TrimSpace(col)
	
	// Check for table.column syntax
	if strings.Contains(col, ".") {
		parts := strings.Split(col, ".")
		if len(parts) == 2 {
			return &ColumnExpr{
				Table:  strings.TrimSpace(parts[0]),
				Column: strings.TrimSpace(parts[1]),
			}
		}
	}
	
	return &ColumnExpr{Column: col}
}

// parseHavingExpression parses a HAVING clause expression 
// HAVING expressions can reference aggregate functions and GROUP BY columns
func parseHavingExpression(having string) Expression {
	// For now, parse it the same as WHERE expressions
	// In a full implementation, we'd handle aggregate function references differently
	return parseComplexExpression(having)
}

// parseComplexExpression parses complex expressions with AND/OR operators
func parseComplexExpression(expr string) Expression {
	expr = strings.TrimSpace(expr)
	upperExpr := strings.ToUpper(expr)
	
	// Handle AND expressions first (higher precedence)
	if andIdx := strings.Index(upperExpr, " AND "); andIdx != -1 {
		left := strings.TrimSpace(expr[:andIdx])
		right := strings.TrimSpace(expr[andIdx+5:]) // Skip " AND "
		
		return &BinaryExpr{
			Left:  parseSimpleExpression(left),
			Op:    OpAnd,
			Right: parseComplexExpression(right),
		}
	}
	
	// Handle OR expressions (lower precedence)
	if orIdx := strings.Index(upperExpr, " OR "); orIdx != -1 {
		left := strings.TrimSpace(expr[:orIdx])
		right := strings.TrimSpace(expr[orIdx+4:]) // Skip " OR "
		
		return &BinaryExpr{
			Left:  parseSimpleExpression(left),
			Op:    OpOr,
			Right: parseComplexExpression(right),
		}
	}
	
	// No AND/OR, parse as simple expression
	return parseSimpleExpression(expr)
}

// parseSimpleExpression parses a simple comparison expression
func parseSimpleExpression(expr string) Expression {
	expr = strings.TrimSpace(expr)
	
	// Check for aggregate functions like COUNT(*) or SUM(amount)
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		// Find comparison operator after the function
		operators := []string{">=", "<=", "!=", "<>", "=", ">", "<"}
		
		for _, op := range operators {
			if idx := strings.Index(expr, op); idx != -1 {
				left := strings.TrimSpace(expr[:idx])
				right := strings.TrimSpace(expr[idx+len(op):])
				
				// Parse left side as function expression
				var leftExpr Expression
				if funcExpr := parseFunctionExpression(left); funcExpr != nil {
					leftExpr = funcExpr
				} else {
					leftExpr = parseColumnExpression(left)
				}
				
				// Parse right side as value
				var rightExpr Expression
				if val, err := parseValue(right); err == nil {
					rightExpr = &ConstantExpr{Value: val}
				} else {
					rightExpr = parseColumnExpression(right)
				}
				
				// Map operator to BinaryOp
				var binOp BinaryOp
				switch op {
				case "=":
					binOp = OpEq
				case "!=", "<>":
					binOp = OpNe
				case "<":
					binOp = OpLt
				case "<=":
					binOp = OpLe
				case ">":
					binOp = OpGt
				case ">=":
					binOp = OpGe
				}
				
				return &BinaryExpr{
					Left:  leftExpr,
					Op:    binOp,
					Right: rightExpr,
				}
			}
		}
	}
	
	// Fallback to regular WHERE expression parsing
	paramIdx := 1
	return parseWhereExpressionWithParams(expr, &paramIdx)
}

// parseUnion parses UNION/UNION ALL statements
func parseUnion(sql string) *UnionStatement {
	// Clean up SQL formatting first
	sql = strings.TrimSpace(sql)
	sql = strings.Join(strings.Fields(sql), " ")
	upperSQL := strings.ToUpper(sql)
	
	// Find all UNION positions to handle chained UNIONs
	return parseChainedUnion(sql, upperSQL)
}

// parseChainedUnion handles chained UNION operations by building a left-associative tree
func parseChainedUnion(sql, upperSQL string) *UnionStatement {
	// First, check if there are any UNIONs at all
	hasUnionAll := findTopLevelUnion(sql, upperSQL, "UNION ALL") != -1
	hasUnion := findTopLevelUnion(sql, upperSQL, "UNION") != -1
	
	if !hasUnionAll && !hasUnion {
		return nil
	}
	
	// Find the first (leftmost) UNION or UNION ALL
	firstUnionAllIdx := findTopLevelUnion(sql, upperSQL, "UNION ALL")
	firstUnionIdx := findTopLevelUnion(sql, upperSQL, "UNION")
	
	// Determine which comes first
	var firstIdx int
	var isUnionAll bool
	var keywordLen int
	
	if firstUnionAllIdx != -1 && (firstUnionIdx == -1 || firstUnionAllIdx < firstUnionIdx) {
		// UNION ALL comes first
		firstIdx = firstUnionAllIdx
		isUnionAll = true
		keywordLen = 9 // len("UNION ALL")
	} else if firstUnionIdx != -1 {
		// UNION comes first, but make sure it's not actually "UNION ALL"
		afterUnion := strings.TrimSpace(sql[firstUnionIdx+5:])
		if strings.HasPrefix(strings.ToUpper(afterUnion), "ALL") {
			// This is actually UNION ALL, but we missed it in our first check
			firstIdx = firstUnionIdx
			isUnionAll = true
			keywordLen = 9
		} else {
			firstIdx = firstUnionIdx
			isUnionAll = false
			keywordLen = 5 // len("UNION")
		}
	} else {
		return nil
	}
	
	// Split at the first UNION
	leftSQL := strings.TrimSpace(sql[:firstIdx])
	rightSQL := strings.TrimSpace(sql[firstIdx+keywordLen:])
	
	// Check if the right side contains more UNIONs
	rightUpperSQL := strings.ToUpper(rightSQL)
	rightHasUnion := findTopLevelUnion(rightSQL, rightUpperSQL, "UNION ALL") != -1 || 
		findTopLevelUnion(rightSQL, rightUpperSQL, "UNION") != -1
	
	var leftStmt Statement
	var rightStmt Statement
	var orderBy []OrderByExpr
	var limit *int64
	
	// Parse left side as a single SELECT
	var err error
	leftStmt, err = parseSingleSelect(leftSQL)
	if err != nil {
		return nil
	}
	
	if rightHasUnion {
		// Right side contains more UNIONs, parse recursively
		// But first, extract ORDER BY and LIMIT from the very end
		rightSQL, orderBy, limit = extractUnionModifiers(rightSQL)
		
		// Parse right side as a chained UNION
		rightUnion := parseChainedUnion(rightSQL, strings.ToUpper(rightSQL))
		if rightUnion != nil {
			rightStmt = rightUnion
		} else {
			// If parsing as chained union fails, try as single SELECT
			rightStmt, err = parseSingleSelect(rightSQL)
			if err != nil {
				return nil
			}
		}
	} else {
		// Right side is a single SELECT, extract modifiers and parse
		rightSQL, orderBy, limit = extractUnionModifiers(rightSQL)
		rightStmt, err = parseSingleSelect(rightSQL)
		if err != nil {
			return nil
		}
	}
	
	return &UnionStatement{
		Left:     leftStmt,
		Right:    rightStmt,
		UnionAll: isUnionAll,
		OrderBy:  orderBy,
		Limit:    limit,
	}
}

// findTopLevelUnion finds UNION/UNION ALL at the top level (not in subqueries)
func findTopLevelUnion(sql, upperSQL, keyword string) int {
	parenDepth := 0
	inString := false
	keywordLen := len(keyword)
	
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		
		// Track string literals
		if ch == '\'' && (i == 0 || sql[i-1] != '\\') {
			inString = !inString
			continue
		}
		
		if inString {
			continue
		}
		
		// Track parentheses depth
		switch ch {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		}
		
		// Look for the keyword at the top level (parenDepth == 0)
		if parenDepth == 0 && i+keywordLen <= len(upperSQL) {
			if upperSQL[i:i+keywordLen] == keyword {
				// Make sure it's a word boundary
				if (i == 0 || upperSQL[i-1] == ' ') && 
				   (i+keywordLen == len(upperSQL) || upperSQL[i+keywordLen] == ' ') {
					return i
				}
			}
		}
	}
	
	return -1
}

// parseSingleSelect parses a single SELECT statement (not UNION)
func parseSingleSelect(sql string) (Statement, error) {
	sql = strings.TrimSpace(sql)
	if !strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
		return nil, fmt.Errorf("expected SELECT statement")
	}
	
	// Clean up SQL formatting
	sql = strings.Join(strings.Fields(sql), " ")
	
	// Parse as a regular SELECT
	parts := strings.Fields(sql)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid SELECT statement")
	}
	
	// Handle simple cases first
	if len(parts) == 2 {
		// SELECT constant
		if num, err := parseNumber(parts[1]); err == nil {
			return &SelectStatement{
				Columns: []Expression{&ConstantExpr{Value: num}},
			}, nil
		}
		return &SelectStatement{
			Columns: []Expression{&ConstantExpr{Value: parts[1]}},
		}, nil
	}
	
	// Parse full SELECT statement
	if strings.Contains(strings.ToUpper(sql), " FROM ") {
		fromIdx := strings.Index(strings.ToUpper(sql), " FROM ")
		selectPart := sql[7:fromIdx] // Skip "SELECT "
		fromPart := sql[fromIdx+6:]   // Skip " FROM "
		
		// Parse columns
		columns, err := parseSelectColumns(selectPart)
		if err != nil {
			return nil, err
		}
		
		// Parse FROM clause
		fromClause, joins, remainder, err := parseFromClause(fromPart)
		if err != nil {
			return nil, err
		}
		
		// Parse remainder (WHERE, GROUP BY, etc.)
		where, groupBy, having, orderBy, limit, err := parseCompleteSelectRemainder(remainder)
		if err != nil {
			return nil, err
		}
		
		return &SelectStatement{
			Columns: columns,
			From:    fromClause,
			Joins:   joins,
			Where:   where,
			GroupBy: groupBy,
			Having:  having,
			OrderBy: orderBy,
			Limit:   limit,
		}, nil
	}
	
	// Handle column-only SELECT (e.g., SELECT 'High Value' as type, id, amount)
	// But first check if this might have WHERE, ORDER BY, etc without FROM
	upperSQL := strings.ToUpper(sql)
	selectEnd := len(sql)
	
	// Look for clauses that might appear after the column list
	clauseKeywords := []string{" WHERE ", " ORDER BY ", " GROUP BY ", " HAVING ", " LIMIT "}
	for _, keyword := range clauseKeywords {
		if idx := strings.Index(upperSQL, keyword); idx != -1 {
			selectEnd = idx
			break
		}
	}
	
	selectPart := sql[7:selectEnd] // Skip "SELECT "
	remainder := ""
	if selectEnd < len(sql) {
		remainder = sql[selectEnd:]
	}
	
	columns, err := parseSelectColumns(selectPart)
	if err != nil {
		return nil, err
	}
	
	stmt := &SelectStatement{
		Columns: columns,
	}
	
	// Parse remainder for WHERE, ORDER BY, etc. if present
	if remainder != "" {
		where, groupBy, having, orderBy, limit, err := parseCompleteSelectRemainder(remainder)
		if err != nil {
			return nil, err
		}
		stmt.Where = where
		stmt.GroupBy = groupBy
		stmt.Having = having
		stmt.OrderBy = orderBy
		stmt.Limit = limit
	}
	
	return stmt, nil
}

// extractUnionModifiers extracts ORDER BY and LIMIT from the end of a UNION query
func extractUnionModifiers(sql string) (string, []OrderByExpr, *int64) {
	sql = strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(sql)
	
	var orderBy []OrderByExpr
	var limit *int64
	
	// Look for ORDER BY
	orderByIdx := strings.LastIndex(upperSQL, " ORDER BY ")
	if orderByIdx != -1 {
		// Extract ORDER BY clause
		orderByPart := sql[orderByIdx+10:] // Skip " ORDER BY "
		
		// Check if there's a LIMIT after ORDER BY
		limitIdx := strings.Index(strings.ToUpper(orderByPart), " LIMIT ")
		if limitIdx != -1 {
			// Parse ORDER BY columns
			orderByStr := orderByPart[:limitIdx]
			orderBy = parseOrderByColumns(orderByStr)
			
			// Parse LIMIT
			limitPart := strings.TrimSpace(orderByPart[limitIdx+7:]) // Skip " LIMIT "
			if limitVal, err := strconv.ParseInt(limitPart, 10, 64); err == nil {
				limit = &limitVal
			}
		} else {
			// No LIMIT, just ORDER BY
			orderBy = parseOrderByColumns(orderByPart)
		}
		
		// Remove ORDER BY and LIMIT from SQL
		sql = strings.TrimSpace(sql[:orderByIdx])
	} else {
		// No ORDER BY, check for LIMIT
		limitIdx := strings.LastIndex(upperSQL, " LIMIT ")
		if limitIdx != -1 {
			limitPart := strings.TrimSpace(sql[limitIdx+7:]) // Skip " LIMIT "
			if limitVal, err := strconv.ParseInt(limitPart, 10, 64); err == nil {
				limit = &limitVal
			}
			
			// Remove LIMIT from SQL
			sql = strings.TrimSpace(sql[:limitIdx])
		}
	}
	
	return sql, orderBy, limit
}

// splitSelectColumns splits SELECT columns properly handling parentheses for subqueries
func splitSelectColumns(selectPart string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	inString := false
	
	for i, ch := range selectPart {
		switch ch {
		case '\'':
			if i == 0 || selectPart[i-1] != '\\' {
				inString = !inString
			}
			current.WriteRune(ch)
		case '(':
			if !inString {
				parenDepth++
			}
			current.WriteRune(ch)
		case ')':
			if !inString {
				parenDepth--
			}
			current.WriteRune(ch)
		case ',':
			if !inString && parenDepth == 0 {
				// Only split on top-level commas
				if current.Len() > 0 {
					parts = append(parts, strings.TrimSpace(current.String()))
					current.Reset()
				}
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}
	
	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}
	
	return parts
}

// findTopLevelFromClause finds the FROM clause at the top level (not in subqueries)
func findTopLevelFromClause(sql string) int {
	upperSQL := strings.ToUpper(sql)
	parenDepth := 0
	inString := false
	
	// Start after "SELECT "
	for i := 7; i < len(sql)-5; i++ { // -5 to ensure we have room for " FROM"
		ch := sql[i]
		
		// Track string literals
		if ch == '\'' && (i == 0 || sql[i-1] != '\\') {
			inString = !inString
			continue
		}
		
		if inString {
			continue
		}
		
		// Track parentheses depth
		switch ch {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		}
		
		// Look for " FROM " at the top level (parenDepth == 0)
		if parenDepth == 0 && i+5 < len(upperSQL) { // Check bounds properly
			if upperSQL[i:i+6] == " FROM " {
				return i
			}
		}
	}
	
	return -1
}

// parseComplexExpressionValue parses expressions that might include subqueries
func parseComplexExpressionValue(s string) Expression {
	s = strings.TrimSpace(s)
	
	// Check for subquery (starts with SELECT in parentheses)
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		inner := strings.TrimSpace(s[1 : len(s)-1])
		if strings.HasPrefix(strings.ToUpper(inner), "SELECT") {
			// This is a subquery
			subqueryStmt, err := parseSelect(inner)
			if err == nil {
				return &SubqueryExpr{
					Subquery: subqueryStmt,
					Type:     SubqueryScalar,
				}
			} else {
				// If subquery parsing fails, log the error but continue
				// In a full implementation, we'd want better error handling
				fmt.Printf("Warning: Failed to parse subquery: %v\n", err)
			}
		}
	}
	
	// Check for function call (but not if it's actually a subquery)
	if strings.Contains(s, "(") && strings.Contains(s, ")") && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(s[strings.Index(s, "(")+1:strings.LastIndex(s, ")")])), "SELECT") {
		expr := parseFunctionExpression(s)
		if expr != nil {
			return expr
		}
	}
	
	// Check for binary operators (arithmetic: +, -, *, /)
	for _, op := range []string{" + ", " - ", " * ", " / "} {
		if strings.Contains(s, op) {
			// Find the operator (need to handle precedence properly)
			// For now, simple left-to-right parsing
			idx := strings.Index(s, op)
			if idx > 0 && idx < len(s)-len(op) {
				left := strings.TrimSpace(s[:idx])
				right := strings.TrimSpace(s[idx+len(op):])
				
				leftExpr := parseComplexExpressionValue(left)
				rightExpr := parseComplexExpressionValue(right)
				
				var binOp BinaryOp
				switch strings.TrimSpace(op) {
				case "+":
					binOp = OpPlus
				case "-":
					binOp = OpMinus
				case "*":
					binOp = OpMult
				case "/":
					binOp = OpDiv
				}
				
				return &BinaryExpr{
					Left:  leftExpr,
					Op:    binOp,
					Right: rightExpr,
				}
			}
		}
	}
	
	// Parse as regular expression value
	return parseExpressionValue(s)
}

// splitFunctionArguments splits function arguments properly handling nested brackets and parentheses
func splitFunctionArguments(args string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	bracketDepth := 0
	inString := false
	
	for i, ch := range args {
		switch ch {
		case '\'':
			if i == 0 || args[i-1] != '\\' {
				inString = !inString
			}
			current.WriteRune(ch)
		case '(':
			if !inString {
				parenDepth++
			}
			current.WriteRune(ch)
		case ')':
			if !inString {
				parenDepth--
			}
			current.WriteRune(ch)
		case '[':
			if !inString {
				bracketDepth++
			}
			current.WriteRune(ch)
		case ']':
			if !inString {
				bracketDepth--
			}
			current.WriteRune(ch)
		case ',':
			if !inString && parenDepth == 0 && bracketDepth == 0 {
				// This comma is at the top level, split here
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}
	
	// Add the last part
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	
	return parts
}


// parseSubqueryInWhere parses subqueries in WHERE clauses (EXISTS, IN)
func parseSubqueryInWhere(where string) Expression {
	where = strings.TrimSpace(where)
	upperWhere := strings.ToUpper(where)
	
	// Handle EXISTS subqueries
	if strings.HasPrefix(upperWhere, "EXISTS") {
		existsPart := strings.TrimSpace(where[6:]) // Skip "EXISTS"
		if strings.HasPrefix(existsPart, "(") && strings.HasSuffix(existsPart, ")") {
			inner := strings.TrimSpace(existsPart[1 : len(existsPart)-1])
			if strings.HasPrefix(strings.ToUpper(inner), "SELECT") {
				subqueryStmt, err := parseSelect(inner)
				if err == nil {
					return &SubqueryExpr{
						Subquery: subqueryStmt,
						Type:     SubqueryExists,
					}
				}
			}
		}
	}
	
	// Handle regular WHERE expressions
	paramIdx := 1
	return parseWhereExpressionWithParams(where, &paramIdx)
}

// parseExpressionValue parses a value that could be a string literal, column, or other expression
func parseExpressionValue(s string) Expression {
	s = strings.TrimSpace(s)
	
	// Check for string literal
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return &ConstantExpr{Value: s[1 : len(s)-1]}
	}
	
	// Try to parse as number
	if num, err := parseNumber(s); err == nil {
		return &ConstantExpr{Value: num}
	}
	
	// Otherwise, treat as column reference
	return parseColumnExpression(s)
}