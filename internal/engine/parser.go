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
		fromIdx := strings.Index(strings.ToUpper(sql), " FROM ")
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
	
	return nil, fmt.Errorf("complex SELECT parsing not implemented")
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

// parseValueTuple parses a single value tuple like (1, 'Hello')
func parseValueTuple(tuple string) ([]Expression, error) {
	expressions, _, err := parseValueTupleWithIndex(tuple, 1)
	return expressions, err
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
		} else if strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'") {
			// String literal
			value := part[1 : len(part)-1]
			expressions = append(expressions, &ConstantExpr{Value: value})
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

// splitByComma splits a string by commas, respecting quoted strings
func splitByComma(s string) []string {
	var parts []string
	var current strings.Builder
	inString := false
	
	for _, ch := range s {
		switch ch {
		case '\'':
			inString = !inString
			current.WriteRune(ch)
		case ',':
			if !inString {
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
	
	// Split by comma (note: this is simplified and doesn't handle all cases)
	parts := strings.Split(colDefs, ",")
	
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		
		// Split column definition into name and type
		fields := strings.Fields(part)
		if len(fields) < 2 {
			continue
		}
		
		colName := fields[0]
		colType := fields[1]
		
		// Convert SQL type to storage type
		var logicalType storage.LogicalType
		switch strings.ToUpper(colType) {
		case "INTEGER", "INT":
			logicalType = storage.LogicalType{ID: storage.TypeInteger}
		case "BIGINT":
			logicalType = storage.LogicalType{ID: storage.TypeBigInt}
		case "VARCHAR", "TEXT", "STRING":
			logicalType = storage.LogicalType{ID: storage.TypeVarchar}
		case "DOUBLE", "REAL":
			logicalType = storage.LogicalType{ID: storage.TypeDouble}
		case "BOOLEAN", "BOOL":
			logicalType = storage.LogicalType{ID: storage.TypeBoolean}
		case "DECIMAL":
			logicalType = storage.LogicalType{ID: storage.TypeDecimal, Width: 18, Scale: 3}
		case "DATE":
			logicalType = storage.LogicalType{ID: storage.TypeDate}
		case "TIMESTAMP":
			logicalType = storage.LogicalType{ID: storage.TypeTimestamp}
		default:
			// Default to varchar
			logicalType = storage.LogicalType{ID: storage.TypeVarchar}
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
	
	// Check for BETWEEN
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
	
	// If no operator found, return nil
	return nil
}

// parseValueExpression parses a value expression (constant, parameter, or column)
func parseValueExpression(s string) Expression {
	paramIdx := 1
	return parseValueExpressionWithParams(s, &paramIdx)
}

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
			// Parse comma-separated arguments
			argParts := strings.Split(argsStr, ",")
			for _, arg := range argParts {
				arg = strings.TrimSpace(arg)
				if arg == "*" {
					args = append(args, &ColumnExpr{Column: "*"})
				} else {
					// Try to parse as constant or column
					if val, err := parseValue(arg); err == nil {
						args = append(args, &ConstantExpr{Value: val})
					} else {
						args = append(args, parseColumnExpression(arg))
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
	colParts := strings.Split(selectPart, ",")
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
				if strings.Contains(exprPart, "(") && strings.Contains(exprPart, ")") {
					expr = parseFunctionExpression(exprPart)
					if expr == nil {
						expr = parseColumnExpression(exprPart)
					} else {
						// Set the alias for function expressions
						if funcExpr, ok := expr.(*FunctionExpr); ok {
							funcExpr.Alias = alias
						}
					}
				} else {
					expr = parseColumnExpression(exprPart)
				}
			} else {
				// No alias, parse the entire column
				if strings.Contains(col, "(") && strings.Contains(col, ")") {
					expr = parseFunctionExpression(col)
					if expr == nil {
						expr = parseColumnExpression(col)
					}
				} else {
					expr = parseColumnExpression(col)
				}
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
func parseSelectRemainder(remainder string) (Expression, *int64, error) {
	remainder = strings.TrimSpace(remainder)
	if remainder == "" {
		return nil, nil, nil
	}
	
	var where Expression
	var limit *int64
	
	// Check for WHERE clause
	if strings.HasPrefix(strings.ToUpper(remainder), "WHERE ") {
		wherePart := remainder[6:] // Skip "WHERE "
		
		// Check if there's a LIMIT after WHERE
		if limitIdx := strings.Index(strings.ToUpper(wherePart), " LIMIT "); limitIdx != -1 {
			whereCondition := strings.TrimSpace(wherePart[:limitIdx])
			limitPart := strings.TrimSpace(wherePart[limitIdx+7:])
			
			if limitVal, err := strconv.ParseInt(limitPart, 10, 64); err == nil {
				limit = &limitVal
			}
			
			where = parseWhereExpression(whereCondition)
		} else {
			where = parseWhereExpression(wherePart)
		}
	} else if strings.HasPrefix(strings.ToUpper(remainder), "LIMIT ") {
		// No WHERE, just LIMIT
		limitPart := strings.TrimSpace(remainder[6:])
		if limitVal, err := strconv.ParseInt(limitPart, 10, 64); err == nil {
			limit = &limitVal
		}
	}
	
	return where, limit, nil
}

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