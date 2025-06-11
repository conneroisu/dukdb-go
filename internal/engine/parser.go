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
	Where      Expression
	GroupBy    []Expression
	Having     Expression
	OrderBy    []OrderByExpr
	Limit      *int64
	Offset     *int64
}

func (s *SelectStatement) Type() StatementType { return StmtSelect }

// InsertStatement represents an INSERT statement
type InsertStatement struct {
	Table   *TableRef
	Columns []string
	Values  [][]Expression
}

func (s *InsertStatement) Type() StatementType { return StmtInsert }

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
		colParts := strings.Split(selectPart, ",")
		var columns []Expression
		for _, col := range colParts {
			col = strings.TrimSpace(col)
			if col == "*" {
				columns = append(columns, &ColumnExpr{Column: "*"})
			} else {
				columns = append(columns, &ColumnExpr{Column: col})
			}
		}
		
		// Parse FROM part for WHERE, LIMIT, etc.
		var where Expression
		var limit *int64
		tableAndMore := strings.TrimSpace(fromPart)
		tableName := tableAndMore
		
		// Check for WHERE clause
		if whereIdx := strings.Index(strings.ToUpper(tableAndMore), " WHERE "); whereIdx != -1 {
			tableName = strings.TrimSpace(tableAndMore[:whereIdx])
			whereAndMore := strings.TrimSpace(tableAndMore[whereIdx+7:])
			
			// Extract WHERE condition
			wherePart := whereAndMore
			
			// Check if there's a LIMIT after WHERE
			if limitIdx := strings.Index(strings.ToUpper(whereAndMore), " LIMIT "); limitIdx != -1 {
				wherePart = strings.TrimSpace(whereAndMore[:limitIdx])
				limitPart := strings.TrimSpace(whereAndMore[limitIdx+7:])
				
				if limitVal, err := strconv.ParseInt(limitPart, 10, 64); err == nil {
					limit = &limitVal
				}
			}
			
			// Parse WHERE expression
			where = parseWhereExpression(wherePart)
		} else if strings.Contains(strings.ToUpper(tableAndMore), " LIMIT ") {
			// No WHERE, just LIMIT
			limitIdx := strings.Index(strings.ToUpper(tableAndMore), " LIMIT ")
			tableName = strings.TrimSpace(tableAndMore[:limitIdx])
			limitPart := strings.TrimSpace(tableAndMore[limitIdx+7:])
			
			if limitVal, err := strconv.ParseInt(limitPart, 10, 64); err == nil {
				limit = &limitVal
			}
		}
		
		return &SelectStatement{
			Columns: columns,
			From:    &TableRef{Table: tableName},
			Where:   where,
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
	where = strings.TrimSpace(where)
	
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
				leftExpr = &ColumnExpr{Column: left}
			} else {
				// Try to parse as constant
				if val, err := parseValue(left); err == nil {
					leftExpr = &ConstantExpr{Value: val}
				} else {
					leftExpr = &ColumnExpr{Column: left}
				}
			}
			
			// Parse right side (value)
			var rightExpr Expression
			if val, err := parseValue(right); err == nil {
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