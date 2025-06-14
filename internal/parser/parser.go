package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/connerohnesorge/dukdb-go/internal/ast"
	"github.com/connerohnesorge/dukdb-go/internal/lexer"
)

// Parser represents a SQL parser
type Parser struct {
	lexer  *lexer.TokenStream
	errors []string
}

// New creates a new parser instance
func New(input string) *Parser {
	return &Parser{
		lexer:  lexer.NewTokenStream(input),
		errors: make([]string, 0),
	}
}

// Parse parses the input and returns an AST node
func (p *Parser) Parse() (ast.Statement, error) {
	// Create a custom lexer interface for goyacc
	yylex := &yyLexerImpl{
		tokenStream: p.lexer,
		parser:      p,
	}

	// Parse using goyacc-generated parser
	result := yyParse(yylex)

	if result != 0 || len(p.errors) > 0 {
		return nil, fmt.Errorf("parse error: %s", strings.Join(p.errors, "; "))
	}

	return yylex.result, nil
}

// AddError adds a parse error
func (p *Parser) AddError(msg string) {
	p.errors = append(p.errors, msg)
}

// GetErrors returns all parse errors
func (p *Parser) GetErrors() []string {
	return p.errors
}

// yyLexerImpl implements the interface required by goyacc
type yyLexerImpl struct {
	tokenStream *lexer.TokenStream
	parser      *Parser
	result      ast.Statement
}

// Lex is called by the goyacc-generated parser to get the next token
func (yl *yyLexerImpl) Lex(lval *yySymType) int {
	token := yl.tokenStream.Current()
	yl.tokenStream.Advance()

	// Convert our token to goyacc token
	switch token.Type {
	case lexer.EOF:
		return 0
	case lexer.IDENT:
		lval.str = token.Value
		return IDENT
	case lexer.INT:
		val, _ := strconv.ParseInt(token.Value, 10, 64)
		lval.integer = val
		return INT
	case lexer.FLOAT:
		val, _ := strconv.ParseFloat(token.Value, 64)
		lval.float = val
		return FLOAT
	case lexer.STRING:
		lval.str = token.Value
		return STRING

	// Operators
	case lexer.EQ:
		return EQ
	case lexer.NEQ:
		return NEQ
	case lexer.LT:
		return LT
	case lexer.LE:
		return LE
	case lexer.GT:
		return GT
	case lexer.GE:
		return GE
	case lexer.PLUS:
		return PLUS
	case lexer.MINUS:
		return MINUS
	case lexer.MULTIPLY:
		return MULTIPLY
	case lexer.DIVIDE:
		return DIVIDE
	case lexer.MODULO:
		return MODULO
	case lexer.POWER:
		return POWER
	case lexer.AND:
		return AND
	case lexer.OR:
		return OR
	case lexer.NOT:
		return NOT
	case lexer.CONCAT:
		return CONCAT
	case lexer.LIKE:
		return LIKE
	case lexer.ILIKE:
		return ILIKE
	case lexer.IN:
		return IN
	case lexer.IS:
		return IS
	case lexer.ASSIGN:
		return ASSIGN

	// Delimiters
	case lexer.COMMA:
		return COMMA
	case lexer.SEMICOLON:
		return SEMICOLON
	case lexer.LPAREN:
		return LPAREN
	case lexer.RPAREN:
		return RPAREN
	case lexer.LBRACKET:
		return LBRACKET
	case lexer.RBRACKET:
		return RBRACKET
	case lexer.LBRACE:
		return LBRACE
	case lexer.RBRACE:
		return RBRACE
	case lexer.DOT:
		return DOT

	// Keywords - Basic SQL
	case lexer.SELECT:
		return SELECT
	case lexer.FROM:
		return FROM
	case lexer.WHERE:
		return WHERE
	case lexer.GROUP:
		return GROUP
	case lexer.BY:
		return BY
	case lexer.HAVING:
		return HAVING
	case lexer.ORDER:
		return ORDER
	case lexer.LIMIT:
		return LIMIT
	case lexer.OFFSET:
		return OFFSET
	case lexer.DISTINCT:
		return DISTINCT
	case lexer.ALL:
		return ALL
	case lexer.AS:
		return AS

	// Keywords - Joins
	case lexer.JOIN:
		return JOIN
	case lexer.INNER:
		return INNER
	case lexer.LEFT:
		return LEFT
	case lexer.RIGHT:
		return RIGHT
	case lexer.FULL:
		return FULL
	case lexer.OUTER:
		return OUTER
	case lexer.CROSS:
		return CROSS
	case lexer.ON:
		return ON
	case lexer.USING:
		return USING

	// Keywords - DML
	case lexer.INSERT:
		return INSERT
	case lexer.INTO:
		return INTO
	case lexer.VALUES:
		return VALUES
	case lexer.UPDATE:
		return UPDATE
	case lexer.SET:
		return SET
	case lexer.DELETE:
		return DELETE

	// Keywords - DDL
	case lexer.CREATE:
		return CREATE
	case lexer.DROP:
		return DROP
	case lexer.ALTER:
		return ALTER
	case lexer.TABLE:
		return TABLE
	case lexer.INDEX:
		return INDEX
	case lexer.VIEW:
		return VIEW

	// Keywords - Data Types
	case lexer.INTEGER:
		return INTEGER
	case lexer.BIGINT:
		return BIGINT
	case lexer.SMALLINT:
		return SMALLINT
	case lexer.TINYINT:
		return TINYINT
	case lexer.BOOLEAN:
		return BOOLEAN
	case lexer.FLOAT_TYPE:
		return FLOAT
	case lexer.DOUBLE:
		return DOUBLE
	case lexer.DECIMAL:
		return DECIMAL
	case lexer.VARCHAR:
		return VARCHAR
	case lexer.CHAR:
		return CHAR
	case lexer.TEXT:
		return TEXT
	case lexer.DATE:
		return DATE
	case lexer.TIME:
		return TIME
	case lexer.TIMESTAMP:
		return TIMESTAMP
	case lexer.INTERVAL:
		return INTERVAL
	case lexer.BLOB:
		return BLOB
	case lexer.UUID:
		return UUID

	// Keywords - DuckDB specific types
	case lexer.LIST:
		return LIST
	case lexer.STRUCT:
		return STRUCT
	case lexer.MAP:
		return MAP
	case lexer.ARRAY:
		return ARRAY

	// Keywords - Other
	case lexer.NULL:
		return NULL
	case lexer.TRUE:
		return TRUE
	case lexer.FALSE:
		return FALSE
	case lexer.DEFAULT:
		return DEFAULT
	case lexer.PRIMARY:
		return PRIMARY
	case lexer.KEY:
		return KEY
	case lexer.FOREIGN:
		return FOREIGN
	case lexer.REFERENCES:
		return REFERENCES
	case lexer.UNIQUE:
		return UNIQUE
	case lexer.CHECK:
		return CHECK
	case lexer.CONSTRAINT:
		return CONSTRAINT
	case lexer.IF:
		return IF
	case lexer.EXISTS:
		return EXISTS
	case lexer.CASCADE:
		return CASCADE
	case lexer.RESTRICT:
		return RESTRICT
	case lexer.ASC:
		return ASC
	case lexer.DESC:
		return DESC

	// Keywords - Functions
	case lexer.COUNT:
		return COUNT
	case lexer.SUM:
		return SUM
	case lexer.AVG:
		return AVG
	case lexer.MIN:
		return MIN
	case lexer.MAX:
		return MAX
	case lexer.CASE:
		return CASE
	case lexer.WHEN:
		return WHEN
	case lexer.THEN:
		return THEN
	case lexer.ELSE:
		return ELSE
	case lexer.END:
		return END

	// Keywords - Window functions
	case lexer.OVER:
		return OVER
	case lexer.PARTITION:
		return PARTITION
	case lexer.ROWS:
		return ROWS
	case lexer.RANGE:
		return RANGE
	case lexer.BETWEEN:
		return BETWEEN
	case lexer.UNBOUNDED:
		return UNBOUNDED
	case lexer.PRECEDING:
		return PRECEDING
	case lexer.FOLLOWING:
		return FOLLOWING
	case lexer.CURRENT:
		return CURRENT
	case lexer.ROW:
		return ROW

	default:
		return int(token.Type)
	}
}

// Error is called by the goyacc-generated parser when a parse error occurs
func (yl *yyLexerImpl) Error(s string) {
	token := yl.tokenStream.Current()
	errorMsg := fmt.Sprintf("%s at line %d, column %d (token: %s)",
		s, token.Line, token.Column, token.Value)
	yl.parser.AddError(errorMsg)
}

// Utility functions for building AST nodes during parsing

// Helper function to create binary expressions with proper precedence
func CreateBinaryExpr(left ast.Expression, op ast.BinaryOperator, right ast.Expression) ast.Expression {
	return &ast.BinaryExpr{
		Left:     left,
		Operator: op,
		Right:    right,
	}
}

// Helper function to create unary expressions
func CreateUnaryExpr(op ast.UnaryOperator, operand ast.Expression) ast.Expression {
	return &ast.UnaryExpr{
		Operator: op,
		Operand:  operand,
	}
}

// Helper function to create function calls
func CreateFunctionCall(name string, args []ast.Expression, distinct bool) ast.Expression {
	var returnType ast.DataType = ast.Varchar

	// Set appropriate return types for known functions
	switch strings.ToUpper(name) {
	case "COUNT", "SUM":
		returnType = ast.BigInt
	case "AVG":
		returnType = ast.Double
	case "MIN", "MAX":
		if len(args) > 0 {
			returnType = args[0].DataType()
		}
	}

	return &ast.FunctionCall{
		Name:       name,
		Arguments:  args,
		Distinct:   distinct,
		ReturnType: returnType,
	}
}

// Helper function to create column references with type inference
func CreateColumnRef(table, column string) ast.Expression {
	return &ast.ColumnRef{
		Table:  table,
		Column: column,
		Type:   ast.Varchar, // Default type, should be resolved during semantic analysis
	}
}

// Helper function to create literal expressions
func CreateLiteral(value any, dataType ast.DataType) ast.Expression {
	return &ast.LiteralExpr{
		Value: value,
		Type:  dataType,
	}
}

// ParseSQL is a convenience function to parse a SQL string
func ParseSQL(sql string) (ast.Statement, error) {
	parser := New(sql)
	return parser.Parse()
}
