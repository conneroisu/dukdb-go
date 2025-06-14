package lexer

import (
	"fmt"
	"strings"
)

// Token represents a SQL token
type Token struct {
	Type     TokenType
	Value    string
	Position int
	Line     int
	Column   int
}

// TokenType represents the type of a token
type TokenType int

const (
	// Special tokens
	ILLEGAL TokenType = iota
	EOF
	
	// Literals
	IDENT    // identifiers
	INT      // integer literals
	FLOAT    // floating point literals
	STRING   // string literals
	
	// Operators
	ASSIGN   // =
	PLUS     // +
	MINUS    // -
	MULTIPLY // *
	DIVIDE   // /
	MODULO   // %
	POWER    // ^
	
	// Comparison operators
	EQ    // =
	NEQ   // <> or !=
	LT    // <
	LE    // <=
	GT    // >
	GE    // >=
	
	// Logical operators
	AND // AND
	OR  // OR
	NOT // NOT
	
	// Other operators
	CONCAT // ||
	LIKE   // LIKE
	ILIKE  // ILIKE
	IN     // IN
	IS     // IS
	
	// Delimiters
	COMMA     // ,
	SEMICOLON // ;
	LPAREN    // (
	RPAREN    // )
	LBRACKET  // [
	RBRACKET  // ]
	LBRACE    // {
	RBRACE    // }
	DOT       // .
	
	// Keywords - Basic SQL
	SELECT
	FROM
	WHERE
	GROUP
	BY
	HAVING
	ORDER
	LIMIT
	OFFSET
	DISTINCT
	ALL
	AS
	
	// Keywords - Joins
	JOIN
	INNER
	LEFT
	RIGHT
	FULL
	OUTER
	CROSS
	ON
	USING
	
	// Keywords - DML
	INSERT
	INTO
	VALUES
	UPDATE
	SET
	DELETE
	
	// Keywords - DDL
	CREATE
	DROP
	ALTER
	TABLE
	INDEX
	VIEW
	
	// Keywords - Data Types
	INTEGER
	BIGINT
	SMALLINT
	TINYINT
	BOOLEAN
	FLOAT_TYPE
	DOUBLE
	DECIMAL
	VARCHAR
	CHAR
	TEXT
	DATE
	TIME
	TIMESTAMP
	INTERVAL
	BLOB
	UUID
	
	// Keywords - DuckDB specific types
	LIST
	STRUCT
	MAP
	ARRAY
	
	// Keywords - Other
	NULL
	TRUE
	FALSE
	DEFAULT
	PRIMARY
	KEY
	FOREIGN
	REFERENCES
	UNIQUE
	CHECK
	CONSTRAINT
	IF
	EXISTS
	CASCADE
	RESTRICT
	ASC
	DESC
	CURRENTLY
	
	// Keywords - Functions and aggregates
	COUNT
	SUM
	AVG
	MIN
	MAX
	CASE
	WHEN
	THEN
	ELSE
	END
	
	// Keywords - Window functions
	OVER
	PARTITION
	ROWS
	RANGE
	BETWEEN
	UNBOUNDED
	PRECEDING
	FOLLOWING
	CURRENT
	ROW
)

// TokenTypeNames maps token types to their string representations
var TokenTypeNames = map[TokenType]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	
	IDENT:  "IDENT",
	INT:    "INT",
	FLOAT:  "FLOAT",
	STRING: "STRING",
	
	ASSIGN:   "=",
	PLUS:     "+",
	MINUS:    "-",
	MULTIPLY: "*",
	DIVIDE:   "/",
	MODULO:   "%",
	POWER:    "^",
	
	EQ:  "=",
	NEQ: "<>",
	LT:  "<",
	LE:  "<=",
	GT:  ">",
	GE:  ">=",
	
	AND: "AND",
	OR:  "OR",
	NOT: "NOT",
	
	CONCAT: "||",
	LIKE:   "LIKE",
	ILIKE:  "ILIKE",
	IN:     "IN",
	IS:     "IS",
	
	COMMA:     ",",
	SEMICOLON: ";",
	LPAREN:    "(",
	RPAREN:    ")",
	LBRACKET:  "[",
	RBRACKET:  "]",
	LBRACE:    "{",
	RBRACE:    "}",
	DOT:       ".",
	
	SELECT:   "SELECT",
	FROM:     "FROM",
	WHERE:    "WHERE",
	GROUP:    "GROUP",
	BY:       "BY",
	HAVING:   "HAVING",
	ORDER:    "ORDER",
	LIMIT:    "LIMIT",
	OFFSET:   "OFFSET",
	DISTINCT: "DISTINCT",
	ALL:      "ALL",
	AS:       "AS",
	
	JOIN:  "JOIN",
	INNER: "INNER",
	LEFT:  "LEFT",
	RIGHT: "RIGHT",
	FULL:  "FULL",
	OUTER: "OUTER",
	CROSS: "CROSS",
	ON:    "ON",
	USING: "USING",
	
	INSERT: "INSERT",
	INTO:   "INTO",
	VALUES: "VALUES",
	UPDATE: "UPDATE",
	SET:    "SET",
	DELETE: "DELETE",
	
	CREATE: "CREATE",
	DROP:   "DROP",
	ALTER:  "ALTER",
	TABLE:  "TABLE",
	INDEX:  "INDEX",
	VIEW:   "VIEW",
	
	INTEGER:   "INTEGER",
	BIGINT:    "BIGINT",
	SMALLINT:  "SMALLINT",
	TINYINT:   "TINYINT",
	BOOLEAN:   "BOOLEAN",
	FLOAT_TYPE: "FLOAT",
	DOUBLE:    "DOUBLE",
	DECIMAL:   "DECIMAL",
	VARCHAR:   "VARCHAR",
	CHAR:      "CHAR",
	TEXT:      "TEXT",
	DATE:      "DATE",
	TIME:      "TIME",
	TIMESTAMP: "TIMESTAMP",
	INTERVAL:  "INTERVAL",
	BLOB:      "BLOB",
	UUID:      "UUID",
	
	LIST:   "LIST",
	STRUCT: "STRUCT",
	MAP:    "MAP",
	ARRAY:  "ARRAY",
	
	NULL:       "NULL",
	TRUE:       "TRUE",
	FALSE:      "FALSE",
	DEFAULT:    "DEFAULT",
	PRIMARY:    "PRIMARY",
	KEY:        "KEY",
	FOREIGN:    "FOREIGN",
	REFERENCES: "REFERENCES",
	UNIQUE:     "UNIQUE",
	CHECK:      "CHECK",
	CONSTRAINT: "CONSTRAINT",
	IF:         "IF",
	EXISTS:     "EXISTS",
	CASCADE:    "CASCADE",
	RESTRICT:   "RESTRICT",
	ASC:        "ASC",
	DESC:       "DESC",
	
	COUNT: "COUNT",
	SUM:   "SUM",
	AVG:   "AVG",
	MIN:   "MIN",
	MAX:   "MAX",
	CASE:  "CASE",
	WHEN:  "WHEN",
	THEN:  "THEN",
	ELSE:  "ELSE",
	END:   "END",
	
	OVER:       "OVER",
	PARTITION:  "PARTITION",
	ROWS:       "ROWS",
	RANGE:      "RANGE",
	BETWEEN:    "BETWEEN",
	UNBOUNDED:  "UNBOUNDED",
	PRECEDING:  "PRECEDING",
	FOLLOWING:  "FOLLOWING",
	CURRENT:    "CURRENT",
	ROW:        "ROW",
}

func (t TokenType) String() string {
	if name, ok := TokenTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("TokenType(%d)", int(t))
}

// Keywords maps keyword strings to their token types
var Keywords = map[string]TokenType{
	"SELECT":   SELECT,
	"FROM":     FROM,
	"WHERE":    WHERE,
	"GROUP":    GROUP,
	"BY":       BY,
	"HAVING":   HAVING,
	"ORDER":    ORDER,
	"LIMIT":    LIMIT,
	"OFFSET":   OFFSET,
	"DISTINCT": DISTINCT,
	"ALL":      ALL,
	"AS":       AS,
	
	"JOIN":  JOIN,
	"INNER": INNER,
	"LEFT":  LEFT,
	"RIGHT": RIGHT,
	"FULL":  FULL,
	"OUTER": OUTER,
	"CROSS": CROSS,
	"ON":    ON,
	"USING": USING,
	
	"INSERT": INSERT,
	"INTO":   INTO,
	"VALUES": VALUES,
	"UPDATE": UPDATE,
	"SET":    SET,
	"DELETE": DELETE,
	
	"CREATE": CREATE,
	"DROP":   DROP,
	"ALTER":  ALTER,
	"TABLE":  TABLE,
	"INDEX":  INDEX,
	"VIEW":   VIEW,
	
	"INTEGER":   INTEGER,
	"BIGINT":    BIGINT,
	"SMALLINT":  SMALLINT,
	"TINYINT":   TINYINT,
	"BOOLEAN":   BOOLEAN,
	"FLOAT":     FLOAT_TYPE,
	"DOUBLE":    DOUBLE,
	"DECIMAL":   DECIMAL,
	"VARCHAR":   VARCHAR,
	"CHAR":      CHAR,
	"TEXT":      TEXT,
	"DATE":      DATE,
	"TIME":      TIME,
	"TIMESTAMP": TIMESTAMP,
	"INTERVAL":  INTERVAL,
	"BLOB":      BLOB,
	"UUID":      UUID,
	
	"LIST":   LIST,
	"STRUCT": STRUCT,
	"MAP":    MAP,
	"ARRAY":  ARRAY,
	
	"NULL":       NULL,
	"TRUE":       TRUE,
	"FALSE":      FALSE,
	"DEFAULT":    DEFAULT,
	"PRIMARY":    PRIMARY,
	"KEY":        KEY,
	"FOREIGN":    FOREIGN,
	"REFERENCES": REFERENCES,
	"UNIQUE":     UNIQUE,
	"CHECK":      CHECK,
	"CONSTRAINT": CONSTRAINT,
	"IF":         IF,
	"EXISTS":     EXISTS,
	"CASCADE":    CASCADE,
	"RESTRICT":   RESTRICT,
	"ASC":        ASC,
	"DESC":       DESC,
	
	"AND": AND,
	"OR":  OR,
	"NOT": NOT,
	"IN":  IN,
	"IS":  IS,
	"LIKE": LIKE,
	"ILIKE": ILIKE,
	
	"COUNT": COUNT,
	"SUM":   SUM,
	"AVG":   AVG,
	"MIN":   MIN,
	"MAX":   MAX,
	"CASE":  CASE,
	"WHEN":  WHEN,
	"THEN":  THEN,
	"ELSE":  ELSE,
	"END":   END,
	
	"OVER":       OVER,
	"PARTITION":  PARTITION,
	"ROWS":       ROWS,
	"RANGE":      RANGE,
	"BETWEEN":    BETWEEN,
	"UNBOUNDED":  UNBOUNDED,
	"PRECEDING":  PRECEDING,
	"FOLLOWING":  FOLLOWING,
	"CURRENT":    CURRENT,
	"ROW":        ROW,
}

// Lexer represents a SQL lexer
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
	line         int  // current line number
	column       int  // current column number
}

// New creates a new lexer instance
func New(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar()
	return l
}

// readChar reads the next character and advances position
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // ASCII null character represents EOF
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
	
	if l.ch == '\n' {
		l.line++
		l.column = 0
	} else {
		l.column++
	}
}

// peekChar returns the next character without advancing position
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// skipComment skips single-line comments (-- comment)
func (l *Lexer) skipComment() {
	if l.ch == '-' && l.peekChar() == '-' {
		for l.ch != '\n' && l.ch != 0 {
			l.readChar()
		}
	}
}

// readIdentifier reads an identifier or keyword
func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber reads a numeric literal (integer or float)
func (l *Lexer) readNumber() (string, TokenType) {
	position := l.position
	tokenType := INT
	
	for isDigit(l.ch) {
		l.readChar()
	}
	
	// Check for decimal point
	if l.ch == '.' && isDigit(l.peekChar()) {
		tokenType = FLOAT
		l.readChar() // consume '.'
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	
	// Check for scientific notation
	if l.ch == 'e' || l.ch == 'E' {
		tokenType = FLOAT
		l.readChar()
		if l.ch == '+' || l.ch == '-' {
			l.readChar()
		}
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	
	return l.input[position:l.position], tokenType
}

// readString reads a string literal
func (l *Lexer) readString() string {
	position := l.position + 1 // skip opening quote
	for {
		l.readChar()
		if l.ch == '\'' {
			if l.peekChar() == '\'' {
				// Escaped quote
				l.readChar()
			} else {
				break
			}
		}
		if l.ch == 0 {
			break // Unterminated string
		}
	}
	return l.input[position:l.position]
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	var tok Token
	
	l.skipWhitespace()
	
	// Skip comments
	if l.ch == '-' && l.peekChar() == '-' {
		l.skipComment()
		l.skipWhitespace()
	}
	
	tok.Position = l.position
	tok.Line = l.line
	tok.Column = l.column
	
	switch l.ch {
	case '=':
		tok = Token{Type: EQ, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '+':
		tok = Token{Type: PLUS, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '-':
		tok = Token{Type: MINUS, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '*':
		tok = Token{Type: MULTIPLY, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '/':
		tok = Token{Type: DIVIDE, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '%':
		tok = Token{Type: MODULO, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '^':
		tok = Token{Type: POWER, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: LE, Value: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NEQ, Value: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else {
			tok = Token{Type: LT, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: GE, Value: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else {
			tok = Token{Type: GT, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		}
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NEQ, Value: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else {
			tok = Token{Type: ILLEGAL, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		}
	case '|':
		if l.peekChar() == '|' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: CONCAT, Value: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else {
			tok = Token{Type: ILLEGAL, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		}
	case ',':
		tok = Token{Type: COMMA, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case ';':
		tok = Token{Type: SEMICOLON, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '(':
		tok = Token{Type: LPAREN, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case ')':
		tok = Token{Type: RPAREN, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '[':
		tok = Token{Type: LBRACKET, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case ']':
		tok = Token{Type: RBRACKET, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '{':
		tok = Token{Type: LBRACE, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '}':
		tok = Token{Type: RBRACE, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '.':
		tok = Token{Type: DOT, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
	case '\'':
		tok.Type = STRING
		tok.Value = l.readString()
	case 0:
		tok.Type = EOF
		tok.Value = ""
	default:
		if isLetter(l.ch) {
			tok.Value = l.readIdentifier()
			tok.Type = lookupIdent(tok.Value)
			return tok // early return to avoid readChar()
		} else if isDigit(l.ch) {
			tok.Value, tok.Type = l.readNumber()
			return tok // early return to avoid readChar()
		} else {
			tok = Token{Type: ILLEGAL, Value: string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		}
	}
	
	l.readChar()
	return tok
}

// lookupIdent checks if an identifier is a reserved keyword
func lookupIdent(ident string) TokenType {
	if tok, ok := Keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENT
}

// isLetter checks if a character is a letter
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

// isDigit checks if a character is a digit
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// TokenStream provides a convenient way to work with tokens
type TokenStream struct {
	lexer   *Lexer
	current Token
	next    Token
}

// NewTokenStream creates a new token stream
func NewTokenStream(input string) *TokenStream {
	ts := &TokenStream{
		lexer: New(input),
	}
	ts.advance() // Load first token
	ts.advance() // Load second token
	return ts
}

// Current returns the current token
func (ts *TokenStream) Current() Token {
	return ts.current
}

// Peek returns the next token without consuming it
func (ts *TokenStream) Peek() Token {
	return ts.next
}

// Advance moves to the next token
func (ts *TokenStream) Advance() {
	ts.advance()
}

func (ts *TokenStream) advance() {
	ts.current = ts.next
	ts.next = ts.lexer.NextToken()
}

// Match checks if the current token matches the expected type and advances if it does
func (ts *TokenStream) Match(expected TokenType) bool {
	if ts.current.Type == expected {
		ts.advance()
		return true
	}
	return false
}

// Expect checks if the current token matches the expected type and returns an error if not
func (ts *TokenStream) Expect(expected TokenType) error {
	if ts.current.Type != expected {
		return fmt.Errorf("expected %s, got %s at line %d, column %d", 
			expected.String(), ts.current.Type.String(), ts.current.Line, ts.current.Column)
	}
	ts.advance()
	return nil
}