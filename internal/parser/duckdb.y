%{
package parser

import (
    "github.com/connerohnesorge/dukdb-go/internal/ast"
)

%}

%union {
    str         string
    integer     int64
    float       float64
    boolean     bool
    node        ast.Node
    stmt        ast.Statement
    expr        ast.Expression
    selectItem  ast.SelectItem
    selectItems []ast.SelectItem
    tableRef    ast.TableRef
    tableRefs   []ast.TableRef
    exprs       []ast.Expression
    orderItem   ast.OrderItem
    orderItems  []ast.OrderItem
    strs        []string
    assignments []ast.UpdateAssignment
    columnDef   ast.ColumnDef
    columnDefs  []ast.ColumnDef
    dataType    ast.DataType
    joinType    ast.JoinType
    binaryOp    ast.BinaryOperator
    unaryOp     ast.UnaryOperator
}

%type <stmt>        statement select_stmt insert_stmt update_stmt delete_stmt create_table_stmt drop_table_stmt
%type <selectItems> select_list
%type <selectItem>  select_item
%type <tableRefs>   from_clause table_list
%type <tableRef>    table_ref join_expr
%type <expr>        expression primary_expr binary_expr unary_expr function_call subquery
%type <expr>        where_clause having_clause literal_expr column_ref
%type <exprs>       expression_list group_by_clause argument_list
%type <orderItems>  order_by_clause order_list
%type <orderItem>   order_item
%type <strs>        column_list identifier_list
%type <assignments> assignment_list
%type <columnDefs>  column_def_list
%type <columnDef>   column_def
%type <dataType>    data_type
%type <joinType>    join_type
%type <binaryOp>    binary_operator comparison_operator arithmetic_operator logical_operator
%type <unaryOp>     unary_operator
%type <str>         identifier string_literal
%type <integer>     integer_literal limit_clause offset_clause
%type <float>       float_literal
%type <boolean>     boolean_literal distinct_clause

%token <str>    IDENT STRING
%token <integer> INT
%token <float>  FLOAT

// Operators
%token EQ NEQ LT LE GT GE
%token PLUS MINUS MULTIPLY DIVIDE MODULO POWER
%token AND OR NOT
%token CONCAT LIKE ILIKE IN IS
%token ASSIGN

// Delimiters
%token COMMA SEMICOLON LPAREN RPAREN LBRACKET RBRACKET LBRACE RBRACE DOT

// Keywords - Basic SQL
%token SELECT FROM WHERE GROUP BY HAVING ORDER LIMIT OFFSET DISTINCT ALL AS

// Keywords - Joins
%token JOIN INNER LEFT RIGHT FULL OUTER CROSS ON USING

// Keywords - DML
%token INSERT INTO VALUES UPDATE SET DELETE

// Keywords - DDL
%token CREATE DROP ALTER TABLE INDEX VIEW

// Keywords - Data Types
%token INTEGER BIGINT SMALLINT TINYINT BOOLEAN
%token FLOAT DOUBLE DECIMAL VARCHAR CHAR TEXT
%token DATE TIME TIMESTAMP INTERVAL BLOB UUID

// Keywords - DuckDB specific types
%token LIST STRUCT MAP ARRAY

// Keywords - Other
%token NULL TRUE FALSE DEFAULT PRIMARY KEY FOREIGN REFERENCES
%token UNIQUE CHECK CONSTRAINT IF EXISTS CASCADE RESTRICT ASC DESC NOT

// Keywords - Functions
%token COUNT SUM AVG MIN MAX CASE WHEN THEN ELSE END

// Keywords - Window functions
%token OVER PARTITION ROWS RANGE BETWEEN UNBOUNDED PRECEDING FOLLOWING CURRENT ROW

// Precedence and associativity (lowest to highest)
%left OR
%left AND
%right NOT
%left EQ NEQ LT LE GT GE
%left IN LIKE ILIKE
%left PLUS MINUS CONCAT
%left MULTIPLY DIVIDE MODULO
%right UMINUS
%left DOT
%left LPAREN RPAREN

%%

// Main entry point
statement:
    select_stmt     { $$ = $1; yylex.(*yyLexerImpl).result = $1 }
  | insert_stmt     { $$ = $1; yylex.(*yyLexerImpl).result = $1 }
  | update_stmt     { $$ = $1; yylex.(*yyLexerImpl).result = $1 }  
  | delete_stmt     { $$ = $1; yylex.(*yyLexerImpl).result = $1 }
  | create_table_stmt { $$ = $1; yylex.(*yyLexerImpl).result = $1 }
  | drop_table_stmt { $$ = $1; yylex.(*yyLexerImpl).result = $1 }
  ;

// SELECT statement
select_stmt:
    SELECT distinct_clause select_list from_clause where_clause group_by_clause having_clause order_by_clause limit_clause offset_clause
    {
        selectStmt := &ast.SelectStmt{
            SelectList: $3,
            From:       $4,
            Where:      $5,
            GroupBy:    $6,
            Having:     $7,
            OrderBy:    $8,
            Distinct:   $2,
        }
        if $9 != 0 {
            limit := $9
            selectStmt.Limit = &limit
        }
        if $10 != 0 {
            offset := $10
            selectStmt.Offset = &offset
        }
        $$ = selectStmt
    }
  ;

distinct_clause:
    /* empty */     { $$ = false }
  | DISTINCT        { $$ = true }
  | ALL             { $$ = false }
  ;

select_list:
    select_item                 { $$ = []ast.SelectItem{$1} }
  | select_list COMMA select_item { $$ = append($1, $3) }
  ;

select_item:
    expression                  { $$ = ast.SelectItem{Expression: $1} }
  | expression AS identifier    { $$ = ast.SelectItem{Expression: $1, Alias: $3} }
  | expression identifier       { $$ = ast.SelectItem{Expression: $1, Alias: $2} }
  ;

from_clause:
    /* empty */         { $$ = nil }
  | FROM table_list     { $$ = $2 }
  ;

table_list:
    table_ref                   { $$ = []ast.TableRef{$1} }
  | table_list COMMA table_ref  { $$ = append($1, $3) }
  ;

table_ref:
    identifier                          { $$ = &ast.TableName{Name: $1} }
  | identifier DOT identifier           { $$ = &ast.TableName{Schema: $1, Name: $3} }
  | identifier AS identifier            { $$ = &ast.TableName{Name: $1, Alias: $3} }
  | identifier identifier               { $$ = &ast.TableName{Name: $1, Alias: $2} }
  | identifier DOT identifier AS identifier { $$ = &ast.TableName{Schema: $1, Name: $3, Alias: $5} }
  | identifier DOT identifier identifier    { $$ = &ast.TableName{Schema: $1, Name: $3, Alias: $4} }
  | join_expr                           { $$ = $1 }
  | LPAREN select_stmt RPAREN AS identifier { 
        $$ = &ast.TableName{Name: "(" + $2.String() + ")", Alias: $5} 
    }
  ;

join_expr:
    table_ref join_type table_ref ON expression
    {
        $$ = &ast.JoinExpr{
            Left:      $1,
            Right:     $3,
            JoinType:  $2,
            Condition: $5,
        }
    }
  | table_ref join_type table_ref
    {
        $$ = &ast.JoinExpr{
            Left:     $1,
            Right:    $3,
            JoinType: $2,
        }
    }
  ;

join_type:
    JOIN            { $$ = ast.InnerJoin }
  | INNER JOIN      { $$ = ast.InnerJoin }
  | LEFT JOIN       { $$ = ast.LeftJoin }
  | LEFT OUTER JOIN { $$ = ast.LeftJoin }
  | RIGHT JOIN      { $$ = ast.RightJoin }
  | RIGHT OUTER JOIN { $$ = ast.RightJoin }
  | FULL JOIN       { $$ = ast.FullJoin }
  | FULL OUTER JOIN { $$ = ast.FullJoin }
  | CROSS JOIN      { $$ = ast.CrossJoin }
  ;

where_clause:
    /* empty */         { $$ = nil }
  | WHERE expression    { $$ = $2 }
  ;

group_by_clause:
    /* empty */                     { $$ = nil }
  | GROUP BY expression_list        { $$ = $3 }
  ;

having_clause:
    /* empty */         { $$ = nil }
  | HAVING expression   { $$ = $2 }
  ;

order_by_clause:
    /* empty */             { $$ = nil }
  | ORDER BY order_list     { $$ = $3 }
  ;

order_list:
    order_item                  { $$ = []ast.OrderItem{$1} }
  | order_list COMMA order_item { $$ = append($1, $3) }
  ;

order_item:
    expression                  { $$ = ast.OrderItem{Expression: $1, Direction: ast.Ascending} }
  | expression ASC              { $$ = ast.OrderItem{Expression: $1, Direction: ast.Ascending} }
  | expression DESC             { $$ = ast.OrderItem{Expression: $1, Direction: ast.Descending} }
  ;

limit_clause:
    /* empty */     { $$ = 0 }
  | LIMIT INT       { $$ = $2 }
  ;

offset_clause:
    /* empty */     { $$ = 0 }
  | OFFSET INT      { $$ = $2 }
  ;

// Expression rules
expression_list:
    expression                      { $$ = []ast.Expression{$1} }
  | expression_list COMMA expression { $$ = append($1, $3) }
  ;

expression:
    primary_expr    { $$ = $1 }
  | binary_expr     { $$ = $1 }
  | unary_expr      { $$ = $1 }
  | function_call   { $$ = $1 }
  | subquery        { $$ = $1 }
  | CASE expression WHEN expression THEN expression ELSE expression END
    {
        $$ = &ast.CaseExpr{
            Expression: $2,
            WhenClauses: []ast.WhenClause{
                {Condition: $4, Result: $6},
            },
            ElseClause: $8,
        }
    }
  | CASE WHEN expression THEN expression ELSE expression END
    {
        $$ = &ast.CaseExpr{
            WhenClauses: []ast.WhenClause{
                {Condition: $3, Result: $5},
            },
            ElseClause: $7,
        }
    }
  ;

primary_expr:
    literal_expr                { $$ = $1 }
  | column_ref                  { $$ = $1 }
  | LPAREN expression RPAREN    { $$ = $2 }
  | LBRACKET expression_list RBRACKET 
    {
        // DuckDB LIST literal
        $$ = &ast.ListExpr{Elements: $2, ElementType: ast.Varchar}
    }
  ;

binary_expr:
    expression binary_operator expression    { $$ = &ast.BinaryExpr{Left: $1, Operator: $2, Right: $3} }
  ;

binary_operator:
    arithmetic_operator { $$ = $1 }
  | comparison_operator { $$ = $1 }
  | logical_operator    { $$ = $1 }
  | CONCAT              { $$ = ast.Concat }
  | LIKE                { $$ = ast.Like }
  | ILIKE               { $$ = ast.ILike }
  | IN                  { $$ = ast.In }
  | IS                  { $$ = ast.Is }
  ;

arithmetic_operator:
    PLUS        { $$ = ast.Add }
  | MINUS       { $$ = ast.Subtract }
  | MULTIPLY    { $$ = ast.Multiply }
  | DIVIDE      { $$ = ast.Divide }
  | MODULO      { $$ = ast.Modulo }
  | POWER       { $$ = ast.Power }
  ;

comparison_operator:
    EQ      { $$ = ast.Equal }
  | NEQ     { $$ = ast.NotEqual }
  | LT      { $$ = ast.LessThan }
  | LE      { $$ = ast.LessThanOrEqual }
  | GT      { $$ = ast.GreaterThan }
  | GE      { $$ = ast.GreaterThanOrEqual }
  ;

logical_operator:
    AND     { $$ = ast.And }
  | OR      { $$ = ast.Or }
  ;

unary_expr:
    unary_operator expression %prec UMINUS   { $$ = &ast.UnaryExpr{Operator: $1, Operand: $2} }
  ;

unary_operator:
    NOT     { $$ = ast.Not }
  | MINUS   { $$ = ast.Minus }
  | PLUS    { $$ = ast.Plus }
  ;

literal_expr:
    integer_literal     { $$ = &ast.LiteralExpr{Value: $1, Type: ast.BigInt} }
  | float_literal       { $$ = &ast.LiteralExpr{Value: $1, Type: ast.Double} }
  | string_literal      { $$ = &ast.LiteralExpr{Value: $1, Type: ast.Varchar} }
  | boolean_literal     { $$ = &ast.LiteralExpr{Value: $1, Type: ast.Boolean} }
  | NULL                { $$ = &ast.LiteralExpr{Value: nil, Type: ast.Varchar} }
  ;

column_ref:
    identifier                  { $$ = &ast.ColumnRef{Column: $1, Type: ast.Varchar} }
  | identifier DOT identifier   { $$ = &ast.ColumnRef{Table: $1, Column: $3, Type: ast.Varchar} }
  ;

function_call:
    identifier LPAREN argument_list RPAREN
    {
        $$ = &ast.FunctionCall{
            Name:       $1,
            Arguments:  $3,
            ReturnType: ast.Varchar,
        }
    }
  | identifier LPAREN RPAREN
    {
        $$ = &ast.FunctionCall{
            Name:       $1,
            Arguments:  nil,
            ReturnType: ast.Varchar,
        }
    }
  | COUNT LPAREN MULTIPLY RPAREN
    {
        $$ = &ast.FunctionCall{
            Name:       "COUNT",
            Arguments:  nil,
            ReturnType: ast.BigInt,
        }
    }
  | COUNT LPAREN DISTINCT expression RPAREN
    {
        $$ = &ast.FunctionCall{
            Name:       "COUNT",
            Arguments:  []ast.Expression{$4},
            Distinct:   true,
            ReturnType: ast.BigInt,
        }
    }
  ;

argument_list:
    expression                      { $$ = []ast.Expression{$1} }
  | argument_list COMMA expression  { $$ = append($1, $3) }
  ;

subquery:
    LPAREN select_stmt RPAREN   { $$ = &ast.SubqueryExpr{Query: $2.(*ast.SelectStmt)} }
  ;

// INSERT statement
insert_stmt:
    INSERT INTO identifier LPAREN column_list RPAREN VALUES LPAREN expression_list RPAREN
    {
        $$ = &ast.InsertStmt{
            Table:   ast.TableName{Name: $3},
            Columns: $5,
            Values:  [][]ast.Expression{$9},
        }
    }
  | INSERT INTO identifier VALUES LPAREN expression_list RPAREN
    {
        $$ = &ast.InsertStmt{
            Table:  ast.TableName{Name: $3},
            Values: [][]ast.Expression{$6},
        }
    }
  | INSERT INTO identifier select_stmt
    {
        $$ = &ast.InsertStmt{
            Table:  ast.TableName{Name: $3},
            Select: $4.(*ast.SelectStmt),
        }
    }
  ;

column_list:
    identifier                  { $$ = []string{$1} }
  | column_list COMMA identifier { $$ = append($1, $3) }
  ;

// UPDATE statement
update_stmt:
    UPDATE identifier SET assignment_list where_clause
    {
        $$ = &ast.UpdateStmt{
            Table: ast.TableName{Name: $2},
            Set:   $4,
            Where: $5,
        }
    }
  ;

assignment_list:
    identifier EQ expression
    {
        $$ = []ast.UpdateAssignment{{Column: $1, Value: $3}}
    }
  | assignment_list COMMA identifier EQ expression
    {
        $$ = append($1, ast.UpdateAssignment{Column: $3, Value: $5})
    }
  ;

// DELETE statement
delete_stmt:
    DELETE FROM identifier where_clause
    {
        $$ = &ast.DeleteStmt{
            Table: ast.TableName{Name: $3},
            Where: $4,
        }
    }
  ;

// CREATE TABLE statement
create_table_stmt:
    CREATE TABLE identifier LPAREN column_def_list RPAREN
    {
        $$ = &ast.CreateTableStmt{
            Name:    ast.TableName{Name: $3},
            Columns: $5,
        }
    }
  | CREATE TABLE IF NOT EXISTS identifier LPAREN column_def_list RPAREN
    {
        $$ = &ast.CreateTableStmt{
            Name:     ast.TableName{Name: $6},
            Columns:  $8,
            IfExists: true,
        }
    }
  ;

column_def_list:
    column_def                      { $$ = []ast.ColumnDef{$1} }
  | column_def_list COMMA column_def { $$ = append($1, $3) }
  ;

column_def:
    identifier data_type
    {
        $$ = ast.ColumnDef{Name: $1, Type: $2}
    }
  | identifier data_type NOT NULL
    {
        $$ = ast.ColumnDef{Name: $1, Type: $2, NotNull: true}
    }
  | identifier data_type PRIMARY KEY
    {
        $$ = ast.ColumnDef{Name: $1, Type: $2, PrimaryKey: true}
    }
  | identifier data_type DEFAULT expression
    {
        $$ = ast.ColumnDef{Name: $1, Type: $2, DefaultVal: $4}
    }
  ;

data_type:
    INTEGER     { $$ = ast.Integer }
  | BIGINT      { $$ = ast.BigInt }
  | SMALLINT    { $$ = ast.SmallInt }
  | TINYINT     { $$ = ast.TinyInt }
  | BOOLEAN     { $$ = ast.Boolean }
  | FLOAT       { $$ = ast.Float }
  | DOUBLE      { $$ = ast.Double }
  | DECIMAL     { $$ = ast.Decimal }
  | VARCHAR     { $$ = ast.Varchar }
  | TEXT        { $$ = ast.Varchar }
  | DATE        { $$ = ast.Date }
  | TIME        { $$ = ast.Time }
  | TIMESTAMP   { $$ = ast.Timestamp }
  | BLOB        { $$ = ast.Blob }
  | UUID        { $$ = ast.UUID }
  | LIST LT data_type GT
    {
        $$ = &ast.ListType{ElementType: $3}
    }
  | STRUCT LT identifier_list GT
    {
        var fields []ast.StructField
        for _, name := range $3 {
            fields = append(fields, ast.StructField{Name: name, Type: ast.Varchar})
        }
        $$ = &ast.StructType{Fields: fields}
    }
  | MAP LT data_type COMMA data_type GT
    {
        $$ = &ast.MapType{KeyType: $3, ValueType: $5}
    }
  ;

identifier_list:
    identifier                      { $$ = []string{$1} }
  | identifier_list COMMA identifier { $$ = append($1, $3) }
  ;

// DROP TABLE statement
drop_table_stmt:
    DROP TABLE identifier
    {
        $$ = &ast.DropTableStmt{Name: ast.TableName{Name: $3}}
    }
  | DROP TABLE IF EXISTS identifier
    {
        $$ = &ast.DropTableStmt{Name: ast.TableName{Name: $5}, IfExists: true}
    }
  ;

// Terminal symbols
identifier:
    IDENT       { $$ = $1 }
  ;

string_literal:
    STRING      { $$ = $1 }
  ;

integer_literal:
    INT         { $$ = $1 }
  ;

float_literal:
    FLOAT       { $$ = $1 }
  ;

boolean_literal:
    TRUE        { $$ = true }
  | FALSE       { $$ = false }
  ;

%%