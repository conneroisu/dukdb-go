package parser

import (
	"fmt"
	"testing"
)

func TestBasicLexing(t *testing.T) {
	input := "SELECT id FROM users"
	parser := New(input)
	
	for i := 0; i < 10; i++ {
		token := parser.lexer.Current()
		fmt.Printf("Token %d: Type=%d, Value='%s'\n", i, token.Type, token.Value)
		if token.Type == 0 { // EOF
			break
		}
		parser.lexer.Advance()
	}
}

func TestSimpleParseDebug(t *testing.T) {
	sql := "SELECT id FROM users"
	parser := New(sql)
	
	fmt.Printf("Attempting to parse: %s\n", sql)
	
	stmt, err := parser.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		fmt.Printf("Parser errors: %v\n", parser.GetErrors())
	} else {
		fmt.Printf("Successfully parsed: %v\n", stmt)
	}
}