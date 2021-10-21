package main

import (
	"os"
	"strings"
	"testing"
)

func TestSortByName(t *testing.T) {
	input := "./data/dummy.txt"
	output := "./data_out/dummy_output_name.txt"
	Sort(input, output, 4096, "name")

	testOutFileExists(t, output)

	tokens, _ := os.ReadFile(output)
	lines := strings.Split(string(tokens), "\n")

	testTotalLines(t, lines, 7)

	// first line
	testTokenMatchLineNum(t, lines[0], Token{
		Name:    "Amp",
		Address: "0xfF20817765cB7f73d4bde2e66e067E58D11095C2",
	})

	// last line
	testTokenMatchLineNum(t, lines[len(lines)-1], Token{
		Name:    "hoge.finance",
		Address: "0xfAd45E47083e4607302aa43c65fB3106F1cd7607",
	})
}

func testOutFileExists(t *testing.T, path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("Expected error = %v, actual = %v", nil, err.Error())
	}
}

func testTotalLines(t *testing.T, lines []string, expectedLength int) {
	if len(lines) != expectedLength {
		t.Errorf("Expected len(lines) = %d, actual = %d", 7, len(lines))
	}
}

func testTokenMatchLineNum(t *testing.T, lineNum string, expectedToken Token) {
	var token Token
	jsonHelper.ToStruct(lineNum, &token)

	if token != expectedToken {
		t.Errorf("Expected token = %v, actual = %v", expectedToken, token)
	}
}

func TestSortByAddress(t *testing.T) {
	input := "./data/dummy.txt"
	output := "./data_out/dummy_output_address.txt"
	Sort(input, output, 4096, "address")

	testOutFileExists(t, output)

	tokens, _ := os.ReadFile(output)
	lines := strings.Split(string(tokens), "\n")

	testTotalLines(t, lines, 7)

	// check some lines sort order
	// first line
	testTokenMatchLineNum(t, lines[0], Token{
		Name:    "Reef.finance",
		Address: "0xFE3E6a25e6b192A42a44ecDDCd13796471735ACf",
	})

	// last line
	testTokenMatchLineNum(t, lines[len(lines)-1], Token{
		Name:    "FalconSwap Token",
		Address: "0xfffffffFf15AbF397dA76f1dcc1A1604F45126DB",
	})
}
