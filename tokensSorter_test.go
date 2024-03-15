package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"testing"
)

var input = "./data/dummy.txt"
var output = "data.out"

func TestIsValidSortField(t *testing.T) {
	var tokenSorter TokenSorter

	if !tokenSorter.isValidSortField("name") {
		t.Errorf("Expected: %t, actual = %t", true, false)
	}

	if !tokenSorter.isValidSortField("address") {
		t.Errorf("Expected: %t, actual = %t", true, false)
	}

	if tokenSorter.isValidSortField("blah") {
		t.Errorf("Expected to be %t, actual = %t", false, true)
	}
}

func TestSortByName(t *testing.T) {
	tokenSorter := TokenSorter{
		inPath:     input,
		outPath:    output,
		bufferSize: 4096,
		byField:    fieldName,
	}
	tokenSorter.Sort()

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

func testTotalLines(t *testing.T, lines []string, expectedLength int) {
	if len(lines) != expectedLength {
		t.Errorf("Expected len(lines) = %d, actual = %d", 7, len(lines))
	}
}

func testTokenMatchLineNum(t *testing.T, lineNum string, expectedToken Token) {
	var token Token
	if err := json.Unmarshal([]byte(lineNum), &token); err != nil {
		log.Fatal(err)
	}

	if token != expectedToken {
		t.Errorf("Expected token = %v, actual = %v", expectedToken, token)
	}
}

func TestSortByAddress(t *testing.T) {
	tokenSorter := TokenSorter{
		inPath:     input,
		outPath:    output,
		bufferSize: 4096,
		byField:    fieldAddress,
	}
	tokenSorter.Sort()

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
