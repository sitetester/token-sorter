package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
)

type TokenSorter struct {
	inPath     string
	outPath    string
	bufferSize int
	byField    string
}

type Token struct {
	Name    string
	Address string
}

func (ts *TokenSorter) Sort() {
	inFile, err := os.Open(ts.inPath)
	if err != nil {
		log.Fatal(err)
	}
	defer func(inFile *os.File) {
		if err := inFile.Close(); err != nil {
			log.Fatal(err)
		}
	}(inFile)

	tokens := ts.scan(inFile)
	ts.sort(tokens)
	ts.save(tokens)
}

func (ts *TokenSorter) isValidSortField(field string) bool {
	sorFields := []string{fieldName, fieldAddress}
	return slices.Contains(sorFields, field)
}

func (ts *TokenSorter) scan(inFile io.Reader) []Token {
	var tokens []Token
	var token Token

	scanner := bufio.NewScanner(inFile)
	buf := make([]byte, bufio.MaxScanTokenSize)
	scanner.Buffer(buf, ts.bufferSize)
	for scanner.Scan() {
		// parse line
		err := json.Unmarshal([]byte(scanner.Text()), &token)
		if err != nil {
			log.Fatal(err)
		}
		tokens = append(tokens, token)
	}
	return tokens
}

func (ts *TokenSorter) sort(tokens []Token) {
	if ts.byField == fieldName {
		sort.Slice(tokens[:], func(i, j int) bool {
			return tokens[i].Name < tokens[j].Name
		})
	} else {
		sort.Slice(tokens[:], func(i, j int) bool {
			return tokens[i].Address < tokens[j].Address
		})
	}
}

func (ts *TokenSorter) save(tokens []Token) {
	outFile, err := os.Create(ts.outPath)
	if err != nil {
		log.Fatal(err)
	}
	defer func(outFile *os.File) {
		if err := outFile.Close(); err != nil {
			log.Fatal(err)
		}
	}(outFile)

	var sb strings.Builder
	lineNum := 0
	for _, token := range tokens {
		lineNum += 1
		bytes, err := json.Marshal(token)
		if err != nil {
			log.Fatal(err)
		}
		jsonStr := string(bytes)
		if lineNum == 1 {
			sb.WriteString(fmt.Sprintf("%s", jsonStr))
		} else {
			sb.WriteString(fmt.Sprintf("\n%s", jsonStr))
		}
	}

	if _, err := outFile.WriteString(sb.String()); err != nil {
		log.Fatal(err)
	}
}
