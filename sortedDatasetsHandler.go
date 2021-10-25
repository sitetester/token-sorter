package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

type SortedDatasetsHandler struct {
	field string
}

func (sdh *SortedDatasetsHandler) splitIntoSortedDatasets(input string, bufferSize int, field string) int {
	sdh.field = field

	file, err := os.Open(input)
	if err != nil {
		panic(err)
	}
	defer closeFile(file)

	var wg sync.WaitGroup
	var tokens []Token

	scanner := getScanner(file, bufferSize)
	createTempDir(tempDir, 0755)

	count := 0
	totalFiles := 0

	// adjust per need & available memory
	// when we get a much larger file, we can split it multiple files of `maxLines` lines each
	const maxLines = 10000

	for scanner.Scan() {
		count += 1

		jsonHelper.ToStruct(scanner.Text(), &token)
		tokens = append(tokens, token)

		if count == maxLines {
			count = 0
			totalFiles += 1

			wg.Add(1)
			go sdh.handleTokens(&wg, tokens, totalFiles)

			tokens = make([]Token, 0) // reinitialize
		}
	}

	// handle remaining tokens (if any)
	if len(tokens) > 0 {
		totalFiles += 1
		wg.Add(1)
		go sdh.handleTokens(&wg, tokens, totalFiles)
	}

	wg.Wait()

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return totalFiles
}

func (sdh *SortedDatasetsHandler) handleTokens(wg *sync.WaitGroup, tokens []Token, fileNameCount int) {
	defer wg.Done()

	sdh.handleSort(tokens)
	tempSave(fileNameCount, tokens) // tokens are now in sorted order
}

func (sdh *SortedDatasetsHandler) handleSort(tokens []Token) {
	if sdh.field == sortByFieldName {
		sortByName(tokens)
	} else {
		sortByAddress(tokens)
	}
}

func sortByName(tokens []Token) {
	sort.Slice(tokens[:], func(i, j int) bool {
		return tokens[i].Name < tokens[j].Name
	})
}

func sortByAddress(tokens []Token) {
	sort.Slice(tokens[:], func(i, j int) bool {
		return tokens[i].Address < tokens[j].Address
	})
}

func tempSave(fileNameCount int, tokens []Token) {
	f := createFile(fmt.Sprintf("%s_%d.txt", dataSortedTemp, fileNameCount))
	defer closeFile(f)

	var sb strings.Builder
	lineNum := 0
	for _, token := range tokens {
		lineNum += 1
		json := jsonHelper.ToJson(token)
		if lineNum == 1 {
			sb.WriteString(fmt.Sprintf("%s", json))
		} else {
			sb.WriteString(fmt.Sprintf("\n%s", json))
		}
	}

	_, err := f.WriteString(sb.String())
	if err != nil {
		log.Fatal(err)
	}
}
