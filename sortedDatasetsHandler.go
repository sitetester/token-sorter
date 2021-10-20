package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

type SortedDatasetsHandler struct{}

// https://stackoverflow.com/questions/39859222/golang-how-to-overcome-scan-buffer-limit-from-bufio
// https://stackoverflow.com/questions/38902092/does-bufio-newscanner-in-golang-reads-the-entire-file-in-memory-instead-of-a-lin
func (h *SortedDatasetsHandler) splitIntoSortedDatasets(input string, bufferSize int, field string) int {
	var wg sync.WaitGroup

	file, err := os.Open(input)
	if err != nil {
		panic(err)
	}
	defer closeFile(file)

	var token Token
	var tokens []Token

	scanner := bufio.NewScanner(file)

	buf := make([]byte, bufio.MaxScanTokenSize)
	scanner.Buffer(buf, bufferSize)

	createTempDir(tempDir, 0755)

	count := 0
	totalFiles := 0
	// const maxLines = 10000 // when we get a much larger file, then we can split it into 10k lines of files each
	const maxLines = 1000

	for scanner.Scan() {
		count += 1

		jsonHelper.ToStruct(scanner.Text(), &token)
		tokens = append(tokens, token)

		if count == maxLines {
			count = 0
			totalFiles += 1

			wg.Add(1)
			go handleTokens(&wg, field, tokens, totalFiles)

			tokens = make([]Token, 0) // reinitialize
		}
	}

	// handle remaining tokens (if any)
	if len(tokens) > 0 {
		totalFiles += 1
		wg.Add(1)
		go handleTokens(&wg, field, tokens, totalFiles)
	}

	wg.Wait()

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return totalFiles
}

func handleTokens(wg *sync.WaitGroup, field string, tokens []Token, fileNameCount int) {
	defer wg.Done()

	handleSort(field, tokens)
	tempSave(fileNameCount, tokens) // tokens are now in sorted order
}

func handleSort(field string, tokens []Token) {
	if field == "name" {
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
	f := createFile(fmt.Sprintf("%s/data_sorted_temp_%d.txt", tempDir, fileNameCount))
	defer closeFile(f)

	tokensStr := ""
	for _, token := range tokens {
		tokensStr += fmt.Sprintf("%s\n", jsonHelper.ToJson(token))
	}

	tokensStr = strings.TrimSuffix(tokensStr, "\n") // remove last `\n`
	_, err := f.WriteString(tokensStr)
	if err != nil {
		log.Fatal(err)
	}
}
