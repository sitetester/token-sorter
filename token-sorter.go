package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

const sortByFieldName = "name"
const tempDir = "temp"
const dataSortedTemp = tempDir + "/data_sorted_temp"

var jsonHelper JsonHelper
var lastFoundSortedToken LastFoundSortedToken

func main() {
	input := flag.String("input", "data/data.in", "Input file to sort")
	output := flag.String("output", "./data.out", "Output file to store sorted data")
	// In general, we don’t need to specify buffer, since Golang bufio scanner uses an internal buffer by default
	bufferSize := flag.Int("buffer-size", 4096, "buffer size to use for file operations")
	flag.Parse()

	field := sortByFieldName
	if len(flag.Args()) > 0 {
		field = flag.Args()[0]
	}

	if field != sortByFieldName && field != "address" {
		println("Only `name` or `address` could be used for sorting")
		return
	}

	Sort(*input, *output, *bufferSize, field)
}

func Sort(input string, output string, bufferSize int, field string) {
	var sortedDatasetsHandler SortedDatasetsHandler
	totalFiles := sortedDatasetsHandler.splitIntoSortedDatasets(input, bufferSize, field)

	// at this point, we have sorted data sets in respective files
	// next, we will take first item from first dataset and compare it with all tokens of each dataset
	// during comparison, if some item from other dataset is in sorted order, then we make it default/initial sorted value
	// at end of comparisons with all datasets, we remove it from specific dataset and put/append in final sorted dataset
	// this process continues, until all entries are matched
	// if some file has no entries, then we simply delete it, so it's not compared next time

	lastFoundSortedToken = LastFoundSortedToken{
		FileNum: 1,
		LineNum: 1,
	}

	var deletedFileNums []int

	isFirstLine := true
	// proceed with final sort
	for len(deletedFileNums) != totalFiles {
		totalFiles, deletedFileNums = proceedWithFinalSort(totalFiles, field, lastFoundSortedToken, deletedFileNums, isFirstLine)
		isFirstLine = false
	}

	// cleanup
	err := os.RemoveAll(tempDir)
	if err != nil {
		log.Fatal(err)
	}

	// finally, rename to expected/given name
	generatedPath := fmt.Sprintf("data_sorted_%s.txt", field)
	if err = os.Rename(generatedPath, output); err != nil {
		log.Fatal(err)
	}
}

func proceedWithFinalSort(totalFiles int, field string, lastFoundSortedToken LastFoundSortedToken, deletedFileNums []int, isFirstLine bool) (int, []int) {

mainLoop:
	for fileNum := 1; fileNum <= totalFiles; fileNum++ {
		if len(deletedFileNums) > 0 && contains(deletedFileNums, fileNum) {
			continue
		}

		filePath := buildPath(fileNum)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			deletedFileNums = append(deletedFileNums, fileNum)
			continue
		}

		lineNum := 0
		f, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNum += 1

			var token Token
			jsonHelper.ToStruct(scanner.Text(), &token)
			initialSortedToken := token

			lastFoundSortedToken = compareWithOtherFiles(fileNum, lineNum, totalFiles, initialSortedToken, field)
			performActionsAfterLastFoundSortedToken(lastFoundSortedToken, field, isFirstLine)
			break mainLoop
		}
	}

	return totalFiles, deletedFileNums
}

func performActionsAfterLastFoundSortedToken(lastFoundSortedToken LastFoundSortedToken, field string, isFirstLine bool) {
	appendToFinalSortedDataset(lastFoundSortedToken.Token, field, isFirstLine)
	removeLineFromFile(buildPath(lastFoundSortedToken.FileNum), lastFoundSortedToken.LineNum)
}

func compareWithOtherFiles(i int, lineNum int, fileNameCount int, initialSortedToken Token, field string) LastFoundSortedToken {
	lastFoundSortedToken = LastFoundSortedToken{
		FileNum: i,
		LineNum: lineNum,
		Token:   initialSortedToken,
	}

	var f *os.File
mainLoop:
	for j := 1; j <= fileNameCount; j++ {
		if i != j {
			filePath := buildPath(j)
			currentLineNum := 0

			if _, err := os.Stat(filePath); err == nil {
				f, _ = os.Open(filePath)
				scanner := bufio.NewScanner(f)

				for scanner.Scan() {
					currentLineNum += 1

					var token Token
					jsonHelper.ToStruct(scanner.Text(), &token)

					if isLastFoundSortedTokenGreater(lastFoundSortedToken, token, field) {
						if j == fileNameCount {
							lastFoundSortedToken = LastFoundSortedToken{
								FileNum: j,
								LineNum: currentLineNum,
								Token:   token,
							}

							// no need to check in remaining tokens, since we are in LAST dataset
							// and this file's tokens are already in sorted
							break mainLoop
						} else {
							lastFoundSortedToken = LastFoundSortedToken{
								FileNum: j,
								LineNum: currentLineNum,
								Token:   token,
							}
						}
					}
				}
			}
		}
	}

	return lastFoundSortedToken
}

func appendToFinalSortedDataset(token Token, field string, isFirstLine bool) {
	filePath := fmt.Sprintf("data_sorted_%s.txt", field)

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer closeFile(f)

	var str string
	if isFirstLine {
		str = fmt.Sprintf("%s", jsonHelper.ToJson(token))
	} else {
		str = fmt.Sprintf("\n%s", jsonHelper.ToJson(token))
	}

	if _, err := f.WriteString(str); err != nil {
		log.Fatal(err)
	}
}

func isLastFoundSortedTokenGreater(lastFoundSortedToken LastFoundSortedToken, token Token, field string) bool {
	var result int
	if field == sortByFieldName {
		result = strings.Compare(lastFoundSortedToken.Token.Name, token.Name)
	} else {
		result = strings.Compare(lastFoundSortedToken.Token.Address, token.Address)
	}

	return result == 1
}

type LastFoundSortedToken struct {
	FileNum int // number of the temporary sorted file
	LineNum int // line where this token was found
	Token   Token
}

type Token struct {
	Name    string
	Address string
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
