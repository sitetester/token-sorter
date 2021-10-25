package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

type LastFoundSortedToken struct {
	fileNum int // number of the temporary sorted file
	lineNum int // line where this token was found
	token   Token
}

type Token struct {
	Name    string
	Address string
}

const tempDir = "temp"
const dataSortedTemp = tempDir + "/data_sorted_temp"

type TokenSorter struct {
	outputPath string
	bufferSize int
	field      string
}

var jsonHelper JsonHelper
var lastFoundSortedToken LastFoundSortedToken
var token Token

func (ts *TokenSorter) Sort(input string, output string, bufferSize int, field string) {

	// clean start
	removeFile(output)
	ts.outputPath = output
	ts.bufferSize = bufferSize
	ts.field = field

	var sortedDatasetsHandler SortedDatasetsHandler
	totalFiles := sortedDatasetsHandler.splitIntoSortedDatasets(input, bufferSize, field)

	// at this point, we have sorted data sets in respective files
	// next, we will take first token from first file and compare it with tokens of all other files
	// during comparison, if some token from other file is in sorted order, then we make it default/initial sorted token
	// & jump to next file, since all remaining tokens in THAT file are already in sorted form
	// at end of comparisons with all files, we remove it from specific file and put/append in final sorted file
	// this process continues, until all entries are matched
	// if some file has no entries, then we simply delete it, so it's not compared next time

	lastFoundSortedToken = LastFoundSortedToken{
		fileNum: 1,
		lineNum: 1,
	}

	// proceed with final sort
	isFirstLine := true
	var deletedFileNums []int
	for len(deletedFileNums) != totalFiles {
		totalFiles, deletedFileNums = ts.proceedWithFinalSort(totalFiles, lastFoundSortedToken, deletedFileNums, isFirstLine)
		isFirstLine = false
	}

	// cleanup, just in case any file left inside previously
	if err := os.RemoveAll(tempDir); err != nil {
		log.Fatal(err)
	}
}

func (ts *TokenSorter) proceedWithFinalSort(
	totalFiles int, lastFoundSortedToken LastFoundSortedToken, deletedFileNums []int, isFirstLine bool,
) (int, []int) {

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

		f, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}

		lineNum := 0
		scanner := getScanner(f, ts.bufferSize)
		for scanner.Scan() {
			lineNum += 1

			jsonHelper.ToStruct(scanner.Text(), &token)
			initialSortedToken := token

			lastFoundSortedToken = ts.compareWithOtherFiles(fileNum, lineNum, totalFiles, initialSortedToken)
			ts.performActionsAfterLastFoundSortedToken(lastFoundSortedToken, isFirstLine)

			// start again from beginning (fileNum=1, lineNum=1)
			// this will ensure `isFirstLine` is `false` next time
			break mainLoop
		}
	}

	return totalFiles, deletedFileNums
}

func (ts *TokenSorter) compareWithOtherFiles(fileNum int, lineNum int, totalFiles int, initialSortedToken Token) LastFoundSortedToken {
	lastFoundSortedToken = LastFoundSortedToken{
		fileNum: fileNum,
		lineNum: lineNum,
		token:   initialSortedToken,
	}

	var f *os.File
mainLoop:
	for otherFileNum := 1; otherFileNum <= totalFiles; otherFileNum++ {
		if fileNum != otherFileNum { // skip matching in same/given file
			filePath := buildPath(otherFileNum)

			if _, err := os.Stat(filePath); err == nil {
				f, _ = os.Open(filePath)
				scanner := getScanner(f, ts.bufferSize)

				currentLineNum := 0
				for scanner.Scan() {
					currentLineNum += 1

					jsonHelper.ToStruct(scanner.Text(), &token)

					if ts.isLastFoundSortedTokenGreater(lastFoundSortedToken, token) {
						lastFoundSortedToken = LastFoundSortedToken{
							fileNum: otherFileNum,
							lineNum: currentLineNum,
							token:   token,
						}

						if otherFileNum == totalFiles {
							// no need to check in remaining tokens, since we are in LAST dataset
							// and this file's tokens are already sorted
							break mainLoop
						} else {
							// jump to next file, since current file/dataset is already in sorted form
							// hence no need to check in remaining tokens
							continue mainLoop
						}
					}
				}
			}
		}
	}

	return lastFoundSortedToken
}

func (ts *TokenSorter) performActionsAfterLastFoundSortedToken(lastFoundSortedToken LastFoundSortedToken, isFirstLine bool) {
	ts.appendToFinalSortedDataset(lastFoundSortedToken.token, isFirstLine)
	removeLineFromFile(buildPath(lastFoundSortedToken.fileNum), lastFoundSortedToken.lineNum, ts.bufferSize)
}

func (ts *TokenSorter) appendToFinalSortedDataset(token Token, isFirstLine bool) {
	f, err := os.OpenFile(ts.outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile(f)

	var str string
	json := jsonHelper.ToJson(token)
	if isFirstLine {
		str = fmt.Sprintf("%s", json)
	} else {
		str = fmt.Sprintf("\n%s", json)
	}

	if _, err := f.WriteString(str); err != nil {
		log.Fatal(err)
	}
}

func (ts *TokenSorter) isLastFoundSortedTokenGreater(lastFoundSortedToken LastFoundSortedToken, token Token) bool {
	var result int
	if ts.field == sortByFieldName {
		result = strings.Compare(lastFoundSortedToken.token.Name, token.Name)
	} else {
		result = strings.Compare(lastFoundSortedToken.token.Address, token.Address)
	}

	return result == 1
}

func contains(nums []int, e int) bool {
	for _, n := range nums {
		if n == e {
			return true
		}
	}

	return false
}
