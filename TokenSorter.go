package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

type LastFoundSortedToken struct {
	FileNum int // number of the temporary sorted file
	LineNum int // line where this token was found
	Token   Token
}

type Token struct {
	Name    string
	Address string
}

const tempDir = "temp"
const dataSortedTemp = tempDir + "/data_sorted_temp"

type TokenSorter struct {
	OutputPath string
}

func (ts *TokenSorter) Sort(input string, output string, bufferSize int, field string) {

	// clean start
	removeFile(output)
	ts.OutputPath = output

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
		FileNum: 1,
		LineNum: 1,
	}

	// proceed with final sort
	isFirstLine := true
	var deletedFileNums []int
	for len(deletedFileNums) != totalFiles {
		totalFiles, deletedFileNums = ts.proceedWithFinalSort(totalFiles, field, lastFoundSortedToken, deletedFileNums, isFirstLine)
		isFirstLine = false
	}

	// cleanup, just in case any file left inside previously
	if err := os.RemoveAll(tempDir); err != nil {
		log.Fatal(err)
	}
}

func (ts *TokenSorter) proceedWithFinalSort(
	totalFiles int, field string, lastFoundSortedToken LastFoundSortedToken, deletedFileNums []int, isFirstLine bool,
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
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lineNum += 1

			var token Token
			jsonHelper.ToStruct(scanner.Text(), &token)
			initialSortedToken := token

			lastFoundSortedToken = compareWithOtherFiles(fileNum, lineNum, totalFiles, initialSortedToken, field)
			ts.performActionsAfterLastFoundSortedToken(lastFoundSortedToken, isFirstLine)
			break mainLoop
		}
	}

	return totalFiles, deletedFileNums
}

func compareWithOtherFiles(fileNum int, lineNum int, totalFiles int, initialSortedToken Token, field string) LastFoundSortedToken {
	lastFoundSortedToken = LastFoundSortedToken{
		FileNum: fileNum,
		LineNum: lineNum,
		Token:   initialSortedToken,
	}

	var f *os.File
mainLoop:
	for otherFileNum := 1; otherFileNum <= totalFiles; otherFileNum++ {
		if fileNum != otherFileNum { // skip matching in same/given file
			filePath := buildPath(otherFileNum)

			if _, err := os.Stat(filePath); err == nil {
				f, _ = os.Open(filePath)
				scanner := bufio.NewScanner(f)

				currentLineNum := 0
				for scanner.Scan() {
					currentLineNum += 1
					var token Token
					jsonHelper.ToStruct(scanner.Text(), &token)

					if isLastFoundSortedTokenGreater(lastFoundSortedToken, token, field) {
						lastFoundSortedToken = LastFoundSortedToken{
							FileNum: otherFileNum,
							LineNum: currentLineNum,
							Token:   token,
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
	ts.appendToFinalSortedDataset(lastFoundSortedToken.Token, isFirstLine)
	removeLineFromFile(buildPath(lastFoundSortedToken.FileNum), lastFoundSortedToken.LineNum)
}

func (ts *TokenSorter) appendToFinalSortedDataset(token Token, isFirstLine bool) {
	f, err := os.OpenFile(ts.OutputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
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

func isLastFoundSortedTokenGreater(lastFoundSortedToken LastFoundSortedToken, token Token, field string) bool {
	var result int
	if field == SortByFieldName {
		result = strings.Compare(lastFoundSortedToken.Token.Name, token.Name)
	} else {
		result = strings.Compare(lastFoundSortedToken.Token.Address, token.Address)
	}

	return result == 1
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
