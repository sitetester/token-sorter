package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

const tempDir = "temp"
const dataSortedTemp = tempDir + "/data_sorted_temp"

var jsonHelper JsonHelper
var lastFoundSortedToken LastFoundSortedToken

func main() {
	start := time.Now()

	// TODO:  check mandatory inputs
	// input := flag.String("input", "data/dummy.txt", "Input file to sort")
	input := flag.String("input", "data/data.txt", "Input file to sort")
	// input := flag.String("input", "data/data_MEDIUM.txt", "Input file to sort")
	// input := flag.String("input", "data/data_LARGE.txt", "Input file to sort")

	output := flag.String("output", "data.out", "Output file to store sorted data")
	field := flag.String("field", "token", "sort by `field` (name or address)")
	bufferSize := flag.Int("buffer-size", 4096, "buffer size to use for file operations")
	flag.Parse()

	if *input == "" {
		println("Please provide an `input` argument, e.g. --input=data.in")
		return
	}

	if *output == "" {
		println("Please provide an `output` argument, e.g. --output=data.out")
		return
	}

	if *field != "name" && *field != "token" {
		println("Only `name` or `token` could be provided for `field` argument")
		return
	}

	var sortedDatasetsHandler SortedDatasetsHandler
	totalFiles := sortedDatasetsHandler.splitIntoSortedDatasets(*input, *bufferSize, *field)

	// at this point, we have sorted data sets in respective files
	// next, we will take first item from first dataset and compare it with all tokens of each dataset
	// during comparison, if some item from other dataset is in sorted order, then we make it default sorted value
	// at end of comparisons with all datasets, we remove it from specific dataset and put/append in final sorted dataset
	// this process continues, until all entries are matched
	// if some file has no entries, then we simply delete it, so it's not compared next time

	filePath := fmt.Sprintf("data_sorted_%s.txt", *field)

	// perhaps we could reuse this final sorted file ?
	removeFile(filePath)

	lastFoundSortedToken = LastFoundSortedToken{
		FileNum: 1,
		LineNum: 1,
	}

	var deletedFileNums []int

	// proceed with final sort
	for len(deletedFileNums) != totalFiles {
		totalFiles, deletedFileNums = proceedWithFinalSort(totalFiles, *field, lastFoundSortedToken, deletedFileNums)
	}

	// cleanup
	err := os.RemoveAll(tempDir)
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	log.Printf("Finished at: %f", elapsed.Seconds())
	os.Exit(0)
}

func proceedWithFinalSort(totalFiles int, field string, lastFoundSortedToken LastFoundSortedToken, deletedFileNums []int) (int, []int) {

mainLoop:
	for fileNum := 1; fileNum <= totalFiles; fileNum++ {
		if len(deletedFileNums) > 0 && contains(deletedFileNums, fileNum) {
			continue
		}

		filePath := buildPath(fileNum)

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			deletedFileNums = append(deletedFileNums, fileNum)

			/*println("filePath NOT found =>", filePath)
			println("len(deletedFileNums) : ", len(deletedFileNums))*/
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

			/*lastFoundSortedTokenLabel:
			lastFoundSortedToken := compareWithOtherFiles(fileNum, lineNum, totalFiles, initialSortedToken, field)
			if lastFoundSortedToken.FileNum == fileNum && lastFoundSortedToken.LineNum == lineNum && lastFoundSortedToken.Token == initialSortedToken {
				goto lastFoundSortedTokenLabel // kinda recursion ;)
			}*/

			lastFoundSortedToken = compareWithOtherFiles(fileNum, lineNum, totalFiles, initialSortedToken, field)
			// printStruct("\n lastFoundSortedToken", lastFoundSortedToken)
			performActionsAfterLastFoundSortedToken(lastFoundSortedToken, field)
			// println()
			break mainLoop
			/*printStruct("lastFoundSortedToken", lastFoundSortedToken)
			println()


			// initially found in temp3
			performActionsAfterLastFoundSortedToken(lastFoundSortedToken, field)

			lastFoundSortedToken = compareWithOtherFiles(fileNum, lineNum, totalFiles, initialSortedToken, field)
			printStruct("lastFoundSortedToken", lastFoundSortedToken)
			println()

			// found in same/current file
			if lastFoundSortedToken.FileNum == fileNum && lastFoundSortedToken.LineNum == lineNum && lastFoundSortedToken.Token == initialSortedToken {
				removeLastFoundSortedToken(lastFoundSortedToken)

				lastFoundSortedToken = compareWithOtherFiles(fileNum, lineNum, totalFiles, initialSortedToken, field)
				printStruct("lastFoundSortedToken", lastFoundSortedToken)
				println()
			}*/
		}
	}

	return totalFiles, deletedFileNums
}

func performActionsAfterLastFoundSortedToken(lastFoundSortedToken LastFoundSortedToken, field string) {
	appendToFinalSortedDataset(lastFoundSortedToken.Token, field)
	removeLineFromFile(buildPath(lastFoundSortedToken.FileNum), lastFoundSortedToken.LineNum)
}

func compareWithOtherFiles(i int, lineNum int, fileNameCount int, initialSortedToken Token, field string) LastFoundSortedToken {
	lastFoundSortedToken = LastFoundSortedToken{
		FileNum: i,
		LineNum: lineNum,
		Token:   initialSortedToken,
	}

	// println(fmt.Sprintf("\n------------ Dataset#: %d, initialSortedToken: %+v", i, initialSortedToken))
	// var msg = ""

	var f *os.File
mainLoop:
	for j := 1; j <= fileNameCount; j++ {
		if i != j { // 1 != 1

			filePath := buildPath(j)
			// println("\n filePath =>", filePath)

			currentLineNum := 0
			if _, err := os.Stat(filePath); err == nil {
				f, _ = os.Open(filePath)
				scanner := bufio.NewScanner(f)

				for scanner.Scan() {
					currentLineNum += 1

					// println("currentLineNum: ", currentLineNum)
					var token Token
					jsonHelper.ToStruct(scanner.Text(), &token)

					result := 0
					if field == "name" {
						// msg = fmt.Sprintf("Comparing %s AND %s", initialSortedToken.Name, token.Name)
						result = strings.Compare(lastFoundSortedToken.Token.Name, token.Name)
						if result != 1 {
							// msg += fmt.Sprintf("   => %s, %d", initialSortedToken.Name, result)
							// println(msg)
						}
					} else {
						// msg = fmt.Sprintf("Comparing %s AND %s", initialSortedToken.Address, token.Address)
						result = strings.Compare(lastFoundSortedToken.Token.Address, token.Address)
					}

					if result == 1 {
						/*msg += fmt.Sprintf("result == 1,,,   => %s, %d", token.Name, result)
						println(msg)*/

						if j == fileNameCount {
							/*appendToFinalSortedDataset(token, field)
							removeLineFromFile(filePath, currentLineNum)*/
							lastFoundSortedToken = LastFoundSortedToken{
								FileNum: j,
								LineNum: currentLineNum,
								Token:   token,
							}

							break mainLoop

							// we are in last dataset
							// repeat comparing same initial token
							// compareWithOtherFiles(i, fileNameCount, initialSortedToken, field)
						} else {
							// track the current line & token
							lastFoundSortedToken = LastFoundSortedToken{
								FileNum: j,
								LineNum: currentLineNum,
								Token:   token,
							}

							// printStruct("lastFoundSortedToken is NOW: ", lastFoundSortedToken)
						}
					}
				}
			}
		}
	}

	/*err := f.Close()
	if err != nil {
		log.Fatal("f.Close(): ", err)
	}*/

	return lastFoundSortedToken
}

func appendToFinalSortedDataset(token Token, field string) {
	filePath := fmt.Sprintf("data_sorted_%s.txt", field)

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer closeFile(f)

	str := fmt.Sprintf("%s\n", jsonHelper.ToJson(token))
	// println("appendToFinalSortedDataset => ", str)
	if _, err := f.WriteString(str); err != nil {
		log.Fatal(err)
	}
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

func printStruct(msg string, s interface{}) {
	println(fmt.Sprintf("%s: %+v", msg, s))
}
