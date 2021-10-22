package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func createFile(filePath string) *os.File {
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}

	return f
}

func removeFile(filePath string) {
	if _, err := os.Stat(filePath); err == nil {
		if err := os.Remove(filePath); err != nil {
			log.Fatal(err)
		}
	}
}

func closeFile(f *os.File) {
	err := f.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func createTempDir(path string, perm os.FileMode) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.Mkdir(path, perm)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func buildPath(fileNum int) string {
	return fmt.Sprintf("%s_%d.txt", dataSortedTemp, fileNum)
}

// TODO: Improve/optimize this functionality
func removeLineFromFile(filePath string, lineNum int) {
	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModeAppend)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(f)
	tokensStr := ""
	currentLineNum := 0

	for scanner.Scan() {
		currentLineNum += 1
		if currentLineNum != lineNum {
			tokensStr += fmt.Sprintf("%s\n", scanner.Text())
		}
	}

	if len(tokensStr) > 0 {
		tokensStr = strings.TrimSuffix(tokensStr, "\n") // remove last `\n`

		removeFile(filePath)
		f = createFile(filePath)

		_, err = f.WriteString(tokensStr)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		removeFile(filePath)
	}
}
