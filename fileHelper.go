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

func removeLineFromFile(filePath string, lineNum int) {
	f, err := os.OpenFile(filePath, os.O_RDWR, os.ModeAppend)
	if err != nil {
		log.Fatal(err)
	}

	var sb strings.Builder
	isFirstLine := true
	currentLineNum := 0
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		currentLineNum += 1
		if currentLineNum == lineNum {
			continue
		}

		if isFirstLine {
			sb.WriteString(scanner.Text())
			isFirstLine = false
		} else {
			sb.WriteString("\n" + scanner.Text())
		}
	}

	tokensStr := sb.String()

	if len(tokensStr) > 0 {
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
