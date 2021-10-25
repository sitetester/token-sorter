package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

// https://stackoverflow.com/questions/39859222/golang-how-to-overcome-scan-buffer-limit-from-bufio
// https://stackoverflow.com/questions/38902092/does-bufio-newscanner-in-golang-reads-the-entire-file-in-memory-instead-of-a-lin
func getScanner(file *os.File, bufferSize int) *bufio.Scanner {
	scanner := bufio.NewScanner(file)

	buf := make([]byte, bufio.MaxScanTokenSize)
	scanner.Buffer(buf, bufferSize)

	return scanner
}

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

func removeLineFromFile(filePath string, lineNum int, bufferSize int) {
	fr, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	var sb strings.Builder
	isFirstLine := true
	currentLineNum := 0
	scanner := getScanner(fr, bufferSize)

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

	tokenStr := sb.String()
	closeFile(fr)

	if len(tokenStr) > 0 {
		// truncate existing file
		fw, err := os.Create(filePath)
		if err != nil {
			log.Fatal(err)
		}

		_, err = fw.WriteString(tokenStr)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		removeFile(filePath)
	}
}
