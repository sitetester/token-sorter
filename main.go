package main

import (
	"flag"
)

const fieldName = "name"
const fieldAddress = "address"

func main() {
	inPath := flag.String("input", "./data/data.in", "Input file to sort")
	outPath := flag.String("output", "./data.out", "Output file to store sorted data")
	// In general, we don’t need to specify buffer, since Golang bufio scanner uses an internal buffer by default
	bufferSize := flag.Int("buffer-size", 1048576, "buffer size to use for file operations")
	flag.Parse()

	field := fieldName
	if len(flag.Args()) > 0 {
		field = flag.Args()[0]
	}

	var tokenSorter TokenSorter
	if !tokenSorter.isValidSortField(field) {
		println("Only `name` or `address` could be used for sorting")
		return
	}

	tokenSorter = TokenSorter{
		inPath:     *inPath,
		outPath:    *outPath,
		bufferSize: *bufferSize,
		byField:    field,
	}
	tokenSorter.Sort()
	println("Success!")
}
