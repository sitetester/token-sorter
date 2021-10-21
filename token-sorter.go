package main

import "flag"

const SortByFieldName = "name"
const SortByFieldAddress = "address"

var jsonHelper JsonHelper
var lastFoundSortedToken LastFoundSortedToken

func main() {
	input := flag.String("input", "data/data.in", "Input file to sort")
	output := flag.String("output", "./data.out", "Output file to store sorted data")
	// In general, we don’t need to specify buffer, since Golang bufio scanner uses an internal buffer by default
	bufferSize := flag.Int("buffer-size", 4096, "buffer size to use for file operations")
	flag.Parse()

	field := SortByFieldName
	if len(flag.Args()) > 0 {
		field = flag.Args()[0]
	}

	if field != SortByFieldName && field != "address" {
		println("Only `name` or `address` could be used for sorting")
		return
	}

	var tokenSorter TokenSorter
	tokenSorter.Sort(*input, *output, *bufferSize, field)
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
