package main

import "flag"

const sortByFieldName = "name"
const sortByFieldAddress = "address"

func main() {
	input := flag.String("input", "./data/data.in", "Input file to sort")
	output := flag.String("output", "./data.out", "Output file to store sorted data")
	// In general, we don’t need to specify buffer, since Golang bufio scanner uses an internal buffer by default
	bufferSize := flag.Int("buffer-size", 1048576, "buffer size to use for file operations")
	flag.Parse()

	field := sortByFieldName
	if len(flag.Args()) > 0 {
		field = flag.Args()[0]
	}

	if field != sortByFieldName && field != sortByFieldAddress {
		println("Only `name` or `address` could be used for sorting")
		return
	}

	var tokenSorter TokenSorter
	tokenSorter.Sort(*input, *output, *bufferSize, field)
}
