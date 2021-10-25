package main

import (
	"encoding/json"
	"log"
)

type JsonHelper struct{}

func (jh *JsonHelper) ToStruct(text string, token *Token) {
	err := json.Unmarshal([]byte(text), token)
	if err != nil {
		log.Fatal(err)
	}
}

func (jh *JsonHelper) ToJson(token Token) string {
	bytes, err := json.Marshal(token)
	if err != nil {
		log.Fatal(err)
	}

	return string(bytes)
}
