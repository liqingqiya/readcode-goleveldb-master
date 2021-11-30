package main

import (
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	db, err := leveldb.OpenFile("./mydb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s_%d", "key", i)
		value := fmt.Sprintf("%s_%d", "value", i)

		err = db.Put([]byte(key), []byte(value), nil)
		if err != nil {
			log.Fatal(err)
		}
	}
}
