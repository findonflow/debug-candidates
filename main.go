package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/dgraph-io/badger/v2"
	"github.com/onflow/flow-go/model/flow"
	"github.com/samber/lo"
	"github.com/vmihailenco/msgpack/v4"
)

type Tx struct {
	Result flow.TransactionResult
	Body   flow.TransactionBody
}

func main() {
	path := os.Args[1]
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	headers := GetHeaders(db)

	transactions := GetTransactions(db)
	transactionResults := GetTransactionResults(db)

	groupedHeaders := lo.GroupBy(lo.Values(headers), func(item flow.Header) uint64 {
		return item.Height
	})

	txByBlock := map[string][]Tx{}
	for txId, tx := range transactions {
		txr := transactionResults[txId]

		t := Tx{
			Body:   tx,
			Result: txr.Result,
		}

		old, ok := txByBlock[txr.BlockId]
		if !ok {
			old = []Tx{t}
		} else {
			old = append(old, t)
		}
		//	fmt.Print(".")
		txByBlock[txr.BlockId] = old
	}

	blocks := map[uint64][]Tx{}

	totalTx := 0
	totalTxWithDups := 0
	for height, headerList := range groupedHeaders {

		var header flow.Header
		var blockId string
		var txList []Tx
		var txLength int

		// we sort the headers to get the one with the largest view first
		sort.SliceStable(headerList, func(i, j int) bool {
			return headerList[i].View < headerList[j].View
		})

		for _, hc := range headerList {

			blockId = hc.ID().String()
			txList = txByBlock[blockId]
			totalTxWithDups = totalTxWithDups + len(txList)
			if height == 7878525 {
				for _, tx := range txList {
					fmt.Printf("block=%s tx=%s", blockId, tx.Result.TransactionID)
				}
			}
			/*
				if txLength > 0 && len(txList) > 0 {
					fmt.Printf("Height %d has two or more blockIds with transactions\n", header.Height)
					fmt.Printf("%s = %d\n", header.ID().String(), txLength)
					fmt.Printf("%s = %d\n", blockId, len(txList))
					// adding the tx that we will then skip
				}
			*/
			txLength = len(txList)
			header = hc
		}
		totalTx = totalTx + txLength
		blocks[header.Height] = txList
	}

	fmt.Printf("total tx in badger %d\n", len(transactions))
	fmt.Printf("total tx-result in badger %d\n", len(transactionResults))
	fmt.Println("total tx:", totalTx)
	fmt.Println("total tx with dup blocks:", totalTxWithDups)
}

type Headers = map[string]flow.Header

func GetHeaders(db *badger.DB) Headers {
	tx := db.NewTransaction(false)

	it := tx.NewIterator(badger.DefaultIteratorOptions)

	prefix := make([]byte, 1)
	prefix[0] = 30
	headers := Headers{}
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		var header flow.Header
		item := it.Item()

		k := item.Key()
		blockID := hex.EncodeToString(k[1:33])

		valueErr := item.Value(func(val []byte) error {
			umarshalErr := msgpack.Unmarshal(val, &header)
			if umarshalErr != nil {
				return umarshalErr
			}
			headers[blockID] = header

			return nil
		})

		if valueErr != nil {
			panic(valueErr)
		}
	}

	return headers
}

func GetTransactionResults(db *badger.DB) map[string]IndexerTransactionResult {
	tx := db.NewTransaction(false)

	it := tx.NewIterator(badger.DefaultIteratorOptions)

	prefix := make([]byte, 1)
	prefix[0] = 104
	transactionResults := map[string]IndexerTransactionResult{}
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		var transactionResult flow.TransactionResult
		item := it.Item()

		k := item.Key()
		blockID := hex.EncodeToString(k[1:33])
		transactionID := hex.EncodeToString(k[33:65])

		valueErr := item.Value(func(val []byte) error {
			umarshalErr := msgpack.Unmarshal(val, &transactionResult)
			if umarshalErr != nil {
				return umarshalErr
			}
			transactionResults[transactionID] = IndexerTransactionResult{
				Result:  transactionResult,
				BlockId: blockID,
			}

			return nil
		})

		if valueErr != nil {
			panic(valueErr)
		}
	}

	return transactionResults
}

type IndexerTransactionResult struct {
	Result  flow.TransactionResult
	BlockId string
}

type IndexerTransaction struct {
	Result flow.TransactionBody
}

func GetTransactions(db *badger.DB) map[string]flow.TransactionBody {
	tx := db.NewTransaction(false)

	it := tx.NewIterator(badger.DefaultIteratorOptions)

	prefix := make([]byte, 1)
	prefix[0] = 34

	transactions := map[string]flow.TransactionBody{}

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		var transactionBody flow.TransactionBody
		item := it.Item()

		k := item.Key()
		transactionID := hex.EncodeToString(k[1:33])
		valueErr := item.Value(func(val []byte) error {
			umarshalErr := msgpack.Unmarshal(val, &transactionBody)
			if umarshalErr != nil {
				return umarshalErr
			}

			transactions[transactionID] = transactionBody
			return nil
		})

		if valueErr != nil {
			panic(valueErr)
		}
	}

	return transactions
}
