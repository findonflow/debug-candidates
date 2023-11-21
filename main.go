package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v2"
	"github.com/onflow/flow-go/model/flow"
	"github.com/vmihailenco/msgpack/v4"
	"golang.org/x/exp/slices"
)

type Tx struct {
	Result flow.TransactionResult
	Body   flow.TransactionBody
}

func main() {
	path := os.Args[1]
	firstHeight := 7601063
	lastHeight := 8742959

	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	headers, blockId := GetHeaders(db, uint64(lastHeight))

	transactions := GetTransactions(db)
	transactionResults := GetTransactionResults(db)

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

	totalTx := 0
	for {
		header := headers[blockId]
		txList := txByBlock[blockId]
		totalTx = totalTx + len(txList)

		txIds := []string{}
		for _, tx := range txList {
			txIds = append(txIds, tx.Result.TransactionID.String())
		}

		slices.Sort(txIds)
		txString := strings.Join(txIds, ";")
		fmt.Println(header.Height, blockId, txString)
		if header.Height == uint64(firstHeight) {
			break
		}

		blockId = header.ParentID.String()
	}

	fmt.Printf("total tx in badger %d\n", len(transactions))
	fmt.Printf("total tx-result in badger %d\n", len(transactionResults))
	fmt.Println("total tx:", totalTx)
}

type Headers = map[string]flow.Header

func GetHeaders(db *badger.DB, lastBlockHeight uint64) (Headers, string) {
	tx := db.NewTransaction(false)

	it := tx.NewIterator(badger.DefaultIteratorOptions)

	prefix := make([]byte, 1)
	prefix[0] = 30
	headers := Headers{}
	lastBlockId := ""
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
			if header.Height == lastBlockHeight {
				lastBlockId = blockID
			}

			return nil
		})

		if valueErr != nil {
			panic(valueErr)
		}
	}

	return headers, lastBlockId
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
