/**
Title:		InvtCommit
Author:		Elizalde Baguinon
Company:	Valiant Group of Companies
Purpose: 	Program to commit invoice in sage
Created: 	May 31, 2019
Release:
*/

package main

import (
	"gosqljobs/invtcommit/functions/sm"
	"log"
	"os"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	cfg "github.com/eaglebush/config"
	du "github.com/eaglebush/datautils"
)

func main() {
	argswp := os.Args[1:]
	tranid := ""
	whse := ""
	//whsekey := int64(0)
	configfile := "config.json"

	for _, kv := range argswp {
		p := strings.Index(kv, "=")
		k := kv[:p]
		v := kv[p+1:]

		if k == "/whse" {
			whse = v
		}
		if k == "/tranid" {
			tranid = v
		}
		if k == "/config" {
			configfile = v
		}
	}

	log.Println(tranid)
	log.Println(whse)

	config, err := cfg.LoadConfig(configfile)
	if err != nil {
		log.Fatal("Configuration file not found!")
	}

	bq := du.NewBatchQuery(config)
	//og.Println(bq)

	connected := bq.Connect("DEST_MDCI")
	defer bq.Disconnect()

	if !connected {
		log.Println(bq.LastErrorText())
	}

	if connected {
		// test get next block surrogate key
		stk, ek := sm.GetNextBlockSurrogateKey(bq, `TestTable`, 10)
		log.Printf("Start Key: %d, End Key: %d\r\n", stk, ek)
		log.Println(bq.LastErrorText())
	}

}
