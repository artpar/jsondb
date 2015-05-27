package main

import (
	"github.com/artpar/jsondb/lib"
	"github.com/op/go-logging"
	"flag"
	"io/ioutil"
	"os"
	"github.com/pquerna/ffjson/ffjson"
	"code.google.com/p/vitess/go/vt/sqlparser"
)

var log = logging.MustGetLogger("example")

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var format = logging.MustStringFormatter(
	"%{time:15:04:05.000} ▶ %{level:.4s} %{id:03x}  %{message}",
)

func main() {
	logging.SetLevel(logging.ERROR, "")
	var sqlString, sqlFile, dataFile string
	flag.StringVar(&sqlString, "sql", "", "-sql='select t.txn_id from D.data t'")
	flag.StringVar(&sqlFile, "sqlfile", "", "-sqlfile=1.sql")
	flag.StringVar(&dataFile, "data", "", "-data='data.json'")
	flag.Parse()

	var data []byte
	var err error


	if (len(sqlString) < 1) && len(sqlFile) < 1 {
		log.Debug("Please specify an sql for a file containing a sql")
		flag.Usage()
		os.Exit(1)
	}
	if len(sqlString) < 1 {
		sqlStringByte, err := ioutil.ReadFile(sqlFile)
		if err != nil {
			panic(err)
		}
		sqlString = string(sqlStringByte)
	}
	sql, err := sqlparser.Parse(string(sqlString))
	if err != nil {
		panic(err)
	}

	if len(dataFile) > 0 {
		data, err = ioutil.ReadFile(dataFile)
		if err != nil {
			panic(err)
		}
	} else {
		data, _ = ioutil.ReadAll(os.Stdin)
	}

	jsondb.TravelNode(sql)
	executionModel := jsondb.TravelNodeRecurse(sql)
	jsondb.PrintExecutionModel(executionModel)
	extractedData := jsondb.Execute(executionModel, data)
	log.Debug("Data - %#v", extractedData)
	result, err := ffjson.Marshal(extractedData)
	//	jsonOut, err := gabs.Consume(extractedData)
	if err != nil {
		log.Debug("Error while making json - %s", err)
	}
	//	result := jsonOut.StringIndent("    ", "  ")
	log.Debug("Final - %#v", string(result))
	os.Stdout.WriteString(string(result))
	os.Stdout.Close()
}

