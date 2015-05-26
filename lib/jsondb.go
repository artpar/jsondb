package jsondb

import (
	"code.google.com/p/vitess/go/vt/sqlparser"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"github.com/artpar/gabs"
	"github.com/op/go-logging"
	"flag"
	"github.com/pquerna/ffjson/ffjson"
)

var log = logging.MustGetLogger("example")

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var format = logging.MustStringFormatter(
	"%{time:15:04:05.000} ▶ %{level:.4s} %{id:03x}  %{message}",
)

func main() {
	logging.SetLevel(logging.DEBUG, "")
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

	TravelNode(sql)
	executionModel := TravelNodeRecurse(sql)
	PrintExecutionModel(executionModel)
	extractedData := Execute(executionModel, data)
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

func PrintExecutionModel(executionModel ExecutionModel) {
	for _, t := range executionModel.Columns {
		log.Debug("Column  - %s", t.Node)
	}
	for _, t := range executionModel.FromTable {
		log.Debug("Table - %s", t.Node)
	}
}



func temp(data []byte) {
	_, e := gabs.ParseJSON(data)
	if e != nil {
		panic(e)
	}
}

func TravelNode(n *sqlparser.Node) {
	PrintNodeRecurse(n, 0)
}


const ROOTNAME = "d"
const (
	COLUMN = iota
	TABLE
)


func TravelNodeRecurse(n *sqlparser.Node) (ExecutionModel) {
	execution := ExecutionModel{IsDistinct: false, State: COLUMN, Columns: make([]ColumnExpression, 0), FromTable: make([]FromExpression, 0)}
	for _, node := range n.Sub {
		switch node.Type {
		case sqlparser.COMMENT_LIST:
		case sqlparser.NO_DISTINCT:
		case sqlparser.DISTINCT:
			execution.IsDistinct = true
		case sqlparser.NODE_LIST:
			switch execution.State {
			case COLUMN:
				for _, columnNode := range node.Sub {
					log.Debug("Add Column - %s\n", columnNode.Value)
					execution.Columns = append(execution.Columns, ColumnExpression{*columnNode})
				}
				execution.State = TABLE
			case TABLE:
				for _, fromNode := range node.Sub {
					log.Debug("Add from table - %s\n", fromNode.Value)
					execution.FromTable = append(execution.FromTable, FromExpression{*fromNode})
				}
			}
		}
	}
	return execution
}

func PrintNodeRecurse(n *sqlparser.Node, i int) {
	log.Debug(strings.Repeat("\t", i), strconv.Itoa(n.Type)+" - "+string(n.Value))
	for _, node := range n.Sub {
		PrintNodeRecurse(node, i+1)
	}
}
