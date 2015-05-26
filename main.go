package main

import (
//	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/vt/sqlparser"
//	"github.com/pquerna/ffjson/ffjson"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"github.com/artpar/gabs"
	"github.com/op/go-logging"
	"fmt"
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

	travelNode(sql)
	executionModel := travelNodeRecurse(sql)
	printExecutionModel(executionModel)
	extractedData := execute(executionModel, data)
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

func printExecutionModel(executionModel ExecutionModel) {
	for _, t := range executionModel.Columns {
		log.Debug("Column  - %s", t.Node)
	}
	for _, t := range executionModel.FromTable {
		log.Debug("Table - %s", t.Node)
	}
}
func execute(executionModel ExecutionModel, data []byte) []map[string]interface{} {
	var aliasMap map[string]string = make(map[string]string)
	from := executionModel.FromTable
	path := from[0].Node.Sub[0]
	alias := from[0].Node.Sub[1]

	for _, aliasInstance := range alias.Sub {
		aliasMap[string(aliasInstance.Value)] = string(path.Value)
	}
	json, err := gabs.ParseJSON(data)
	if err != nil {
		panic(err)
	}
	numColumns := len(executionModel.Columns)
	var columnData [][]string = make([][]string, numColumns)
	var columnName []string = make([]string, numColumns)
	var isConstant []bool = make([]bool, numColumns)
	var maxRowCount []int = make([]int, numColumns)
	var rowCountConstraints []int = make([]int, 0)
	for i, column := range executionModel.Columns {
		printNodeRecurse(&column.Node, 0)
		columnData[i], columnName[i], isConstant[i], maxRowCount[i] = extractColumn(column, *json, aliasMap)
		if (maxRowCount[i] > -1) {
			rowCountConstraints = append(rowCountConstraints, maxRowCount[i])
		}
		log.Debug("Column Data [%s](%s) - %s", columnName[i], isConstant[i], columnData[i])
		log.Debug("\n\n")
	}

	finalMaxRowCount := max(rowCountConstraints...)
	log.Debug("Total number of rows - ", finalMaxRowCount)

	for i := 0; i <numColumns; i++ {
		if (len(columnData[i])) < finalMaxRowCount {
			columnData[i] = extrapolate(columnData[i], finalMaxRowCount)
		}
	}
	outArray := make([]map[string]interface{}, finalMaxRowCount)
	for i := 0; i<finalMaxRowCount; i++ {
		outArray[i] = make(map[string]interface{})
		for j := 0; j<numColumns; j++ {
			outArray[i][columnName[j]] = columnData[j][i]
		}
	}
	return outArray
}

type TableMap  map[string]string
func (t TableMap) GetTableByAlias(s string) string {
	val, ok := t[s]
	if !ok {
		log.Error("Table not found for alias - %s", s)
		return s
	}
	return val
}

func extractColumn(column ColumnExpression, jsonNode gabs.Container, tableMap TableMap) ([]string, string, bool, int) {
	return evaluateNode(column.Node, jsonNode, tableMap)
}

func evaluateNode(node sqlparser.Node, jsonNode gabs.Container, tableMap TableMap) ([]string, string, bool, int) {
	isConstant := true
	maxRowCount := -1
	var title string
	var data []string = []string{"ok"}
	switch node.Type {
	case sqlparser.NODE_LIST:
	case sqlparser.NUMBER:
		return []string{string(node.Value)}, string(node.Value), true, maxRowCount
	case sqlparser.AS :
		data, _, isConstant, maxRowCount = evaluateNode(*node.Sub[0], jsonNode, tableMap)
		title = string(node.Sub[1].Value)
	case sqlparser.FUNCTION:
		//		functionName := string(node.Value)
		data, title, isConstant, maxRowCount = evaluateNode(*node.Sub[0], jsonNode, tableMap)
	// todo: apply functionName

	default:
		if (len(node.Value) > 0) {
			switch node.Value[0] {
			case '.':
				isConstant = false
				tableAlias := node.Sub[0]
				actualTableName := tableMap.GetTableByAlias(string(tableAlias.Value))
				tableColumn := node.Sub[1]
				fullJsonPath := actualTableName + "." + string(tableColumn.Value)
				container := jsonNode.Path(fullJsonPath)
				log.Debug("Value of the column '%s' from table '%s' - \n%s", tableColumn, actualTableName, container)
				dataArray := container.Data().([]interface{})
				data = make([]string, 0)
				for _, d := range dataArray {
					strVal, ok := d.(string)
					if ok {
						data = append(data, strVal)
					}else {
						data = append(data, fmt.Sprintf("%#v", d))
					}
				}
				maxRowCount = len(data)
				title = string(tableColumn.Value)
			case '+':
				valLeft, titleLeft, isLeftConstant, maxRowLeft := evaluateNode(*node.Sub[0], jsonNode, tableMap)
				valRight, titleRight, isRightConstant, maxRowRight := evaluateNode(*node.Sub[1], jsonNode, tableMap)
				maxRowCount = max(maxRowLeft, maxRowRight)
				if !isLeftConstant || !isRightConstant {
					isConstant = false
					if !isLeftConstant && !isRightConstant {
						if len(valLeft) != len(valRight) {
							log.Error("Data from the left and right expression are of unequal length %d==%d", len(valLeft), len(valRight))
						}
					} else {
						if !isRightConstant {
							log.Debug("Left is constant - %d", valLeft[0])
							valLeft = extrapolate(valLeft, len(valRight))
						} else {
							log.Debug("Right is constant - %d", valRight[0])
							valRight = extrapolate(valRight, len(valLeft))
						}
					}
				}


				leftCount := len(valLeft)
				rightCount := len(valRight)
				maxCount := max(leftCount, rightCount)
				data = make([]string, maxCount)
				for i := 0; i< maxCount; i++ {
					valLeftFloat, ok := strconv.ParseFloat(valLeft[i], 64)
					if ok != nil {
						log.Error("Failed to parse valLeft %s as float", valLeft[i])
						valLeftFloat = 0
					}

					valRightFloat, ok := strconv.ParseFloat(valRight[i], 64)
					if ok != nil {
						log.Error("Failed to parse valRight %s as float", valRight[i])
						valRightFloat = 0
					}
					result := valLeftFloat + valRightFloat
					data[i] = string(strconv.FormatFloat(result, 'f', -1, 64))
				}


				title = titleLeft + titleRight
			}
		}
	}
	return data, title, isConstant, maxRowCount
}

func extrapolate(data []string, finalLength int) []string {
	if len(data) > 1 {
		log.Error("I dont know how to extrapolate this data")
		return make([]string, finalLength)
	}
	finalData := make([]string, finalLength)
	if len(data) == 0 {
		return finalData
	}
	for i := 0; i< finalLength; i++ {
		finalData[i] = data[0]
	}
	return finalData

}

func max(n... int) int {
	if (len(n)) < 1 {
		return 0
	}
	max := n[0]
	for i := 1; i<len(n); i++ {
		if (n[i] > max) {
			max = n[i]
		}
	}
	return max
}

func temp(data []byte) {
	_, e := gabs.ParseJSON(data)
	if e != nil {
		panic(e)
	}
}

func travelNode(n *sqlparser.Node) {
	printNodeRecurse(n, 0)
}


const ROOTNAME = "d"
const (
	COLUMN = iota
	TABLE
)

type ColumnExpression struct {
	Node sqlparser.Node
}

type FromExpression struct {
	Node sqlparser.Node
}

type ExecutionModel struct {
	IsDistinct bool
	State      int
	Columns    []ColumnExpression
	FromTable  []FromExpression
}

func travelNodeRecurse(n *sqlparser.Node) (ExecutionModel) {
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

func printNodeRecurse(n *sqlparser.Node, i int) {
	log.Debug(strings.Repeat("\t", i), strconv.Itoa(n.Type)+" - "+string(n.Value))
	for _, node := range n.Sub {
		printNodeRecurse(node, i+1)
	}
}
