package jsondb
import (
	"strconv"
	"fmt"
	"github.com/artpar/gabs"
	"code.google.com/p/vitess/go/vt/sqlparser"
)


func evaluateNodeList(node sqlparser.Node, jsonNode gabs.Container, tableMap TableMap) ([][]string, string, bool, int) {
	var nodeCount = len(node.Sub)
	allData := make([][]string, nodeCount)
	title := ""
	isConstant := true
	paramConstants := make([]bool, nodeCount)
	subMaxRow := -1
	for i := 0; i<nodeCount; i++ {
		tempData, tempTitle, tempConstant, subMaxRowCount := EvaluateNode(*node.Sub[i], jsonNode, tableMap)
		if !tempConstant {
			isConstant = false
		}
		allData[i] = tempData
		title += tempTitle
		paramConstants[i] = tempConstant
		if subMaxRowCount > -1 && subMaxRow > subMaxRowCount {
			subMaxRow = subMaxRowCount
		}
	}
	for i := 0; i< nodeCount; i++ {
		if len(allData[i]) < subMaxRow {
			allData[i] = Extrapolate(allData[i], subMaxRow)
		}
	}
	return allData, title, isConstant, subMaxRow
}

func EvaluateNode(node sqlparser.Node, jsonNode gabs.Container, tableMap TableMap) ([]string, string, bool, int) {
	isConstant := true
	maxRowCount := -1
	var title string
	var data []string = []string{"ok"}
	switch node.Type {
	case sqlparser.NODE_LIST:

	case sqlparser.NUMBER:
		return []string{string(node.Value)}, string(node.Value), true, maxRowCount
	case sqlparser.AS :
		data, _, isConstant, maxRowCount = EvaluateNode(*node.Sub[0], jsonNode, tableMap)
		title = string(node.Sub[1].Value)
	case sqlparser.FUNCTION:
		functionName := string(node.Value)
		var arguments [][]string
		arguments, title, isConstant, maxRowCount = evaluateNodeList(*node.Sub[0], jsonNode, tableMap)
		log.Info("Data for function %s - %s", functionName, arguments)
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
				valLeft, titleLeft, isLeftConstant, maxRowLeft := EvaluateNode(*node.Sub[0], jsonNode, tableMap)
				valRight, titleRight, isRightConstant, maxRowRight := EvaluateNode(*node.Sub[1], jsonNode, tableMap)
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
							valLeft = Extrapolate(valLeft, len(valRight))
						} else {
							log.Debug("Right is constant - %d", valRight[0])
							valRight = Extrapolate(valRight, len(valLeft))
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



func extractColumn(column ColumnExpression, jsonNode gabs.Container, tableMap TableMap) ([]string, string, bool, int) {
	return EvaluateNode(column.Node, jsonNode, tableMap)
}


func Extrapolate(data []string, finalLength int) []string {
	if len(data) > 1 {
		log.Error("I dont know how to extrapolate this data - %s", data)
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

