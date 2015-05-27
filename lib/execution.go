package jsondb

import (
	"github.com/artpar/gabs"
)

func Execute(executionModel ExecutionModel, data []byte) map[string][]map[string]interface{} {
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
		PrintNodeRecurse(&column.Node, 0)
		columnData[i], columnName[i], isConstant[i], maxRowCount[i] = extractColumn(column, *json, aliasMap)
		if maxRowCount[i] > -1 {
			rowCountConstraints = append(rowCountConstraints, maxRowCount[i])
		}
		log.Debug("Column Data [%s](%s) - %s", columnName[i], isConstant[i], columnData[i])
		log.Debug("\n\n")
	}

	finalMaxRowCount := min(rowCountConstraints...)
	log.Debug("Total number of rows - ", finalMaxRowCount)

	for i := 0; i < numColumns; i++ {
		if (len(columnData[i])) < finalMaxRowCount {
			columnData[i] = Extrapolate(columnData[i], finalMaxRowCount)
		}
	}
	outArray := make([]map[string]interface{}, finalMaxRowCount)
	for i := 0; i < finalMaxRowCount; i++ {
		outArray[i] = make(map[string]interface{})
		for j := 0; j < numColumns; j++ {
			outArray[i][columnName[j]] = columnData[j][i]
		}
	}
	return map[string][]map[string]interface{}{"data": outArray}
}
