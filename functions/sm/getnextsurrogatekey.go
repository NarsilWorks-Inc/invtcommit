package sm

import (
	du "github.com/eaglebush/datautils"
)

// GetNextSurrogateKey - get the next key of a table
func GetNextSurrogateKey(bq *du.BatchQuery, tableName string) int {
	bq.ScopeName("GetNextSurrogateKey")

	qr := bq.Get(`SELECT NextKey FROM tciSurrogateKey WHERE TableName=?;`, tableName)
	if qr.HasData {
		bq.Set(`UPDATE tciSurrogateKey SET NextKey = NextKey + 1 WHERE TableName=?;`, tableName)
		return int(qr.Get(0).ValueInt64Ord(0))
	}

	bq.Set(`INSERT INTO tciSurrogateKey
			SELECT ?,1`, tableName)
	return 1
}
