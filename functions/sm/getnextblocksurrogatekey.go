package sm

import du "github.com/eaglebush/datautils"

// GetNextBlockSurrogateKey - get the next key of a table
func GetNextBlockSurrogateKey(
	bq *du.BatchQuery,
	tableName string,
	frequency int) (StartKey int, EndKey int) {

	bq.ScopeName("GetNextBlockSurrogateKey")

	if frequency == 0 {
		return 0, 0
	}

	qr := bq.Get(`SELECT NextKey FROM tciSurrogateKey WHERE TableName=?;`, tableName)
	if qr.HasData {
		key := int(qr.Get(0).ValueInt64Ord(0)) // value of the key is what was defined before (therefore NextKey)

		bq.Set(`UPDATE tciSurrogateKey SET NextKey = NextKey + ? WHERE TableName=?;`, frequency+1, tableName)
		return key, key + frequency
	}

	bq.Set(`INSERT INTO tciSurrogateKey
			SELECT ?,?`, tableName, frequency+1)

	return 1, frequency

	// 10
	// outputs the next key as 11 to prepare for next
}
