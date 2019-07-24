package sm

import (
	"fmt"

	du "github.com/eaglebush/datautils"
)

// LogErrors - function to log errors
func LogErrors(bq *du.BatchQuery, batchKey int, sessionKey int) int {
	bq.ScopeName("LogErrors")

	// If there is no data, we must exit
	qr := bq.Get(`SELECT COUNT(*) FROM (SELECT BatchKey
										FROM #tciError
										GROUP BY BatchKey, StringNo, ErrorType, 
											Severity, StringData1, StringData2, 
											StringData3, StringData4, StringData5) AS X `)
	recs := int(qr.Get(0).ValueInt64Ord(0))
	if recs == 0 {
		return 0
	}

	// Prepare an error temporary table
	bq.Set(`IF OBJECT_ID('tempdb..#tciErrorTmp') IS NOT NULL
				TRUNCATE TABLE #tciErrorTmp
			ELSE
				CREATE TABLE #tciErrorTmp
				(
					ID int IDENTITY (1,1) NOT NULL
					,EntryNo     int      NOT NULL
					,BatchKey    int      NOT NULL
					,StringNo    int      NOT NULL
					,StringData1 VARCHAR(30) NULL
					,StringData2 VARCHAR(30) NULL
					,StringData3 VARCHAR(30) NULL
					,StringData4 VARCHAR(30) NULL
					,StringData5 VARCHAR(30) NULL
					,ErrorType   smallint NOT NULL
					,Severity    smallint NOT NULL
				);`)

	lsessionKey := 0
	if sessionKey != 0 {
		lsessionKey = sessionKey
	}
	if batchKey != 0 {
		lsessionKey = batchKey
	}

	// Get the next entry no
	qr = bq.Get(`SELECT ISNULL(MAX(EntryNo),0)	FROM tciErrorLog WITH (NOLOCK) WHERE SessionID = ?;`, lsessionKey)
	en := qr.Get(0).ValueInt64Ord(0)

	// Insert records in the tciError to the temporary table
	dt := bq.Set(`INSERT #tciErrorTmp (
					EntryNo, BatchKey, StringNo, ErrorType, Severity, 
					StringData1, StringData2, StringData3, StringData4, StringData5)
				SELECT DISTINCT
					0, BatchKey, StringNo, ErrorType, Severity, 
					StringData1, StringData2, StringData3, StringData4, StringData5
				FROM #tciError;`)
	dt.Get(0).ValueInt64("Affected")

	// Update the entry numbers
	for i := 0; i < recs; i++ {
		bq.Set(`UPDATE #tciErrorTmp SET EntryNo=ID + ? WHERE ID=?`, en, i)
	}

	// Insert errors into the error log
	bq.Set(`INSERT tciErrorLog
				(SessionID
				,StringNo
				,EntryNo
				,StringData1
				,StringData2
				,StringData3
				,StringData4
				,StringData5
				,ErrorType
				,Severity
				,BatchKey)
			SELECT COALESCE(?, BatchKey)
				,StringNo
				,EntryNo
				,StringData1
				,StringData2
				,StringData3
				,StringData4
				,StringData5
				,ErrorType
				,Severity
				,BatchKey
			FROM #tciErrorTmp
			WHERE COALESCE(?, BatchKey) IS NOT NULL;`, lsessionKey, lsessionKey)

	if qr = bq.Get(`SELECT ISNULL(OBJECT_ID('tempdb..#tciErrorLogExt'),0);`); qr.Get(0).ValueInt64Ord(0) != 0 {
		// Check if #tciError has been expanded to include all the extended info columns.
		qr = bq.Get(`SELECT c.name 
					 FROM tempdb..sysobjects o 
						JOIN tempdb..syscolumns c ON o.ID = c.ID
					 WHERE o.id = object_id('tempdb..#tciError')`)

		extc := 0
		for _, d := range qr.Data {
			switch d.ValueStringOrd(0) {
			case "TranType", "TranKey", "TranLineKey", "InvtTranKey":
				extc++
			}
		}

		// These four columns are expected
		if extc == 4 {
			bq.Set(`INSERT #tciErrorLogExt (
						EntryNo, SessionID, TranType, TranKey, TranLineKey, InvtTranKey)
					SELECT etmp.EntryNo, ?, e.TranType, e.TranKey, e.TranLineKey, e.InvtTranKey
					FROM #tciError e 
						JOIN #tciErrorTmp etmp ON e.BatchKey = etmp.BatchKey AND e.StringNo = etmp.StringNo AND e.Severity = etmp.Severity
						AND COALESCE(e.StringData1, '') = COALESCE(etmp.StringData1, '')
						AND COALESCE(e.StringData2, '') = COALESCE(etmp.StringData2, '')
						AND COALESCE(e.StringData3, '') = COALESCE(etmp.StringData3, '')
						AND COALESCE(e.StringData4, '') = COALESCE(etmp.StringData4, '')
						AND COALESCE(e.StringData5, '') = COALESCE(etmp.StringData5, '');`, fmt.Sprintf("%d", lsessionKey))
		}
	}

	bq.Set(`TRUNCATE TABLE #tciError;`)

	qr = bq.Get(`SELECT ISNULL(MAX(Severity),0)
				FROM tciErrorLog WITH (NOLOCK)
				WHERE SessionID=?;`, lsessionKey)
	if qr.HasData {
		return int(qr.Get(0).ValueInt64Ord(0))
	}

	return -1
}
