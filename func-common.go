package main

import (
	"fmt"

	du "github.com/eaglebush/datautils"
)

// GetNextSurrogateKey - get the next key of a table
func GetNextSurrogateKey(bq *du.BatchQuery, tableName string) int {
	bq.ScopeName("GetNextSurrogateKey")

	qr := bq.Get(`SELECT NextKey tciSurrogateKey WHERE TableName=?;`, tableName)
	if qr.HasData {
		bq.Set(`UPDATE tciSurrogateKey NextKey = NextKey + 1 WHERE TableName=?;`)
		return int(qr.Get(0).ValueInt64Ord(0))
	}

	bq.Set(`INSERT INTO tciSurrogateKey
			SELECT ?,1`, tableName)
	return 1
}

// CreateInvtCommitDisposableBatch - This SP creates disposable batches for those transactions that are in a temp table called
//                     #tciTransToCommit.  At the end of the routine, the DispoBatchKey field will have a new
//                     key value that represents the disposable batch for the transaction.
//	---------------------------------------------------------------------
//   					This procedure will create a disposable batch for any #tciTransToCommit row that does no
//   					have a DispoBatchKey.
//
//    Assumptions:     This SP assumes that the #tciTransToCommit has been populated appropriately and completely.
//
//                       CREATE TABLE #tciTransToCommit (
//                        	CompanyID         VARCHAR(3) NOT NULL,
//                        	TranType          INTEGER NOT NULL,  -- Shipment tran types including 810, 811, 812.
//                        	PostDate          DATETIME NOT NULL, -- Post date of the transaction.
//                        	InvcDate          DATETIME NULL,     -- Date use to create the invoice or credit memo.
//                        	TranKey           INTEGER NOT NULL,  -- Represents the ShipKey of the transactions to commit.
//                        	PreCommitBatchKey INTEGER NOT NULL,  -- Represents the module's hidden system batch for uncommitted trans.
//                        	DispoBatchKey     INTEGER NULL,      -- Temporary batch used to run through the posting routines.
//                        	CommitStatus      INTEGER DEFAULT 0  -- Status use to determine progress of each transaction.
//						)
//   ---------------------------------------------------------------------
//    Parameters
//       INPUT:  <None>
//      OUTPUT:  @oRetVal  = Return Value
//   ---------------------------------------------------------------------
//      RETURN Codes
//       0 - Unexpected Error (SP Failure)
//       1 - Successful
func CreateInvtCommitDisposableBatch(bq *du.BatchQuery, UserID string) BatchReturnConstant {
	bq.ScopeName("CreateInvtCommitDisposableBatch")

	qr := bq.Get(`SELECT CompanyID,
					? AS BatchTranType,
					PostDate,
					COALESCE(InvcDate, PostDate) As InvcDate
				FROM #tcitranstocommit 
				WHERE ISNULL(DispoBatchKey,0)=0 AND TranType IN (?,?,?)
				UNION
				SELECT CompanyID,
					? AS BatchTranType,
					PostDate,
					COALESCE(InvcDate, PostDate) As InvcDate
				FROM #tcitranstocommit 
				WHERE ISNULL(DispoBatchKey,0)=0 AND TranType IN (?);`,
		BatchTranTypeSOProcShip, SOTranTypeDropShip, SOTranTypeCustShip, SOTranTypeTransShip,
		BatchTranTypeSOProcCustRtrn, SOTranTypeCustRtrn)

	if !qr.HasData {
		return BatchReturnError
	}

	var qr2 du.QueryResult

	for _, r := range qr.Data {

		bt := int(r.ValueInt64("BatchTranType"))
		cid := r.ValueString("CompanyID")
		pd := r.ValueTime("PostDate")
		idt := r.ValueTime("InvcDate")

		res, batchKey, _ := GetNextBatch(bq, cid, 8, bt, UserID, `Disposable Batch for Inventory Commit`, pd, 1, &idt)

		if res != BatchReturnValid {
			return res
		}

		upd := `UPDATE #tcitranstocommit SET DispoBatchKey=? WHERE CompanyID=? AND PostDate=? AND COALESCE(InvcDate, PostDate)=? AND TranType IN `

		switch BatchTranTypeConstant(bt) {
		case BatchTranTypeSOProcShip:
			qr2 = bq.Set(upd+` (?,?,?);`, batchKey, cid, pd, idt, SOTranTypeDropShip, SOTranTypeCustShip, SOTranTypeTransShip)
		case BatchTranTypeSOProcCustRtrn:
			qr2 = bq.Set(upd+` (?);`, batchKey, cid, pd, idt, SOTranTypeCustRtrn)
		}

		if &qr2 == nil {
			return BatchReturnError
		}

		if qr2.HasData {
			if qr2.Get(0).ValueInt64("Affected") == 0 {
				return BatchReturnError
			}
		}
	}

	return BatchReturnValid
}

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

// LogError - function to log errors
func LogError(bq *du.BatchQuery,
	iBatchKey int,
	iEntryNo int,
	iStringNo int,
	iStrData1 string,
	iStrData2 string,
	iStrData3 string,
	iStrData4 string,
	iStrData5 string,
	iErrorType int,
	iSeverity int,
	optiSessionKey int,
	optiExtInfoTranType int,
	optiExtInfoTranKey int,
	optiExtInfoInvtTranKey int,
	optiExtTranLineKey int) int {

	bq.ScopeName("LogError")

	lSessionKey := 0

	if optiSessionKey != 0 {
		lSessionKey = optiSessionKey
	}

	if iBatchKey != 0 {
		lSessionKey = iBatchKey
	}

	// If the BatchKey does not exist in the tciBatchLog table,
	// don't insert record into tciErrorLog (due to RI)

	qr := bq.Get(`SELECT TOP 1 BatchKey FROM tciBatchLog WITH (NOLOCK) WHERE BatchKey=?;`, iBatchKey)
	if !qr.HasData {
		return 0
	}

	// If session key is zero, let's quit
	if lSessionKey == 0 {
		return -1
	}

	if iEntryNo == 0 {
		qr := bq.Get(`SELECT MAX(EntryNo) FROM tciErrorLog WITH (NOLOCK) WHERE SessionID=?;`, lSessionKey)
		if qr.HasData {
			iEntryNo = int(qr.Get(0).ValueInt64Ord(0))
		}
	}
	iEntryNo++

	bq.Set(`INSERT tciErrorLog (
					SessionID, EntryNo, BatchKey,
					StringNo, Severity, ErrorType,
					StringData1, StringData2, StringData3,
					StringData4, StringData5)
			VALUES (?,?,?,?,?,?,?,?,?,?,?);`,
		lSessionKey, iEntryNo, iBatchKey,
		iStringNo, iSeverity, iErrorType,
		iStrData1, iStrData2, iStrData3,
		iStrData4, iStrData5)

	bq.Set(`IF OBJECT_ID('tempdb..#tciErrorLogExt') IS NOT NULL
			BEGIN
				INSERT #tciErrorLogExt (EntryNo, SessionID, TranType, TranKey, TranLineKey,InvtTranKey)
				VALUES (?,?,?,?,?,?)
			END;`, iEntryNo, lSessionKey, optiExtInfoTranType, optiExtInfoTranKey, optiExtTranLineKey, optiExtInfoInvtTranKey)

	return 0
}
