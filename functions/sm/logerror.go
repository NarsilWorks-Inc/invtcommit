package sm

import du "github.com/eaglebush/datautils"

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
