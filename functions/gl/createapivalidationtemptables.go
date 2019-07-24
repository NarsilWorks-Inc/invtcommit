package gl

import (
	du "github.com/eaglebush/datautils"
)

// helper function to create validation temp tables
func createAPIValidationTempTables(bq *du.BatchQuery) {
	bq.Set(`IF OBJECT_ID('tempdb..#tciErrorStg') IS NOT NULL
				TRUNCATE TABLE #tciErrorStg
			ELSE
				CREATE TABLE #tciErrorStg
					(GLAcctKey   int      NOT NULL
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

	bq.Set(`IF OBJECT_ID('tempdb..#tciError') IS NOT NULL
				TRUNCATE TABLE #tciError
			ELSE
				CREATE TABLE #tciError
					(EntryNo     int      NULL
					,BatchKey    int      NOT NULL
					,StringNo    int      NOT NULL
					,StringData1 VARCHAR(30) NULL
					,StringData2 VARCHAR(30) NULL
					,StringData3 VARCHAR(30) NULL
					,StringData4 VARCHAR(30) NULL
					,StringData5 VARCHAR(30) NULL
					,ErrorType   smallint NOT NULL
					,Severity    smallint NOT NULL
					,TranType    int      NULL
					,TranKey     int      NULL
					,InvtTranKey int      NULL
				);`)

	bq.Set(`IF OBJECT_ID('tempdb..#tglAcctMask') IS NOT NULL
				TRUNCATE TABLE #tglAcctMask
			ELSE
				CREATE TABLE #tglAcctMask (
				GLAcctNo       varchar(100) NOT NULL,
				MaskedGLAcctNo varchar(114) NULL
				);`)
}
