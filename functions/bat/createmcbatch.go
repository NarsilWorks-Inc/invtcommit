package bat

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// CreateMcBatch - Creates a batch log record
// ---------------------------------------------------------------------
// Input Parameters:
// @_iCompanyId   	current Company ID
// @_iUserID      	Acuity User Id
// @_iDefBatchCmnt	Default batch Comment (for tarBatch)
// @_iBatchKey     Key of the batch to insert
// Output Parameters:
// oNextBatch   batch Number
// oRetVal      ReturnValue:
//    0 - Did not make it through the procedure
//    1 - Successfully created the record
//    5 - Failed to create record
//    6 - McBatch record already exists for this BatchKey
func CreateMcBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	iBatchType int,
	iBatchKey int) constants.BatchReturnConstant {

	bq.ScopeName("CreateMcBatch")

	qr := bq.Get(`SELECT BatchKey FROM tmcBatch WHERE BatchKey=?;`, iBatchKey)
	if qr.HasData {
		return constants.BatchReturnExists
	}

	lRevaluationDate := time.Now().Format("2006-01-02")
	lReversalDate := lRevaluationDate
	lExchRateSchdKey := 0

	qr = bq.Get(`SELECT a.EndDate, 
						CASE 
							WHEN ? = 1001 THEN b.GLRevalExchSchdKey  /* GL Revaluation Gain */
							WHEN ? = 1002 THEN b.APRevalExchSchdKey  /* AP Revaluation Gain */
							WHEN ? = 1003 THEN b.ARRevalExchSchdKey  /* AR Revaluation Gain */ 
						END
				FROM tglFiscalPeriod a WITH (NOLOCK)
					INNER JOIN tmcOptions b WITH (NOLOCK) 
						ON a.FiscYear  = b.CurntFiscYear
							AND a.FiscPer   = b.CurntFiscPer
							AND a.CompanyID = b.CompanyID
				WHERE b.CompanyID=?;`, iBatchType, iBatchType, iBatchType, iCompanyID)
	if qr.HasData {
		lExchRateSchdKey = int(qr.Get(0).ValueInt64Ord(1))
		ed := qr.Get(0).ValueTimeOrd(0)
		lRevaluationDate = ed.Format("2006-01-02")
		lReversalDate = ed.Add(time.Hour * 24).Format("2006-01-02")
	}

	qr = bq.Set(`INSERT INTO tmcBatch
					(BatchKey,
					BatchCmnt, 
					Hold, 
					OrigUserID, 
					PostDate, 
					Private, 
					Processed, 
					ReversingDate,
					CurrExchSchdKey)
				VALUES (?,?,0,?,?,0,0,?,?);`, iBatchKey, iDefBatchCmnt, iUserID, lRevaluationDate, lReversalDate, lExchRateSchdKey)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return constants.BatchReturnFailed
		}
	}

	return constants.BatchReturnValid
}
