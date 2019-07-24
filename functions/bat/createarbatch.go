package bat

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// CreateArBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//      @_iCompanyId      Current Company ID
//      @_iUserId         Acuity User Id
//      @_iDefBatchCmnt   Default batch comment (for tarBatch)
//    	@_dPostDate     Post date
//      @_iBatchKey     Key of the batch to insert
//   Output Parameters:
//      oNextBatch   Batch number
//      oRetVal      ReturnValue:
//         0 - Did not make it through the procedure
//         1 - Successfully created the record
//         5 - Failed to create record
//         6 - McBatch record already exists for this batchkey
func CreateArBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) constants.BatchReturnConstant {

	bq.ScopeName("CreateArBatch")

	cashAcctKey := 0
	qr := bq.Get(`SELECT DfltCashAcctKey FROM tarOptions WHERE CompanyID=?`, iCompanyID)
	if qr.HasData {
		cashAcctKey = int(qr.Get(0).ValueInt64Ord(0))
	}

	qr = bq.Set(`INSERT INTO tarBatch (
					BatchKey, 
					BankDepAmt,  
					Hold, 
					InterCompany, 
					BatchOvrdSegValue,
					CashAcctKey, 
					BatchCmnt, 
					OrigUserID, 
					PostDate, 
					Private, 
					TranCtrl,
					SourceModule) VALUES (?,0,0,0,NULL,?,?,?,?,0,0,5);`, iBatchKey, cashAcctKey, iDefBatchCmnt, iUserID, dPostDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return constants.BatchReturnFailed
		}
	}

	return constants.BatchReturnValid
}
