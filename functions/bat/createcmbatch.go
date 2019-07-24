package bat

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// CreateCmBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//    @_iCompanyId   	Current Company ID
//    @_iUserId      	User Id
//    @_iDefBatchCmnt	Default batch comment (for tcmBatch)
//    @_dPostDate     Post date
//    @_iBatchKey     Key of the batch to insert
// Output Parameters:
//    oNextBatch   Batch number
//    oRetVal      ReturnValue:
// 	  0 - Did not make it through the procedure
// 	  1 - Successfully created the record
// 	  5 - Failed to create record
// 	  6 - McBatch record already exists for this batchkey
func CreateCmBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) constants.BatchReturnConstant {

	bq.ScopeName("CreateCmBatch")

	qr := bq.Set(`INSERT INTO tcmBatch (
					BatchKey,
					BankStmtKey,
					BatchCmnt,
					CashAcctKey,
					CurrExchSchdKey,
					DepSlipPrinted,
					Hold,
					InterCompany,
					InterCompBatchKey,
					OrigUserID,
					PostDate,
					Private,
					UpdateCounter
				) VALUES (?,NULL,?,0,NULL,0,0,0,NULL,?,?,0,0);`, iBatchKey, iDefBatchCmnt, iUserID, dPostDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return constants.BatchReturnFailed
		}
	}

	return constants.BatchReturnValid
}
