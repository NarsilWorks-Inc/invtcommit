package bat

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// GetNextBatchNo - Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//     @_iCompanyID        Current Company ID
//     @_iBatchType        Type of Batch (Numeric Code)
//     @_iUserId           User Id
//     @_iModuleNo         Module No
//     @_optHiddenBatch    Optional parameter. Tells if batch being created is hidden,
//                         which results in a BatchNo = 0.
// Output Parameters:
//     @_oBatchKey         Batch Key
//     @_oNextBatch        Batch number
//     @_oRetVal           ReturnValue:
//                             0 - Did not make it through the procedure
//                             1 - Valid number found
//                             2 - No numbers could be found to use  (00001-99999)
//                             3 - No tciBatchTypCompany Record for the Batch/Co specified
//                             4 - Unable to create tciBatchLog record
// ---------------------------------------------------------------------
func GetNextBatchNo(
	bq *du.BatchQuery,
	iCompanyID string,
	iBatchType int,
	iUserID string,
	iModuleNo constants.ModuleConstant,
	optHiddenBatch int) (Result constants.BatchReturnConstant, BatchKey int, NextBatchNo int) {

	bq.ScopeName("GetNextBatchNo")

	var qr du.QueryResult

	lNextBatchNo := 0
	lBatchTypeID := ""
	lBatchKey := 0

	if optHiddenBatch == 0 {
		/* Override batch number for hidden batches.  Set them to zero. */
		qr = bq.Get(`SELECT nextbatchno, batchtypeid
					  FROM tciBatchTypCompany WITH (NOLOCK)
					  WHERE companyid=? AND batchtype=?;`, iCompanyID, iBatchType)
		if !qr.HasData {
			return constants.BatchReturnNoRecord, 0, 0
		}
		lNextBatchNo = int(qr.Get(0).ValueInt64Ord(0))
		lBatchTypeID = qr.Get(0).ValueStringOrd(1)

		/* Lock the table for updates. Notice the UPDLOCK table hint */
		bq.Get(`SELECT nextbatchno
				FROM tciBatchTypCompany WITH (UPDLOCK)
				WHERE companyid=? AND batchtype=?;`, iCompanyID, iBatchType)
	}

	if lNextBatchNo > 999999 {
		lNextBatchNo = 1
	}

	/* Initialize loop starting number */
	lStartingNumber := lNextBatchNo
	lNoRecFound := false

	for {
		valid := constants.BatchReturnError
		// Check if batch number exists
		qr = bq.Get(`SELECT TOP 1 batchno FROM tcibatchlog WITH (nolock) WHERE sourcecompanyid=? AND batchno=? AND batchtype=?;`, iCompanyID, lNextBatchNo, iBatchType)
		if !qr.HasData || optHiddenBatch != 0 {
			// If not found, create
			valid, lBatchKey = CreateBatchLog(bq, iCompanyID, iModuleNo, iBatchType, lBatchTypeID, lNextBatchNo, iUserID, 0)
			if valid != constants.BatchReturnValid {
				return constants.BatchReturnError, 0, 0
			}
			lNoRecFound = true
		}

		if optHiddenBatch == 0 {
			if lNextBatchNo > 999999 {
				lNextBatchNo = 1
			} else {
				lNextBatchNo++
			}

			if lNoRecFound {
				qr = bq.Set(`UPDATE tcibatchtypcompany SET nextbatchno=?
							 WHERE  companyid=?	AND batchtype=?;`, lNextBatchNo, iCompanyID, iBatchType)
				if qr.HasData {
					return constants.BatchReturnValid, lBatchKey, lNextBatchNo
				}
			}
		}

		if optHiddenBatch != 0 {
			return constants.BatchReturnValid, lBatchKey, lNextBatchNo
		}

		if lStartingNumber == lStartingNumber {
			return constants.BatchReturnNoNum, lBatchKey, lNextBatchNo
		}
	}
}
