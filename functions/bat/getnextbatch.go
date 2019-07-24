package bat

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// GetNextBatch - Create the next batch Number
// ---------------------------------------------------------------------
// Input Parameters:
// @_iCompanyId   	current Company ID
// @_iBatchType   	Type of batch (Numeric Code)
// @_iUserId      	Acuity User Id
// @_iDefBatchCmnt	Default batch Comment (for tarBatch)
// @_iPostDate     Post Date used to create the batch.
// @_optHiddenBatch Optional parameter. Tells if batch being created is hidden,
// which results in a BatchNo = 0.
// Output Parameters:
// oNextBatch   batch Number
// oRetVal      ReturnValue:
// 0 - Did not make it through the procedure
// 1 - Valid Number found
// 2 - No numbers could be found to use  (0000001-9999999)
// 3 - No tciBatchTypCompany Record for the batch/Co specified
// 4 - Unable to create tciBatchLog record
// 5 - Unable to create tmcBatch record
// 6 - tmcBatch record already exists for that key
func GetNextBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iModuleNo constants.ModuleConstant,
	iBatchType int,
	iUserID string,
	iDefBatchCmnt string,
	iPostDate time.Time,
	optHiddenBatch int,
	optInvcDate *time.Time) (Result constants.BatchReturnConstant, BatchKey int, NextBatchNo int) {

	bq.ScopeName("GetNextBatch")

	nextBatchNo := 0
	batchKey := 0
	var res constants.BatchReturnConstant

	if iModuleNo == 10 {
		qr := bq.Get(`SELECT batchno, batchkey
					  FROM tcibatchlog
					  WHERE  batchtype=?
							AND sourcecompanyid=?
							AND status <> 6
							AND poststatus <> 999;`, iBatchType, iCompanyID)
		if qr.HasData {
			nextBatchNo = int(qr.Get(0).ValueInt64Ord(0))
			batchKey = int(qr.Get(0).ValueInt64Ord(1))
			return constants.BatchReturnInterrupted, batchKey, nextBatchNo
		}
	}

	// GetNextBatch will get a good batch Number,
	// create the tciBatchLog Record, increment the nextno
	// and write it back to tciBatchTypCompany.
	// (or it will return something other than 1).
	res, batchKey, nextBatchNo = GetNextBatchNo(bq, iCompanyID, iBatchType, iUserID, iModuleNo, optHiddenBatch)
	if res != constants.BatchReturnValid {
		return res, 0, 0
	}

	switch iModuleNo {
	case constants.ModuleAP: // AP
		res = CreateApBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case constants.ModuleAR: // AR
		res = CreateArBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case constants.ModuleIM: // IM
		res = CreateImBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case constants.ModuleSO: // SO
		res = CreateSoBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey, optInvcDate)
	case constants.ModuleCM: // CM
		res = CreateCmBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case constants.ModuleMC: // MC
		res = CreateMcBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iBatchType, batchKey)
	case constants.ModulePO: // PO
		res = CreatePoBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case constants.ModuleMF: // MF
		res = CreateMfBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	}

	ures := UpdateBatchLogCmnt(bq, batchKey, iModuleNo)
	if ures != constants.BatchReturnValid {
		return ures, batchKey, nextBatchNo
	}

	return res, batchKey, nextBatchNo
}
