package bat

import (
	"fmt"
	"gosqljobs/invtcommit/functions/constants"
	"gosqljobs/invtcommit/functions/sm"

	du "github.com/eaglebush/datautils"
)

// CreateBatchLog - Creates a batch log record
// ---------------------------------------------------------------------
// Input Parameters:
// @_iCompanyId        current Company ID
// @_iModuleNo         Module Number (5=AR,10=MC, etc..)
// @_iBatchType        Type of batch (Numeric Code)
// @_iBatchTypeID      Type of batch (ID)
// @_iBatchNo          batch Number
// @_iUserID           Acuity User Id
// @_iRevBatchKey      Key of the reverse batch
//
// Output Parameters:
// @_oBatchKey         Key of the batch added to batch log
// @_oRetVal           ReturnValue:
// 0 - Did not make it through the procedure
// 1 - batch log record created
// 4 - Unable to create tciBatchLog record
// Modified:
// MM/DD/YY 		BY		COMMENT
// 05/10/2007		JY		Added code to insert value into CreateDate, CreateType,
// 						UpdateCounter to avoid the trigger to be fired
// 						when insert data into tciBatchLog.
// ---------------------------------------------------------------------
func CreateBatchLog(
	bq *du.BatchQuery,
	iCompanyID string,
	iModuleNo constants.ModuleConstant,
	iBatchType int,
	iBatchTypeID string,
	iBatchNo int,
	iUserID string,
	iRevBatchKey int) (Result constants.BatchReturnConstant, BatchKey int) {

	bq.ScopeName("CreateBatchLog")

	qr := bq.Get(`SELECT ModuleID FROM tsmModuleDef WITH (NOLOCK) WHERE ModuleNo=?`, iModuleNo)
	if !qr.HasData {
		return constants.BatchReturnError, 0
	}

	batchID := qr.Get(0).ValueStringOrd(0) + fmt.Sprintf("%0d", iBatchType) + `-` + fmt.Sprintf("%0d", iBatchNo)
	batchKey := sm.GetNextSurrogateKey(bq, `tciBatchLog`)

	var rev interface{}
	if iRevBatchKey != 0 {
		rev = iRevBatchKey
	}

	qr = bq.Set(`INSERT INTO tciBatchLog (
					BatchKey,
					BatchID,
					BatchNo,
					Status,
					PostStatus,
					BatchType,
					OrigUserID,
					SourceCompanyID,
					PostCompanyID,
					RevrsBatchKey,
					CreateDate,
					CreateType,
					UpdateCounter
				) VALUES (?,?,?,?,?,?,?,?,?,?,GETDATE(),?,0);`,
		batchKey, batchID, iBatchNo, constants.BatchStatusBalanced, constants.BatchPostStatusOpen,
		iBatchType, iUserID, iCompanyID, iCompanyID, rev, constants.BatchCreateTypeStandard)

	if qr.Get(0).ValueInt64("Affected") == 0 {
		return constants.BatchReturnNoLog, 0
	}

	return constants.BatchReturnValid, batchKey
}
