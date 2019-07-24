package bat

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// CreatePoBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//      @_iCompanyId      Current Company ID
//      @_iUserId         Acuity User Id
//      @_iDefBatchCmnt   Default batch comment (for tpoBatch)
//    	@_dPostDate     Post date
//      @_iBatchKey     Key of the batch to insert
//   Output Parameters:
//      oNextBatch   Batch number
//      oRetVal      ReturnValue:
//         0 - Did not make it through the procedure
//         1 - Successfully created the tmcBatch record
//         5 - Failed to create mcBatch record
//         6 - McBatch record already exists for this batchkey
func CreatePoBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) constants.BatchReturnConstant {

	bq.ScopeName("CreatePoBatch")

	qr := bq.Set(`INSERT INTO tpoBatch ( 
					batchkey,
					batchcmnt,
					hold,
					intercompany,
					intercompbatchkey,
					origuserid,
					postdate,
					private,
					updatecounter,
					createdate)
				VALUES (?,?,0,0,NULL,?,?,0,0,GETDATE());`, iBatchKey, iDefBatchCmnt, iUserID, dPostDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return constants.BatchReturnFailed
		}
	}

	return constants.BatchReturnValid
}
