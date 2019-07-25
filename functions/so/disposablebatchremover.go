package so

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// DisposableBatchRemover -
// 					This will remove any disposable batches that have been
// 					left out in the system.  This can happen if a disposable
// 					batch is created by a process but something happens to
// 					that process before it can remove the disposable batch.
//
// 					Note that if this procedure returns an unsuccessful status
// 					that it is likely that the disposable batch was not simply
// 					orphaned, and the batch will need further analysis before
// 					it can be safely deleted.  At the time of this writing,
// 					there is no reporting mechanism available to this
// 					procedure to notify a user that action is required.  The
// 					indication that further action is required is the
// 					continued existence of the logical lock.
// ------------------------------------------------------------------------------
//  PARAMETERS
// 	@iDisposableBatchKey:
// 					Required.  This is the disposable batch's BatchKey.
//
// 	@NonParam2:		Unused.  Required for cleanup procedure standard
// 					interface.
//
// 	@iCompanyID:	Required, but only used for further validation that the
// 					passed @iDisposableBatchKey really belongs to the
// 					company
// .
// 	@NonParam4:		Unused. Required for cleanup procedure standard
// 					interface.
//
// 	@NonParam5:		Unused. Required for cleanup procedure standard
// 					interface.
//
// 	@oRetVal:		The status of this procedure's processing.  These values
// 					are defined as part of the logical lock processing
// 					system.
// 					0 = Unexpected.  This value should never be returned. It
// 						indicates that there is a logic hole in this procedure,
// 					or that the procedure terminated unexpectedly.
// 					1 = Successful.  Logical lock can be removed.
// 					2 = Unsuccessful.  Logical lock should not be removed.
// ------------------------------------------------------------------------------
func DisposableBatchRemover(
	bq *du.BatchQuery,
	iDisposableBatchKey int,
	NonParam2 int,
	iCompanyID string,
	NonParam4 string,
	NonParam5 string) constants.ResultConstant {

	bq.ScopeName("DisposableBatchRemover")

	ps := constants.BatchPostStatusUndefined
	s := constants.BatchStatusUndefined

	qr := bq.Get(`SELECT PostStatus, Status
				  FROM tciBatchLog WITH (NOLOCK)
				  WHERE	BatchKey=? AND SourceCompanyID=?
					AND BatchNo=0
					AND BatchType=?
					AND BatchTotal=0
					AND NextSeqNo=1
					AND RevrsBatchKey IS NULL
					AND PostStatus IN (?,?,?)						
					AND Status IN (?,?);`,
		iDisposableBatchKey, iCompanyID, constants.BatchTranTypeSOProcShip,
		constants.BatchPostStatusOpen, constants.BatchPostStatusCompleted, constants.BatchPostStatusDeleted,
		constants.BatchStatusBalanced, constants.BatchStatusPosted)
	if !qr.HasData {
		return constants.ResultFail
	}

	ps = constants.BatchPostStatusConstant(qr.First().ValueInt64Ord(0))
	s = constants.BatchStatusConstant(qr.First().ValueInt64Ord(1))

	if !(ps == constants.BatchPostStatusOpen && s == constants.BatchStatusBalanced) {
		return constants.ResultFail
	}

	bks := make([]interface{}, 7)
	for i := range bks {
		bks[i] = iDisposableBatchKey
	}

	qr = bq.Get(`SELECT 1 FROM tciBatchLog BL WITH (NOLOCK)
				WHERE BL.BatchKey=?
					AND BL.BatchKey NOT IN (SELECT BatchKey FROM tsoPendShipment WITH (NOLOCK) WHERE BatchKey=?)
					AND BL.BatchKey NOT IN (SELECT BatchKey FROM tarPendInvoice WITH (NOLOCK) WHERE BatchKey=?)
					AND BL.BatchKey NOT IN (SELECT BatchKey FROM tsoShipment WITH (NOLOCK) WHERE BatchKey=?)
					AND BL.BatchKey NOT IN (SELECT BatchKey	FROM timInvtTran WITH (NOLOCK) WHERE BatchKey=?)
					AND BL.BatchKey NOT IN (SELECT BatchKey	FROM tglPosting WITH (NOLOCK) WHERE BatchKey=?)
					AND BL.BatchKey NOT IN (SELECT BatchKey FROM tglTransaction WITH (NOLOCK) WHERE BatchKey=?);`, bks...)
	if !qr.HasData {
		return constants.ResultFail
	}

	// ------------------------------------------------------------
	// -- At this point, the batch appears to be a disposable batch
	// -- that can safely be deleted.
	// ------------------------------------------------------------
	qr = bq.Set(`UPDATE tciBatchLog SET	PostStatus=? WHERE BatchKey=?;`, constants.BatchPostStatusDeleted, iDisposableBatchKey)
	if !qr.HasAffectedRows {
		return constants.ResultFail
	}

	// Delete an associated tsoBatch record if it exists.
	bq.Set(`DELETE tsoBatch WHERE BatchKey IN (SELECT BatchKey 
													FROM tciBatchLog WITH (NOLOCK) 
													WHERE BatchKey=?
														AND PostStatus=?);`, iDisposableBatchKey, constants.BatchPostStatusDeleted)
	if !bq.OK() {
		return constants.ResultError
	}

	return constants.ResultSuccess

}
