package main

import (
	du "github.com/eaglebush/datautils"
)

// SOTranTypeConstant - sales order tran types
type SOTranTypeConstant int16

// SOShipLogTranStatus - shipment log tran status
type SOShipLogTranStatus int8

// Sales Order Tran Type constants
const (
	SOTranTypeSalesOrder        SOTranTypeConstant = 801 // Sales Order (Standard)
	SOTranTypeSalesOrderBlanket SOTranTypeConstant = 802 // Sales Order (Blanket)
	SOTranTypeChangeOrder       SOTranTypeConstant = 804 // SO Change Order
	SOTranTypeCustShip          SOTranTypeConstant = 810 // Customer Shipment
	SOTranTypeCustRtrn          SOTranTypeConstant = 811 // Customer Return
	SOTranTypeTransShip         SOTranTypeConstant = 812 // Transfer Shipment
	SOTranTypeTransIn           SOTranTypeConstant = 813 // Transfer In (Transit Warehouse)
	SOTranTypeDropShip          SOTranTypeConstant = 814 // Drop Shipment
	SOTranTypePackList          SOTranTypeConstant = 820 // Packing List
	SOTranTypeBillLdng          SOTranTypeConstant = 830 // Bill of Lading
	SOTranTypeRMA               SOTranTypeConstant = 835 // RMA
	SOTranTypeQuote             SOTranTypeConstant = 840 // SO Quote
)

// shipment log tran status
const (
	SOShipLogIncomplete SOShipLogTranStatus = 1
	SOShipLogPending    SOShipLogTranStatus = 2
	SOShipLogPosted     SOShipLogTranStatus = 3
	SOShipLogPurged     SOShipLogTranStatus = 4
	SOShipLogVoid       SOShipLogTranStatus = 5
	SOShipLogCommitted  SOShipLogTranStatus = 6
)

// SODisposableBatchRemover -
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
func SODisposableBatchRemover(
	bq *du.BatchQuery,
	iDisposableBatchKey int,
	NonParam2 int,
	iCompanyID string,
	NonParam4 string,
	NonParam5 string) ResultConstant {

	bq.ScopeName("DisposableBatchRemover")

	ps := BatchPostStatusUndefined
	s := BatchStatusUndefined

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
		iDisposableBatchKey, iCompanyID, BatchTranTypeSOProcShip,
		BatchPostStatusOpen, BatchPostStatusCompleted, BatchPostStatusDeleted,
		BatchStatusBalanced, BatchStatusPosted)
	if !qr.HasData {
		return ResultFail
	}

	ps = BatchPostStatusConstant(qr.First().ValueInt64Ord(0))
	s = BatchStatusConstant(qr.First().ValueInt64Ord(1))

	if !(ps == BatchPostStatusOpen && s == BatchStatusBalanced) {
		return ResultFail
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
		return ResultFail
	}

	// ------------------------------------------------------------
	// -- At this point, the batch appears to be a disposable batch
	// -- that can safely be deleted.
	// ------------------------------------------------------------
	qr = bq.Set(`UPDATE tciBatchLog SET	PostStatus=? WHERE BatchKey=?;`, BatchPostStatusDeleted, iDisposableBatchKey)
	if !qr.HasAffectedRows {
		return ResultFail
	}

	// Delete an associated tsoBatch record if it exists.
	bq.Set(`DELETE tsoBatch WHERE BatchKey IN (SELECT BatchKey 
													FROM tciBatchLog WITH (NOLOCK) 
													WHERE BatchKey=?
														AND PostStatus=?);`, iDisposableBatchKey, BatchPostStatusDeleted)
	if !bq.OK() {
		return ResultError
	}

	return ResultSuccess
}

// SOPermanentHiddenBatchRecovery -
// This SP will re-assign those shipment/return transactions that references the disposable
// 				batch key passed in back to the pre-commit batch key so that they can be available
// 				for commit once again.  It also calls the undo cost tier routine and cleans up the other
// 				posting related tables that were populated during pre-commit for this batch.  Finally,
// 				it marks the disposable batch as "Deleted" in the batch log.
//
// Parameters
// INPUT:  @iDisposableBatchKey
// 		@iCompanyID
// OUTPUT:  @oRetVal  = See Return codes
//
// RETURN Codes
//
// 0 - Unexpected Error (SP Failure)
// 1 - Successful
func SOPermanentHiddenBatchRecovery(
	bq *du.BatchQuery,
	iDisposableBatchKey int,
	NonParam2 int,
	iCompanyID string,
	NonParam4 string,
	NonParam5 string) ResultConstant {

	bq.ScopeName("PermanentHiddenBatchRecovery")

	// Unable to undo when post status is > 200 (Module Posting Started)
	qr := bq.Get(`SELECT 1 FROM tciBatchLog WITH (nolock) WHERE BatchKey=? AND PostStatus > ?;`, iDisposableBatchKey, BatchPostStatusModStarted)
	if qr.HasData {
		return ResultError
	}

	qr = bq.Set(`IF OBJECT_ID('tempdb..#timposting') IS NOT NULL
					TRUNCATE TABLE #timposting
				ELSE
					CREATE TABLE #timposting
					(
						batchkey          INT NOT NULL,
						tranqty           DECIMAL(16, 8) NOT NULL,
						invttrankey       INT NOT NULL,
						sourceinvttrankey INT NULL,
						addinvt           SMALLINT NOT NULL,
						trantype          INT NOT NULL
					);`)
	if !bq.OK() {
		return ResultError
	}

	// Populate #timPosting as spimPostAPIUndoCostTiersUpdate references this table.
	qr = bq.Set(`INSERT #timposting
					(batchkey,
					tranqty,
					invttrankey,
					sourceinvttrankey,
					addinvt,
					trantype)
				SELECT ps.batchkey,
					1,
					sl.invttrankey,
					NULL,
					CASE
						WHEN ps.trantype IN (?,?) THEN ?
						WHEN ps.trantype IN (?) THEN ?
					ELSE tt.qtyonhandeffect
					END,
					ps.trantype
				FROM tsoShipLine sl WITH (nolock)
					JOIN tsoPendShipment ps WITH (nolock) ON sl.shipkey = ps.shipkey
					JOIN timTranType tt WITH (nolock) ON ps.trantype = tt.trantype
				WHERE ps.batchkey=?
					AND ps.trantype IN (?,?,?);`,
		SOTranTypeCustShip, SOTranTypeTransShip, InventoryDecrease,
		SOTranTypeCustRtrn, InventoryIncrease,
		iDisposableBatchKey, SOTranTypeCustShip, SOTranTypeCustRtrn, SOTranTypeTransShip)
	if !bq.OK() {
		return ResultError
	}

	qr = bq.Set(`INSERT #timposting
					(batchkey,
					tranqty,
					invttrankey,
					sourceinvttrankey,
					addinvt,
					trantype)
				SELECT ps.batchkey,
					1,
					sl.transitinvttrankey,
					invttrankey,
					?,
					ps.trantype
				FROM tsoShipLine sl WITH (nolock)
					JOIN tsoPendShipment ps WITH (nolock) ON sl.shipkey = ps.shipkey
				WHERE  ps.batchkey=?
					AND ps.trantype IN (?)
						AND sl.transitinvttrankey IS NOT NULL`,
		InventoryIncrease, iDisposableBatchKey, SOTranTypeTransShip)
	if !bq.OK() {
		return ResultError
	}

	upd := PostAPIUndoCostTiersUpdate(bq, iDisposableBatchKey, iCompanyID, ModuleSO)
	if upd != ResultSuccess {
		return ResultError
	}

	lPreCommitBatchKey := 0
	qr = bq.Get(`SELECT ShipmentHiddenBatchKey FROM tsoOptions WITH (nolock) WHERE CompanyID=?;`, iCompanyID)
	if qr.HasData {
		lPreCommitBatchKey = int(qr.First().ValueInt64Ord(0))
	}

	if lPreCommitBatchKey == 0 {
		return ResultError
	}

	// Assign the transactions tied to the disposable batch passed in back to the
	// pre-commit batch key so that they can be available for commit once again.
	// (Non-Drop Ship transactions)
	bq.Set(`UPDATE tsoPendShipment	SET Batchkey=? WHERE BatchKey=?	AND TranType<>?;`, lPreCommitBatchKey, iDisposableBatchKey, SOTranTypeDropShip)
	if !bq.OK() {
		return ResultError
	}

	// For drop ship transactions, delete the shipment table records as they will all
	// be re-created when the drop ship is reselected for commit.
	bq.Set(`DELETE sld
			FROM tsoPendShipment ps WITH (nolock)
				INNER JOIN tsoShipLine sl WITH (nolock) ON ps.shipkey = sl.shipkey
				INNER JOIN tsoShipLineDist sld ON sl.ShipLineKey = sld.ShipLineKey
			WHERE  ps.BatchKey=?
				AND ps.trantype=?;`, iDisposableBatchKey, SOTranTypeDropShip)
	if !bq.OK() {
		return ResultError
	}

	bq.Set(`DELETE sl
			FROM tsoPendShipment ps WITH (nolock)
				INNER JOIN tsoShipline sl ON ps.ShipKey=sl.ShipKey
			WHERE ps.BatchKey=?
				AND ps.trantype=?;`, iDisposableBatchKey, SOTranTypeDropShip)
	if !bq.OK() {
		return ResultError
	}

	bq.Set(`DELETE log
			FROM tsoPendShipment ps WITH (nolock)
				INNER JOIN tsoShipmentLog log ON ps.ShipKey=log.ShipKey
			WHERE ps.BatchKey=?
		   		AND ps.TranType=?;`, iDisposableBatchKey, SOTranTypeDropShip)
	if !bq.OK() {
		return ResultError
	}

	bq.Set(`DELETE tsoPendshipment
			WHERE BatchKey=?
		   		AND TranType=?;`, iDisposableBatchKey, SOTranTypeDropShip)
	if !bq.OK() {
		return ResultError
	}

	// Clean up the other posting related tables that were populated
	// during pre-commit for this batch.
	bq.Set(`DELETE timPostingAcct
			FROM timPostingAcct pa
				JOIN timPosting p ON pa.impostingkey=p.impostingkey
			WHERE p.BatchKey=?;`, iDisposableBatchKey)
	if !bq.OK() {
		return ResultError
	}

	bq.Set(`DELETE timPosting WHERE BatchKey=?;`, iDisposableBatchKey)
	if !bq.OK() {
		return ResultError
	}

	bq.Set(`DELETE tglPosting WHERE BatchKey=?;`, iDisposableBatchKey)
	if !bq.OK() {
		return ResultError
	}

	bq.Set(`UPDATE tciBatchLog SET PostStatus=?	WHERE BatchKey=?;`, BatchPostStatusDeleted, iDisposableBatchKey)
	if !bq.OK() {
		return ResultError
	}

	return ResultSuccess
}

// PickListRecovery - This will change the PickingComplete flag from False to True for the ShipLines in the
// 		specified pick list which had failed to be updated when exiting Create Pick process. This will
// 		make the specified pick list available to be edited by the Reprint Pick List, Process SO and
// 		Cancel Pick process.
//
//  Parameters
//     INPUT:  @iPickListKey
//
//    OUTPUT:  @oRetVal  = See Return codes
//
//    RETURN Codes
//     0 - Unexpected Error (SP Failure)
//     1 - Successful
func PickListRecovery(
	bq *du.BatchQuery,
	iPickListKey int,
	NonParam2 int,
	NonParam3 string,
	NonParam4 string,
	NonParam5 string) ResultConstant {

	bq.ScopeName("PickListRecovery")

	//Delete SOLineDist lock keys
	bq.Set(`DELETE lck
			FROM tsoSOLineDistPick lck
				JOIN tsoShipLineDist sld WITH (NOLOCK) ON lck.SOLineDistKey=sld.SOLineDistKey
				JOIN tsoShipLine sl WITH (NOLOCK) ON sl.ShipLineKey=sld.ShipLineKey
			WHERE sl.PickListKey=?;`, iPickListKey)
	if !bq.OK() {
		return ResultError
	}

	bq.Set(`DELETE lck
			FROM timTrnsfrOrdLinePick lck
				JOIN tsoShipLine sl WITH (NOLOCK) ON lck.TrnsfrOrderLineKey = sl.TrnsfrOrderLineKey
			WHERE sl.PickListKey=?;`, iPickListKey)
	if !bq.OK() {
		return ResultError
	}

	bq.Set(`UPDATE tsoShipLine
			SET	PickingComplete=1
			WHERE PickListKey=?
				AND	PickingComplete=0;`, iPickListKey)
	if !bq.OK() {
		return ResultError
	}

	return ResultSuccess
}
