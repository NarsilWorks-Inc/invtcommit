package so

import (
	"gosqljobs/invtcommit/functions/constants"
	"gosqljobs/invtcommit/functions/im"

	du "github.com/eaglebush/datautils"
)

//PermanentHiddenBatchRecovery -
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
func PermanentHiddenBatchRecovery(
	bq *du.BatchQuery,
	iDisposableBatchKey int,
	NonParam2 int,
	iCompanyID string,
	NonParam4 string,
	NonParam5 string) constants.ResultConstant {

	bq.ScopeName("PermanentHiddenBatchRecovery")

	// Unable to undo when post status is > 200 (Module Posting Started)
	qr := bq.Get(`SELECT 1 FROM tciBatchLog WITH (nolock) WHERE BatchKey=? AND PostStatus > ?;`, iDisposableBatchKey, constants.BatchPostStatusModStarted)
	if qr.HasData {
		return constants.ResultError
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
		return constants.ResultError
	}

	// Populate #timPosting as spimPostAPIUndoCostTiersUpdate references this table.
	qr = bq.Set(`INSERT INTO #timposting
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
		constants.SOTranTypeCustShip, constants.SOTranTypeTransShip, constants.InventoryDecrease,
		constants.SOTranTypeCustRtrn, constants.InventoryIncrease,
		iDisposableBatchKey, constants.SOTranTypeCustShip, constants.SOTranTypeCustRtrn, constants.SOTranTypeTransShip)
	if !bq.OK() {
		return constants.ResultError
	}

	qr = bq.Set(`INSERT INTO #timposting
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
		constants.InventoryIncrease, iDisposableBatchKey, constants.SOTranTypeTransShip)
	if !bq.OK() {
		return constants.ResultError
	}

	upd := im.PostAPIUndoCostTiersUpdate(bq, iDisposableBatchKey, iCompanyID, constants.ModuleSO)
	if upd != constants.ResultSuccess {
		return constants.ResultError
	}

	lPreCommitBatchKey := 0
	qr = bq.Get(`SELECT ShipmentHiddenBatchKey FROM tsoOptions WITH (nolock) WHERE CompanyID=?;`, iCompanyID)
	if qr.HasData {
		lPreCommitBatchKey = int(qr.First().ValueInt64Ord(0))
	}

	if lPreCommitBatchKey == 0 {
		return constants.ResultError
	}

	// Assign the transactions tied to the disposable batch passed in back to the
	// pre-commit batch key so that they can be available for commit once again.
	// (Non-Drop Ship transactions)
	bq.Set(`UPDATE tsoPendShipment	SET Batchkey=? WHERE BatchKey=?	AND TranType<>?;`, lPreCommitBatchKey, iDisposableBatchKey, constants.SOTranTypeDropShip)
	if !bq.OK() {
		return constants.ResultError
	}

	// For drop ship transactions, delete the shipment table records as they will all
	// be re-created when the drop ship is reselected for commit.
	bq.Set(`DELETE sld
			FROM tsoPendShipment ps WITH (nolock)
				INNER JOIN tsoShipLine sl WITH (nolock) ON ps.shipkey = sl.shipkey
				INNER JOIN tsoShipLineDist sld ON sl.ShipLineKey = sld.ShipLineKey
			WHERE  ps.BatchKey=?
				AND ps.trantype=?;`, iDisposableBatchKey, constants.SOTranTypeDropShip)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`DELETE sl
			FROM tsoPendShipment ps WITH (nolock)
				INNER JOIN tsoShipline sl ON ps.ShipKey=sl.ShipKey
			WHERE ps.BatchKey=?
				AND ps.trantype=?;`, iDisposableBatchKey, constants.SOTranTypeDropShip)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`DELETE log
			FROM tsoPendShipment ps WITH (nolock)
				INNER JOIN tsoShipmentLog log ON ps.ShipKey=log.ShipKey
			WHERE ps.BatchKey=?
		   		AND ps.TranType=?;`, iDisposableBatchKey, constants.SOTranTypeDropShip)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`DELETE tsoPendshipment
			WHERE BatchKey=?
		   		AND TranType=?;`, iDisposableBatchKey, constants.SOTranTypeDropShip)
	if !bq.OK() {
		return constants.ResultError
	}

	// Clean up the other posting related tables that were populated
	// during pre-commit for this batch.
	bq.Set(`DELETE timPostingAcct
			FROM timPostingAcct pa
				JOIN timPosting p ON pa.impostingkey=p.impostingkey
			WHERE p.BatchKey=?;`, iDisposableBatchKey)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`DELETE timPosting WHERE BatchKey=?;`, iDisposableBatchKey)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`DELETE tglPosting WHERE BatchKey=?;`, iDisposableBatchKey)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`UPDATE tciBatchLog SET PostStatus=?	WHERE BatchKey=?;`, constants.BatchPostStatusDeleted, iDisposableBatchKey)
	if !bq.OK() {
		return constants.ResultError
	}

	return constants.ResultSuccess
}
