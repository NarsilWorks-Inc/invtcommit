package im

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// PostAPIUndoCostTiersUpdate - post undo
func PostAPIUndoCostTiersUpdate(
	bq *du.BatchQuery,
	iBatchKey int,
	iCompanyID string,
	iModuleNo constants.ModuleConstant) constants.ResultConstant {

	bq.ScopeName("PostAPIUndoCostTiersUpdate")

	//BatchTranTypeIMProcCostTierAdj = 703
	//BatchTranTypeMFCostRollup = 9004

	// SOTranTypeTransIn = 813
	//POTranTypeTransOut = 1113
	//IMTranTypeAdjustment = 710
	// IMTranTypeTransfOut

	// Input to spimAPIUndoCostTiersVector

	// Get batch log info. Cost Tier Adj batch/ MF BOM Cost Rollup
	qr := bq.Get(`SELECT 1
				 FROM tciBatchLog WITH (nolock)
				 WHERE BatchKey=?
					   AND (batchtype IN (?,?) OR PostStatus > ?);`,
		iBatchKey,
		constants.BatchTranTypeIMProcCostTierAdj, constants.BatchTranTypeMFCostRollup, constants.BatchPostStatusModStarted)
	if qr.HasData {
		// Module Posting Started or higher
		return constants.ResultSuccess
	}

	// Create temporary table
	bq.Set(`IF OBJECT_ID('tempdb..#timUndoCostTierTranWrk') IS NOT NULL
				TRUNCATE TABLE #timUndoCostTierTranWrk
			ELSE
				CREATE TABLE #timUndoCostTierTranWrk				
				(
					invttrankey        INTEGER NOT NULL,
					trnsfrorderlinekey INTEGER NULL,
					eoi                SMALLINT NOT NULL
				);`)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`INSERT #timUndoCostTierTranWrk
			SELECT p.invttrankey,
				NULL,
				CASE
					WHEN p.addinvt = 1 THEN 1
					WHEN p.addinvt = 0 THEN -1
					ELSE 0
				END
			FROM #timposting p WITH (nolock)
				INNER JOIN timTranType tt WITH (nolock) ON p.trantype = tt.trantype
			WHERE p.batchkey = ?
				AND p.tranqty <> 0
				AND tt.qtyonhandeffect IN (-1,1)
				AND p.invttrankey IS NOT NULL;`, iBatchKey)
	if !bq.OK() {
		return constants.ResultError
	}

	// we skipped some code here as it does not make sense
	// UPDATE ct
	// SET    ct.status = ct.status
	// FROM   timcosttier ct WITH (INDEX (1))
	// 	   JOIN (SELECT itc.costtierkey
	// 			 FROM   #timUndoCostTierTranWrk uctw WITH (nolock)
	// 					JOIN timinvttrancost itc WITH (nolock)
	// 					  ON uctw.invttrankey = itc.invttrankey) drv
	// 		 ON ct.costtierkey = drv.costtierkey
	// OPTION (keepfixed PLAN)
	//

	// Update the Shipment related records.
	qr = bq.Get(`SELECT DISTINCT s.trnsfrorderlinekey, p.invttrankey
				 FROM #timposting p WITH (nolock)
					JOIN #timposting p1 WITH (nolock) ON p.sourceinvttrankey = p1.invttrankey
					JOIN tsoShipLine s WITH (nolock) ON s.invttrankey = p1.invttrankey
				 WHERE p.TranType=?;`, constants.SOTranTypeTransIn)
	if qr.HasData {
		for _, v := range qr.Data {

			tolk := v.ValueInt64Ord(0)
			itk := v.ValueInt64Ord(1)

			bq.Set(`UPDATE #timUndoCostTierTranWrk SET TrnsfrOrderLineKey=? WHERE InvtTranKey=?;`, tolk, itk)
		}
	}

	if !bq.OK() {
		return constants.ResultError
	}

	// Update the Receipt related records.
	qr = bq.Get(`SELECT DISTINCT r.trnsfrorderlinekey, p.invttrankey
				 FROM #timposting p WITH (nolock)
					JOIN #timposting p1 WITH (nolock) ON p.sourceinvttrankey = p1.invttrankey
					JOIN tpoRcvrLine r WITH (nolock) ON r.invttrankey = p1.invttrankey
				 WHERE p.TranType=?;`, constants.POTranTypeTransOut)
	if qr.HasData {
		for _, v := range qr.Data {

			tolk := v.ValueInt64Ord(0)
			itk := v.ValueInt64Ord(1)

			bq.Set(`UPDATE #timUndoCostTierTranWrk SET TrnsfrOrderLineKey=? WHERE InvtTranKey=?;`, tolk, itk)
		}
	}

	if !bq.OK() {
		return constants.ResultError
	}

	// Update the IM Trnsfr Out adjustment related records.
	qr = bq.Get(`SELECT DISTINCT pit.trnsfrorderlinekey,
					p.invttrankey
				FROM #timposting p WITH (nolock)
					JOIN #timposting p1 WITH (nolock) ON p.sourceinvttrankey=p1.invttrankey
					JOIN timPendInvtTran pit WITH (nolock) ON pit.invttrankey=p1.invttrankey
				WHERE p.TranType=?
					AND pit.trnsfrorderlinekey IS NOT NULL`, constants.IMTranTypeTransfOut)
	if qr.HasData {
		for _, v := range qr.Data {

			tolk := v.ValueInt64Ord(0)
			itk := v.ValueInt64Ord(1)

			bq.Set(`UPDATE #timUndoCostTierTranWrk SET TrnsfrOrderLineKey=? WHERE InvtTranKey=?;`, tolk, itk)
		}
	}

	if !bq.OK() {
		return constants.ResultError
	}

	// Update the IM Misc Adjustment (aka. Adjustment Trnsfr Reconcile if tied to a TrnsfrOrderLineKey) related records.
	qr = bq.Get(`SELECT DISTINCT pit.trnsfrorderlinekey,
					p.invttrankey
				FROM #timposting p WITH (nolock)
					JOIN timPendInvtTran pit WITH (nolock) ON pit.invttrankey=p.invttrankey
				WHERE  p.trantype=?
					AND pit.trnsfrorderlinekey IS NOT NULL;`, constants.IMTranTypeAdjustment)
	if qr.HasData {
		for _, v := range qr.Data {

			tolk := v.ValueInt64Ord(0)
			itk := v.ValueInt64Ord(1)

			bq.Set(`UPDATE #timUndoCostTierTranWrk SET TrnsfrOrderLineKey=? WHERE InvtTranKey=?;`, tolk, itk)
		}
	}

	if !bq.OK() {
		return constants.ResultError
	}

	return APIUndoCostTiersVector(bq)
}
