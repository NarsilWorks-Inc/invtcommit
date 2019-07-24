package main

import du "github.com/eaglebush/datautils"

// InventoryActionConstant - action on the inventory
type InventoryActionConstant int8

// InventoryStatusConstant - inventory status
type InventoryStatusConstant int8

// InventoryTranTypeConstant - tran types
type InventoryTranTypeConstant int16

// Inventory action constants
const (
	InventoryIncrease InventoryActionConstant = 1
	InventoryDecrease InventoryActionConstant = -1
)

// Inventory status
const (
	InventoryStatusPending InventoryStatusConstant = 1
	InventoryStatusActive  InventoryStatusConstant = 2
	InventoryStatusClosed  InventoryStatusConstant = 3
)

// Inventory tran types
const (
	IMTranTypeSale           InventoryTranTypeConstant = 701 // IM Sale
	IMTranTypeSaleRtrn       InventoryTranTypeConstant = 702 // IM Sale Return
	IMTranTypePurchase       InventoryTranTypeConstant = 703 // IM Purchase
	IMTranTypePrchRtrn       InventoryTranTypeConstant = 704 // IM Purchase Return
	IMTranTypeTransfIn       InventoryTranTypeConstant = 705 // Transfer In
	IMTranTypeTransfOut      InventoryTranTypeConstant = 706 // Transfer Out
	IMTranTypeIssue          InventoryTranTypeConstant = 707 // Issue
	IMTranTypePhysCount      InventoryTranTypeConstant = 708 // Physical Count
	IMTranTypeCostTierAdj    InventoryTranTypeConstant = 709 // Cost Tier Adjustment
	IMTranTypeAdjustment     InventoryTranTypeConstant = 710 // Adjustment
	IMTranTypeKitAssembly    InventoryTranTypeConstant = 711 // Kit Assembly
	IMTranTypeKitAssComp     InventoryTranTypeConstant = 712 // Kit Assembly (Component)
	IMTranTypeKitDisassembly InventoryTranTypeConstant = 713 // Kit Disassembly
	IMTranTypeKitDisComp     InventoryTranTypeConstant = 714 // Kit Disassembly (Component)
	IMTranTypeThreeStepTrans InventoryTranTypeConstant = 715 // Three Step Transfer
	IMTranTypeBinTrans       InventoryTranTypeConstant = 716 // Bin Transfer
	IMTranTypeBegBal         InventoryTranTypeConstant = 749 // IM Beginning Balance
)

// APIUndoCostTiersVector - Undoes the update of cost tiers
//   Input Values:
//   ENVIRONMENT:
//   1) #timUndoCostTierTranWrk must be populated with the records to be deleted.
//   Output Values:
//   @_oRetVal                    Return value
//   0 = success
//   1 = failure
func APIUndoCostTiersVector(bq *du.BatchQuery) ResultConstant {
	bq.ScopeName("APIUndoCostTiersVector")

	bq.Set(`SET NOCOUNT ON;`)

	qr := bq.Set(`IF OBJECT_ID('tempdb..#timUndoCostTierWrk') IS NOT NULL
					TRUNCATE TABLE #timUndoCostTierWrk
				  ELSE
					CREATE TABLE #timUndoCostTierWrk
					(
						costtierkey     INTEGER NULL,
						pendqtydecrease DECIMAL(16, 8) NULL,
						pendqtyincrease DECIMAL(16, 8) NULL
					);`)
	if !qr.HasAffectedRows {
		return ResultError
	}

	qr = bq.Set(`IF Object_id('tempdb..#timUndoCostTierWrk1') IS NOT NULL
					TRUNCATE TABLE #timUndoCostTierWrk1
				ELSE
					BEGIN
						CREATE TABLE #timUndoCostTierWrk1
						(
							whsekey         INTEGER NULL,
							itemkey         INTEGER NULL,
							costingdate     DATETIME NULL,
							costtierkey     INTEGER NULL,
							pendqtydecrease DECIMAL(16, 8) NULL,
							pendqtyincrease DECIMAL(16, 8) NULL
						)

						CREATE CLUSTERED INDEX #timUndoCostTierWrk1_cls
							ON #timUndoCostTierWrk1 (
								whsekey, itemkey, costingdate, costtierkey
							)
					END;`)
	if !qr.HasAffectedRows {
		return ResultError
	}

	qr = bq.Get(`SELECT COUNT(*) FROM #timUndoCostTierTranWrk;`)
	if cnt := qr.First().ValueInt64Ord(0); cnt == 0 {
		return ResultSuccess
	}

	defer bq.Set(`TRUNCATE TABLE #timUndoCostTierTranWrk;`)

	bq.Set(`INSERT #timUndoCostTierWrk
				SELECT DISTINCT itc.costtierkey,0,0
				FROM #timUndoCostTierTranWrk ucttw
					INNER JOIN timInvtTranCost itc WITH (nolock) ON ucttw.invttrankey = itc.invttrankey
				WHERE ucttw.eoi IN (?,?);`, InventoryDecrease, InventoryIncrease)

	// Just checking if the query went through without errors. Not necessarily has affected rows
	if !bq.OK() {
		return ResultFail
	}

	// Update decrease
	bq.Set(`UPDATE uctw
				 SET uctw.PendQtyDecrease=invact.DistQty 
				 FROM #timUndoCostTierWrk utcw
				 	INNER JOIN (SELECT COALESCE(SUM(itc.distqty),0) DistQty, itc.CostTierKey
							  	FROM timInvtTranCost itc WITH (nolock)
									INNER JOIN #timUndoCostTierTranWrk ucttw ON itc.invttrankey = ucttw.invttrankey  
								  WHERE ucttw.eoi=?) AS invact ON utcw.CostTierKey=invact.CostTierKey;`, InventoryDecrease)

	// Just checking if the query went through without errors. Not necessarily has affected rows
	if !bq.OK() {
		return ResultFail
	}

	// Update increase
	bq.Set(`UPDATE uctw
				 SET uctw.PendQtyIncrease=invact.DistQty 
				 FROM #timUndoCostTierWrk utcw
				 	INNER JOIN (SELECT COALESCE(SUM(itc.distqty),0) DistQty, itc.CostTierKey
							  	FROM timInvtTranCost itc WITH (nolock)
									INNER JOIN #timUndoCostTierTranWrk ucttw ON itc.invttrankey = ucttw.invttrankey  
								  WHERE ucttw.eoi=?) AS invact ON utcw.CostTierKey=invact.CostTierKey;`, InventoryIncrease)

	// Just checking if the query went through without errors. Not necessarily has affected rows
	if !bq.OK() {
		return ResultFail
	}

	// To resolve deadlock issues, use index to sort the records in the temp table
	// before join with the permanent table, so that the locks placed
	// on timCostTier will be in the same order as other processes.
	qr = bq.Set(`INSERT INTO #timUndoCostTierWrk1
				 SELECT ct.WhseKey, ct.ItemKey, ct.CostingDate,	ct.CostTierKey,	uctw.PendQtyDecrease, uctw.PendQtyIncrease
				 FROM #timUndoCostTierWrk uctw
					INNER JOIN timCostTier ct WITH (nolock) ON ct.CostTierKey=uctw.CostTierKey;`)
	if qr.HasAffectedRows {
		// We don't use the UPDATE FROM statement here to update the pending quantities here because this is often unreliable
		qr = bq.Get(`SELECT ct.CostTierKey, 
							ct.PendingQtyIncrease - uctw.PendQtyIncrease AS NewPendingQtyIncrease,
							ct.PendingQtyDecrease - uctw.PendingQtyDecrease AS NewPendingQtyDecrease
					 FROM #timUndoCostTierWrk1 uctw
						INNER JOIN timCostTier ct ON ct.WhseKey=uctw.WhseKey
					 		AND ct.ItemKey=uctw.ItemKey
							AND ct.CostingDate=uctw.CostingDate
							AND ct.CostTierKey=uctw.CostTierKey);`)

		if qr.HasData {
			// Instead we loop to update the quantities to be sure
			for _, v := range qr.Data {

				ck := v.ValueInt64Ord(0)
				pqi := v.ValueFloat64Ord(1)
				pqd := v.ValueFloat64Ord(2)

				qr2 := bq.Set(`UPDATE timCostTier SET PendingQtyIncrease=?, PendingQtyDecrease=? WHERE CostTierKey=?;`, pqi, pqd, ck)
				if !qr2.HasAffectedRows {
					return ResultFail
				}
			}
		}
	}

	// Changed the query to an inner join
	bq.Set(`DELETE timCostTier
			FROM timCostTier ct
				INNER JOIN #timUndoCostTierWrk uctw	
					ON ct.costtierkey=uctw.costtierkey 
						AND ct.origqty=0
						AND ct.pendqtydecrease=0
						AND ct.pendqtyincrease=0
						AND ct.qtyused=0
						AND ct.status=?;`, InventoryStatusPending)
	if !bq.OK() {
		return ResultFail
	}

	// 	We'll only delete timTrnsfrCost records for transit warehouse
	//  transfer-in transactions, because they're the only ones that
	//  could have created these records. Only consider the transfer
	//  cost records for the inventory transactions in this set.
	//  It is possible for a cost tier is to be increased in IM Adj.
	//  if we are reconciling a transfer order line that has been
	//  over received.  Do not delete the timTrnsfrCost record if
	//  this is the case.  Otherwise, user will not be able to re-
	//  selected the transfer order line for reconcile.
	bq.Set(`DELETE tc
			FROM #timUndoCostTierTranWrk w
				INNER JOIN timInvtTranCost t (nolock) ON w.invttrankey=t.invttrankey
				INNER JOIN timTrnsfrCost tc ON t.CostTierKey=tc.CostTierKey AND w.trnsfrorderlinekey=tc.trnsfrorderlinekey
				LEFT JOIN timPendInvtTran pit (nolock) ON t.invttrankey=pit.invttrankey
			WHERE w.eoi=? AND pit.invttrankey IS NULL;`, InventoryIncrease)
	if !bq.OK() {
		return ResultFail
	}

	// Delete from timInvtTranCost
	bq.Set(`DELETE tc
			FROM #timUndoCostTierTranWrk ucttw
				INNER JOIN timInvtTranCost tc ON tc.InvtTranKey=ucttw.InvtTranKey
			WHERE ucttw.eoi IN (?,?);`, InventoryIncrease, InventoryDecrease)
	if !bq.OK() {
		return ResultFail
	}

	return ResultSuccess
}

// PostAPIUndoCostTiersUpdate - post undo
func PostAPIUndoCostTiersUpdate(
	bq *du.BatchQuery,
	iBatchKey int,
	iCompanyID string,
	iModuleNo ModuleConstant) ResultConstant {

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
		BatchTranTypeIMProcCostTierAdj, BatchTranTypeMFCostRollup, BatchPostStatusModStarted)
	if qr.HasData {
		// Module Posting Started or higher
		return ResultSuccess
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
		return ResultError
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
		return ResultError
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
				 WHERE p.TranType=?;`, SOTranTypeTransIn)
	if qr.HasData {
		for _, v := range qr.Data {

			tolk := v.ValueInt64Ord(0)
			itk := v.ValueInt64Ord(1)

			bq.Set(`UPDATE #timUndoCostTierTranWrk SET TrnsfrOrderLineKey=? WHERE InvtTranKey=?;`, tolk, itk)
		}
	}

	if !bq.OK() {
		return ResultError
	}

	// Update the Receipt related records.
	qr = bq.Get(`SELECT DISTINCT r.trnsfrorderlinekey, p.invttrankey
				 FROM #timposting p WITH (nolock)
					JOIN #timposting p1 WITH (nolock) ON p.sourceinvttrankey = p1.invttrankey
					JOIN tpoRcvrLine r WITH (nolock) ON r.invttrankey = p1.invttrankey
				 WHERE p.TranType=?;`, POTranTypeTransOut)
	if qr.HasData {
		for _, v := range qr.Data {

			tolk := v.ValueInt64Ord(0)
			itk := v.ValueInt64Ord(1)

			bq.Set(`UPDATE #timUndoCostTierTranWrk SET TrnsfrOrderLineKey=? WHERE InvtTranKey=?;`, tolk, itk)
		}
	}

	if !bq.OK() {
		return ResultError
	}

	// Update the IM Trnsfr Out adjustment related records.
	qr = bq.Get(`SELECT DISTINCT pit.trnsfrorderlinekey,
					p.invttrankey
				FROM #timposting p WITH (nolock)
					JOIN #timposting p1 WITH (nolock) ON p.sourceinvttrankey=p1.invttrankey
					JOIN timPendInvtTran pit WITH (nolock) ON pit.invttrankey=p1.invttrankey
				WHERE p.TranType=?
					AND pit.trnsfrorderlinekey IS NOT NULL`, IMTranTypeTransfOut)
	if qr.HasData {
		for _, v := range qr.Data {

			tolk := v.ValueInt64Ord(0)
			itk := v.ValueInt64Ord(1)

			bq.Set(`UPDATE #timUndoCostTierTranWrk SET TrnsfrOrderLineKey=? WHERE InvtTranKey=?;`, tolk, itk)
		}
	}

	if !bq.OK() {
		return ResultError
	}

	// Update the IM Misc Adjustment (aka. Adjustment Trnsfr Reconcile if tied to a TrnsfrOrderLineKey) related records.
	qr = bq.Get(`SELECT DISTINCT pit.trnsfrorderlinekey,
					p.invttrankey
				FROM #timposting p WITH (nolock)
					JOIN timPendInvtTran pit WITH (nolock) ON pit.invttrankey=p.invttrankey
				WHERE  p.trantype=?
					AND pit.trnsfrorderlinekey IS NOT NULL;`, IMTranTypeAdjustment)
	if qr.HasData {
		for _, v := range qr.Data {

			tolk := v.ValueInt64Ord(0)
			itk := v.ValueInt64Ord(1)

			bq.Set(`UPDATE #timUndoCostTierTranWrk SET TrnsfrOrderLineKey=? WHERE InvtTranKey=?;`, tolk, itk)
		}
	}

	if !bq.OK() {
		return ResultError
	}

	return APIUndoCostTiersVector(bq)
}

// PostAPIGLPosting - post api GL posting
func PostAPIGLPosting(bq *du.BatchQuery, iBatchKey int, iCompanyID string, iModuleNo int, iIntegrateWithGL bool) ResultConstant {
	bq.ScopeName("PostAPIGLPosting")

	qr := bq.Get(`SELECT COUNT(*) FROM tglPosting WITH (NOLOCK) WHERE BatchKey=?;`, iBatchKey)
	lCount := qr.First().ValueInt64Ord(0)

	if lCount == 0 || !iIntegrateWithGL {
		return ResultSuccess
	}

	res := SetAPIGLPosting(bq, iCompanyID, iBatchKey, iIntegrateWithGL)
	if res != ResultSuccess {
		return ResultError
	}

	bq.Set(`DELETE tglPosting WHERE BatchKey=?;`, iBatchKey)

	return ResultSuccess
}
