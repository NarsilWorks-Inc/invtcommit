package im

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// APIUndoCostTiersVector - Undoes the update of cost tiers
//   Input Values:
//   ENVIRONMENT:
//   1) #timUndoCostTierTranWrk must be populated with the records to be deleted.
//   Output Values:
//   @_oRetVal                    Return value
//   0 = success
//   1 = failure
func APIUndoCostTiersVector(bq *du.BatchQuery) constants.ResultConstant {
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
		return constants.ResultError
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
		return constants.ResultError
	}

	qr = bq.Get(`SELECT COUNT(*) FROM #timUndoCostTierTranWrk;`)
	if cnt := qr.First().ValueInt64Ord(0); cnt == 0 {
		return constants.ResultSuccess
	}

	defer bq.Set(`TRUNCATE TABLE #timUndoCostTierTranWrk;`)

	bq.Set(`INSERT #timUndoCostTierWrk
				SELECT DISTINCT itc.costtierkey,0,0
				FROM #timUndoCostTierTranWrk ucttw
					INNER JOIN timInvtTranCost itc WITH (nolock) ON ucttw.invttrankey = itc.invttrankey
				WHERE ucttw.eoi IN (?,?);`, constants.InventoryDecrease, constants.InventoryIncrease)

	// Just checking if the query went through without errors. Not necessarily has affected rows
	if !bq.OK() {
		return constants.ResultFail
	}

	// Update decrease
	bq.Set(`UPDATE uctw
				 SET uctw.PendQtyDecrease=invact.DistQty 
				 FROM #timUndoCostTierWrk utcw
				 	INNER JOIN (SELECT COALESCE(SUM(itc.distqty),0) DistQty, itc.CostTierKey
							  	FROM timInvtTranCost itc WITH (nolock)
									INNER JOIN #timUndoCostTierTranWrk ucttw ON itc.invttrankey = ucttw.invttrankey  
								  WHERE ucttw.eoi=?) AS invact ON utcw.CostTierKey=invact.CostTierKey;`, constants.InventoryDecrease)

	// Just checking if the query went through without errors. Not necessarily has affected rows
	if !bq.OK() {
		return constants.ResultFail
	}

	// Update increase
	bq.Set(`UPDATE uctw
				 SET uctw.PendQtyIncrease=invact.DistQty 
				 FROM #timUndoCostTierWrk utcw
				 	INNER JOIN (SELECT COALESCE(SUM(itc.distqty),0) DistQty, itc.CostTierKey
							  	FROM timInvtTranCost itc WITH (nolock)
									INNER JOIN #timUndoCostTierTranWrk ucttw ON itc.invttrankey = ucttw.invttrankey  
								  WHERE ucttw.eoi=?) AS invact ON utcw.CostTierKey=invact.CostTierKey;`, constants.InventoryIncrease)

	// Just checking if the query went through without errors. Not necessarily has affected rows
	if !bq.OK() {
		return constants.ResultFail
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
					return constants.ResultFail
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
						AND ct.status=?;`, constants.InventoryStatusPending)
	if !bq.OK() {
		return constants.ResultFail
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
			WHERE w.eoi=? AND pit.invttrankey IS NULL;`, constants.InventoryIncrease)
	if !bq.OK() {
		return constants.ResultFail
	}

	// Delete from timInvtTranCost
	bq.Set(`DELETE tc
			FROM #timUndoCostTierTranWrk ucttw
				INNER JOIN timInvtTranCost tc ON tc.InvtTranKey=ucttw.InvtTranKey
			WHERE ucttw.eoi IN (?,?);`, constants.InventoryIncrease, constants.InventoryDecrease)
	if !bq.OK() {
		return constants.ResultFail
	}

	return constants.ResultSuccess
}
