package im

import (
	"gosqljobs/invtcommit/functions/bat"
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// CreateInvtCommitDisposableBatch - This SP creates disposable batches for those transactions that are in a temp table called
//                     #tciTransToCommit.  At the end of the routine, the DispoBatchKey field will have a new
//                     key value that represents the disposable batch for the transaction.
//	---------------------------------------------------------------------
//   					This procedure will create a disposable batch for any #tciTransToCommit row that does no
//   					have a DispoBatchKey.
//
//    Assumptions:     This SP assumes that the #tciTransToCommit has been populated appropriately and completely.
//
//                       CREATE TABLE #tciTransToCommit (
//                        	CompanyID         VARCHAR(3) NOT NULL,
//                        	TranType          INTEGER NOT NULL,  -- Shipment tran types including 810, 811, 812.
//                        	PostDate          DATETIME NOT NULL, -- Post date of the transaction.
//                        	InvcDate          DATETIME NULL,     -- Date use to create the invoice or credit memo.
//                        	TranKey           INTEGER NOT NULL,  -- Represents the ShipKey of the transactions to commit.
//                        	PreCommitBatchKey INTEGER NOT NULL,  -- Represents the module's hidden system batch for uncommitted trans.
//                        	DispoBatchKey     INTEGER NULL,      -- Temporary batch used to run through the posting routines.
//                        	CommitStatus      INTEGER DEFAULT 0  -- Status use to determine progress of each transaction.
//						)
//   ---------------------------------------------------------------------
//    Parameters
//       INPUT:  <None>
//      OUTPUT:  @oRetVal  = Return Value
//   ---------------------------------------------------------------------
//      RETURN Codes
//       0 - Unexpected Error (SP Failure)
//       1 - Successful
func CreateInvtCommitDisposableBatch(bq *du.BatchQuery, UserID string) constants.BatchReturnConstant {
	bq.ScopeName("CreateInvtCommitDisposableBatch")

	qr := bq.Get(`SELECT CompanyID,
					? AS BatchTranType,
					PostDate,
					COALESCE(InvcDate, PostDate) As InvcDate
				FROM #tcitranstocommit 
				WHERE ISNULL(DispoBatchKey,0)=0 AND TranType IN (?,?,?)
				UNION
				SELECT CompanyID,
					? AS BatchTranType,
					PostDate,
					COALESCE(InvcDate, PostDate) As InvcDate
				FROM #tcitranstocommit 
				WHERE ISNULL(DispoBatchKey,0)=0 AND TranType IN (?);`,
		constants.BatchTranTypeSOProcShip, constants.SOTranTypeDropShip, constants.SOTranTypeCustShip, constants.SOTranTypeTransShip,
		constants.BatchTranTypeSOProcCustRtrn, constants.SOTranTypeCustRtrn)

	if !qr.HasData {
		return constants.BatchReturnError
	}

	var qr2 du.QueryResult

	for _, r := range qr.Data {

		bt := int(r.ValueInt64("BatchTranType"))
		cid := r.ValueString("CompanyID")
		pd := r.ValueTime("PostDate")
		idt := r.ValueTime("InvcDate")

		res, batchKey, _ := bat.GetNextBatch(bq, cid, 8, bt, UserID, `Disposable Batch for Inventory Commit`, pd, 1, &idt)

		if res != constants.BatchReturnValid {
			return res
		}

		upd := `UPDATE #tcitranstocommit SET DispoBatchKey=? WHERE CompanyID=? AND PostDate=? AND COALESCE(InvcDate, PostDate)=? AND TranType IN `

		switch constants.BatchTranTypeConstant(bt) {
		case constants.BatchTranTypeSOProcShip:
			qr2 = bq.Set(upd+` (?,?,?);`, batchKey, cid, pd, idt, constants.SOTranTypeDropShip, constants.SOTranTypeCustShip, constants.SOTranTypeTransShip)
		case constants.BatchTranTypeSOProcCustRtrn:
			qr2 = bq.Set(upd+` (?);`, batchKey, cid, pd, idt, constants.SOTranTypeCustRtrn)
		}

		if &qr2 == nil {
			return constants.BatchReturnError
		}

		if qr2.HasData {
			if qr2.Get(0).ValueInt64("Affected") == 0 {
				return constants.BatchReturnError
			}
		}
	}

	return constants.BatchReturnValid
}
