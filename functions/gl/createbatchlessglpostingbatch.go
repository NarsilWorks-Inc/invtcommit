package gl

import (
	"gosqljobs/invtcommit/functions/bat"
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// CreateBatchlessGLPostingBatch - This SP creates disposable batches for those transactions that are in a temp table called
//                  #tciTransToPost.  At the end of the routine, the GLBatchKey field will have a new
//                  key value that represents the disposable batch for the transaction.
//
// Assumptions:     This SP assumes that the #tciTransToPost has been populated appropriately and completely.
//
//                     CREATE TABLE #tciTransToPost (
//                     	CompanyID         VARCHAR(3) NOT NULL,
//                     	TranID            VARCHAR(13) NOT NULL, -- TranID used for reporting.
//                     	TranType          INTEGER NOT NULL,  -- Supported transaction types.
//                     	TranKey           INTEGER NOT NULL,  -- Represents the TranKey of the transactions to post. (ie. ShipKey for SO)
//                     	GLBatchKey        INTEGER NOT NULL,  -- Represents the GL batch to post the transactions.
//                     	PostStatus        INTEGER DEFAULT 0) -- Status use to determine progress of each transaction.
//
// Parameters
//    INPUT:  <None>
//   OUTPUT:  @oRetVal  = Return Value
//   RETURN Codes
//
//    0 - Unexpected Error (SP Failure)
//    1 - Successful
func CreateBatchlessGLPostingBatch(bq *du.BatchQuery, loginID string, iBatchCmnt string) constants.ResultConstant {
	bq.ScopeName("CreateBatchlessGLPostingBatch")

	qr := bq.Get(`SELECT DISTINCT 
					tmp.CompanyID, btt.BatchType, s.PostDate, COALESCE(p.TranDate, s.PostDate) AS InvcDate, bt.ModuleNo
				  FROM #tciTransToPost tmp
					JOIN tciBatchTranType btt WITH (NOLOCK) ON tmp.TranType = btt.TranType
					JOIN tciBatchType bt WITH (NOLOCK) ON btt.BatchType = bt.BatchType
					JOIN tsoShipment s WITH (NOLOCK) ON tmp.TranKey = s.ShipKey
					JOIN tsoShipLine sl WITH (NOLOCK) ON s.ShipKey = sl.ShipKey
					LEFT JOIN tarInvoiceDetl d WITH (NOLOCK) ON sl.ShipLineKey = d.ShipLineKey 	-- Outer join because Trnsfrs do not have invoices.
					LEFT JOIN tarPendInvoice p WITH (NOLOCK) ON d.InvcKey = p.InvcKey
				WHERE COALESCE(tmp.GLBatchKey, 0) = 0 
					AND tmp.PostStatus=0 -- Default status.  Means it is a new transaction.
					AND tmp.TranType IN (?,?,?);`,
		constants.SOTranTypeCustShip, constants.SOTranTypeTransShip, constants.SOTranTypeCustRtrn)
	if qr.HasData {
		for _, v := range qr.Data {
			cid := v.ValueString("CompanyID")
			mod := constants.ModuleConstant(v.ValueInt64("ModuleNo"))
			bt := int(v.ValueInt64("BatchType"))
			pdt := v.ValueTime("PostDate")
			idt := v.ValueTime("InvcDate")

			res, batchKey, _ := bat.GetNextBatch(bq, cid, mod, bt, loginID, iBatchCmnt, pdt, 0, &idt)

			bq.Set(`UPDATE tmp
					SET tmp.GLBatchKey=?
					FROM #tciTransToPost tmp
						JOIN tciBatchTranType btt WITH (NOLOCK) ON tmp.TranType = btt.TranType
						JOIN tsoShipment s WITH (NOLOCK) ON tmp.TranKey = s.ShipKey AND s.CompanyID=? AND s.PostDate=?
						JOIN tsoShipLine sl WITH (NOLOCK) ON s.ShipKey = sl.ShipKey
						LEFT JOIN tarInvoiceDetl i WITH (NOLOCK) ON sl.ShipLineKey = i.ShipLineKey
						LEFT JOIN tarPendInvoice p WITH (NOLOCK) ON i.InvcKey = p.InvcKey
					WHERE tmp.BatchType=? AND COALESCE(p.TranDate, s.PostDate)=?
						AND COALESCE(tmp.GLBatchKey,0)=0;`, batchKey, cid, pdt.Format("01-02-2006"), bt, idt.Format("01/02/2006"))

		}
	}

	if !bq.OK() {
		return constants.ResultError
	}

	return constants.ResultSuccess
}
