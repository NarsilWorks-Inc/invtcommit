package gl

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// SummarizeBatchlessTglPosting - This SP designed to take a list of transaction keys and identify its corresponding
//                   GL posting records (tglPosting).  It will then look at the GL summarize options of
//                   the Inventory and Sales Clearing account listings and summarize the GL posting
//                   records accordingly.  All other entries are posted in detail.  Next, it will replace
//                   the GL posting record's BatchKey with the one passed into this routine.
//
//  Important:       The list of transaction keys (#tglPostingDetlTran.PostingDetlTranKey) should join
//                   against tglPosting.TranKey.  This should represent the InvtTranKey of a shipment line.
//                   It is not the ShipKey or ShipLineKey.
//
//  Assumptions:     This SP assumes that the #tglPostingDetlTran has been populated with a list of TranKeys
//                   found in tglPosting.
//                      CREATE TABLE #tglPostingDetlTran (
//                         PostingDetlTranKey INTEGER NOT NULL,
//                         TranType INTEGER NOT NULL)
//
// ****************************************************************************************************************
//  Parameters
//     INPUT:  @iCompanyID = Represents the CompanyID.
//             @iBatchKey = Represents GL batch key to be used to post the transactions to GL.
//             @opt_UseTempTable = (Optional).  Determines which table to use (tglPosting or #tglPostingRPT).
//    OUTPUT:  @oRetVal  = Return Value
// ****************************************************************************************************************
//    RETURN Codes
//
//     0 - Unexpected Error (SP Failure)
//     1 - Successful
func SummarizeBatchlessTglPosting(bq *du.BatchQuery, iCompanyID string, iBatchKey int, optUseTempTable bool) constants.ResultConstant {

	var qr du.QueryResult

	bq.ScopeName("SummarizeBatchlessTglPosting")

	bq.Set(`SELECT * INTO #tglPostingTmp FROM tglPosting WHERE 1=2;`)

	qr = bq.Get(`SELECT ISNULL(OBJECT_ID('tempdb..#tglPostingDetlTran'),0);`)
	if qr.First().ValueFloat64Ord(0) == 0 {
		return constants.ResultError
	}

	ttbl := "tglPosting"
	if optUseTempTable {
		ttbl := "#tglPostingRPT"
	}
	qr = bq.Get(`SELECT 1 FROM #tglPostingDetlTran tmp JOIN ` + ttbl + ` gl ON tmp.TranType = gl.TranType AND tmp.PostingDetlTranKey = gl.TranKey;`)
	if !qr.HasData {
		// Nothing to do.
		return constants.ResultSuccess
	}

	lPostInDetlInventory := 0
	lPostInDetlSalesClearing := 0

	const summarizeInventory int = 709
	const summarizeSalesClr int = 800

	qr = bq.Get(`SELECT im.PostInDetlInvt, so.PostInDetlSalesClr
				FROM timOptions im WITH (NOLOCK)
					JOIN tsoOptions so WITH (NOLOCK) ON im.CompanyID = so.CompanyID
				WHERE im.CompanyID=?;`, iCompanyID)
	if qr.HasData {
		lPostInDetlInventory = int(qr.First().ValueInt64Ord(0))
		lPostInDetlSalesClearing = int(qr.First().ValueInt64Ord(1))
	}

	if lPostInDetlInventory == 1 && lPostInDetlSalesClearing == 1 {
		// Nothing to summarize.
		return constants.ResultSuccess
	}

	// Identify the GL posting records we are dealing with and store them off in a work table.
	// Use ABS() funtion for the tglPosting.Summarize as it can be represented in a (+) or (-)
	// number depending if it is a DR or CR but... it is always the same number.
	// IM Posting options:
	if lPostInDetlInventory == 1 {
		bq.Set(`INSERT INTO #tglPostingTmp (
					AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
					GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
					PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
					PostDate,         Summarize,           TranDate,         SourceModuleNo,
					TranKey,          TranNo,              TranType)
					SELECT
					AcctRefKey,       ?,         			 CurrID,           ExtCmnt,
					GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
					PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
					PostDate,         Summarize,           TranDate,         SourceModuleNo,
					TranKey,          TranNo,              gl.TranType
				FROM `+ttbl+` gl WITH (NOLOCK)
					JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
				WHERE ABS(Summarize)=?;`, iBatchKey, summarizeInventory)
	} else {
		bq.Set(`INSERT INTO #tglPostingTmp (
				AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
				GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
				PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
				PostDate,         Summarize,           TranDate,         SourceModuleNo,
				TranKey,          TranNo,              TranType)
				SELECT
				gl.AcctRefKey,    ?,				   gl.CurrID,        '',
				gl.GLAcctKey,     gl.JrnlKey,          gl.JrnlNo,        gl.NatCurrBegBal,
				SUM(gl.PostAmt),  SUM(gl.PostAmtHC),   SUM(gl.PostQty),  '',
				gl.PostDate,      gl.Summarize,        NULL,             MIN(gl.SourceModuleNo),
				NULL,             NULL,                NULL
            FROM `+ttbl+` gl WITH (NOLOCK)
            	JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
            WHERE ABS(gl.Summarize)=?
            GROUP BY gl.JrnlKey, gl.JrnlNo, gl.GLAcctKey, gl.Summarize, gl.AcctRefKey, gl.CurrID, gl.NatCurrBegBal, gl.PostDate, gl.Summarize;`, iBatchKey, summarizeInventory)
	}

	// SO Posting options:
	if lPostInDetlSalesClearing == 1 {
		bq.Set(`INSERT INTO #tglPostingTmp (
					AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
					GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
					PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
					PostDate,         Summarize,           TranDate,         SourceModuleNo,
					TranKey,          TranNo,              TranType)
				SELECT
					AcctRefKey,       ?,          CurrID,           ExtCmnt,
					GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
					PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
					PostDate,         Summarize,           TranDate,         SourceModuleNo,
					TranKey,          TranNo,              gl.TranType
				FROM `+ttbl+` gl WITH (NOLOCK) 
				JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
				WHERE ABS(Summarize)=?;`, iBatchKey, summarizeSalesClr)
	} else {
		bq.Set(`INSERT INTO #tglPostingTmp (
					AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
					GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
					PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
					PostDate,         Summarize,           TranDate,         SourceModuleNo,
					TranKey,          TranNo,              TranType)
				SELECT 
					gl.AcctRefKey,    ?,				   gl.CurrID,        '',
					gl.GLAcctKey,     gl.JrnlKey,          gl.JrnlNo,        gl.NatCurrBegBal,
					SUM(gl.PostAmt),  SUM(gl.PostAmtHC),   SUM(gl.PostQty),  '',
					gl.PostDate,      gl.Summarize,        NULL,             MIN(gl.SourceModuleNo),
					NULL,             NULL,                NULL
				FROM `+ttbl+` gl WITH (NOLOCK) 
				JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
				WHERE ABS(gl.Summarize) = ?
				GROUP BY gl.JrnlKey,
						gl.JrnlNo,
						gl.GLAcctKey,
						gl.Summarize,
						gl.AcctRefKey,
						gl.CurrID,
						gl.NatCurrBegBal,
						gl.PostDate,
						gl.Summarize;`, iBatchKey, summarizeSalesClr)
	}

	// Get the rest of the GL Posting records that are not covered in the
	// account listing posting options and store them in detail.
	bq.Set(`INSERT INTO #tglPostingTmp (
				AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
				GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
				PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
				PostDate,         Summarize,           TranDate,         SourceModuleNo,
				TranKey,          TranNo,              TranType)
			SELECT
				AcctRefKey,       ?,		          CurrID,           ExtCmnt,
				GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
				PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
				PostDate,         Summarize,           TranDate,         SourceModuleNo,
				TranKey,          TranNo,              gl.TranType
			FROM `+ttbl+` gl WITH (NOLOCK) 
			JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
			WHERE ABS(Summarize) NOT IN (?,?);`, iBatchKey, summarizeSalesClr, summarizeSalesClr)

	// See if there is anything to do.
	qr = bq.Get(`SELECT 1 FROM #tglPostingTmp`)
	if !qr.HasData {
		return constants.ResultError
	}

	bq.Set(`DELETE ` + ttbl + ` FROM tglPosting gl
			JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey;`)

	bq.Set(` INSERT INTO ` + ttbl + ` (
				AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
				GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
				PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
				PostDate,         Summarize,           TranDate,         SourceModuleNo,
				TranKey,          TranNo,              TranType)
			 SELECT
				AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
				GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
				PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
				PostDate,         Summarize,           TranDate,         SourceModuleNo,
				TranKey,          TranNo,              TranType
			 FROM #tglPostingTmp`)

	if !bq.OK() {
		return constants.ResultError
	}

	return constants.ResultSuccess
}
