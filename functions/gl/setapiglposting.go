package gl

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// SetAPIGLPosting - Posts GL Accounts to the GL Module.
//
// This stored procedure takes a set of GL accounts from a permanent
// table called tglPosting, and posts them into the appropriate
// rows into the permanent table tglTransaction using set operations.
// In addition, history tables are updated, like tglAcctHist,
// tglAcctHistCurr and tglAcctHistAcctRef.  This sp replaces the
// spglGLPosting sp which extensively used cursors to loop through
// tglPosting one row at a time.  This new sp does not use cursors
// to process the rows in tglPosting for a particular batch.
//
// This stored procedure ASSUMES:
//       (1)  That tglPosting has been correctly populated with n rows
//            which will become n rows in the permanent table tglTransaction.
//       (2)  That validation of GL accounts, Acct. Ref. Keys, etc., has
//            already been performed and that this sp is not executing
//            unless all is OK (i.e., tglPosting data is not validated again).
//       (3)  That the PostDate value in all tglPosting rows is either NULL
//            or equal to the PostDate value in tciBatchLog for that batch.
//            The sp automatically updates any NULL PostDate values in tglPosting
//            with the batch's PostDate value in tciBatchLog.
//
// Use this sp with other Acuity API's that begin with spglSetAPI...
//
// Input Parameters:
//    @iCompanyID = [IN: Valid Acuity Company; No Default]
//    @iBatchKey  = [IN: Batch Key]
//    @iPostToGL  = [IN: Does the calling module want to post to GL?]
//
// Output Parameters:
//    @oRetVal = [OUT: Return flag indicating outcome of the procedure.]
//
//    0 = Failure.  General SP Failure.
//    1 = Successful.
//    2 = Failure.  Retained Earnings Account(s) don't exist.
//    3 = Failure.  Fiscal period is closed.
//
// Standard / Transaction Transactions from GL or Other Subsidiary Modules:
//    4 = Failure in spglSetAPIInsertGLTrans.
//        The insert into #tglTransaction failed.
//    5 = Failure in spglSetAPIInsertGLTrans.
//        Updating #tglTransaction surrogate keys failed.
//    6 = Failure in spglSetAPIInsertGLTrans.
//        The insert into tglTransaction (from tglPosting) failed.
//    7 = Failure in spglSetAPIUpdAcctHist.
//        The insert into tglAcctHist (all accounts, debits/credits) failed.
//    8 = Failure in spglSetAPIUpdAcctHist.
//        The update to tglAcctHist (all accounts, debits/credits) failed.
//    9 = Failure in spglSetAPIUpdAcctHistCurr.
//        The insert into tglAcctHistCurr (non-home curr accts, debits/credits) failed.
//   10 = Failure in spglSetAPIUpdAcctHistCurr.
//        The update to tglAcctHistCurr (non-home curr accts, debits/credits) failed.
//   11 = Failure in spglSetAPIUpdAcctHistRef.
//        The insert into tglAcctHistAcctRef (debits/credits) failed.
//   12 = Failure in spglSetAPIUpdAcctHistCurr.
//        The update to tglAcctHistAcctRef (debits/credits) failed.
//   13 = Failure in spglSetAPIUpdFutBegBal.
//        The Retained Earnings Account in tglOptions does not exist (Std trans).
//   14 = Failure in spglSetAPIUpdFutBegBal.
//        The insert to tglAcctHist (balance sheet accts, beg bal) failed (Std trans).
//   15 = Failure in spglSetAPIUpdFutBegBal.
//        The update to tglAcctHist (balance sheet accts, beg bal) failed (Std trans).
//   16 = Failure in spglSetAPIUpdFutBegBal.
//        An error occurred constructing applicable Retained Earnings Accounts (Std trans).
//   17 = Failure in spglSetAPIUpdFutBegBal.
//        The insert to tglAcctHist (masked Retained Earnings, beg bal) failed (Std trans).
//   18 = Failure in spglSetAPIUpdFutBegBal.
//        The update to tglAcctHist (masked Retained Earnings, beg bal) failed (Std trans).
//   19 = Failure in spglSetAPIUpdFutBegBal.
//        The Retained Earnings Account does not exist in tglAccount (Std trans).
//   20 = Failure in spglSetAPIUpdFutBegBal.
//        The insert to tglAcctHist (unmasked Retained Earnings, beg bal) failed (Std trans).
//   21 = Failure in spglSetAPIUpdFutBegBal.
//        The update to tglAcctHist (unmasked Retained Earnings, beg bal) failed (Std trans).
//   22 = Failure in spglSetAPIUpdFutBegBalCurr.
//        The insert to tglAcctHistCurr (balance sheet accts, beg bal) failed (Std trans).
//   23 = Failure in spglSetAPIUpdFutBegBalCurr.
//        The update to tglAcctHistCurr (balance sheet accts, beg bal) failed (Std trans).
//
// Beginning Balance Transactions from GL Only:
//   24 = Failure in spglSetAPIUpdAcctHistBB.
//        The insert into tglAcctHist (all accounts) failed (BB trans).
//   25 = Failure in spglSetAPIUpdAcctHistBB.
//        The update to tglAcctHist (all accounts) failed (BB trans).
//   26 = Failure in spglSetAPIUpdFutBegBalBB.
//        The Retained Earnings Account in tglOptions does not exist (BB trans).
//   27 = Failure in spglSetAPIUpdFutBegBalBB.
//        The insert to tglAcctHist (balance sheet accts, beg bal) failed (BB trans).
//   28 = Failure in spglSetAPIUpdFutBegBalBB.
//        The update to tglAcctHist (balance sheet accts, beg bal) failed (BB trans).
//   29 = Failure in spglSetAPIUpdFutBegBalBB.
//        An error occurred constructing applicable Retained Earnings Accounts (BB trans).
//   30 = Failure in spglSetAPIUpdFutBegBalBB.
//        The insert to tglAcctHist (masked Retained Earnings, beg bal) failed (BB trans).
//   31 = Failure in spglSetAPIUpdFutBegBalBB.
//        The update to tglAcctHist (masked Retained Earnings, beg bal) failed (BB trans).
//   32 = Failure in spglSetAPIUpdFutBegBalBB.
//        The Retained Earnings Account does not exist in tglAccount (BB trans).
//   33 = Failure in spglSetAPIUpdFutBegBalBB.
//        The insert to tglAcctHist (unmasked Retained Earnings, beg bal) failed (BB trans).
//   34 = Failure in spglSetAPIUpdFutBegBalBB.
//        The update to tglAcctHist (unmasked Retained Earnings, beg bal) failed (BB trans).
//   35 = Failure in spglSetAPIUpdAcctHistCurrBB.
//        The insert into tglAcctHistCurr (non-home curr accts, beg bal) failed (BB trans).
//   36 = Failure in spglSetAPIUpdAcctHistCurrBB.
//        The update to tglAcctHistCurr (non-home curr accts, beg bal) failed (BB trans).
//   37 = Failure in spglSetAPIUpdFutBegBalCurrBB.
//        The insert to tglAcctHistCurr (balance sheet accts, beg bal) failed (BB trans).
//   38 = Failure in spglSetAPIUpdFutBegBalCurrBB.
//        The update to tglAcctHistCurr (balance sheet accts, beg bal) failed (BB trans).
func SetAPIGLPosting(bq *du.BatchQuery, iCompanyID string, iBatchKey int, iPostToGL bool) constants.ResultConstant {
	bq.ScopeName("SetAPIGLPosting")

	if !iPostToGL {
		return constants.ResultSuccess
	}

	lSuspenseAcctKey := int64(0)
	lRetEarnGLAcctNo := ``
	lClearNonFin := false
	lUseMultCurr := false
	lAcctRefUsage := 0

	// Retrieve GL Options Info
	qr := bq.Get(`SELECT SuspenseAcctKey, RetainedEarnAcct, 
						ClearNonFin, UseMultCurr, AcctRefUsage 
				 FROM tglOptions WITH (NOLOCK) 
				 WHERE CompanyID=?;`, iCompanyID)
	if !qr.HasData {
		return constants.ResultError
	}

	lSuspenseAcctKey = qr.First().ValueInt64("SuspenseAcctKey")
	lRetEarnGLAcctNo = qr.First().ValueString("RetainedEarnAcct")
	lClearNonFin = qr.First().ValueInt64("lClearNonFin") == 1
	lUseMultCurr = qr.First().ValueInt64("UseMultCurr") == 1
	lAcctRefUsage = int(qr.First().ValueInt64("AcctRefUsage"))

	lSourceModuleNo := 0
	// Retrieve Batch Info
	qr = bq.Get(`SELECT a.ModuleNo
				  FROM tciBatchType a WITH (NOLOCK), 
						tciBatchLog b WITH (NOLOCK)
				  WHERE a.BatchType = b.BatchType
					AND b.BatchKey=?;`, iBatchKey)
	if !qr.HasData {
		return constants.ResultError
	}
	lSourceModuleNo = int(qr.First().ValueInt64("ModuleNo"))

	// Check to see if there are any rows in tglPosting
	qr = bq.Get(`SELECT BatchKey
				 FROM tglPosting WITH (NOLOCK)
				 WHERE BatchKey=?;`, iBatchKey)
	if !qr.HasData {
		return constants.ResultSuccess
	}

	// Retrieve Batch PostDate
	var lBatchPostDate *time.Time
	qr = bq.Get(`SELECT MIN(PostDate)
				 FROM tglPosting WITH (NOLOCK)
				 WHERE BatchKey=?;`, iBatchKey)
	if qr.HasData {
		*lBatchPostDate = qr.First().ValueTimeOrd(0)
		if lBatchPostDate == nil {
			return constants.ResultFail
		}
	}

	// Determine if there are any tglPosting rows with NULL Post Dates.
	lNullPostDateRows := int64(0)
	qr = bq.Get(`SELECT COUNT(*)
				FROM tglPosting WITH (NOLOCK)
				WHERE BatchKey = ?
				AND COALESCE(DATALENGTH(LTRIM(RTRIM(PostDate))),0) = 0;`, iBatchKey)
	if qr.HasData {
		lNullPostDateRows = qr.First().ValueInt64Ord(0)
	}
	if lNullPostDateRows > 0 {
		// Yes, there ARE some tglPosting rows with NULL Post Dates, so fix them.
		bq.Set(`UPDATE tglPosting
				SET PostDate = ?
				WHERE BatchKey=?
				AND COALESCE(DATALENGTH(LTRIM(RTRIM(PostDate))),0)=0;`, lBatchPostDate, iBatchKey)

	}

	// Retrieve Fiscal Year Info
	lFiscYear := ``

}
