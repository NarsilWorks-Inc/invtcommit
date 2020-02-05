package gl

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// SetAPIInsertGLTrans - Inserts transactions into tglTransaction from
//                 entries in tglPosting for a given batch.
//
// This stored procedure takes a set of GL accounts from a permanent
// table called tglPosting, and posts them into the appropriate
// rows into the permanent table tglTransaction using set operations.
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
//    @iBatchKey         = [IN: Batch Key]
//    @iBatchPostDate    = [IN: Batch Post Date]
//    @iFiscYear         = [IN: Fiscal Year for These GL Transactions]
//    @iFiscPer          = [IN: Fiscal Period for These GL Transactions]
//    @iSourceModuleNo   = [IN: Module Number That Is the Source for These GL Transactions]
//    @iRowsToBeInserted = [IN: The Number of Rows to Be Inserted Into tglTransaction]
//
// Output Parameters:
//    @oRetVal = [OUT: Return flag indicating outcome of the procedure.]
//
//    0 = Failure.  General SP Failure.
//    1 = Successful.
//
// Standard / Transaction Transactions from GL or Other Subsidiary Modules:
//    4 = Failure.  The insert into #tglTransaction failed.
//    5 = Failure.  Updating #tglTransaction surrogate keys failed.
//    6 = Failure.  The insert into tglTransaction (from tglPosting) failed.
func SetAPIInsertGLTrans(
	bq *du.BatchQuery,
	iBatchKey int,
	iBatchPostDate *time.Time,
	iFiscYear string,
	iFiscPer int,
	iSourceModuleNo int,
	iRowsToBeInserted int) constants.ResultConstant {

	bq.ScopeName("SetAPIInsertGLTrans")

	var qr du.QueryResult
	var rc int64

	// Get the Batch Post Date if not passed in.
	if iBatchPostDate == nil {
		qr = bq.Get(`SELECT MIN(PostDate) FROM tglPosting WITH (NOLOCK) WHERE BatchKey=?;`, iBatchKey)
		if qr.HasData {
			*iBatchPostDate = qr.First().ValueTimeOrd(0)
		}
	}

	// Get the number of rows to be inserted into tglTransaction if not passed in.
	if iRowsToBeInserted == 0 {
		qr = bq.Get(`SELECT COUNT(1) FROM tglPosting WITH (NOLOCK) WHERE BatchKey=?	AND NatCurrBegBal=0;`, iBatchKey)
		if qr.HasData {
			iRowsToBeInserted = int(qr.First().ValueInt64Ord(0))
		}
	}

	// Create a temporary #tglTransaction table now.
	bq.Set(`CREATE TABLE #tglTransaction (
				glTranKey      int           NOT NULL,
				AcctRefKey     int           NULL,
				BatchKey       int           NOT NULL,
				CreateType     smallint      NOT NULL,
				CurrExchRate   float         NOT NULL,
				CurrID         VARCHAR(3)       NOT NULL,
				ExtCmnt        varchar(255)  NULL,
				FiscPer        smallint      NOT NULL,
				FiscYear       VARCHAR(5)       NOT NULL,
				GLAcctKey      int           NOT NULL,
				JrnlKey        int           NULL,
				JrnlNo         int           NULL,
				PostAmt        decimal(15,3) NOT NULL,
				PostAmtHC      decimal(15,3) NOT NULL,
				PostCmnt       varchar(50)   NULL,
				PostQty        decimal(16,8) NOT NULL,
				SourceModuleNo smallint      NOT NULL,
				TranDate       datetime      NULL,
				TranKey        int           NULL,
				TranNo         VARCHAR(10)      NULL,
				TranType       int           NULL,
				PostingKey     int           NOT NULL
			);`)

	// Insert rows from tglPosting into tglTransaction.
	// NOTE: Make sure that PostAmt cannot be zero or an error will occur.
	// Also, PostQty is intentionally set to zero if the GL Account's Posting Type is 'Financial Only'.
	// CreateType = 1
	// PostingType = 1
	qr = bq.Set(`INSERT INTO #tglTransaction (glTranKey,
						AcctRefKey,
						BatchKey,
						CreateType,
						CurrExchRate,
						CurrID,
						ExtCmnt,
						FiscPer,
						FiscYear,
						GLAcctKey,
						JrnlKey,
						JrnlNo,
						PostAmt,
						PostAmtHC,
						PostCmnt,
						PostQty,
						SourceModuleNo,
						TranDate,
						TranKey,
						TranNo,
						TranType,
						PostingKey)
				SELECT 0, /* glTranKey */
					a.AcctRefKey,
					a.BatchKey,
					1, /* CreateType */
					CASE
						WHEN a.PostAmt = 0 THEN 1 /* CurrExchRate */
						WHEN a.PostAmt <> 0 THEN a.PostAmtHC / a.PostAmt /* CurrExchRate */
					END,
					a.CurrID,
					a.ExtCmnt,
					?,
					?,
					a.GLAcctKey,
					a.JrnlKey,
					a.JrnlNo,
					a.PostAmt,
					a.PostAmtHC,
					a.PostCmnt, 
					CASE WHEN b.PostingType = 1 THEN 0
						ELSE a.PostQty
					END, /* PostQty */
					?, 
					a.TranDate, 
					a.TranKey,
					a.TranNo, 
					a.TranType, 
					a.PostingKey
				FROM tglPosting a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND a.BatchKey = ?
					AND a.NatCurrBegBal = 0;`, iFiscPer, iFiscYear, iSourceModuleNo, iBatchKey)

	if !qr.HasAffectedRows {
		bq.Set(`DROP TABLE #tglTransaction;`)
		return constants.ResultConstant(4)
	}

	//  Did any standard / transaction GL entries get inserted into tglTransaction?
	qr = bq.Get(`IF OBJECT_ID('tempdb..#tglRsvpTransactionKeysWrk') IS NOT NULL
					SELECT 1 FROM #tglRsvpTransactionKeysWrk WHERE glTranKey <> 0
				 ELSE
					 SELECT 0;`)

	if rc = qr.First().ValueInt64Ord(0); rc > 0 {

		bq.Set(`UPDATE #tglTransaction
				SET #tglTransaction.glTranKey = rsvp.glTranKey
	 			FROM #tglRsvpTransactionKeysWrk rsvp
					 WHERE #tglTransaction.PostingKey = rsvp.PostingKey;`)

	}

	// Check if we still need to get more keys
	qr = bq.Get(`SELECT COUNT(*) FROM #tglTransaction WHERE glTranKey = 0;`)
	if rc = qr.First().ValueInt64Ord(0); rc > 0 {
		iRowsToBeInserted = int(qr.First().ValueInt64Ord(0))
	}

	if iRowsToBeInserted > 0 {

	}
}
