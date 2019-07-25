package gl

import (
	"gosqljobs/invtcommit/functions/constants"
	"gosqljobs/invtcommit/functions/sm"

	du "github.com/eaglebush/datautils"
)

// APIPostBatchlessGLPosting - designed to process GL postings for those transactions that were committed
//                  by the inventory batchless process.  It assumes that a temp table called #tciTransToPost
//                  exists and contains data about the transactions to post.  It also assumes that the records
//                  in tglPosting that correspond to the transactions were posted in detail.  This allows us
//                to tie each GL entry to its source transaction using the InvtTranKey.
//                 The process starts by validating the contents of #tciTransToPost.  It then creates a
//                 logical lock against the GL posting records for those valid transactions.  We then
//                  re-validate the GL accounts.  If a GL account fails validation for any reason, it will
//                 be conditionally replaced with the suspense account and a warning will be logged.  If
//                 the GL period we are posting to is closed, we will stop posting and the user will have
//                 to re-open the period and re-process the transactions.
//
//                  Next, we will use the current posting settings to post the lines in detail or summary.
//                  Upon successful completion, the transaction's shipment log will have a status of "Posted"
//                  and its GL transactions will exists in tglTransaction.
// Assumptions:     This SP assumes that the #tciTransToPost has been populated appropriately and completely
//                  using the following table definition.
//                     CREATE TABLE #tciTransToPost (
//                     CompanyID         VARCHAR(3) NOT NULL,
//                     TranID            VARCHAR(13) NOT NULL, -- TranID used for reporting.
//                     TranType          INTEGER NOT NULL,  -- Supported transaction types.
//                     TranKey           INTEGER NOT NULL,  -- Represents the TranKey of the transactions to post. (ie. ShipKey for SO)
//                     GLBatchKey        INTEGER NOT NULL,  -- Represents the GL batch to post the transactions.
//                     PostStatus        INTEGER DEFAULT 0) -- Status use to determine progress of each transaction.
//
// PostStatus Enum:
//   0 = New Transaction, have not been processed (Default Value).
//   1 = Posted successfully.
//   2 = Invalid GL Account exists.  Not considered as a fatal error since it can be replaced with the suspense account.
//  -1 = TranType not supported.
//  -2 = Transactions have not yet been committed.
//  -3 = GL posting batch does not exists or is invalid.
//  -4 = Posting to a prior SO period.
//  -5 = Posting to a closed GL period.
//  -6 = Transactions have been locked by another user.
//  -7 = Debits and Credits do not equal.
// Parameters
//    INPUT:  @iBatchCmnt = Comment use for ALL batches.
//   OUTPUT:  @ioSessionID = SessionID used for reporting errors. (Input / Output)
//            @oRetVal = Return Value
//               0 = Failure
//               1 = Success <Transaction(s) posted to GL>
//               2 = Success <NO Transaction was posted to GL>
//
// OPTIONAL:  @optReplcInvalidAcctWithSuspense = Indicates whether invalid account are replaced by the suspense acct.
//            @optPostToGL = Defaults to true.  However, when set to false, final GL posting will not be performed.  Use
//             this option when the user decides to preview the GL register instead of actually proceeding with the posting.
//                Note: Each time this routine is called, a GL Batch number is used even if this option is set
//                to false.  This way the final GL Batch number is seen during preview or after posting.
//
//   RETURN Codes
//    0 - Unexpected Error (SP Failure)
//    1 - Successful
func APIPostBatchlessGLPosting(
	bq *du.BatchQuery,
	iBatchCmnt string,
	iSessionID int,
	loginID string,
	optReplcInvalidAcctWithSuspense bool,
	optPostToGL bool) (Result constants.ResultConstant, SessionID int) {

	bq.ScopeName("APIPostBatchlessGLPosting")

	bq.Set(`IF OBJECT_ID('tempdb..#tglPostingRpt') IS NOT NULL
				TRUNCATE TABLE #tglpostingrpt
			ELSE
			BEGIN
				SELECT *
				INTO   #tglpostingrpt FROM tglPosting WHERE 1=2
	
				CREATE CLUSTERED INDEX cls_tglposting_idx ON #tglpostingrpt (batchkey)
			END;`)

	bq.Set(`CREATE TABLE #UniqueTransToPost	(
				companyid  VARCHAR(3) NOT NULL,
				tranid     VARCHAR(13) NOT NULL,
				trantype   INTEGER NOT NULL,
				trankey    INTEGER NOT NULL,
				glbatchkey INTEGER NOT NULL,
				poststatus INTEGER DEFAULT 0
			);`)

	bq.Set(`IF OBJECT_ID('tempdb..#tglPosting') IS NULL
				SELECT *
				INTO #tglPosting FROM tglPosting WHERE 1=2;`)

	bq.Set(`IF OBJECT_ID('tempdb..#tglPostingDetlTran') IS NOT NULL
				TRUNCATE TABLE #tglPostingDetlTran
			ELSE
			BEGIN
				CREATE TABLE #tglPostingDetlTran (
					postingdetltrankey INTEGER NOT NULL,
					trantype           INTEGER NOT NULL
				)

				CREATE CLUSTERED INDEX cls_tglpostingdetltran_idx ON #tglPostingDetlTran (trantype, postingdetltrankey)
			END;`)

	bq.Set(`IF OBJECT_ID('tempdb..#LogicalLocks') IS NOT NULL
				TRUNCATE TABLE #LogicalLocks
			ELSE
			BEGIN
				CREATE TABLE #LogicalLocks
				(
					id INT IDENTITY(1,1),
					logicallocktype   SMALLINT,
					logicallockid     VARCHAR(80),
					userkey           INTEGER NULL,
					locktype          SMALLINT,
					logicallockkey    INT NULL,
					status            INTEGER NULL,
					lockcleanupparam1 INTEGER NULL,
					lockcleanupparam2 INTEGER NULL,
					lockcleanupparam3 VARCHAR(255) NULL,
					lockcleanupparam4 VARCHAR(255) NULL,
					lockcleanupparam5 VARCHAR(255) NULL
				)
				
				CREATE CLUSTERED INDEX cls_logicallocks_idx	ON #LogicalLocks (userkey, logicallockkey, logicallocktype)
			END;`)

	bq.Set(`IF OBJECT_ID('tempdb..#tciErrorLogExt') IS NOT NULL
				TRUNCATE TABLE #tciErrorLogExt
			ELSE
				CREATE TABLE #tciErrorLogExt
				(
					entryno     INTEGER NOT NULL,
					sessionid   INTEGER NOT NULL,
					trantype    INTEGER NULL,
					trankey     INTEGER NULL,
					tranlinekey INTEGER NULL,
					invttrankey INTEGER NULL
				);`)

	bq.Set(`IF OBJECT_ID('tempdb..#tciError') IS NOT NULL
				TRUNCATE TABLE #tciError
			ELSE
				CREATE TABLE #tciError
				(
					entryno     INT NULL,
					batchkey    INT NULL,
					stringno    INT NOT NULL,
					stringdata1 VARCHAR(30) NULL,
					stringdata2 VARCHAR(30) NULL,
					stringdata3 VARCHAR(30) NULL,
					stringdata4 VARCHAR(30) NULL,
					stringdata5 VARCHAR(30) NULL,
					errortype   SMALLINT NOT NULL,
					severity    SMALLINT NOT NULL,
					trantype    INT NULL,
					trankey     INT NULL,
					tranlinekey INT NULL,
					invttrankey INT NULL
				);`)

	bq.Set(`IF OBJECT_ID('tempdb..#tglValidateAcct') IS NOT NULL
				TRUNCATE TABLE #tglValidateAcct
			ELSE
			BEGIN
				CREATE TABLE #tglValidateAcct
				(
					glacctkey        INT NOT NULL,
					acctrefkey       INT NULL,
					currid           VARCHAR(3) NOT NULL,
					validationretval INT NOT NULL,
					errormsgno       INT NULL
				)
				
				CREATE CLUSTERED INDEX #tglvalidateacct_idx_cls	ON #tglvalidateacct (glacctkey, acctrefkey, currid)
			END;`)

	bq.Set(`IF OBJECT_ID('tempdb..#tciTransToPostDetl') IS NOT NULL
				TRUNCATE TABLE #tciTransToPostDetl
			ELSE
			BEGIN
				CREATE TABLE #tciTransToPostDetl
				(
					glbatchkey     INTEGER NOT NULL,
					companyid      VARCHAR(3) NOT NULL,
					tranid         VARCHAR(13) NOT NULL,
					trantype       INTEGER NOT NULL,
					trankey        INTEGER NOT NULL,
					-- Represents the TranKey of the transactions to post. (ie. ShipKey for SO)
					invttrankey    INTEGER NOT NULL,
					postingkey     INTEGER NOT NULL,
					sourcemoduleno SMALLINT NOT NULL,
					glacctkey      INTEGER NOT NULL,
					acctrefkey     INTEGER NULL,
					currid         VARCHAR(3) NOT NULL,
					postdate       DATETIME NOT NULL,
					postamthc      DECIMAL(15, 3) NOT NULL,
					poststatus     INTEGER DEFAULT 0
				)
				
				CREATE CLUSTERED INDEX cls_tcitranstopostdetl_idx ON #tciTransToPostDetl (postingkey, trankey, invttrankey,	glacctkey)
			END;`)

	bq.Set(`CREATE TABLE #UniqueGLBatchKeys
			(
				batchcount       SMALLINT NOT NULL IDENTITY (1, 1),
				companyid        VARCHAR(3) NOT NULL,
				glbatchkey       INTEGER NOT NULL,
				moduleno         SMALLINT NOT NULL,
				postdate         DATETIME NOT NULL,
				integratedwithgl SMALLINT NOT NULL
			);`)

	// Set the default value on the PostStatus if its value is not one that is supported.
	bq.Set(`UPDATE #tciTransToPost SET PostStatus=?
			WHERE  PostStatus NOT IN (?,?,?,?,?,?,?,?,?);`, constants.GLPostStatusDefault,
		constants.GLPostStatusSuccess, constants.GLPostStatusInvalid, constants.GLPostStatusTTypeNotSupported,
		constants.GLPostStatusTranNotCommitted, constants.GLPostStatusPostingPriorSOPeriod,
		constants.GLPostStatusPostingClosedGLPeriod, constants.GLPostStatusTranLockedByUser, constants.GLPostStatusDebitCreditNotEqual)

	// Make sure the rows in #tciTransToPost are Unique rows.
	bq.Set(`INSERT #UniqueTransToPost (CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus)
			SELECT DISTINCT CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus FROM #tciTransToPost;`)

	bq.Set(`TRUNCATE TABLE #tciTransToPost;`)

	bq.Set(`INSERT #tciTransToPost (CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus)
			SELECT CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus FROM #UniqueTransToPost;`)

	// These will be executed before the function exits
	defer sm.LogicalLockRemoveMultiple(bq)
	defer sm.LogErrors(bq, iSessionID, iSessionID)

	res := CreateBatchlessGLPostingBatch(bq, loginID, iBatchCmnt)
	if res != constants.ResultSuccess {
		//-- This is a bad return value.  We should not proceed with the posting.
		return constants.ResultError, iSessionID
	}

	oSessionID := 0
	if iSessionID != 0 {
		oSessionID = iSessionID
	}

	var qr du.QueryResult
	if oSessionID == 0 {
		// Use a one SessionID for all batches we are about to post.  This will help in reporting.
		qr = bq.Get(`SELECT ISNULL(MIN(GLBatchKey),0) FROM #tciTransToPost;`)
		if qr.HasData {
			oSessionID = int(qr.First().ValueInt64Ord(0))
		}
	}

	//-- Clear the error tables.
	bq.Set(`DELETE tciErrorLog WHERE  SessionID = ? OR BatchKey IN (SELECT GLBatchKey FROM #tciTransToPost);`, oSessionID)

	// -------------------------------------------------------------------------------
	// Validate the data in #tciTransToPost: (Following considered to be fatal errors)
	// If an error is logged here, we will not post any transactions in the set.
	// -------------------------------------------------------------------------------

	// Validate the GLBatchKey found in #tciTransToPost.
	qr = bq.Set(`UPDATE tmp
					SET PostStatus = ?
					FROM #tciTransToPost tmp
					LEFT JOIN tciBatchLog bl WITH (NOLOCK) ON tmp.GLBatchKey = bl.BatchKey
				WHERE (bl.PostStatus <> 0 OR bl.Status <> 4) OR bl.BatchKey IS NULL
						AND tmp.PostStatus IN (?,?);`, constants.GLPostStatusPostBatchNotExist, constants.GLPostStatusDefault, constants.GLPostStatusInvalid)
	if qr.HasAffectedRows {
		// Specified batch key ({1}) is not found in the tciBatchLog table.
		bq.Set(`INSERT INTO #tciError (EntryNo, BatchKey, StringNo, StringData1, StringData2, ErrorType, Severity, TranType, TranKey)
				SELECT NULL, tmp.GLBatchKey, 164027, '', CONVERT(VARCHAR(10), tmp.GLBatchKey),2,?,tmp.TranType,tmp.TranKey
				FROM   #tciTransToPost tmp
				WHERE  tmp.PostStatus = ?;`, constants.GLErrorFatal, constants.GLPostStatusPostBatchNotExist)
	}

	// Make sure only supported TranTypes are in #tciTransToPost.
	qr = bq.Set(`UPDATE #tciTransToPost
				SET PostStatus=?
				WHERE TranType NOT IN (?,?,?,?)
					AND PostStatus IN (?,?);`,
		constants.GLPostStatusTTypeNotSupported,
		constants.SOTranTypeCustShip, constants.SOTranTypeDropShip, constants.SOTranTypeTransShip, constants.SOTranTypeCustRtrn,
		constants.GLPostStatusDefault, constants.GLPostStatusInvalid)
	if qr.HasAffectedRows {
		// Batch {0}, Transaction {1}: Invalid transation type
		bq.Set(`INSERT INTO #tciError (EntryNo, BatchKey, StringNo, StringData1, StringData2, ErrorType, Severity, TranType, TranKey)
				SELECT NULL, tmp.GLBatchKey, 160151, bl.BatchID, tmp.TranID,2,?,tmp.TranType,tmp.TranKey
				FROM   #tciTransToPost tmp
					JOIN tciBatchLog bl WITH (NOLOCK) ON tmp.GLBatchKey = bl.BatchKey
				WHERE  tmp.PostStatus=?;`, constants.GLErrorFatal, constants.GLPostStatusTTypeNotSupported)
	}

	// SO Tran Types
	qr = bq.Get(`SELECT 1 FROM #tciTransToPost WHERE TranType IN (?,?,?,?) AND PostStatus IN (?,?);`,
		constants.SOTranTypeCustShip, constants.SOTranTypeDropShip, constants.SOTranTypeTransShip, constants.SOTranTypeCustRtrn,
		constants.GLPostStatusDefault, constants.GLPostStatusInvalid)
	if qr.HasData {
		// -- For SO transactions, mark those transaction where the TranStatus is NOT set to Committed.
		bq.Set(`UPDATE tmp
				SET tmp.PostStatus=?
				FROM  #tciTransToPost tmp
					JOIN tsoShipmentLog sl WITH (NOLOCK) ON tmp.TranKey = sl.ShipKey
				WHERE  tmp.TranType IN (?,?,?,?)
					AND tmp.PostStatus IN (?,?) AND sl.TranStatus <> ?;`,
			constants.GLPostStatusTranNotCommitted,
			constants.SOTranTypeCustShip, constants.SOTranTypeDropShip, constants.SOTranTypeTransShip, constants.SOTranTypeCustRtrn,
			constants.GLPostStatusDefault, constants.GLPostStatusInvalid,
			constants.SOShipLogCommitted)

		if !bq.OK() {
			bq.Waive()

			// -- {0} transactions have not been successfully Pre-Committed.
			bq.Set(`INSERT INTO #tciError (EntryNo,BatchKey,StringNo,StringData1,ErrorType,Severity,TranType,TranKey)
					SELECT NULL, tmp.GLBatchKey, 250893, tmp.TranID, 2, ?, tmp.TranType, tmp.TranKey
					FROM   #tciTransToPost tmp WHERE tmp.PostStatus=?;`, constants.GLErrorFatal, constants.GLPostStatusTranNotCommitted)
		}

		// -- --------------------------------------------------------------------------
		// -- Based on the TranType, populate #tciTransToPostDetl for those transactions
		// -- that are still valid.
		// -- --------------------------------------------------------------------------
		// -- Sales Order TranTypes:
		bq.Set(`INSERT INTO #tciTransToPostDetl (
					CompanyID,TranID,TranType,TranKey,InvtTranKey,GLBatchKey,
					PostStatus,PostingKey,SourceModuleNo,GLAcctKey,AcctRefKey,
					CurrID,PostDate,PostAmtHC )
				SELECT DISTINCT s.CompanyID, s.TranID, s.TranType, s.ShipKey, sl.InvtTranKey, tmp.GLBatchKey,
					tmp.PostStatus, gl.PostingKey, gl.SourceModuleNo, gl.GLAcctKey,	gl.AcctRefKey,
					gl.CurrID, gl.PostDate,	gl.PostAmtHC
				FROM #tciTransToPost tmp
					JOIN tsoShipment s WITH (NOLOCK) ON tmp.TranKey = s.ShipKey
					JOIN tsoShipLine sl WITH (NOLOCK) ON s.ShipKey = sl.ShipKey
					JOIN tglPosting gl WITH (NOLOCK) ON ( tmp.TranType = gl.TranType AND sl.InvtTranKey = gl.TranKey ) 
														OR 
														( gl.TranType=? AND sl.TransitInvtTranKey = gl.TranKey )
				WHERE tmp.PostStatus IN (?,?) AND tmp.TranType IN (?,?,?,?);`,
			constants.SOTranTypeTransIn,
			constants.GLPostStatusDefault, constants.GLPostStatusInvalid,
			constants.SOTranTypeCustShip, constants.SOTranTypeDropShip, constants.SOTranTypeTransShip, constants.SOTranTypeCustRtrn)
	}

	// -- Check if there is anything to process.  If any of the above validations failed,
	// -- then we will not post any transactions in the set.  This is the all or nothing approach.
	qr = bq.Get(`SELECT 1 FROM #tciTransToPostDetl;`)
	if !qr.HasData {
		return constants.ResultFail, oSessionID
	}

	// ------------------------------------------------
	// Create Logical Locks against the posting record:
	// ------------------------------------------------
	// Place a logical lock on the posting records that tie to the transactions found in #tciTransToPostDetl
	// so other processes trying to post the same records will get an exclusive lock error.

	bq.Set(`INSERT INTO #LogicalLocks (LogicalLockType,UserKey,LockType,LogicalLockID)
			SELECT 1, tmp.PostingKey, 2, 'GLPostTrans:' + CONVERT(VARCHAR(10), tmp.TranType) + ':' + CONVERT(VARCHAR(10), tmp.PostingKey)
			FROM #tciTransToPostDetl tmp WHERE tmp.PostStatus IN (?,?);`,
		constants.GLPostStatusDefault, constants.GLPostStatusInvalid)

	res, _, _ = LogicalLockAddMultiple(bq, true, loginID)
	if res == constants.ResultUnknown {
		return constants.ResultError, oSessionID
	}

	qr = bq.Get(`SELECT 1 FROM #LogicalLocks WHERE Status <> 1;`)
	if qr.HasData {

		//-- Tran {0}: User {1} currently has a lock against this transaction.
		bq.Set(`INSERT INTO #tciError (EntryNo,BatchKey,StringNo,StringData1,StringData2,StringData3,ErrorType,Severity,TranType,TranKey)
				SELECT DISTINCT NULL, tmp.GLBatchKey, 100524, tmp.TranID, ll.ActualUserID, '', 2, ?, tmp.TranType,	tmp.TranKey
				FROM tsmLogicalLock ll WITH (NOLOCK)
					JOIN #LogicalLocks tmpll ON ll.LogicalLockID=tmpll.LogicalLockID AND ll.LogicalLockType=tmpll.LogicalLockType
					JOIN #tciTransToPostDetl tmp ON tmpll.UserKey=tmp.PostingKey
				WHERE  tmpll.Status=102;`, constants.GLErrorFatal)

		// -- Exclusive lock not created due to existing locks.
		// -- Tran {0}: Unable to create a lock against this transaction.
		bq.Set(`INSERT INTO #tciError (EntryNo,BatchKey,StringNo,StringData1,StringData2,StringData3,ErrorType,Severity,TranType,TranKey)
				SELECT DISTINCT NULL, tmp.GLBatchKey, 100524, tmp.TranID, ll.ActualUserID, '', 2, ?, tmp.TranType, tmp.TranKey
				FROM tsmLogicalLock ll WITH (NOLOCK)
					JOIN #LogicalLocks tmpll ON ll.LogicalLockID=tmpll.LogicalLockID AND ll.LogicalLockType=tmpll.LogicalLockType
					JOIN #tciTransToPostDetl tmp ON tmpll.UserKey=tmp.PostingKey
				WHERE  tmpll.Status NOT IN (1,102);`, constants.GLErrorFatal)

		// -- NOT(Locked Successfully, Exclusive lock not created due to existing locks)
		// -- Mark those transactions which locks could not be created.  This will exclude
		// -- them from the list of transactions to be processed.
		bq.Set(`UPDATE tmp
				SET PostStatus=?
				FROM #tciTransToPostDetl tmp JOIN #LogicalLocks ll ON tmp.PostingKey=ll.UserKey
				WHERE ll.Status <> 1;`, constants.GLPostStatusTranLockedByUser)
	}

	// -- Validate the post dates of the posting records.
	// -- Making sure they fall within a valid GL period.
	qr = bq.Set(`UPDATE t1
				 SET t1.PostStatus=?
				 FROM #tciTransToPostDetl t1
					JOIN (SELECT DISTINCT tmp.TranKey, tmp.PostDate
							FROM #tciTransToPostDetl tmp
							WHERE  tmp.PostStatus IN (?,?)) PostDates ON t1.TranKey = PostDates.TranKey
					JOIN tglFiscalPeriod p WITH (NOLOCK) ON t1.CompanyID = p.CompanyID AND PostDates.PostDate BETWEEN p.StartDate AND p.EndDate
				WHERE  p.Status = 2;`,
		constants.GLPostStatusPostingClosedGLPeriod,
		constants.GLPostStatusDefault, constants.GLPostStatusInvalid)
	if qr.HasAffectedRows {
		// -- Transaction {0}: GL Fiscal Period for Posting Date {1} is Closed.
		bq.Set(`INSERT INTO #tciError(EntryNo,BatchKey,StringNo,StringData1,StringData2,ErrorType,Severity,TranType,TranKey)
				SELECT DISTINCT NULL,tmp.GLBatchKey,130317,tmp.TranID,CONVERT(VARCHAR(10), tmp.PostDate, 101),2,?,tmp.TranType,tmp.TranKey
				FROM #tciTransToPostDetl tmp
				WHERE tmp.PostStatus=?;`, constants.GLErrorFatal, constants.GLPostStatusPostingClosedGLPeriod)
	}

	//-- Finally, make sure the balance of the posting rows nets to zero.
	qr = bq.Set(`UPDATE t1
				 SET t1.PostStatus = ?
				 FROM #tciTransToPostDetl t1
					JOIN (SELECT tmp.GLBatchKey,tmp.TranKey,SUM(tmp.PostAmtHC) 'Balance'
						 FROM #tciTransToPostDetl tmp
						 WHERE  tmp.PostStatus IN (?,?)
						 GROUP  BY tmp.GLBatchKey, tmp.TranKey
						HAVING Sum(tmp.PostAmtHC) <> 0) BatchTot ON t1.GLBatchKey = BatchTot.GLBatchKey
				AND t1.TranKey = BatchTot.TranKey;`,
		constants.GLPostStatusDebitCreditNotEqual, constants.GLPostStatusDefault, constants.GLPostStatusInvalid)

	if qr.HasAffectedRows {
		// -- Transaction {0}: Debits and Credits do not equal.
		bq.Set(`INSERT INTO #tciError (EntryNo,BatchKey,StringNo,StringData1,StringData2,ErrorType,Severity,TranType,TranKey)
				SELECT DISTINCT NULL, tmp.GLBatchKey, 130315, tmp.TranID,'',2,?,tmp.TranType,tmp.TranKey
				FROM #tciTransToPostDetl tmp
				WHERE tmp.PostStatus=?;`,
			constants.GLErrorFatal, constants.GLPostStatusDebitCreditNotEqual)
	}

	//-- Update the parent table's PostStatus.
	bq.Set(`UPDATE tmp
			SET tmp.PostStatus=Detl.PostStatus
			FROM #tciTransToPost tmp
				JOIN #tciTransToPostDetl Detl ON tmp.TranKey = Detl.TranKey;`)

	//-- If a posting record is invalid, we need to delete the record as well as all of the other
	//-- posting records associated with the same transaction.
	bq.Set(`DELETE Detl
			FROM   #tciTransToPostDetl Detl
			WHERE  TranKey IN (SELECT TranKey 
							  FROM #tciTransToPostDetl 
							  WHERE PostStatus NOT IN (?,?));`,
		constants.GLPostStatusDefault, constants.GLPostStatusInvalid)

	// -- Check if there is anything to process.  If any of the above validations failed,
	// -- then we will not post any transactions in the set.  This is the all or nothing approach.
	qr = bq.Get(`SELECT 1 FROM #tciTransToPostDetl WHERE PostStatus NOT IN (?,?);`, constants.GLPostStatusDefault, constants.GLPostStatusInvalid)
	if qr.HasData {
		return constants.ResultFail, oSessionID
	}

	// -- ----------------------------
	// -- Start GL Account Validation:
	// -- ----------------------------
	// -- Validate the GL Accounts
	lInvalidAcctExist := false
	lGLSuspenseAcctKey := 0
	qr = bq.Get(`SELECT DISTINCT tmp.CompanyID, tmp.GLBatchKey, tmp.SourceModuleNo, tmp.PostDate, 1	FROM #tciTransToPostDetl tmp`)
	for _, v := range qr.Data {
		lCompanyID := v.ValueStringOrd(0)
		lGLBatchKey := int(v.ValueInt64Ord(1))
		lModuleNo := v.ValueInt64Ord(2)
		lPostDate := v.ValueTimeOrd(3)
		lIntegrateWithGL := 1

		lHomeCurrID := ""
		qr2 := bq.Get(`SELECT CurrID FROM tsmCompany WITH (NOLOCK) WHERE CompanyID=?;`, lCompanyID)
		if qr2.HasData {
			lHomeCurrID = qr2.First().ValueStringOrd(0)
		}

		lIsCurrIDUsed := false
		qr2 = bq.Get(`SELECT IsUsed FROM tmcCurrency WITH (NOLOCK) WHERE CurrID=?;`, lHomeCurrID)
		if qr2.HasData {
			lIsCurrIDUsed = qr2.First().ValueInt64Ord(0) == 1
		}

		lAutoAcctAdd := false
		lUseMultCurr := false
		lGLAcctMask := ""
		lAcctRefUsage := 0

		qr2 = bq.Get(`SELECT AutoAcctAdd, UseMultCurr, AcctMask, AcctRefUsage 
					  FROM tglOptions WITH (NOLOCK) WHERE CompanyID=?;`, lCompanyID)
		lAutoAcctAdd = qr2.First().ValueInt64Ord(0) == 1
		lUseMultCurr = qr2.First().ValueInt64Ord(1) == 1
		lGLAcctMask = qr2.First().ValueStringOrd(2)
		lAcctRefUsage = int(qr2.First().ValueInt64Ord(0))

		lLanguageID := sm.GetLanguage(bq)

		bq.Set(`INSERT #tglValidateAcct (GLAcctKey, AcctRefKey, CurrID, ValidationRetVal)
						SELECT DISTINCT tmp.GLAcctKey, tmp.AcctRefKey, tmp.CurrID, 0
						FROM #tciTransToPostDetl tmp
						WHERE tmp.GLBatchKey=?
							AND NOT EXISTS (SELECT 1 FROM #tglValidateAcct v
											WHERE tmp.GLAcctKey = v.GLAcctKey
												AND tmp.AcctRefKey = v.AcctRefKey
												AND tmp.CurrID = v.CurrID);`, lGLBatchKey)

		bq.Set(`TRUNCATE TABLE #tglPosting;`)

		bq.Set(`SET IDENTITY_INSERT #tglPosting ON;`)

		bq.Set(`INSERT INTO #tglPosting (
					PostingKey, AcctRefKey, BatchKey,       CurrID,
					ExtCmnt,    GLAcctKey,  JrnlKey,        JrnlNo,
					PostAmt,    PostAmtHC,  PostCmnt,       NatCurrBegBal,
					PostDate,   PostQty,    SourceModuleNo, Summarize,
					TranDate,   TranKey,    TranNo,         TranType)
				SELECT
					PostingKey, AcctRefKey, BatchKey,       CurrID,
					ExtCmnt,    GLAcctKey,  JrnlKey,        JrnlNo,
					PostAmt,    PostAmtHC,  PostCmnt,       NatCurrBegBal,
					PostDate,   PostQty,    SourceModuleNo, Summarize,
					TranDate,   TranKey,    TranNo,         TranType
				FROM tglPosting WITH (NOLOCK)
				WHERE PostingKey IN (SELECT PostingKey FROM #tciTransToPostDetl
					WHERE GLBatchKey=?);`, lGLBatchKey)

		bq.Set(`SET IDENTITY_INSERT #tglPosting OFF;`)

		// Call the routine to validate the accounts.
		rv, _, _ := SetAPIValidateAccount(bq, lCompanyID, lGLBatchKey, iSessionID, loginID, lLanguageID,
			lHomeCurrID, lIsCurrIDUsed, lAutoAcctAdd, lUseMultCurr,
			lGLAcctMask, lAcctRefUsage, false, true, -1, 1, 3, &lPostDate, true, true, true, true)

		if rv == constants.ResultError {
			goto Exit
		}
	}

	// Check if an account number failed validation

	qr = bq.Get(`SELECT 1 FROM #tglValidateAcct WHERE ValidationRetVal <> 0;`)
	if qr.HasData {

		// Update the PostStatus for those transactions that have an invalid account.
		bq.Set(`UPDATE tmp
				SET PostStatus=?
				FROM #tciTransToPostDetl tmp
					JOIN #tglValidateAcct BadGL	ON tmp.GLAcctKey = BadGL.GLAcctKey AND tmp.CurrID = BadGL.CurrID
					AND COALESCE(tmp.AcctRefKey, 0) = COALESCE(BadGL.AcctRefKey, 0)
				WHERE BadGL.ValidationRetVal <> 0;`, constants.GLPostStatusInvalid)

		// Transaction {0}: Invalid GL account.  Replaced with suspense account.
		bq.Set(`INSERT #tciError
					(EntryNo, BatchKey, StringNo,
					StringData1, StringData2,
					ErrorType, Severity, TranType,
					TranKey)
				SELECT DISTINCT
					NULL, tmp.GLBatchKey, 130316,
					tmp.TranID, '',
					2, ?, tmp.TranType,
					tmp.TranKey
				FROM #tciTransToPostDetl tmp
				WHERE tmp.PostStatus = ?`, constants.Warning, constants.GLPostStatusInvalid)

		lInvalidAcctExist = true

		if !optReplcInvalidAcctWithSuspense {
			res = constants.ResultFail
			goto Exit
		}
	}

	// -- -------------------------
	// -- Start GL Posting Routine:
	// -- -------------------------
	qr = bq.Get(`SELECT DISTINCT CompanyID, GLBatchKey, ModuleNo FROM #tciTransToPostDetl WHERE PostStatus IN (?, ?);`, constants.GLPostStatusDefault, constants.GLPostStatusInvalid)
	for _, v := range qr.Data {

		lCompanyID := v.ValueStringOrd(0)
		lGLBatchKey := int(v.ValueInt64Ord(1))
		lModuleNo := int(v.ValueInt64Ord(2))
		lPostDate := v.ValueTimeOrd(3)
		lIntegrateWithGL := true

		// Update tglPosting with the GLBatchKey we will be posting to.
		bq.Set(`UPDATE p
				SET p.BatchKey = tmp.GLBatchKey
				FROM tglPosting WITH (NOLOCK) p
					JOIN #tciTransToPostDetl tmp ON p.PostingKey = tmp.PostingKey
				WHERE tmp.PostStatus IN (?,?)
					AND tmp.GLBatchKey=?;`, constants.GLPostStatusDefault, constants.GLPostStatusInvalid, lGLBatchKey)

		if lInvalidAcctExist {
			lGLSuspenseAcctKey = 0
			qr2 := bq.Get(`SELECT SuspenseAcctKey FROM tglOptions WITH (NOLOCK) WHERE CompanyID=?;`, lCompanyID)
			if qr2.HasData {
				lGLSuspenseAcctKey = int(qr.First().ValueInt64Ord(0))
			}

			if lGLSuspenseAcctKey == 0 {
				goto Exit
			}

			// We need to update tglPosting with the suspense AcctKey for those GL accounts that failed.
			bq.Set(`UPDATE gl
					SET GLAcctKey = ?
					FROM tglPosting gl WITH (NOLOCK)
						JOIN (SELECT DISTINCT GLAcctKey, AcctRefKey, CurrID
								FROM #tglValidateAcct WHERE ValidationRetVal <> 0) BadGL
						ON gl.GLAcctKey = BadGL.GLAcctKey AND gl.CurrID = BadGL.CurrID
						AND COALESCE(gl.AcctRefKey, 0) = COALESCE(BadGL.AcctRefKey, 0)
					WHERE gl.BatchKey=?;`, lGLSuspenseAcctKey, lGLBatchKey)
		}

		// Summarize the GL Posting records based on the current posting settings.
		qr = bq.Set(`INSERT #tglPostingDetlTran (PostingDetlTranKey, TranType)
					 SELECT DISTINCT InvtTranKey, TranType
					 FROM #tciTransToPostDetl
					 WHERE PostStatus IN (?,?)
						AND GLBatchKey=?;`, constants.GLPostStatusDefault, constants.GLPostStatusInvalid, lGLBatchKey)

		res = SummarizeBatchlessTglPosting(bq, lCompanyID, lGLBatchKey, false)
		if res != constants.ResultSuccess {
			goto Exit
		}

		// Write out the GL Posting records to the report table.
		bq.Set(`INSERT #tglPostingRpt (
					AcctRefKey, BatchKey, CurrID, ExtCmnt, GLAcctKey, JrnlKey, JrnlNo, NatCurrBegBal, PostAmt, PostAmtHC,
					PostCmnt, PostDate, PostQty, SourceModuleNo, Summarize, TranDate, TranKey, TranNo, TranType)
				SELECT
					AcctRefKey, BatchKey, CurrID, ExtCmnt, GLAcctKey, JrnlKey, JrnlNo, NatCurrBegBal, PostAmt, PostAmtHC,
					PostCmnt, PostDate, PostQty, SourceModuleNo, Summarize, TranDate, TranKey, TranNo, TranType
				FROM tglPosting WITH (NOLOCK) WHERE BatchKey=?;`, lGLBatchKey)

		// 	-- ------------------------
		// -- GL Posting
		// -- ------------------------
		if optPostToGL {
			PostAPIGLPosting(bq, lGLBatchKey, lCompanyID, lModuleNo, lIntegrateWithGL)
		}
	}

Exit:
	sm.LogErrors(bq, oSessionID, oSessionID)

	sm.LogicalLockRemoveMultiple(bq)
}
