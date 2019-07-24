package main

import (
	"time"

	du "github.com/eaglebush/datautils"
)

// GLPostStatusConstant - General ledger post status constant
type GLPostStatusConstant int8

// GLErrorLevelConstant - error levels
type GLErrorLevelConstant int8

// GLPostStatusConstant - members of the constant
const (
	GLPostStatusDefault               GLPostStatusConstant = 0  // New Transaction, have not been processed (Default Value).
	GLPostStatusSuccess               GLPostStatusConstant = 1  // Posted successfully.
	GLPostStatusInvalid               GLPostStatusConstant = 2  // Invalid GL Account exists.  Not considered as a fatal error since it can be replaced with the suspense account.
	GLPostStatusTTypeNotSupported     GLPostStatusConstant = -1 // TranType not supported.
	GLPostStatusTranNotCommitted      GLPostStatusConstant = -2 // Transactions have not yet been committed.
	GLPostStatusPostBatchNotExist     GLPostStatusConstant = -3 // GL posting batch does not exists or is invalid.
	GLPostStatusPostingPriorSOPeriod  GLPostStatusConstant = -4 // Posting to a prior SO period.
	GLPostStatusPostingClosedGLPeriod GLPostStatusConstant = -5 // Posting to a closed GL period.
	GLPostStatusTranLockedByUser      GLPostStatusConstant = -6 // Transactions have been locked by another user.
	GLPostStatusDebitCreditNotEqual   GLPostStatusConstant = -7 // Debits and Credits do not equal.
)

// GLErrorLevelConstant - Error levels
const (
	GLErrorWarning GLErrorLevelConstant = 1
	GLErrorFatal   GLErrorLevelConstant = 2
)

// various constants
const lInterfaceError int = 3
const lFatalError int = 2
const lWarning int = 1

// helper function to create validation temp tables
func createAPIValidationTempTables(bq *du.BatchQuery) {
	bq.Set(`IF OBJECT_ID('tempdb..#tciErrorStg') IS NOT NULL
				TRUNCATE TABLE #tciErrorStg
			ELSE
				CREATE TABLE #tciErrorStg
					(GLAcctKey   int      NOT NULL
					,BatchKey    int      NOT NULL
					,StringNo    int      NOT NULL
					,StringData1 VARCHAR(30) NULL
					,StringData2 VARCHAR(30) NULL
					,StringData3 VARCHAR(30) NULL
					,StringData4 VARCHAR(30) NULL
					,StringData5 VARCHAR(30) NULL
					,ErrorType   smallint NOT NULL
					,Severity    smallint NOT NULL
				);`)

	bq.Set(`IF OBJECT_ID('tempdb..#tciError') IS NOT NULL
				TRUNCATE TABLE #tciError
			ELSE
				CREATE TABLE #tciError
					(EntryNo     int      NULL
					,BatchKey    int      NOT NULL
					,StringNo    int      NOT NULL
					,StringData1 VARCHAR(30) NULL
					,StringData2 VARCHAR(30) NULL
					,StringData3 VARCHAR(30) NULL
					,StringData4 VARCHAR(30) NULL
					,StringData5 VARCHAR(30) NULL
					,ErrorType   smallint NOT NULL
					,Severity    smallint NOT NULL
					,TranType    int      NULL
					,TranKey     int      NULL
					,InvtTranKey int      NULL
				);`)

	bq.Set(`IF OBJECT_ID('tempdb..#tglAcctMask') IS NOT NULL
				TRUNCATE TABLE #tglAcctMask
			ELSE
				CREATE TABLE #tglAcctMask (
				GLAcctNo       varchar(100) NOT NULL,
				MaskedGLAcctNo varchar(114) NULL
				);`)
}

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
	optPostToGL bool) (Result ResultConstant, SessionID int) {

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
			WHERE  PostStatus NOT IN (?,?,?,?,?,?,?,?,?);`, GLPostStatusDefault,
		GLPostStatusSuccess, GLPostStatusInvalid, GLPostStatusTTypeNotSupported,
		GLPostStatusTranNotCommitted, GLPostStatusPostingPriorSOPeriod,
		GLPostStatusPostingClosedGLPeriod, GLPostStatusTranLockedByUser, GLPostStatusDebitCreditNotEqual)

	// Make sure the rows in #tciTransToPost are Unique rows.
	bq.Set(`INSERT #UniqueTransToPost (CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus)
			SELECT DISTINCT CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus FROM #tciTransToPost;`)

	bq.Set(`TRUNCATE TABLE #tciTransToPost;`)

	bq.Set(`INSERT #tciTransToPost (CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus)
			SELECT CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus FROM #UniqueTransToPost;`)

	// These will be executed before the function exits
	defer LogicalLockRemoveMultiple(bq)
	defer LogErrors(bq, iSessionID, iSessionID)

	res := CreateBatchlessGLPostingBatch(bq, loginID, iBatchCmnt)
	if res != ResultSuccess {
		//-- This is a bad return value.  We should not proceed with the posting.
		return ResultError, iSessionID
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
						AND tmp.PostStatus IN (?,?);`, GLPostStatusPostBatchNotExist, GLPostStatusDefault, GLPostStatusInvalid)
	if qr.HasAffectedRows {
		// Specified batch key ({1}) is not found in the tciBatchLog table.
		bq.Set(`INSERT INTO #tciError (EntryNo, BatchKey, StringNo, StringData1, StringData2, ErrorType, Severity, TranType, TranKey)
				SELECT NULL, tmp.GLBatchKey, 164027, '', CONVERT(VARCHAR(10), tmp.GLBatchKey),2,?,tmp.TranType,tmp.TranKey
				FROM   #tciTransToPost tmp
				WHERE  tmp.PostStatus = ?;`, GLErrorFatal, GLPostStatusPostBatchNotExist)
	}

	// Make sure only supported TranTypes are in #tciTransToPost.
	qr = bq.Set(`UPDATE #tciTransToPost
				SET PostStatus=?
				WHERE TranType NOT IN (?,?,?,?)
					AND PostStatus IN (?,?);`,
		GLPostStatusTTypeNotSupported,
		SOTranTypeCustShip, SOTranTypeDropShip, SOTranTypeTransShip, SOTranTypeCustRtrn,
		GLPostStatusDefault, GLPostStatusInvalid)
	if qr.HasAffectedRows {
		// Batch {0}, Transaction {1}: Invalid transation type
		bq.Set(`INSERT INTO #tciError (EntryNo, BatchKey, StringNo, StringData1, StringData2, ErrorType, Severity, TranType, TranKey)
				SELECT NULL, tmp.GLBatchKey, 160151, bl.BatchID, tmp.TranID,2,?,tmp.TranType,tmp.TranKey
				FROM   #tciTransToPost tmp
					JOIN tciBatchLog bl WITH (NOLOCK) ON tmp.GLBatchKey = bl.BatchKey
				WHERE  tmp.PostStatus=?;`, GLErrorFatal, GLPostStatusTTypeNotSupported)
	}

	// SO Tran Types
	qr = bq.Get(`SELECT 1 FROM #tciTransToPost WHERE TranType IN (?,?,?,?) AND PostStatus IN (?,?);`,
		SOTranTypeCustShip, SOTranTypeDropShip, SOTranTypeTransShip, SOTranTypeCustRtrn,
		GLPostStatusDefault, GLPostStatusInvalid)
	if qr.HasData {
		// -- For SO transactions, mark those transaction where the TranStatus is NOT set to Committed.
		bq.Set(`UPDATE tmp
				SET tmp.PostStatus=?
				FROM  #tciTransToPost tmp
					JOIN tsoShipmentLog sl WITH (NOLOCK) ON tmp.TranKey = sl.ShipKey
				WHERE  tmp.TranType IN (?,?,?,?)
					AND tmp.PostStatus IN (?,?) AND sl.TranStatus <> ?;`,
			GLPostStatusTranNotCommitted,
			SOTranTypeCustShip, SOTranTypeDropShip, SOTranTypeTransShip, SOTranTypeCustRtrn,
			GLPostStatusDefault, GLPostStatusInvalid,
			SOShipLogCommitted)

		if !bq.OK() {
			bq.Waive()

			// -- {0} transactions have not been successfully Pre-Committed.
			bq.Set(`INSERT INTO #tciError (EntryNo,BatchKey,StringNo,StringData1,ErrorType,Severity,TranType,TranKey)
					SELECT NULL, tmp.GLBatchKey, 250893, tmp.TranID, 2, ?, tmp.TranType, tmp.TranKey
					FROM   #tciTransToPost tmp WHERE tmp.PostStatus=?;`, GLErrorFatal, GLPostStatusTranNotCommitted)
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
			SOTranTypeTransIn,
			GLPostStatusDefault, GLPostStatusInvalid,
			SOTranTypeCustShip, SOTranTypeDropShip, SOTranTypeTransShip, SOTranTypeCustRtrn)
	}

	// -- Check if there is anything to process.  If any of the above validations failed,
	// -- then we will not post any transactions in the set.  This is the all or nothing approach.
	qr = bq.Get(`SELECT 1 FROM #tciTransToPostDetl;`)
	if !qr.HasData {
		return ResultFail, oSessionID
	}

	// ------------------------------------------------
	// Create Logical Locks against the posting record:
	// ------------------------------------------------
	// Place a logical lock on the posting records that tie to the transactions found in #tciTransToPostDetl
	// so other processes trying to post the same records will get an exclusive lock error.

	bq.Set(`INSERT INTO #LogicalLocks (LogicalLockType,UserKey,LockType,LogicalLockID)
			SELECT 1, tmp.PostingKey, 2, 'GLPostTrans:' + CONVERT(VARCHAR(10), tmp.TranType) + ':' + CONVERT(VARCHAR(10), tmp.PostingKey)
			FROM #tciTransToPostDetl tmp WHERE tmp.PostStatus IN (?,?);`,
		GLPostStatusDefault, GLPostStatusInvalid)

	res, _, _ = LogicalLockAddMultiple(bq, true, loginID)
	if res == ResultUnknown {
		return ResultError, oSessionID
	}

	qr = bq.Get(`SELECT 1 FROM #LogicalLocks WHERE Status <> 1;`)
	if qr.HasData {

		//-- Tran {0}: User {1} currently has a lock against this transaction.
		bq.Set(`INSERT INTO #tciError (EntryNo,BatchKey,StringNo,StringData1,StringData2,StringData3,ErrorType,Severity,TranType,TranKey)
				SELECT DISTINCT NULL, tmp.GLBatchKey, 100524, tmp.TranID, ll.ActualUserID, '', 2, ?, tmp.TranType,	tmp.TranKey
				FROM tsmLogicalLock ll WITH (NOLOCK)
					JOIN #LogicalLocks tmpll ON ll.LogicalLockID=tmpll.LogicalLockID AND ll.LogicalLockType=tmpll.LogicalLockType
					JOIN #tciTransToPostDetl tmp ON tmpll.UserKey=tmp.PostingKey
				WHERE  tmpll.Status=102;`, GLErrorFatal)

		// -- Exclusive lock not created due to existing locks.
		// -- Tran {0}: Unable to create a lock against this transaction.
		bq.Set(`INSERT INTO #tciError (EntryNo,BatchKey,StringNo,StringData1,StringData2,StringData3,ErrorType,Severity,TranType,TranKey)
				SELECT DISTINCT NULL, tmp.GLBatchKey, 100524, tmp.TranID, ll.ActualUserID, '', 2, ?, tmp.TranType, tmp.TranKey
				FROM tsmLogicalLock ll WITH (NOLOCK)
					JOIN #LogicalLocks tmpll ON ll.LogicalLockID=tmpll.LogicalLockID AND ll.LogicalLockType=tmpll.LogicalLockType
					JOIN #tciTransToPostDetl tmp ON tmpll.UserKey=tmp.PostingKey
				WHERE  tmpll.Status NOT IN (1,102);`, GLErrorFatal)

		// -- NOT(Locked Successfully, Exclusive lock not created due to existing locks)
		// -- Mark those transactions which locks could not be created.  This will exclude
		// -- them from the list of transactions to be processed.
		bq.Set(`UPDATE tmp
				SET PostStatus=?
				FROM #tciTransToPostDetl tmp JOIN #LogicalLocks ll ON tmp.PostingKey=ll.UserKey
				WHERE ll.Status <> 1;`, GLPostStatusTranLockedByUser)
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
		GLPostStatusPostingClosedGLPeriod,
		GLPostStatusDefault, GLPostStatusInvalid)
	if qr.HasAffectedRows {
		// -- Transaction {0}: GL Fiscal Period for Posting Date {1} is Closed.
		bq.Set(`INSERT INTO #tciError(EntryNo,BatchKey,StringNo,StringData1,StringData2,ErrorType,Severity,TranType,TranKey)
				SELECT DISTINCT NULL,tmp.GLBatchKey,130317,tmp.TranID,CONVERT(VARCHAR(10), tmp.PostDate, 101),2,?,tmp.TranType,tmp.TranKey
				FROM #tciTransToPostDetl tmp
				WHERE tmp.PostStatus=?;`, GLErrorFatal, GLPostStatusPostingClosedGLPeriod)
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
		GLPostStatusDebitCreditNotEqual, GLPostStatusDefault, GLPostStatusInvalid)
	if qr.HasAffectedRows {
		// -- Transaction {0}: Debits and Credits do not equal.
		bq.Set(`INSERT INTO #tciError (EntryNo,BatchKey,StringNo,StringData1,StringData2,ErrorType,Severity,TranType,TranKey)
				SELECT DISTINCT NULL, tmp.GLBatchKey, 130315, tmp.TranID,'',2,?,tmp.TranType,tmp.TranKey
				FROM #tciTransToPostDetl tmp
				WHERE tmp.PostStatus=?;`,
			GLErrorFatal, GLPostStatusDebitCreditNotEqual)
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
		GLPostStatusDefault, GLPostStatusInvalid)

	// -- Check if there is anything to process.  If any of the above validations failed,
	// -- then we will not post any transactions in the set.  This is the all or nothing approach.
	qr = bq.Get(`SELECT 1 FROM #tciTransToPostDetl WHERE PostStatus NOT IN (?,?);`, GLPostStatusDefault, GLPostStatusInvalid)
	if qr.HasData {
		return ResultFail, oSessionID
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

		lLanguageID := GetLanguage(bq)

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

		if rv == ResultError {
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
				WHERE BadGL.ValidationRetVal <> 0;`, GLPostStatusInvalid)

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
				WHERE tmp.PostStatus = ?`, lWarning, GLPostStatusInvalid)

		lInvalidAcctExist = true

		if !optReplcInvalidAcctWithSuspense {
			res = ResultFail
			goto Exit
		}
	}

	// -- -------------------------
	// -- Start GL Posting Routine:
	// -- -------------------------
	qr = bq.Get(`SELECT DISTINCT CompanyID, GLBatchKey, ModuleNo FROM #tciTransToPostDetl WHERE PostStatus IN (?, ?);`, GLPostStatusDefault, GLPostStatusInvalid)
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
					AND tmp.GLBatchKey=?;`, GLPostStatusDefault, GLPostStatusInvalid, lGLBatchKey)

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
						AND GLBatchKey=?;`, GLPostStatusDefault, GLPostStatusInvalid, lGLBatchKey)

		res = SummarizeBatchlessTglPosting(bq, lCompanyID, lGLBatchKey, false)
		if res != ResultSuccess {
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
	LogErrors(bq, oSessionID, oSessionID)

	LogicalLockRemoveMultiple(bq)
}

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
func CreateBatchlessGLPostingBatch(bq *du.BatchQuery, loginID string, iBatchCmnt string) ResultConstant {
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
		SOTranTypeCustShip, SOTranTypeTransShip, SOTranTypeCustRtrn)
	if qr.HasData {
		for _, v := range qr.Data {
			cid := v.ValueString("CompanyID")
			mod := ModuleConstant(v.ValueInt64("ModuleNo"))
			bt := int(v.ValueInt64("BatchType"))
			pdt := v.ValueTime("PostDate")
			idt := v.ValueTime("InvcDate")

			res, batchKey, _ := GetNextBatch(bq, cid, mod, bt, loginID, iBatchCmnt, pdt, 0, &idt)

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
		return ResultError
	}

	return ResultSuccess
}

// SetAPIValidateAccount -Validates GL Accounts to be Posted to GL Module.
//
// This stored procedure takes a set of GL accounts from a temporary
// table called #tglValidateAcct and validates them in the same way that
// the spglAPIValidateAccount sp validates GL accounts one at a time.
// This sp replaces the spglAPIValidateAccount sp which only operated on
// one row at a time (one GL Account) and was called repetitively by the
// spglAPIAcctPostRow sp in the subsidiary modules.
// This new sp will only be called once by the spglSetAPIAcctPostRow sp.
//
// This stored procedure ASSUMES:
//       (1)  The existence of a temporary table called #tglValidateAcct.
//       (2)  That #tglValidateAcct has been correctly populated with n rows
//            of distinct combinations of GLAcctKey+AcctRefKey+CurrID.
//       (3)  That all GLAcctKey's in #tglValidateAcct are only for @iCompanyID.
//       (4)  That if a @iVerifyParams value other than one (1) is passed in,
//            all parameter values in the NOTE below are guaranteed to be valid.
//       (5)  The calling program is NOT relying on GL Accounts to be created
//            if the AutoAcctAdd option is ON in tglOptions.  No GL Accounts
//            are created when this sp is used for validation.
//       (6)  The calling program is NOT relying on Account Reference Codes to
//            be created if AcctRefUsage is set to '2' in tglOptions.  No Account
//            Reference Codes are created when this sp is used for validation.
// Use this sp with other Acuity API's that begin with spglSetAPI...
//
// Input Parameters:
//    @iCompanyID        = [IN: Valid Acuity Company; No Default]
//    @iBatchKey         = [IN: Valid Batch Key or NULL; Default = NULL]
//    @ioSessionID     = [IN/OUT: Valid No. or NULL; No Default]
//    @iUserID           = [IN: Valid User or NULL; Default = spGetLoginName]
//    @iLanguageID       = [IN: Valid Language ID or NULL; Default = NULL]
//    @iHomeCurrID       = [IN: Valid Curr ID for @iCompanyID or NULL; Default = NULL]
//    @iIsCurrIDUsed     = [IN: 0, 1 or NULL; Default = 0]
//    @iAutoAcctAdd      = [IN: 0, 1 or NULL; Default = 0]
//    @iUseMultCurr      = [IN: 0, 1 or NULL; Default = 0]
//    @iGLAcctMask       = [IN: A Valid GL Account Mask or NULL; Default = NULL]
//    @iAcctRefUsage     = [IN: 0, 1 or NULL; Default = 0]
//    @iAllowWildCard    = [IN: 0, 1 or NULL; Default = 0]
//    @iAllowActiveOnly  = [IN: 0, 1 or NULL; Default = 1]
//    @iFinancial        = [IN: 0, 1 or NULL; Default = NULL]
//    @iPostTypeFlag     = [IN: 0, 1 or NULL; Default = 1]
//    @iPostingType      = [IN: 1, 2, 3 or NULL; Default = 3]
//    @iEffectiveDate    = [IN: Effective Date or NULL]
//    @iVerifyParams     = [IN: 0, 1 or NULL; Default = 1]
//    @iValidateGLAccts  = [IN: 0, 1 or NULL; Default = 1]
//    @iValidateAcctRefs = [IN: 0, 1 or NULL; Default = 1]
//    @iValidateCurrIDs  = [IN: 0, 1 or NULL; Default = 1]
//
// NOTE: The following parameters MUST be passed in with a valid value from the
// calling stored procedure IF the @iVerifyParams parameter is passed in
// with a value of anything OTHER THAN one (1):
//    @iCompanyID
//    @ioSessionID
//    @iUserID
//    @iLanguageID
//    @iHomeCurrID
//    @iIsCurrIDUsed
//    @iAutoAcctAdd
//    @iUseMultCurr
//    @iGLAcctMask
//    @iAcctRefUsage
//
// Output Parameters:
//    @ioSessionID = [IN/OUT: Valid No. or NULL; No Default]
//    @oSeverity     = [OUT: 0=None, 1=Warning, 2=Fatal; Default=0]
//    @oRetVal       = [OUT: return flag indicating outcome of the procedure]
//           0 = Failure.  General SP Failure.
//           1 = Successful.
//           4 = Failure.  Masked GL account not allowed.
//           9 = Failure.  Account doesn't exist not for the Company supplied (or at all).
//          10 = Failure.  Account Key supplied does not exist.
//          12 = Failure.  Account exists.  Failure of Active Account Restriction.
//          13 = Failure.  Account exists.  Failure of Effective Dates Restriction.
//          14 = Warning Only.  Account exists.  Failure of Home Currency Only Restriction.
//          15 = Warning Only.  Account exists.  Failure of Specific Currency = Home Currency Restriction.
//          16 = Failure.  Account Exists.  Failure of Currency not Specific Currency Restriction.
//          17 = Failure.  Account exists.  Failure of Financial Type Restriction.
//          19 = Failure.  Error Log Key not supplied and cannot be derived.
//          20 = Failure.  Company ID not supplied.
//          21 = Failure.  Company ID supplied does not exist or has no Home Currency ID.
//          23 = Failure.  Currency ID for this Company exists but is not used in MC.
//          24 = Failure.  GL Options row for this Company does not exist.
//          25 = Failure.  Currency ID for this Company does not exist in MC.
//          26 = Failure.  Multicurrency is not enabled for entered Company.
//          27 = Failure.  Account Reference Key exists but not for the correct Company.
//          30 = Failure.  Account Reference Key supplied does not exist.
//          31 = Failure.  Failure of Account Reference Code Account Segments Restriction.
//          32 = Failure.  Failure of Account Reference Code Effective Dates Restriction.
//          33 = Failure.  User ID not supplied and cannot be derived.
//          34 = Failure.  Language ID cannot be determined.
//          37 = Failure.  Account Reference Code is not active.
//          38 = Failure.  Accounts exists.  Failure of Posting Type Restriction.
//          42 = Failure.  tglOptions.AcctRefUsage Flag not enabled.
//          43 = Failure.  GL Account requires an Account Reference Code.
func SetAPIValidateAccount(
	bq *du.BatchQuery,
	iCompanyID string,
	iBatchKey int,
	iSessionID int,
	iUserID string,
	iLanguageID int,
	iHomeCurrID string,
	iIsCurrIDUsed bool,
	iAutoAcctAdd bool,
	iUseMultCurr bool,
	iGLAcctMask string,
	iAcctRefUsage int,
	iAllowWildCard bool,
	iAllowActiveOnly bool,
	iFinancial int,
	iPostTypeFlag int,
	iPostingType int,
	iEffectiveDate *time.Time,
	iVerifyParams bool,
	iValidateGLAccts bool,
	iValidateAcctRefs bool,
	iValidateCurrIDs bool) (Result ResultConstant, Severity int, SessionID int) {

	var qr du.QueryResult
	var lErrMsgNo int

	bq.ScopeName("SetAPIValidateAccount")

	createAPIValidationTempTables(bq)

	switch iFinancial {
	case 0, 1:
		break
	default:
		iFinancial = -1 // Default
	}

	if iPostTypeFlag != -1 {
		switch iPostTypeFlag {
		case 0, 1:
			break
		default:
			iPostTypeFlag = 1 // Default
		}
	}

	switch iPostingType {
	case 1, 2, 3:
		break
	default:
		iPostingType = 3
	}

	const lInvalidCurr int = 19103
	const lNotUsedCurr int = 19104

	lLanguageID := iLanguageID
	lIsCurrIDUsed := iIsCurrIDUsed
	lAcctRefUsage := iAcctRefUsage
	lAutoAcctAdd := iAutoAcctAdd
	lUseMultCurr := iUseMultCurr
	lGLAcctMask := iGLAcctMask
	lHomeCurrID := iHomeCurrID

	if iVerifyParams {

		if iSessionID == 0 {
			iSessionID = GetNextSurrogateKey(bq, "tciErrorLog")
			if iSessionID == 0 {
				return ResultConstant(19), 2, 0
			}
		}

		if iUserID == "" {
			return ResultConstant(33), 2, 0
		}

		if iUserID != "" {
			qr = bq.Get(`SELECT MIN(LanguageID)	FROM tsmUser WITH (NOLOCK) WHERE UserID=?;`, iUserID)
			if qr.HasData {
				lLanguageID = int(qr.First().ValueInt64Ord(0))
			}

			if lLanguageID == 0 {
				return ResultConstant(34), 2, 0
			}
		}

		if iCompanyID == "" {
			LogError(bq, iBatchKey, 0, 19101, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(20), 2, 0
		}

		// CompanyID must be valid (Get CurrID in the process)

		qr = bq.Get(`SELECT CurrID FROM tsmCompany WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
		if !qr.HasData {
			LogError(bq, iBatchKey, 0, 19102, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(21), 2, 0
		}

		// Does the Home Currency Exist?
		qr = bq.Get(`SELECT IsUsed FROM tmcCurrency WITH (NOLOCK) WHERE CurrID=?;`, iHomeCurrID)
		if !qr.HasData {
			LogError(bq, iBatchKey, 0, lInvalidCurr, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(25), 2, 0
		}
		lIsCurrIDUsed = qr.First().ValueInt64Ord(0) == 1

		if !lIsCurrIDUsed {
			LogError(bq, iBatchKey, 0, lNotUsedCurr, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(23), 2, 0
		}

		// Get the GL Options information. (Just check if this information exists on the company)
		qr = bq.Get(`SELECT  AutoAcctAdd, UseMultCurr, AcctMask, AcctRefUsage FROM tglOptions WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
		if !qr.HasData {
			LogError(bq, iBatchKey, 0, 19105, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(24), 2, 0
		}
		lAutoAcctAdd = qr.First().ValueInt64Ord(0) == 1
		lUseMultCurr = qr.First().ValueInt64Ord(1) == 1
		lGLAcctMask = qr.First().ValueStringOrd(2)
		lAcctRefUsage = int(qr.First().ValueInt64Ord(3))
	}

	// Validate the GL accounts in #tglValidateAcct now
	lErrorsOccurred := false
	lValidateAcctRetVal := ResultError
	lValidateAcctRefRetVal := ResultError
	lValidateAcctRefSeverity := 0
	lMaxAccountSegments := 0
	lAcctRefValFail := 0
	oSeverity := 0

	const lMissingAcctKey int = 19200
	const lInvalidAcctCo int = 19214
	const lMaskedGLAcct int = 19202
	const lInactiveGLAcct int = 19206
	const lDeletedGLAcct int = 1921
	const lNonFinlGLAcct int = 19207
	const lFinlGLAcct int = 19208
	const lFinlPostType int = 19240
	const lStatPostType int = 19241
	const lGLAcctStartDateError int = 19212
	const lGLAcctEndDateError int = 19213
	const lAcctRefSegs int = 19223
	const lMultCurrError int = 19112
	const lInvalidHomeCurr int = 19210
	const lCurrIsHomeCurr int = 19210
	const lNotSpecificCurrency int = 19216
	const lConvertToMMDDYYYYDate int = 101

	if iValidateGLAccts {

		/* -------------- Make sure all GL accounts exist in tglAccount -------------- */
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal=25, ErrorMsgNo = @lInvalidCurr
					 WHERE CurrID NOT IN (SELECT CurrID 
										 FROM tmcCurrency WITH (NOLOCK))
										 AND COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
						AND ValidationRetVal=0;`)
		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 10
			oSeverity = lFatalError

			bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
					SELECT GLAcctKey, ?, ?,	?, GLAcctKey, '',  '', '', '', ?
					FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 10
						AND ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, lMissingAcctKey, lMissingAcctKey)
			goto FinishFunc
		}

		/* -------------- Make sure all GL accounts exist in tglAccount for this Company. -------------- */
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 9, ErrorMsgNo=?
					 WHERE GLAcctKey NOT IN (SELECT GLAcctKey FROM tglAccount WITH (NOLOCK)	WHERE CompanyID = ?)
						 AND ValidationRetVal = 0;`, lInvalidAcctCo, iCompanyID)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 9
			oSeverity = lFatalError

			bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

			// Format the GL accounts used in the error message
			bq.Set(`INSERT INTO #tglAcctMask
					SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
					FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID=?
						AND a.ValidationRetVal = 9
						AND a.ErrorMsgNo=?;`, iCompanyID, lInvalidAcctCo)

			// Populate the temporary error log
			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), ?, '', '', '',	?
					FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo
						AND a.ValidationRetVal = 9
						AND a.ErrorMsgNo=?;`, iBatchKey, lInterfaceError, lFatalError, iCompanyID, lInvalidAcctCo, lInvalidAcctCo)
		}

		/* -------------- Check for Mask Characters in the GL Account Number -------------- */
		if iAllowWildCard {
			qr = bq.Set(`UPDATE #tglValidateAcct
							SET ValidationRetVal = 4, ErrorMsgNo=?
						WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE CHARINDEX('*', GLAcctNo) > 0)
							AND ValidationRetVal=0;`, lMaskedGLAcct)
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 4
				oSeverity = lFatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo,	c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK),	vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID=?
							AND a.ValidationRetVal = 4
							AND a.ErrorMsgNo=?;`, iCompanyID, lInvalidAcctCo)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), '', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 4
							AND a.ErrorMsgNo=?;`, iBatchKey, lInterfaceError, lFatalError, lMaskedGLAcct, lMaskedGLAcct)
			}
		}

		/* -------------- Active Account Validation -------------- */
		if iAllowActiveOnly {
			qr = bq.Set(`UPDATE #tglValidateAcct
							SET ValidationRetVal = 12, ErrorMsgNo=?
						 WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE Status = 2)
							AND ValidationRetVal=0;`, lInactiveGLAcct)
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 12
				oSeverity = lFatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo 
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID=?
							AND a.ValidationRetVal = 12
							AND a.ErrorMsgNo = ?;`, iCompanyID, lInactiveGLAcct)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), '', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 12
							AND a.ErrorMsgNo=?;`, iBatchKey, lInterfaceError, lFatalError, lInactiveGLAcct, lInactiveGLAcct)

			}

			// check for deleted GL accounts
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 12, ErrorMsgNo = ?
						WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE Status = 3)
						AND ValidationRetVal = 0;`, lDeletedGLAcct)
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 12
				oSeverity = lFatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo,	c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 12
							AND a.ErrorMsgNo = ?;`, iCompanyID, lDeletedGLAcct)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), '', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK),	#tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 12
							AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, lDeletedGLAcct, lDeletedGLAcct)
			}
		}

		/* -------------- Financial Account Restriction -------------- */
		if iFinancial != -1 {

			iFinGL := 0
			iFinExpression := ``

			/* Allow Financial Accounts Only */
			if iFinancial == 1 {
				iFinGL = lNonFinlGLAcct
				iFinExpression = `=`
			}

			/* Allow Non-Financial Accounts Only */
			if iFinancial == 0 {
				iFinGL = lFinlGLAcct
				iFinExpression = `<>`
			}

			qr = bq.Set(`UPDATE #tglValidateAcct
							SET ValidationRetVal = 17, ErrorMsgNo = ?
							WHERE GLAcctKey IN (SELECT GLAcctKey 
												FROM tglAccount a WITH (NOLOCK),
														tglNaturalAcct b WITH (NOLOCK),
														tglAcctType c WITH (NOLOCK)
												WHERE a.NaturalAcctKey = b.NaturalAcctKey
												AND b.AcctTypeKey = c.AcctTypeKey
												AND a.CompanyID = ?
												AND c.AcctTypeID `+iFinExpression+` 901)
							AND ValidationRetVal = 0;`, iFinGL, iCompanyID)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 17
				oSeverity = lFatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo,	c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 17
							AND a.ErrorMsgNo = ?;`, iCompanyID, iFinGL)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,	?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), '', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 17
							AND a.ErrorMsgNo=?;`, iBatchKey, lInterfaceError, lFatalError, iFinGL, iFinGL)
			}
		}

		/* -------------- Post Type Restriction  -------------- */
		if iPostTypeFlag != -1 {

			iPostTF := 0
			iPostExpression := ``

			/* Allow Financial Accounts Only */
			if iPostTypeFlag == 1 {
				iPostTF = lFinlPostType
				iPostExpression = `(1,3)`
			}

			/* Allow Non-Financial Accounts Only */
			if iPostTypeFlag == 0 {
				iPostTF = lStatPostType
				iPostExpression = `(2,3)`
			}

			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 38, ErrorMsgNo = ?
						WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE CompanyID = @iCompanyID
												AND Status = 1
												AND PostingType NOT IN `+iPostExpression+`)
							AND ValidationRetVal=0;`, iPostTF)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 38
				oSeverity = lFatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
							c.FormattedGLAcctNo  /* MaskedGLAcctNo */
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
							AND a.ValidationRetVal = 38
							AND a.ErrorMsgNo = ?;`, iCompanyID, iPostTF)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo),'', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 38
							AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, iPostTF, iPostTF)
			}
		}

		/* -------------- Effective Date Restrictions -------------- */
		if iEffectiveDate != nil {

			// Check Effective Start Date
			qr = bq.Set(`UPDATE #tglValidateAcct
						 SET ValidationRetVal = 13, ErrorMsgNo = ?
						 WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE CompanyID =?
												AND Status = 1
												AND EffStartDate IS NOT NULL
												AND EffStartDate > '?')
							AND ValidationRetVal = 0;`, lGLAcctStartDateError, iCompanyID, iEffectiveDate.Format(`2006-01-02`))

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 13
				oSeverity = lFatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 13
							AND a.ErrorMsgNo = ?;`, iCompanyID, lGLAcctStartDateError)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(10), '?', ?), 
								CONVERT(VARCHAR(30), c.MaskedGLAcctNo),  
								CONVERT(VARCHAR(10), b.EffStartDate, ?), '', '', ?
							FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
							WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 13
							AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, iEffectiveDate.Format(`2006-01-02`), lConvertToMMDDYYYYDate,
					lConvertToMMDDYYYYDate, lGLAcctStartDateError, lGLAcctStartDateError)

			}

			// Check Effective End Date
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 13, ErrorMsgNo = ?
						WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE CompanyID =?
												AND Status = 1
												AND EffEndDate IS NOT NULL
												AND EffEndDate < '?')
							AND ValidationRetVal = 0;`, lGLAcctEndDateError, iCompanyID, iEffectiveDate.Format(`2006-01-02`))
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 13
				oSeverity = lFatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo,	c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 13
							AND a.ErrorMsgNo = ?;`, iCompanyID, lGLAcctEndDateError)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?,  CONVERT(VARCHAR(10), '?', ?),
							CONVERT(VARCHAR(30), c.MaskedGLAcctNo), 
							CONVERT(VARCHAR(10), b.EffEndDate, ?), '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 13
							AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, iEffectiveDate.Format(`2006-01-02`), lConvertToMMDDYYYYDate,
					lConvertToMMDDYYYYDate, lGLAcctEndDateError, lGLAcctEndDateError)
			}
		}
	}

	// Validate the Account Reference ID's in #tglValidateAcct now
	if iValidateAcctRefs && iAcctRefUsage != 0 {

		lValidateAcctRefRetVal, lValidateAcctRefSeverity, iSessionID = SetAPIValidateAcctRef(bq, iCompanyID, iBatchKey, iSessionID, iUserID, iLanguageID, iAcctRefUsage, iEffectiveDate, false)

		/* Did the Account Reference Code validation go OK? */
		switch lValidateAcctRefRetVal {
		case 19, 20, 21, 23, 24, 25, 30, 33, 34:
			lAcctRefValFail = 1
			lValidateAcctRetVal = lValidateAcctRefRetVal

			goto FinishFunc
		}

		if lValidateAcctRefRetVal != 0 || lValidateAcctRefRetVal != 1 {
			lValidateAcctRetVal = lValidateAcctRefRetVal
		}

		if lValidateAcctRefSeverity > 0 && oSeverity != 2 {
			oSeverity = lValidateAcctRefSeverity
		}

		// Verify that Account Reference Codes are valid for all Account Segments
		if lAcctRefUsage == 1 {

			qr = bq.Get(`SELECT COUNT(SegmentKey) FROM tglSegment WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
			lMaxAccountSegments = int(qr.First().ValueInt64Ord(0))

			if lMaxAccountSegments > 0 {

				// Validating that the ARCs are valid for all Account Segments
				qr = bq.Set(`UPDATE #tglValidateAcct
							SET ValidationRetVal = 31, ErrorMsgNo = ?
							WHERE AcctRefKey NOT IN
							(SELECT c.AcctRefKey
								FROM tglAcctSegment a WITH (NOLOCK),
										tglAcctRefUsage b WITH (NOLOCK),
										tglAcctRef c WITH (NOLOCK),
										(SELECT DISTINCT AcctRefKey, GLAcctKey 
											FROM #tglValidateAcct WITH (NOLOCK)) d
											WHERE a.SegmentKey = b.SegmentKey
												AND a.AcctSegValue = b.AcctSegValue
												AND b.AcctRefGroupKey = c.AcctRefGroupKey
												AND d.GLAcctKey = a.GLAcctKey
												AND d.AcctRefKey = c.AcctRefKey
											GROUP BY d.GLAcctKey, c.AcctRefKey
											HAVING COUNT(c.AcctRefKey) = ?)
							AND COALESCE(DATALENGTH(AcctRefKey), 0) > 0
							AND ValidationRetVal = 0;`, lAcctRefSegs, lMaxAccountSegments)

				if qr.HasAffectedRows {

					lErrorsOccurred = true
					lValidateAcctRetVal = 31
					oSeverity = lFatalError

					bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

					bq.Set(`INSERT INTO #tglAcctMask
							SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
							FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
							WHERE a.GLAcctKey = b.GLAcctKey
								AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
								AND a.ValidationRetVal = 31
								AND a.ErrorMsgNo = ?;`, iCompanyID, lAcctRefSegs)

					bq.Set(`INSERT INTO #tciErrorStg (
								GLAcctKey,   BatchKey,    ErrorType,   Severity, 
								StringData1, StringData2, StringData3, StringData4, 
								StringData5, StringNo)
							SELECT a.GLAcctKey,	?, ?, ?, CONVERT(VARCHAR(30), c.AcctRefCode), CONVERT(VARCHAR(30), d.MaskedGLAcctNo), '', '', '', ?
							FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), tglAcctRef c WITH (NOLOCK), #tglAcctMask d WITH (NOLOCK)
							WHERE a.GLAcctKey = b.GLAcctKey
								AND a.AcctRefKey = c.AcctRefKey
								AND b.GLAcctNo = d.GLAcctNo
								AND a.ValidationRetVal = 31
								AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, lAcctRefSegs, lAcctRefSegs)
				}
			}
		}
	}

	// Validate the Currency ID's in #tglValidateAcct now
	if iValidateCurrIDs {

		// Validating that Currency IDs exist in tmcCurrency
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 25,	ErrorMsgNo = ?
					 WHERE CurrID NOT IN (SELECT CurrID 
										FROM tmcCurrency WITH (NOLOCK))
										AND COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
						AND ValidationRetVal = 0;`, lInvalidCurr)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 25
			oSeverity = lFatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT GLAcctKey, ?, ?, ?, CurrID, '', '', '', '', FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 25 AND ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, lInvalidCurr, lInvalidCurr)

			goto FinishFunc

		}

		// Validating that Currency IDs are used in tmcCurrency
		qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 23, ErrorMsgNo = ?
						WHERE CurrID IN (SELECT CurrID FROM tmcCurrency WITH (NOLOCK) WHERE IsUsed = 0)
						AND ValidationRetVal = 0;`, lNotUsedCurr)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 23
			oSeverity = lFatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT GLAcctKey, ?, ?, ?, ?, CurrID, '', '', '', '', ?
					FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 23 AND ErrorMsgNo=?;`, iBatchKey, lInterfaceError, lFatalError, lNotUsedCurr, lNotUsedCurr)
		}

		//Make sure CurrID's are Home Currency IF Multicurrency is NOT used.
		if !lUseMultCurr {

			// Validating that Curr IDs are Home Curr IDs (No MC)
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 26, ErrorMsgNo=?
						WHERE CurrID <> ? AND ValidationRetVal = 0;`, lMultCurrError, lHomeCurrID)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 26
				oSeverity = lFatalError

				bq.Set(`INSERT #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT GLAcctKey, ?, ?, ?, ?, '', '', '', '', ?
						FROM #tglValidateAcct WITH (NOLOCK)
						WHERE ValidationRetVal = 26 AND ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, iCompanyID, lMultCurrError, lMultCurrError)

			}

		}

		// Multicurrency Restriction
		if lUseMultCurr {

			// Validating that GL Accounts don't violate Home Curr Only restriction
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 14, ErrorMsgNo = ?
						WHERE COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
						AND CurrID <> ?
						AND GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount a WITH (NOLOCK),
												tglNaturalAcct b WITH (NOLOCK),
												tglAcctType c WITH (NOLOCK)
											WHERE a.NaturalAcctKey = b.NaturalAcctKey
											AND b.AcctTypeKey = c.AcctTypeKey
											AND a.CompanyID = ?
											AND a.CurrRestriction = 0
											AND c.AcctTypeID <> 901)
						AND ValidationRetVal = 0;`, lInvalidHomeCurr, lHomeCurrID, iCompanyID)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 14
				if oSeverity != lFatalError {
					oSeverity = lWarning
				}

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey	AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 14
							AND a.ErrorMsgNo = ?;`, iCompanyID, lInvalidHomeCurr)

				bq.Set(`INSERT #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), a.CurrID, 'Specific Foreign Curr',	b.RestrictedCurrID,	a.CurrID, ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 15
							AND a.ErrorMsgNo=?;`, iBatchKey, lInterfaceError, lWarning, lCurrIsHomeCurr, lCurrIsHomeCurr)
			}

			// Specific Foreign Currency Restriction #1 (Check Financial Accounts Only)
			// Validating that GL Accounts don't violate Specific Foreign Curr restriction (#1)
			qr = bq.Set(`UPDATE #tglValidateAcct
						 SET ValidationRetVal = 15, ErrorMsgNo = ?
						 WHERE COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
							AND CurrID = ?
							AND GLAcctKey IN (SELECT GLAcctKey 
												FROM tglAccount a WITH (NOLOCK), tglNaturalAcct b WITH (NOLOCK), tglAcctType c WITH (NOLOCK)
												WHERE a.NaturalAcctKey = b.NaturalAcctKey
													AND b.AcctTypeKey = c.AcctTypeKey
													AND a.CompanyID = ?
													AND a.CurrRestriction = 1
													AND c.AcctTypeID <> 901)
							AND ValidationRetVal = 0;`, lCurrIsHomeCurr, lHomeCurrID, iCompanyID)
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 15
				if oSeverity != lFatalError {
					oSeverity = lWarning
				}

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
							AND a.ValidationRetVal = 15
							AND a.ErrorMsgNo=?;`, lCurrIsHomeCurr)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,	?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), a.CurrID, 'Specific Foreign Curr', b.RestrictedCurrID, a.CurrID, ?
						FROM #tglValidateAcct a WITH (NOLOCK),	tglAccount b WITH (NOLOCK),	#tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 15
							AND a.ErrorMsgNo=?;`, iBatchKey, lInterfaceError, lWarning, lCurrIsHomeCurr, lCurrIsHomeCurr)
			}

			// Specific Foreign Currency Restriction #2 (Check Financial Accounts Only)
			// Validating that GL Accounts don't violate Specific Foreign Curr restriction (#2)
			qr = bq.Set(`UPDATE t
						SET #tglValidateAcct.ValidationRetVal = 16,	#tglValidateAcct.ErrorMsgNo=?
						FROM #tglValidateAcct t, tglAccount a WITH (NOLOCK), tglNaturalAcct b WITH (NOLOCK), tglAcctType c WITH (NOLOCK)
						WHERE t.GLAcctKey = a.GLAcctKey
							AND a.NaturalAcctKey = b.NaturalAcctKey
							AND b.AcctTypeKey = c.AcctTypeKey
							AND a.CompanyID = ?
							AND a.CurrRestriction = 1
							AND c.AcctTypeID <> 901
							AND COALESCE(DATALENGTH(LTRIM(RTRIM(t.CurrID))), 0) > 0
							AND t.CurrID <> ?
							AND t.CurrID <> a.RestrictedCurrID
							AND t.ValidationRetVal = 0;`, lNotSpecificCurrency, iCompanyID, lHomeCurrID)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 16
				oSeverity = lFatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID=?
							AND a.ValidationRetVal = 16
							AND a.ErrorMsgNo=?;`, iCompanyID, lNotSpecificCurrency)

				bq.Set(`INSERT #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,?,?,?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), a.CurrID, 'Specific Foreign Curr', b.RestrictedCurrID, '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 16
							AND a.ErrorMsgNo = ?;`, lNotSpecificCurrency, lNotSpecificCurrency)

			}
		}

	}

FinishFunc:

	if lErrorsOccurred {

		bq.Set(`TRUNCATE TABLE #tciError;`)

		bq.Set(`INSERT INTO #tciError
					(BatchKey,        StringNo,        ErrorType,       Severity, 
					StringData1,     StringData2,     StringData3,     StringData4, 
					StringData5,     TranType,        TranKey,         InvtTranKey)
				SELECT
					tmp.BatchKey,    tmp.StringNo,    tmp.ErrorType,   tmp.Severity, 
					tmp.StringData1, tmp.StringData2, tmp.StringData3, tmp.StringData4, 
					tmp.StringData5, gl.TranType,     NULL,            gl.TranKey
				FROM #tciErrorStg tmp
					JOIN #tglPosting gl ON tmp.GLAcctKey = gl.GLAcctKey`)

		LogErrors(bq, iBatchKey, iSessionID)

		if lValidateAcctRetVal == 0 {
			lValidateAcctRetVal = ResultSuccess
		} else {
			if lAcctRefValFail == 1 {
				lValidateAcctRetVal = lValidateAcctRefRetVal
			}
		}
	}

	return lValidateAcctRetVal, oSeverity, iSessionID
}

// SetAPIValidateAcctRef - Validates Account Reference Codes to be Posted to GL.
//
// This stored procedure takes a set of Account Reference Codes from a
// temporary table called #tglValidateAcct and validates them in the same
// way that the spglAPIAcctRef sp validates Account Reference Codes one at
// a time.  This sp replaces the spglAPIAcctRef sp which only operated on
// one row at a time (one Account Reference Code) and was called repetitively
// by the spglAPIAcctPostRow sp in the subsidiary modules.
// This new sp will only be called once by the spglSetAPIValidateAcct sp.
//
// This stored procedure ASSUMES:
//       (1)  The existence of a temporary table called #tglValidateAcct.
//       (2)  That #tglValidateAcct has been correctly populated with n rows
//            of distinct combinations of GLAcctKey+AcctRefKey+CurrID.
//       (3)  That all GLAcctKey's in #tglValidateAcct are only for @iCompanyID.
//       (4)  That if a @iVerifyParams value other than one (1) is passed in,
//            all parameter values in the NOTE below are guaranteed to be valid.
//       (5)  The calling program is NOT relying on Account Reference Codes to
//            be created if AcctRefUsage is set to '2' in tglOptions.  No Account
//            Reference Codes are created when this sp is used for validation.
//
// Use this sp with other Acuity API's that begin with spglSetAPI...
//
// Input Parameters:
//    @iCompanyID        = [IN: Valid Acuity Company; No Default]
//    @iBatchKey         = [IN: Valid Batch Key or NULL; Default = NULL]
//    @ioSessionID     = [IN/OUT: Valid No. or NULL; No Default]
//    @iUserID           = [IN: Valid User or NULL; Default = spGetLoginName]
//    @iLanguageID       = [IN: Valid Language ID or NULL; Default = NULL]
//    @iAcctRefUsage     = [IN: 0, 1 or NULL; Default = 0]
//    @iEffectiveDate    = [IN: Effective Date or NULL]
//    @iVerifyParams     = [IN: 0, 1 or NULL; Default = 1]
//
// NOTE: The following parameters MUST be passed in with a valid value from the
// calling stored procedure IF the @iVerifyParams parameter is passed in
// with a value of anything OTHER THAN one (1):
//    @iCompanyID
//    @ioSessionID
//    @iUserID
//    @iLanguageID
//    @iAcctRefUsage
//
// Output Parameters:
//    @ioSessionID = [IN/OUT: Valid No. or NULL; No Default]
//    @oSeverity     = [OUT: 0=None, 1=Warning, 2=Fatal; Default=0]
//    @oRetVal       = [OUT: return flag indicating outcome of the procedure]
//           0 = Failure.  General SP Failure.
//           1 = Successful.
//          19 = Failure.  Error Log Key not supplied and cannot be derived.
//          20 = Failure.  Company ID not supplied.
//          21 = Failure.  Company ID supplied does not exist.
//          24 = Failure.  GL Options row for this Company does not exist.
//          27 = Failure.  Account Reference Key exists but not for the correct Company.
//          30 = Failure.  Account Reference Key supplied does not exist.
//          32 = Failure.  Failure of Account Reference Code Effective Dates Restriction.
//          33 = Failure.  User ID not supplied and cannot be derived.
//          34 = Failure.  Language ID cannot be determined.
//          37 = Failure.  Account Reference Code is not active.
//          42 = Failure.  tglOptions.AcctRefUsage Flag not enabled.
//          43 = Failure.  GL Account requires an Account Reference Code.
func SetAPIValidateAcctRef(bq *du.BatchQuery,
	iCompanyID string,
	iBatchKey int,
	iSessionID int,
	iUserID string,
	iLanguageID int,
	iAcctRefUsage int,
	iEffectiveDate *time.Time,
	iVerifyParams bool) (Result ResultConstant, Severity int, SessionID int) {

	var qr du.QueryResult

	bq.ScopeName("SetAPIValidateAcctRef")

	createAPIValidationTempTables(bq)

	lLanguageID := iLanguageID
	lAcctRefUsage := iAcctRefUsage

	if iVerifyParams {

		if iSessionID == 0 {
			iSessionID = GetNextSurrogateKey(bq, "tciErrorLog")
			if iSessionID == 0 {
				return ResultConstant(19), 2, 0
			}
		}

		if iUserID != "" {
			qr = bq.Get(`SELECT MIN(LanguageID)	FROM tsmUser WITH (NOLOCK) WHERE UserID=?;`, iUserID)
			if qr.HasData {
				lLanguageID = int(qr.First().ValueInt64Ord(0))
			}

			if lLanguageID == 0 {
				return ResultConstant(34), 2, 0
			}
		}

		if iCompanyID == "" {
			LogError(bq, iBatchKey, 0, 19101, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(20), 2, 0
		}

		// CompanyID must be valid (Get CurrID in the process)
		qr = bq.Get(`SELECT CompanyName FROM tsmCompany WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
		if !qr.HasData {
			LogError(bq, iBatchKey, 0, 19102, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(21), 2, 0
		}

		// Get the GL Options information. (Just check if this information exists on the company)
		qr = bq.Get(`SELECT AcctRefUsage FROM tglOptions WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
		if !qr.HasData {
			LogError(bq, iBatchKey, 0, 19105, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(24), 2, 0
		}
		lAcctRefUsage = int(qr.First().ValueInt64Ord(0))

		if lAcctRefUsage == 0 {
			LogError(bq, iBatchKey, 0, 19230, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return ResultConstant(42), 2, 0
		}
	}

	lErrorsOccurred := false
	lValidateAcctRetVal := ResultError
	oSeverity := 0

	const lAcctRefKeyReqd int = 19235
	const lAcctRefExist int = 19221
	const lAcctRefCo int = 19222
	const lAcctRefInactive int = 19227
	const lAcctRefStart int = 19224
	const lAcctRefEnd int = 19225

	// Validate the required Account Reference ID's in #tglValidateAcct now
	// This validation only applies when @lAcctRefUsage = 1 [Validated ARC's]
	if lAcctRefUsage == 0 {
		qr = bq.Set(`UPDATE #tglValidateAcct
					SET ValidationRetVal = 43,
						ErrorMsgNo = ?
					WHERE GLAcctKey IN (SELECT GLAcctKey 
										FROM tglAccount a WITH (NOLOCK),
											tglNaturalAcct b WITH (NOLOCK)
										WHERE a.NaturalAcctKey = b.NaturalAcctKey
										AND b.ReqAcctRefCode = 1)
						AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) = 0
						AND ValidationRetVal = 0;`, lAcctRefKeyReqd)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 43
			oSeverity = lFatalError

			bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

			bq.Set(`INSERT INTO #tglAcctMask
					SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
						c.FormattedGLAcctNo  /* MaskedGLAcctNo */
					FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
						AND a.ValidationRetVal = 43
						AND a.ErrorMsgNo = ?;`, iCompanyID, lAcctRefKeyReqd)

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), AcctRefKey), '', '', '', '', ?
					FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 30
						AND ErrorMsgNo=?;`, iBatchKey, lInterfaceError, lFatalError, lAcctRefExist, lAcctRefExist)
		}
	}

	// Do all the Reference Keys exist? This validation applies when @lAcctRefUsage = 1 or 2
	if lAcctRefUsage == 1 || lAcctRefUsage == 2 {

		// Validating that the Account Reference Keys exist in tglAcctRef
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 30, ErrorMsgNo = ?
					 WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
											 FROM tglAcctRef WITH (NOLOCK))
											 AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
						AND ValidationRetVal = 0;`, lAcctRefExist)
		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 30
			oSeverity = lFatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), AcctRefKey), '', '', '', '', ?
					FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 30
						AND ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, lAcctRefExist, lAcctRefExist)

			goto FinishFunc
		}

		// Validating that all Account Reference Keys are for the correct Company
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 27,	ErrorMsgNo = ?
					 WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
											FROM tglAcctRef WITH (NOLOCK)
											WHERE CompanyID = ?)
					 AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
					 AND ValidationRetVal=0;`, lAcctRefCo, iCompanyID)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 27
			oSeverity = lFatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), b.AcctRefCode), ?, '', '', '', ?
					FROM #tglValidateAcct a WITH (NOLOCK), tglAcctRef b WITH (NOLOCK)
					WHERE a.AcctRefKey = b.AcctRefKey
						AND a.ValidationRetVal = 27
						AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, iCompanyID, lAcctRefCo, lAcctRefCo)
		}
	}

	if lAcctRefUsage == 1 {

		// Validating that the Account Reference Keys have an active status
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 37, ErrorMsgNo = ?
					 WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
											FROM tglAcctRef WITH (NOLOCK)
										WHERE CompanyID =?
						AND Status = 1)
						AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
						AND ValidationRetVal = 0;`, lAcctRefInactive, iCompanyID)
		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 37
			oSeverity = lFatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT a.GLAcctKey, ?,?,?, CONVERT(VARCHAR(30), b.AcctRefCode), '', '', '', '', ?
					FROM #tglValidateAcct a WITH (NOLOCK), tglAcctRef b WITH (NOLOCK)
					WHERE a.AcctRefKey = b.AcctRefKey
						AND a.ValidationRetVal = 37
						AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, lAcctRefInactive, lAcctRefInactive)
		}

		// Reference Code Effective Date Restrictions
		if iEffectiveDate != nil {

			// Validating that there are no ARC effective start date violations
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 32, ErrorMsgNo = ?
						WHERE AcctRefKey IN (SELECT AcctRefKey 
											FROM tglAcctRef WITH (NOLOCK) 
											WHERE CompanyID = ? AND Status = 1 AND EffStartDate IS NOT NULL AND EffStartDate > '?')
						AND ValidationRetVal = 0;`, lAcctRefStart, iCompanyID, iEffectiveDate.Format(`2006-01-02`))

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 32
				oSeverity = lFatalError

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, '?', CONVERT(VARCHAR(30), b.AcctRefCode), b.EffStartDate, '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAcctRef b WITH (NOLOCK)
						WHERE a.AcctRefKey = b.AcctRefKey
							AND a.ValidationRetVal = 32
							AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, iEffectiveDate.Format(`2006-01-02`), lAcctRefStart)

			}

			// Validating that there are no ARC effective end date violations
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 32, ErrorMsgNo = ?
						WHERE AcctRefKey IN (SELECT AcctRefKey 
											FROM tglAcctRef WITH (NOLOCK)
											WHERE CompanyID = ?
											AND Status = 1
											AND EffEndDate IS NOT NULL
											AND EffEndDate < '?')
						AND ValidationRetVal = 0;`, lAcctRefEnd, iCompanyID, iEffectiveDate.Format(`2006-01-02`))

			if qr.HasAffectedRows {
				lErrorsOccurred = true
				lValidateAcctRetVal = 32
				oSeverity = lFatalError

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,	?, ?, ?, '?', CONVERT(VARCHAR(30), b.AcctRefCode), b.EffEndDate, '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAcctRef b WITH (NOLOCK)
						WHERE a.AcctRefKey = b.AcctRefKey
							AND a.ValidationRetVal = 32
							AND a.ErrorMsgNo = ?;`, iBatchKey, lInterfaceError, lFatalError, iEffectiveDate.Format(`2006-01-02`), lAcctRefEnd, lAcctRefEnd)
			}
		}
	}

FinishFunc:

	if lErrorsOccurred {

		bq.Set(`TRUNCATE TABLE #tciError;`)

		bq.Set(`INSERT INTO #tciError
					(BatchKey,        StringNo,        ErrorType,       Severity, 
					StringData1,     StringData2,     StringData3,     StringData4, 
					StringData5,     TranType,        TranKey,         InvtTranKey)
				SELECT
					tmp.BatchKey,    tmp.StringNo,    tmp.ErrorType,   tmp.Severity, 
					tmp.StringData1, tmp.StringData2, tmp.StringData3, tmp.StringData4, 
					tmp.StringData5, gl.TranType,     NULL,            gl.TranKey
				FROM #tciErrorStg tmp
					JOIN #tglPosting gl ON tmp.GLAcctKey = gl.GLAcctKey;`)

		LogErrors(bq, iBatchKey, iSessionID)
	}

	if lValidateAcctRetVal == 0 {
		lValidateAcctRetVal = ResultSuccess
	}

	return lValidateAcctRetVal, oSeverity, iSessionID
}

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
func SummarizeBatchlessTglPosting(bq *du.BatchQuery, iCompanyID string, iBatchKey int, optUseTempTable bool) ResultConstant {

	var qr du.QueryResult

	bq.ScopeName("SummarizeBatchlessTglPosting")

	bq.Set(`SELECT * INTO #tglPostingTmp FROM tglPosting WHERE 1=2;`)

	qr = bq.Get(`SELECT ISNULL(OBJECT_ID('tempdb..#tglPostingDetlTran'),0);`)
	if qr.First().ValueFloat64Ord(0) == 0 {
		return ResultError
	}

	ttbl := "tglPosting"
	if optUseTempTable {
		ttbl := "#tglPostingRPT"
	}
	qr = bq.Get(`SELECT 1 FROM #tglPostingDetlTran tmp JOIN ` + ttbl + ` gl ON tmp.TranType = gl.TranType AND tmp.PostingDetlTranKey = gl.TranKey;`)
	if !qr.HasData {
		// Nothing to do.
		return ResultSuccess
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
		return ResultSuccess
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
		return ResultError
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
		return ResultError
	}

	return ResultSuccess
}

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
func SetAPIGLPosting(bq *du.BatchQuery, iCompanyID string, iBatchKey int, iPostToGL bool) ResultConstant {
	bq.ScopeName("SetAPIGLPosting")

	if !iPostToGL {
		return ResultSuccess
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
		return ResultError
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
		return ResultError
	}
	lSourceModuleNo = int(qr.First().ValueInt64("ModuleNo"))

	// Check to see if there are any rows in tglPosting
	qr = bq.Get(`SELECT BatchKey
				 FROM tglPosting WITH (NOLOCK)
				 WHERE BatchKey=?;`, iBatchKey)
	if !qr.HasData {
		return ResultSuccess
	}

	// Retrieve Batch PostDate
	var lBatchPostDate *time.Time
	qr = bq.Get(`SELECT MIN(PostDate)
				 FROM tglPosting WITH (NOLOCK)
				 WHERE BatchKey=?;`, iBatchKey)
	if qr.HasData {
		*lBatchPostDate = qr.First().ValueTimeOrd(0)
		if lBatchPostDate == nil {
			return ResultFail
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
