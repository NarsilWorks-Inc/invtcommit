USE [MDCI_MAS500_APP]
GO
/****** Object:  StoredProcedure [dbo].[spglAPIPostBatchlessGLPosting]    Script Date: 7/4/2019 2:14:01 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER procedure [dbo].[spglAPIPostBatchlessGLPosting] ( @iBatchCmnt VARCHAR(50),
@ioSessionID INTEGER OUTPUT, -- Input / Output
@oRetVal    INTEGER OUTPUT,
@optReplcInvalidAcctWithSuspense BIT = 1, -- Optional
@optPostToGL BIT = 1) -- Optional
AS
/*******************************************************************************************************************
* Procedure Name:  spglAPIPostBatchlessGLPosting
* Creation date:   11/15/2004
* Author:          Gerard Tan-Torres
* Copyright:       Copyright (c) 1995-2004 Best Software, Inc.
* Description:     This SP is designed to process GL postings for those transactions that were committed
*                  by the inventory batchless process.  It assumes that a temp table called #tciTransToPost
*                  exists and contains data about the transactions to post.  It also assumes that the records
*                  in tglPosting that correspond to the transactions were posted in detail.  This allows us
*                  to tie each GL entry to its source transaction using the InvtTranKey.
*
*                  The process starts by validating the contents of #tciTransToPost.  It then creates a
*                  logical lock against the GL posting records for those valid transactions.  We then
*                  re-validate the GL accounts.  If a GL account fails validation for any reason, it will
*                  be conditionally replaced with the suspense account and a warning will be logged.  If
*                  the GL period we are posting to is closed, we will stop posting and the user will have
*                  to re-open the period and re-process the transactions.
*
*                  Next, we will use the current posting settings to post the lines in detail or summary.
*                  Upon successful completion, the transaction's shipment log will have a status of "Posted"
*                  and its GL transactions will exists in tglTransaction.
*
* Assumptions:     This SP assumes that the #tciTransToPost has been populated appropriately and completely
*                  using the following table definition.
*                     CREATE TABLE #tciTransToPost (
*                     CompanyID         VARCHAR(3) NOT NULL,
*                     TranID            VARCHAR(13) NOT NULL, -- TranID used for reporting.
*                     TranType          INTEGER NOT NULL,  -- Supported transaction types.
*                     TranKey           INTEGER NOT NULL,  -- Represents the TranKey of the transactions to post. (ie. ShipKey for SO)
*                     GLBatchKey        INTEGER NOT NULL,  -- Represents the GL batch to post the transactions.
*                     PostStatus        INTEGER DEFAULT 0) -- Status use to determine progress of each transaction.
*
* PostStatus Enum:
*   0 = New Transaction, have not been processed (Default Value).
*   1 = Posted successfully.
*   2 = Invalid GL Account exists.  Not considered as a fatal error since it can be replaced with the suspense account.
*  -1 = TranType not supported.
*  -2 = Transactions have not yet been committed.
*  -3 = GL posting batch does not exists or is invalid.
*  -4 = Posting to a prior SO period.
*  -5 = Posting to a closed GL period.
*  -6 = Transactions have been locked by another user.
*  -7 = Debits and Credits do not equal.
********************************************************************************************************************
* Parameters
*    INPUT:  @iBatchCmnt = Comment use for ALL batches.
*   OUTPUT:  @ioSessionID = SessionID used for reporting errors. (Input / Output)
*            @oRetVal = Return Value
*               0 = Failure
*               1 = Success <Transaction(s) posted to GL>
*               2 = Success <NO Transaction was posted to GL>
*
* OPTIONAL:  @optReplcInvalidAcctWithSuspense = Indicates whether invalid account are replaced by the suspense acct.
*            @optPostToGL = Defaults to true.  However, when set to false, final GL posting will not be performed.  Use
*             this option when the user decides to preview the GL register instead of actually proceeding with the posting.
*                Note: Each time this routine is called, a GL Batch number is used even if this option is set
*                to false.  This way the final GL Batch number is seen during preview or after posting.
*
********************************************************************************************************************
*   RETURN Codes
*    0 - Unexpected Error (SP Failure)
*    1 - Successful
********************************************************************************************************************/

BEGIN
	SET NOCOUNT ON

	-- Create temp tables.
	IF OBJECT_ID('tempdb..#tglPostingRpt') IS NOT NULL
		TRUNCATE TABLE #tglPostingRpt
	ELSE
		BEGIN
			SELECT * INTO #tglPostingRpt FROM tglPosting WHERE 1=2
			CREATE CLUSTERED INDEX cls_tglPosting_idx ON #tglPostingRpt (BatchKey)
		END

	DECLARE @UniqueTransToPost TABLE (
		CompanyID         VARCHAR(3) NOT NULL,
		TranID            VARCHAR(13) NOT NULL,
		TranType          INTEGER NOT NULL,
		TranKey           INTEGER NOT NULL,
		GLBatchKey        INTEGER NOT NULL,
		PostStatus        INTEGER DEFAULT 0)


	-- Create a new copy of #tglPosting since one can already be created by with the wrong schema.
	SELECT * INTO #tglPosting FROM tglPosting WHERE 1=2

	IF OBJECT_ID('tempdb..#tglPostingDetlTran') IS NOT NULL
		TRUNCATE TABLE #tglPostingDetlTran
	ELSE
		BEGIN
			CREATE TABLE #tglPostingDetlTran (
			PostingDetlTranKey INTEGER NOT NULL,
			TranType INTEGER NOT NULL)
			
			CREATE CLUSTERED INDEX cls_tglPostingDetlTran_idx ON #tglPostingDetlTran (TranType, PostingDetlTranKey)
		END

	IF OBJECT_ID('tempdb..#LogicalLocks') IS NOT NULL
		TRUNCATE TABLE #LogicalLocks
	ELSE
		BEGIN
			CREATE TABLE #LogicalLocks
			(LogicalLockType   SMALLINT
			,LogicalLockID     VARCHAR(80)
			,UserKey           INTEGER NULL
			,LockType          SMALLINT
			,LogicalLockKey    INT NULL
			,Status            INTEGER NULL
			,LockCleanupParam1 INTEGER NULL
			,LockCleanupParam2 INTEGER NULL
			,LockCleanupParam3 VARCHAR(255) NULL
			,LockCleanupParam4 VARCHAR(255) NULL
			,LockCleanupParam5 VARCHAR(255) NULL
			)
			
			CREATE CLUSTERED INDEX cls_LogicalLocks_idx ON #LogicalLocks (UserKey, LogicalLockKey, LogicalLockType)
		END

	IF OBJECT_ID('tempdb..#tciErrorLogExt') IS NOT NULL
		TRUNCATE TABLE #tciErrorLogExt
	ELSE
		CREATE TABLE #tciErrorLogExt (
			EntryNo       INTEGER NOT NULL,  SessionID INTEGER NOT NULL,
			TranType      INTEGER NULL,      TranKey   INTEGER NULL,
			TranLineKey   INTEGER NULL,      InvtTranKey   INTEGER NULL
		)

	IF OBJECT_ID('tempdb..#tciError') IS NOT NULL
		TRUNCATE TABLE #tciError
	ELSE
		CREATE TABLE #tciError
				(EntryNo     int      NULL
				,BatchKey    int      NULL
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
				,TranLineKey int      NULL
				,InvtTranKey int      NULL
		)

	IF OBJECT_ID('tempdb..#tglValidateAcct') IS NOT NULL
		TRUNCATE TABLE #tglValidateAcct
	ELSE
		BEGIN
			CREATE TABLE #tglValidateAcct (GLAcctKey          int      NOT NULL,
			AcctRefKey         int      NULL,
			CurrID             VARCHAR(3)  NOT NULL,
			ValidationRetVal   int      NOT NULL,
			ErrorMsgNo         int      NULL)

			CREATE CLUSTERED INDEX #tglValidateAcct_idx_cls ON #tglValidateAcct (GLAcctKey, AcctRefKey, CurrID)
		END

	IF OBJECT_ID('tempdb..#tciTransToPostDetl') IS NOT NULL
		TRUNCATE TABLE #tciTransToPostDetl
	ELSE 
		BEGIN
			CREATE TABLE #tciTransToPostDetl (
			GLBatchKey        INTEGER NOT NULL,
			CompanyID         VARCHAR(3) NOT NULL,
			TranID            VARCHAR(13) NOT NULL,
			TranType          INTEGER NOT NULL,
			TranKey           INTEGER NOT NULL, -- Represents the TranKey of the transactions to post. (ie. ShipKey for SO)
			InvtTranKey       INTEGER NOT NULL,
			PostingKey        INTEGER NOT NULL,
			SourceModuleNo    SMALLINT NOT NULL,
			GLAcctKey         INTEGER NOT NULL,
			AcctRefKey        INTEGER NULL,
			CurrID            VARCHAR(3) NOT NULL,
			PostDate          DATETIME NOT NULL,
			PostAmtHC         DECIMAL(15,3) NOT NULL,
			PostStatus        INTEGER DEFAULT 0
			)
			
			CREATE CLUSTERED INDEX cls_tciTransToPostDetl_idx ON #tciTransToPostDetl (PostingKey, TranKey, InvtTranKey, GLAcctKey)
		END

	DECLARE @UniqueGLBatchKeys TABLE (
		BatchCount SMALLINT NOT NULL IDENTITY (1,1),
		CompanyID VARCHAR(3) NOT NULL,
		GLBatchKey INTEGER NOT NULL,
		ModuleNo SMALLINT NOT NULL,
		PostDate DATETIME NOT NULL,
		IntegratedWithGL SMALLINT NOT NULL)

	-- Declare local variables
	DECLARE @lDebugFlag SMALLINT       DECLARE @lRetVal SMALLINT            DECLARE @lLoginName VARCHAR(30)
	DECLARE @lSessionID INTEGER        DECLARE @lCompanyID VARCHAR(3)          DECLARE @lBatchCount SMALLINT
	DECLARE @lEntity VARCHAR(64)       DECLARE @lLockID INTEGER             DECLARE @lLocksCreated SMALLINT
	DECLARE @lLocksRejected SMALLINT   DECLARE @lLogicalLockID VARCHAR(80)  DECLARE @lGLBatchKey INTEGER
	DECLARE @lModuleNo SMALLINT        DECLARE @lIntegrateWithGL SMALLINT   DECLARE @lPostDate DATETIME
	DECLARE @lInvalidAcctExist BIT     DECLARE @lTranFlag BIT               DECLARE @lHomeCurrID VARCHAR(3)
	DECLARE @lIsCurrIDUsed SMALLINT    DECLARE @lAutoAcctAdd SMALLINT       DECLARE @lUseMultCurr SMALLINT
	DECLARE @lGLAcctMask VARCHAR(114)  DECLARE @lAcctRefUsage SMALLINT      DECLARE @lGLSuspenseAcctKey INTEGER
	DECLARE @lLanguageID INTEGER

	-- Declare Constants
	DECLARE @LOCKTYPE_GL_BATCH_RECOVERY SMALLINT       DECLARE @LOCK_MODE_EXCLUSIVE SMALLINT
	DECLARE @CLEANUP_LOCKS_FIRST INTEGER               DECLARE @POST_STATUS_DEFAULT SMALLINT
	DECLARE @POST_STATUS_GL_POSTED SMALLINT            DECLARE @POST_STATUS_TRAN_TYPE_NOT_SUPPORTED SMALLINT
	DECLARE @POST_STATUS_TRNXS_NOT_COMMITTED SMALLINT  DECLARE @POST_STATUS_PRIOR_SO_PERIOD SMALLINT
	DECLARE @POST_STATUS_GLBATCHKEY_INVALID SMALLINT   DECLARE @POST_STATUS_GL_PERIOD_CLOSED SMALLINT
	DECLARE @POST_STATUS_TRAN_LOCKED SMALLINT          DECLARE @POST_STATUS_INVALID_ACCT_EXIST SMALLINT
	DECLARE @POST_STATUS_DR_CR_NOT_EQUAL SMALLINT

	DECLARE @SHIP_LOG_TRAN_STATUS_POSTED SMALLINT      DECLARE @SHIP_LOG_TRAN_STATUS_COMMITTED SMALLINT
	DECLARE @TRANTYPE_WHSESHIPMENT INTEGER             DECLARE @TRANTYPE_WHSESHIPMENT_TRNSFR INTEGER
	DECLARE @TRANTYPE_DROPSHIPMENT INTEGER             DECLARE @TRANTYPE_TRANSIT_TRNSFR_IN INTEGER
	DECLARE @TRANTYPE_CUSTOMER_RETURN INTEGER          DECLARE @WARNING_ERR SMALLINT
	DECLARE @FATAL_ERR SMALLINT                        DECLARE @SUCCESS_GL_POSTED INTEGER
	DECLARE @SUCCESS_NO_GL_POSTED INTEGER              DECLARE @SP_FAILURE INTEGER
	DECLARE @POST_STATUS_COMPLETE SMALLINT             DECLARE @BATCH_STATUS_POSTED SMALLINT

	-- Set Local constants
	SELECT @LOCKTYPE_GL_BATCH_RECOVERY = 2             SELECT @LOCK_MODE_EXCLUSIVE = 2
	SELECT @CLEANUP_LOCKS_FIRST = 1                    SELECT @WARNING_ERR = 1
	SELECT @FATAL_ERR = 2                              SELECT @POST_STATUS_DEFAULT = 0
	SELECT @POST_STATUS_GL_POSTED = 1                  SELECT @POST_STATUS_INVALID_ACCT_EXIST = 2
	SELECT @POST_STATUS_TRAN_TYPE_NOT_SUPPORTED = -1   SELECT @POST_STATUS_TRNXS_NOT_COMMITTED = -2
	SELECT @POST_STATUS_PRIOR_SO_PERIOD = -4           SELECT @POST_STATUS_GLBATCHKEY_INVALID = -3
	SELECT @POST_STATUS_GL_PERIOD_CLOSED = -5          SELECT @POST_STATUS_TRAN_LOCKED = -6
	SELECT @POST_STATUS_DR_CR_NOT_EQUAL = -7
	SELECT @SHIP_LOG_TRAN_STATUS_POSTED = 3            SELECT @SHIP_LOG_TRAN_STATUS_COMMITTED = 6
	SELECT @TRANTYPE_WHSESHIPMENT = 810                SELECT @TRANTYPE_WHSESHIPMENT_TRNSFR = 812
	SELECT @TRANTYPE_CUSTOMER_RETURN = 811             SELECT @TRANTYPE_DROPSHIPMENT = 814
	SELECT @TRANTYPE_TRANSIT_TRNSFR_IN = 813
	SELECT @SUCCESS_GL_POSTED = 1
	SELECT @SUCCESS_NO_GL_POSTED = 2                   SELECT @SP_FAILURE = 0
	SELECT @POST_STATUS_COMPLETE = 500                 SELECT @BATCH_STATUS_POSTED = 6

	-- Initialize variables.
	SELECT @lInvalidAcctExist = 0                      SELECT @lTranFlag = 0
	SELECT @lDebugFlag = 0                             SELECT @lIsCurrIDUsed = 0
	SELECT @lAutoAcctAdd = 0                           SELECT @lUseMultCurr = 0
	SELECT @lAcctRefUsage = 0                          SELECT @oRetVal = @SP_FAILURE

	-- Get login name.
	EXEC spGetLoginName @lLoginName OUTPUT

	-- Set the default value on the PostStatus if its value is not one that is supported.
	UPDATE #tciTransToPost
	SET PostStatus = @POST_STATUS_DEFAULT
	WHERE PostStatus NOT IN (
		@POST_STATUS_GL_POSTED, 					@POST_STATUS_INVALID_ACCT_EXIST,
		@POST_STATUS_TRAN_TYPE_NOT_SUPPORTED, 	@POST_STATUS_TRNXS_NOT_COMMITTED,
		@POST_STATUS_PRIOR_SO_PERIOD, 			@POST_STATUS_GLBATCHKEY_INVALID,
		@POST_STATUS_GL_PERIOD_CLOSED, 			@POST_STATUS_TRAN_LOCKED,
		@POST_STATUS_DR_CR_NOT_EQUAL)

	-- Make sure the rows in #tciTransToPost are Unique rows.
	INSERT @UniqueTransToPost (CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus)
	SELECT DISTINCT CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus
	FROM #tciTransToPost

	TRUNCATE TABLE #tciTransToPost
	INSERT #tciTransToPost (CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus)
	SELECT CompanyID, TranID, TranType, TranKey, GLBatchKey, PostStatus
	FROM @UniqueTransToPost

	-- Create the GL Batches if needed.
	EXEC spglCreateBatchlessGLPostingBatch @iBatchCmnt, @lRetVal OUTPUT

	IF @lRetVal <> 1
	BEGIN
		-- This is a bad return value.  We should not proceed with the posting.
		GOTO ErrorOccurred
	END

	-- Determine the SessionID.
	IF @ioSessionID <> 0
	BEGIN
		SELECT @lSessionID = @ioSessionID
	END
	ELSE
	BEGIN
		-- Use a one SessionID for all batches we are about to post.  This will help in reporting.
		SELECT @lSessionID = MIN(GLBatchKey) FROM #tciTransToPost
		SELECT @ioSessionID = COALESCE(@lSessionID, 0)
	END

	-- Clear the error tables.
	DELETE tciErrorLog WHERE SessionID = @lSessionID OR BatchKey IN (SELECT GLBatchKey FROM #tciTransToPost)

	-- -------------------------------------------------------------------------------
	-- Validate the data in #tciTransToPost: (Following considered to be fatal errors)
	-- If an error is logged here, we will not post any transactions in the set.
	-- -------------------------------------------------------------------------------

	-- Validate the GLBatchKey found in #tciTransToPost.
	UPDATE #tciTransToPost
	SET PostStatus = @POST_STATUS_GLBATCHKEY_INVALID
	FROM #tciTransToPost
		LEFT OUTER JOIN tciBatchLog bl WITH (NOLOCK) ON #tciTransToPost.GLBatchKey = bl.BatchKey
	WHERE (bl.PostStatus <> 0 OR bl.Status <> 4) OR bl.BatchKey IS NULL
		AND #tciTransToPost.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)

	-- Log the error.
	IF @@ROWCOUNT <> 0
	BEGIN
		-- Specified batch key ({1}) is not found in the tciBatchLog table.
		INSERT #tciError
			(EntryNo, BatchKey, StringNo,
			StringData1, StringData2,
			ErrorType, Severity, TranType,
			TranKey)
		SELECT
			NULL, tmp.GLBatchKey, 164027,
			'', CONVERT(VARCHAR(10), tmp.GLBatchKey),
			2, @FATAL_ERR, tmp.TranType,
			tmp.TranKey
		FROM #tciTransToPost tmp
		WHERE tmp.PostStatus = @POST_STATUS_GLBATCHKEY_INVALID
	END

	-- Make sure only supported TranTypes are in #tciTransToPost.
	UPDATE #tciTransToPost
	SET PostStatus = @POST_STATUS_TRAN_TYPE_NOT_SUPPORTED
	WHERE TranType NOT IN (@TRANTYPE_WHSESHIPMENT, @TRANTYPE_DROPSHIPMENT, @TRANTYPE_WHSESHIPMENT_TRNSFR, @TRANTYPE_CUSTOMER_RETURN)
	AND PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)

	-- Log the error.
	IF @@ROWCOUNT <> 0
	BEGIN
		-- Batch {0}, Transaction {1}: Invalid transation type
		INSERT #tciError
			(EntryNo, BatchKey, StringNo,
			StringData1, StringData2,
			ErrorType, Severity, TranType,
			TranKey)
		SELECT
			NULL, tmp.GLBatchKey, 160151,
			bl.BatchID, tmp.TranID,
			2, @FATAL_ERR, tmp.TranType,
			tmp.TranKey
		FROM #tciTransToPost tmp
		JOIN tciBatchLog bl WITH (NOLOCK) ON tmp.GLBatchKey = bl.BatchKey
			WHERE tmp.PostStatus = @POST_STATUS_TRAN_TYPE_NOT_SUPPORTED
	END

-- SO Tran Types:
	IF EXISTS (SELECT 1 FROM #tciTransToPost
		WHERE TranType IN (@TRANTYPE_WHSESHIPMENT, @TRANTYPE_DROPSHIPMENT, @TRANTYPE_WHSESHIPMENT_TRNSFR, @TRANTYPE_CUSTOMER_RETURN)
		AND PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST) )
	BEGIN
		-- For SO transactions, mark those transaction where the TranStatus is NOT set to Committed.
		UPDATE #tciTransToPost
			SET PostStatus = @POST_STATUS_TRNXS_NOT_COMMITTED
		FROM #tciTransToPost
			JOIN tsoShipmentLog sl WITH (NOLOCK) ON #tciTransToPost.TranKey = sl.ShipKey
		WHERE #tciTransToPost.TranType IN (@TRANTYPE_WHSESHIPMENT, @TRANTYPE_DROPSHIPMENT, @TRANTYPE_WHSESHIPMENT_TRNSFR, @TRANTYPE_CUSTOMER_RETURN)
			AND #tciTransToPost.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)
			AND sl.TranStatus <> @SHIP_LOG_TRAN_STATUS_COMMITTED

		-- Log the error.
		IF @@ROWCOUNT <> 0
		BEGIN
			-- {0} transactions have not been successfully Pre-Committed.
			INSERT #tciError
				(EntryNo, BatchKey, StringNo,
				StringData1,
				ErrorType, Severity, TranType,
				TranKey)
			SELECT
				NULL, tmp.GLBatchKey, 250893,
				tmp.TranID,
				2, @FATAL_ERR, tmp.TranType,
				tmp.TranKey
			FROM #tciTransToPost tmp
			WHERE tmp.PostStatus = @POST_STATUS_TRNXS_NOT_COMMITTED
		END

		-- --------------------------------------------------------------------------
		-- Based on the TranType, populate #tciTransToPostDetl for those transactions
		-- that are still valid.
		-- --------------------------------------------------------------------------
		-- Sales Order TranTypes:
		INSERT #tciTransToPostDetl (
			CompanyID, TranID, TranType, TranKey,
			InvtTranKey, GLBatchKey, PostStatus,
			PostingKey, SourceModuleNo, GLAcctKey,
			AcctRefKey, CurrID, PostDate, PostAmtHC)
		SELECT DISTINCT
			s.CompanyID, s.TranID, s.TranType, s.ShipKey,
			sl.InvtTranKey, tmp.GLBatchKey, tmp.PostStatus,
			gl.PostingKey, gl.SourceModuleNo, gl.GLAcctKey,
			gl.AcctRefKey, gl.CurrID, gl.PostDate, gl.PostAmtHC
		FROM #tciTransToPost tmp
			JOIN tsoShipment s  WITH (NOLOCK) ON tmp.TranKey = s.ShipKey
			JOIN tsoShipLine sl WITH (NOLOCK) ON s.ShipKey = sl.ShipKey
			JOIN tglPosting gl  WITH (NOLOCK) ON (tmp.TranType = gl.TranType AND sl.InvtTranKey = gl.TranKey) OR (gl.TranType = @TRANTYPE_TRANSIT_TRNSFR_IN AND sl.TransitInvtTranKey = gl.TranKey)
		WHERE tmp.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)
			AND tmp.TranType IN (@TRANTYPE_WHSESHIPMENT, @TRANTYPE_DROPSHIPMENT, @TRANTYPE_WHSESHIPMENT_TRNSFR, @TRANTYPE_CUSTOMER_RETURN)
	END

	-- Check if there is anything to process.  If any of the above validations failed,
	-- then we will not post any transactions in the set.  This is the all or nothing approach.
	IF NOT EXISTS (SELECT 1 FROM #tciTransToPostDetl)
	BEGIN
		--R--
		SELECT @oRetVal = @SUCCESS_NO_GL_POSTED
		GOTO EarlyExit
	END

	-- ------------------------------------------------
	-- Create Logical Locks against the posting record:
	-- ------------------------------------------------
	-- Place a logical lock on the posting records that tie to the transactions found in #tciTransToPostDetl
	-- so other processes trying to post the same records will get an exclusive lock error.
	INSERT #LogicalLocks
		(LogicalLockType,
		UserKey,
		LockType,
		LogicalLockID)
		SELECT
		1,
		tmp.PostingKey,
		@LOCK_MODE_EXCLUSIVE,
		'GLPostTrans:' + CONVERT(VARCHAR(10), tmp.TranType) + ':' + CONVERT(VARCHAR(10), tmp.PostingKey)
	FROM #tciTransToPostDetl tmp
	WHERE tmp.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)

	EXEC spsmLogicalLockAddMultiple @lRetVal OUTPUT, @lLocksCreated OUTPUT, @lLocksRejected OUTPUT, @CLEANUP_LOCKS_FIRST
	IF @lRetVal = -1
		GOTO ErrorOccurred

	-- See if the locks were successfully placed.
	IF EXISTS (SELECT 1 FROM #LogicalLocks WHERE Status <> 1)
	BEGIN
		-- Tran {0}: User {1} currently has a lock against this transaction.
		INSERT #tciError
			(EntryNo, BatchKey, StringNo,
			StringData1, StringData2, StringData3,
			ErrorType, Severity, TranType,
			TranKey)
		SELECT DISTINCT
			NULL, tmp.GLBatchKey, 100524,
			tmp.TranID, ll.ActualUserID, '',
			2, @FATAL_ERR, tmp.TranType,
			tmp.TranKey
		FROM tsmLogicalLock ll WITH (NOLOCK)
			JOIN #LogicalLocks tmpll ON ll.LogicalLockID = tmpll.LogicalLockID AND ll.LogicalLockType = tmpll.LogicalLockType
			JOIN #tciTransToPostDetl tmp ON tmpll.UserKey = tmp.PostingKey
		WHERE tmpll.Status = 102 -- Exclusive lock not created due to existing locks.

		-- Tran {0}: Unable to create a lock against this transaction.
		INSERT #tciError
			(EntryNo, BatchKey, StringNo,
			StringData1, StringData2, StringData3,
			ErrorType, Severity, TranType,
			TranKey)
		SELECT DISTINCT
			NULL, tmp.GLBatchKey, 100524,
			tmp.TranID, ll.ActualUserID, '',
			2, @FATAL_ERR, tmp.TranType,
			tmp.TranKey
		FROM tsmLogicalLock ll WITH (NOLOCK)
			JOIN #LogicalLocks tmpll ON ll.LogicalLockID = tmpll.LogicalLockID AND ll.LogicalLockType = tmpll.LogicalLockType
			JOIN #tciTransToPostDetl tmp ON tmpll.UserKey = tmp.PostingKey
		WHERE tmpll.Status NOT IN (1, 102) -- NOT(Locked Successfully, Exclusive lock not created due to existing locks)

		-- Mark those transactions which locks could not be created.  This will exclude
		-- them from the list of transactions to be processed.
		UPDATE tmp
		SET PostStatus = @POST_STATUS_TRAN_LOCKED
		FROM #tciTransToPostDetl tmp
			JOIN #LogicalLocks ll ON tmp.PostingKey = ll.UserKey
		WHERE ll.Status <> 1 -- Not Locked Successfully

	END


	-- Validate the post dates of the posting records.
	-- Making sure they fall within a valid GL period.
	UPDATE t1
		SET PostStatus = @POST_STATUS_GL_PERIOD_CLOSED
	FROM #tciTransToPostDetl t1
		JOIN (SELECT DISTINCT tmp.TranKey, tmp.PostDate
		FROM #tciTransToPostDetl tmp
	WHERE tmp.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST) ) PostDates
		ON t1.TranKey = PostDates.TranKey
		JOIN tglFiscalPeriod p WITH (NOLOCK) ON t1.CompanyID = p.CompanyID AND PostDates.PostDate BETWEEN p.StartDate AND p.EndDate
	WHERE p.Status = 2 -- Closed Period.

	-- Log the error.
	IF @@ROWCOUNT <> 0
	BEGIN
		-- Transaction {0}: GL Fiscal Period for Posting Date {1} is Closed.
		INSERT #tciError
			(EntryNo, BatchKey, StringNo,
			StringData1, StringData2,
			ErrorType, Severity, TranType,
			TranKey)
		SELECT DISTINCT
			NULL, tmp.GLBatchKey, 130317,
			tmp.TranID, convert(varchar(10), tmp.PostDate, 101),
			2, @FATAL_ERR, tmp.TranType,
			tmp.TranKey
		FROM #tciTransToPostDetl tmp
		WHERE tmp.PostStatus = @POST_STATUS_GL_PERIOD_CLOSED
	END

	-- Finally, make sure the balance of the posting rows nets to zero.
	UPDATE t1
		SET PostStatus = @POST_STATUS_DR_CR_NOT_EQUAL
	FROM #tciTransToPostDetl t1
		JOIN (SELECT tmp.GLBatchKey, tmp.TranKey, SUM(tmp.PostAmtHC) 'Balance'
				FROM #tciTransToPostDetl tmp
				WHERE tmp.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)
				GROUP BY tmp.GLBatchKey, tmp.TranKey
	HAVING SUM(tmp.PostAmtHC) <> 0) BatchTot
		ON t1.GLBatchKey = BatchTot.GLBatchKey AND t1.TranKey = BatchTot.TranKey

	-- Log the error.
	IF @@ROWCOUNT <> 0
	BEGIN
		-- Transaction {0}: Debits and Credits do not equal.
		INSERT #tciError
			(EntryNo, BatchKey, StringNo,
			StringData1, StringData2,
			ErrorType, Severity, TranType,
			TranKey)
		SELECT DISTINCT
			NULL, tmp.GLBatchKey, 130315,
			tmp.TranID, '',
			2, @FATAL_ERR, tmp.TranType,
			tmp.TranKey
		FROM #tciTransToPostDetl tmp
		WHERE tmp.PostStatus = @POST_STATUS_DR_CR_NOT_EQUAL
	END

	-- Update the parent table's PostStatus.
	UPDATE tmp
	SET PostStatus = Detl.PostStatus
	FROM #tciTransToPost tmp
		JOIN #tciTransToPostDetl Detl ON tmp.TranKey = Detl.TranKey

	-- If a posting record is invalid, we need to delete the record as well as all of the other
	-- posting records associated with the same transaction.
	DELETE Detl
	FROM #tciTransToPostDetl Detl
	WHERE TranKey IN (SELECT TranKey FROM #tciTransToPostDetl
						WHERE PostStatus NOT IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST) )

	IF @lDebugFlag = 1
		SELECT '#tciTransToPostDetl', * FROM #tciTransToPostDetl

	-- Check if there is anything to process.  If any of the above validations failed,
	-- then we will not post any transactions in the set.  This is the all or nothing approach.
	IF EXISTS (SELECT 1 FROM #tciTransToPostDetl
				WHERE PostStatus NOT IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST) )
	BEGIN
		--R--
		SELECT @oRetVal = @SUCCESS_NO_GL_POSTED
		GOTO EarlyExit
	END

	-- Create a list of batches to be posted.  Each row in @UniqueGLBatchKeys will require a
	-- call to the GL Posting Routines.  This table drives the WHILE LOOPS within the routine.
	INSERT @UniqueGLBatchKeys (CompanyID, GLBatchKey, ModuleNo, PostDate, IntegratedWithGL)
	SELECT DISTINCT tmp.CompanyID, tmp.GLBatchKey, tmp.SourceModuleNo, tmp.PostDate, 1
	FROM #tciTransToPostDetl tmp

	IF @@ERROR <> 0
		GOTO ErrorOccurred


	-- ----------------------------
	-- Start GL Account Validation:
	-- ----------------------------
	-- Validate the GL Accounts
	SELECT @lBatchCount = MIN(BatchCount) FROM @UniqueGLBatchKeys

	IF COALESCE(@lBatchCount, 0) > 0
	BEGIN
		TRUNCATE TABLE #tglValidateAcct

		-- Loop through @UniqueGLBatchKeys and call the GL validation routine.
		-- Note: At this point, any validation errors encounted should not
		--       prevent the GL records from being posted.  Invalid accounts
		--       will be conditionally replaced by the suspense account.
		WHILE @lBatchCount IS NOT NULL
		BEGIN

			SELECT @lCompanyID = CompanyID, @lGLBatchKey = GLBatchKey,
				@lModuleNo = ModuleNo,   @lPostDate = PostDate,
				@lIntegrateWithGL = 1
			FROM @UniqueGLBatchKeys WHERE BatchCount = @lBatchCount

			SELECT @lHomeCurrID = CurrID
			FROM tsmCompany WITH (NOLOCK)
			WHERE CompanyID = @lCompanyID

			SELECT @lIsCurrIDUsed = IsUsed
			FROM tmcCurrency WITH (NOLOCK)
			WHERE CurrID = @lHomeCurrID

			SELECT @lAutoAcctAdd = AutoAcctAdd,
				@lUseMultCurr = UseMultCurr,
				@lGLAcctMask = AcctMask,
				@lAcctRefUsage = AcctRefUsage
			FROM tglOptions WITH (NOLOCK)
			WHERE CompanyID = @lCompanyID

			EXEC spciGetLanguage @lCompanyID, @lLanguageID OUTPUT

			-- Populate #tglValidateAcct.
			INSERT #tglValidateAcct (GLAcctKey, AcctRefKey, CurrID, ValidationRetVal)
			SELECT DISTINCT tmp.GLAcctKey, tmp.AcctRefKey, tmp.CurrID, 0
			FROM #tciTransToPostDetl tmp
			WHERE tmp.GLBatchKey = @lGLBatchKey
				AND NOT EXISTS (SELECT 1 FROM #tglValidateAcct v
								WHERE tmp.GLAcctKey = v.GLAcctKey
									AND tmp.AcctRefKey = v.AcctRefKey
									AND tmp.CurrID = v.CurrID)

			-- Populate #tglPosting
			TRUNCATE TABLE #tglPosting

			SET IDENTITY_INSERT #tglPosting ON

			INSERT #tglPosting (
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
				WHERE GLBatchKey = @lGLBatchKey)

			SET IDENTITY_INSERT #tglPosting OFF

			-- Reset RetVal to zero.
			SELECT @lRetVal = 0
			-- Call the routine to validate the accounts.
			EXECUTE spglSetAPIValidateAccount @lCompanyID,                   @lGLBatchKey,
				@lSessionID,                   @lLoginName,
				@lLanguageID,                  @lHomeCurrID,
				@lIsCurrIDUsed,                @lAutoAcctAdd,
				@lUseMultCurr,                 @lGLAcctMask,
				@lAcctRefUsage,
				0,                             -- AllowWildCard (No)
				1,                             -- AllowActiveOnly (Yes)
				NULL,                          -- Financial (Don't Care)
				1,                             -- PostTypeFlag (Financial)
				3,                             -- PostingType (Both)
				@lPostDate,                    -- EffectiveDate
				1,                             -- VerifyParams (Yes)
				1,                             -- ValidateGLAccts (Yes)
				1,                             -- ValidateAcctRefs (Yes)
				1,                             -- ValidateCurrIDs (Yes)
				0,                             @lRetVal OUTPUT

			-- Did the GL account validation go OK?
			IF @lRetVal = 0
			BEGIN
				-- This is a bad return value.  We should not proceed with the posting.
				GOTO ErrorOccurred
			END

			-- Get the next batch to process.
			SELECT @lBatchCount = MIN(BatchCount)
			FROM @UniqueGLBatchKeys WHERE BatchCount > @lBatchCount

		END -- End of WHILE Loop

	END -- End of "IF COALESCE(@lBatchCount, 0) > 0"


	-- Check if an account number failed validation
	IF EXISTS (SELECT 1 FROM #tglValidateAcct WHERE ValidationRetVal <> 0)
	BEGIN
		IF @lDebugFlag = 1
			SELECT '#tglValidateAcct' as 'InvalidAccts', * FROM #tglValidateAcct WHERE ValidationRetVal <> 0

		-- Update the PostStatus for those transactions that have an invalid account.
		UPDATE tmp
		SET PostStatus = @POST_STATUS_INVALID_ACCT_EXIST
		FROM #tciTransToPostDetl tmp
			JOIN #tglValidateAcct BadGL
		ON tmp.GLAcctKey = BadGL.GLAcctKey AND tmp.CurrID = BadGL.CurrID
			AND COALESCE(tmp.AcctRefKey, 0) = COALESCE(BadGL.AcctRefKey, 0)
		WHERE BadGL.ValidationRetVal <> 0

		-- Log a warning.
		-- Transaction {0}: Invalid GL account.  Replaced with suspense account.
		INSERT #tciError
			(EntryNo, BatchKey, StringNo,
			StringData1, StringData2,
			ErrorType, Severity, TranType,
			TranKey)
		SELECT DISTINCT
			NULL, tmp.GLBatchKey, 130316,
			tmp.TranID, '',
			2, @WARNING_ERR, tmp.TranType,
			tmp.TranKey
		FROM #tciTransToPostDetl tmp
		WHERE tmp.PostStatus = @POST_STATUS_INVALID_ACCT_EXIST

		-- An account validation failed.
		SELECT @lInvalidAcctExist = 1
		SELECT @lGLSuspenseAcctKey = SuspenseAcctKey FROM tglOptions WITH (NOLOCK) WHERE CompanyID = @lCompanyID

		IF @optReplcInvalidAcctWithSuspense <> 1
		BEGIN
			-- Now, give user a chance to fix the problem by exiting the SP.
			--R--
			SELECT @oRetVal = @SUCCESS_NO_GL_POSTED
			GOTO EarlyExit
		END
	END

	-- -------------------------
	-- Start GL Posting Routine:
	-- -------------------------
	SELECT @lBatchCount = MIN(gl.BatchCount)
	FROM @UniqueGLBatchKeys gl
		JOIN #tciTransToPostDetl tmp ON gl.GLBatchKey = tmp.GLBatchKey
	WHERE tmp.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)

	IF COALESCE(@lBatchCount, 0) > 0
	BEGIN
		-- Loop through @UniqueGLBatchKeys and call the GL posting routine.
		WHILE @lBatchCount IS NOT NULL
		BEGIN

			SELECT @lCompanyID = CompanyID, @lGLBatchKey = GLBatchKey, @lModuleNo = ModuleNo, @lIntegrateWithGL = 1
			FROM @UniqueGLBatchKeys WHERE BatchCount = @lBatchCount

			IF @@TRANCOUNT = 0
			BEGIN
				SELECT @lTranFlag = 1
				BEGIN TRANSACTION
			END

			-- Update tglPosting with the GLBatchKey we will be posting to.
			UPDATE tglPosting
			SET BatchKey = tmp.GLBatchKey
			FROM tglPosting WITH (NOLOCK)
				JOIN #tciTransToPostDetl tmp ON tglPosting.PostingKey = tmp.PostingKey
				WHERE tmp.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)
					AND tmp.GLBatchKey = @lGLBatchKey

			IF @lInvalidAcctExist= 1
			BEGIN
				IF @lDebugFlag = 1
					SELECT @lGLSuspenseAcctKey 'Replacing invalid accounts with Suspense account.'

				IF COALESCE(@lGLSuspenseAcctKey, 0) = 0
					GOTO ErrorOccurred

				-- We need to update tglPosting with the suspense AcctKey for those GL accounts that failed.
				UPDATE gl
				SET GLAcctKey = @lGLSuspenseAcctKey
				FROM tglPosting gl WITH (NOLOCK)
					JOIN (SELECT DISTINCT GLAcctKey, AcctRefKey, CurrID
							FROM #tglValidateAcct WHERE ValidationRetVal <> 0) BadGL
					ON gl.GLAcctKey = BadGL.GLAcctKey AND gl.CurrID = BadGL.CurrID
					AND COALESCE(gl.AcctRefKey, 0) = COALESCE(BadGL.AcctRefKey, 0)
				WHERE gl.BatchKey = @lGLBatchKey

			END

			-- Summarize the GL Posting records based on the current posting settings.
			INSERT #tglPostingDetlTran (PostingDetlTranKey, TranType)
			SELECT DISTINCT InvtTranKey, TranType
			FROM #tciTransToPostDetl
			WHERE PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)
				AND GLBatchKey = @lGLBatchKey

			-- Reset RetVal to zero.
			SELECT @lRetVal = 0
			EXEC spglSummarizeBatchlessTglPosting @lCompanyID, @lGLBatchKey, @lRetVal OUTPUT

			IF @lRetVal <> 1
				GOTO ErrorOccurred

			-- Write out the GL Posting records to the report table.
			INSERT #tglPostingRpt (
				AcctRefKey, BatchKey, CurrID, ExtCmnt, GLAcctKey, JrnlKey, JrnlNo, NatCurrBegBal, PostAmt, PostAmtHC,
				PostCmnt, PostDate, PostQty, SourceModuleNo, Summarize, TranDate, TranKey, TranNo, TranType)
			SELECT
				AcctRefKey, BatchKey, CurrID, ExtCmnt, GLAcctKey, JrnlKey, JrnlNo, NatCurrBegBal, PostAmt, PostAmtHC,
				PostCmnt, PostDate, PostQty, SourceModuleNo, Summarize, TranDate, TranKey, TranNo, TranType
			FROM tglPosting WITH (NOLOCK) WHERE BatchKey = @lGLBatchKey

			-- ------------------------
			-- GL Posting
			-- ------------------------
			IF @optPostToGL = 1
			BEGIN
				-- Reset RetVal to zero.
				SELECT @lRetVal = 0
				-- Call the routine to post the batches.
				EXEC spimPostAPIGLPosting @lGLBatchKey, @lCompanyID, @lModuleNo, @lIntegrateWithGL, @lRetVal OUTPUT

				IF @lRetVal <> 0
				BEGIN
					IF @lTranFlag = 1 AND @@TRANCOUNT <> 0
					ROLLBACK TRANSACTION
				END
				ELSE
				BEGIN

					--  Set PostStatus to indicate posting completed successfully.
					UPDATE #tciTransToPostDetl
					SET PostStatus = @POST_STATUS_GL_POSTED
					WHERE GLBatchKey = @lGLBatchKey
					AND PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)

					-- Update timInvtTran's BatchKey with the GLBatchKey.
					UPDATE it
					SET BatchKey = @lGLBatchKey
					FROM timInvtTran it WITH (NOLOCK)
						JOIN #tciTransToPostDetl tmp ON it.InvtTranKey = tmp.InvtTranKey
					WHERE tmp.PostStatus = @POST_STATUS_GL_POSTED
						AND tmp.GLBatchKey = @lGLBatchKey

					IF @@ERROR <> 0 OR @@ROWCOUNT = 0
						GOTO ErrorOccurred

					-- SO Module Table Updates.
					IF EXISTS (SELECT 1 FROM #tciTransToPostDetl WHERE PostStatus = @POST_STATUS_GL_POSTED
							AND TranType IN (@TRANTYPE_WHSESHIPMENT, @TRANTYPE_DROPSHIPMENT, @TRANTYPE_WHSESHIPMENT_TRNSFR, @TRANTYPE_CUSTOMER_RETURN) )
					BEGIN
						-- Set the TranStatus to Posted.
						UPDATE slog
						SET TranStatus = @SHIP_LOG_TRAN_STATUS_POSTED
						FROM tsoShipmentLog slog WITH (NOLOCK)
							JOIN #tciTransToPostDetl tmp ON slog.ShipKey = tmp.TranKey
						WHERE tmp.PostStatus = @POST_STATUS_GL_POSTED
							AND tmp.GLBatchKey = @lGLBatchKey
							AND tmp.TranType IN (@TRANTYPE_WHSESHIPMENT, @TRANTYPE_DROPSHIPMENT, @TRANTYPE_WHSESHIPMENT_TRNSFR, @TRANTYPE_CUSTOMER_RETURN)

						IF @@ERROR <> 0 OR @@ROWCOUNT = 0
							GOTO ErrorOccurred

						-- Update tsoShipment BatchKey with the GLBatchKey.
						UPDATE s
						SET BatchKey = @lGLBatchKey
						FROM tsoShipment s WITH (NOLOCK)
							JOIN #tciTransToPostDetl tmp ON s.ShipKey = tmp.TranKey
						WHERE tmp.PostStatus = @POST_STATUS_GL_POSTED
							AND tmp.GLBatchKey = @lGLBatchKey
							AND tmp.TranType IN (@TRANTYPE_WHSESHIPMENT, @TRANTYPE_DROPSHIPMENT, @TRANTYPE_WHSESHIPMENT_TRNSFR, @TRANTYPE_CUSTOMER_RETURN)

						IF @@ERROR <> 0 OR @@ROWCOUNT = 0
							GOTO ErrorOccurred

						-- Update tciBatchLog's status and post status.
						UPDATE tciBatchLog
						SET Status = @BATCH_STATUS_POSTED,
							PostStatus = @POST_STATUS_COMPLETE,
							PostUserID = @lLoginName
						WHERE BatchKey = @lGLBatchKey

						IF @@ERROR <> 0 OR @@ROWCOUNT = 0
							GOTO ErrorOccurred

					END

					-- Update tciBatchJrnl with the final GL BatchKey to support proper drill down to the GL Journal.
					UPDATE Jrnl
					SET BatchKey = drv.BatchKey
					FROM tciBatchJrnl Jrnl WITH (NOLOCK)
					JOIN (SELECT gl.BatchKey, gl.JrnlKey, gl.JrnlNo
							FROM tglTransaction gl WITH (NOLOCK)
							JOIN @UniqueGLBatchKeys tmp ON gl.BatchKey = tmp.GLBatchKey
					WHERE gl.JrnlKey IS NOT NULL AND gl.JrnlNo IS NOT NULL) drv
						ON Jrnl.JrnlKey = drv.JrnlKey AND Jrnl.JrnlNo = drv.JrnlNo

					IF @@ERROR <> 0 OR @@ROWCOUNT = 0
						GOTO ErrorOccurred

				END -- End of @optPostToGL = 1

				-- Commit the transaction
				IF @lTranFlag = 1 AND @@TRANCOUNT <> 0
					COMMIT TRANSACTION
			END

			-- Get the next batch to process.
			SELECT @lBatchCount = MIN(gl.BatchCount)
			FROM @UniqueGLBatchKeys gl
			JOIN #tciTransToPostDetl tmp ON gl.GLBatchKey = tmp.GLBatchKey
			WHERE gl.BatchCount > @lBatchCount
			AND tmp.PostStatus IN (@POST_STATUS_DEFAULT, @POST_STATUS_INVALID_ACCT_EXIST)

		END -- End of WHILE Loop

	END -- End of "IF COALESCE(@lBatchCount, 0) > 0"

	SELECT @oRetVal = @SUCCESS_GL_POSTED
	--R--
	GOTO ExitSP

	ErrorOccurred:
	SELECT @oRetVal = @SP_FAILURE

EarlyExit:
	IF @lTranFlag = 1 AND @@TRANCOUNT <> 0
		ROLLBACK TRANSACTION

ExitSP:
	EXEC spciLogErrors @lSessionID, @lRetVal OUTPUT, @lSessionID

	-- Remove the posting record's logical locks.
	EXEC spsmLogicalLockRemoveMultiple @lRetVal OUTPUT

END
