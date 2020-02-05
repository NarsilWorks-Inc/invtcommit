USE [MDCI_MAS500_APP]
GO
/****** Object:  StoredProcedure [dbo].[spglSetAPIGLPosting]    Script Date: 7/9/2019 2:19:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/****************************************************************************************
Procedure Name: spglSetAPIGLPosting
Author:         Chuck Lohr
Creation Date:  09/28/1999
Copyright:      Copyright (c) 1995-2001 Best Software, Inc.
                All Rights Reserved.

Description:    Posts GL Accounts to the GL Module.

This stored procedure takes a set of GL accounts from a permanent
table called tglPosting, and posts them into the appropriate
rows into the permanent table tglTransaction using set operations.
In addition, history tables are updated, like tglAcctHist,
tglAcctHistCurr and tglAcctHistAcctRef.  This sp replaces the 
spglGLPosting sp which extensively used cursors to loop through
tglPosting one row at a time.  This new sp does not use cursors
to process the rows in tglPosting for a particular batch.

This stored procedure ASSUMES:
      (1)  That tglPosting has been correctly populated with n rows
           which will become n rows in the permanent table tglTransaction.
      (2)  That validation of GL accounts, Acct. Ref. Keys, etc., has
           already been performed and that this sp is not executing
           unless all is OK (i.e., tglPosting data is not validated again).
      (3)  That the PostDate value in all tglPosting rows is either NULL
           or equal to the PostDate value in tciBatchLog for that batch.
           The sp automatically updates any NULL PostDate values in tglPosting
           with the batch's PostDate value in tciBatchLog.

Use this sp with other Acuity API's that begin with spglSetAPI...

Input Parameters:
   @iCompanyID = [IN: Valid Acuity Company; No Default]
   @iBatchKey  = [IN: Batch Key] 
   @iPostToGL  = [IN: Does the calling module want to post to GL?]

Output Parameters:
   @oRetVal = [OUT: Return flag indicating outcome of the procedure.]

   0 = Failure.  General SP Failure.
   1 = Successful.
   2 = Failure.  Retained Earnings Account(s) don't exist.
   3 = Failure.  Fiscal period is closed.

Standard / Transaction Transactions from GL or Other Subsidiary Modules:
   4 = Failure in spglSetAPIInsertGLTrans.
       The insert into #tglTransaction failed.
   5 = Failure in spglSetAPIInsertGLTrans.
       Updating #tglTransaction surrogate keys failed.
   6 = Failure in spglSetAPIInsertGLTrans.
       The insert into tglTransaction (from tglPosting) failed.
   7 = Failure in spglSetAPIUpdAcctHist.
       The insert into tglAcctHist (all accounts, debits/credits) failed.
   8 = Failure in spglSetAPIUpdAcctHist.
       The update to tglAcctHist (all accounts, debits/credits) failed.
   9 = Failure in spglSetAPIUpdAcctHistCurr.
       The insert into tglAcctHistCurr (non-home curr accts, debits/credits) failed.
  10 = Failure in spglSetAPIUpdAcctHistCurr.
       The update to tglAcctHistCurr (non-home curr accts, debits/credits) failed.
  11 = Failure in spglSetAPIUpdAcctHistRef.
       The insert into tglAcctHistAcctRef (debits/credits) failed.
  12 = Failure in spglSetAPIUpdAcctHistCurr.
       The update to tglAcctHistAcctRef (debits/credits) failed.
  13 = Failure in spglSetAPIUpdFutBegBal.
       The Retained Earnings Account in tglOptions does not exist (Std trans).
  14 = Failure in spglSetAPIUpdFutBegBal.
       The insert to tglAcctHist (balance sheet accts, beg bal) failed (Std trans).
  15 = Failure in spglSetAPIUpdFutBegBal.
       The update to tglAcctHist (balance sheet accts, beg bal) failed (Std trans).
  16 = Failure in spglSetAPIUpdFutBegBal.
       An error occurred constructing applicable Retained Earnings Accounts (Std trans).
  17 = Failure in spglSetAPIUpdFutBegBal.
       The insert to tglAcctHist (masked Retained Earnings, beg bal) failed (Std trans).
  18 = Failure in spglSetAPIUpdFutBegBal.
       The update to tglAcctHist (masked Retained Earnings, beg bal) failed (Std trans).
  19 = Failure in spglSetAPIUpdFutBegBal.
       The Retained Earnings Account does not exist in tglAccount (Std trans).
  20 = Failure in spglSetAPIUpdFutBegBal.
       The insert to tglAcctHist (unmasked Retained Earnings, beg bal) failed (Std trans).
  21 = Failure in spglSetAPIUpdFutBegBal.
       The update to tglAcctHist (unmasked Retained Earnings, beg bal) failed (Std trans).
  22 = Failure in spglSetAPIUpdFutBegBalCurr.
       The insert to tglAcctHistCurr (balance sheet accts, beg bal) failed (Std trans).
  23 = Failure in spglSetAPIUpdFutBegBalCurr.
       The update to tglAcctHistCurr (balance sheet accts, beg bal) failed (Std trans).

Beginning Balance Transactions from GL Only:
  24 = Failure in spglSetAPIUpdAcctHistBB.
       The insert into tglAcctHist (all accounts) failed (BB trans).
  25 = Failure in spglSetAPIUpdAcctHistBB.
       The update to tglAcctHist (all accounts) failed (BB trans).
  26 = Failure in spglSetAPIUpdFutBegBalBB.
       The Retained Earnings Account in tglOptions does not exist (BB trans).
  27 = Failure in spglSetAPIUpdFutBegBalBB.
       The insert to tglAcctHist (balance sheet accts, beg bal) failed (BB trans).
  28 = Failure in spglSetAPIUpdFutBegBalBB.
       The update to tglAcctHist (balance sheet accts, beg bal) failed (BB trans).
  29 = Failure in spglSetAPIUpdFutBegBalBB.
       An error occurred constructing applicable Retained Earnings Accounts (BB trans).
  30 = Failure in spglSetAPIUpdFutBegBalBB.
       The insert to tglAcctHist (masked Retained Earnings, beg bal) failed (BB trans).
  31 = Failure in spglSetAPIUpdFutBegBalBB.
       The update to tglAcctHist (masked Retained Earnings, beg bal) failed (BB trans).
  32 = Failure in spglSetAPIUpdFutBegBalBB.
       The Retained Earnings Account does not exist in tglAccount (BB trans).
  33 = Failure in spglSetAPIUpdFutBegBalBB.
       The insert to tglAcctHist (unmasked Retained Earnings, beg bal) failed (BB trans).
  34 = Failure in spglSetAPIUpdFutBegBalBB.
       The update to tglAcctHist (unmasked Retained Earnings, beg bal) failed (BB trans).
  35 = Failure in spglSetAPIUpdAcctHistCurrBB.
       The insert into tglAcctHistCurr (non-home curr accts, beg bal) failed (BB trans).
  36 = Failure in spglSetAPIUpdAcctHistCurrBB.
       The update to tglAcctHistCurr (non-home curr accts, beg bal) failed (BB trans).
  37 = Failure in spglSetAPIUpdFutBegBalCurrBB.
       The insert to tglAcctHistCurr (balance sheet accts, beg bal) failed (BB trans).
  38 = Failure in spglSetAPIUpdFutBegBalCurrBB.
       The update to tglAcctHistCurr (balance sheet accts, beg bal) failed (BB trans).

****************************************************************************************/

ALTER PROCEDURE [dbo].[spglSetAPIGLPosting] (@iCompanyID VARCHAR(3),
                                      @iBatchKey  int,
                                      @iPostToGL  smallint,
                                      @oRetVal    int OUTPUT)
AS

   BEGIN

/* Local Variables ***************************************** */
   DECLARE @lGLPostingRetVal          int,
           @lSuspenseAcctKey          int,
           @lRetEarnGLAcctNo          varchar(100),
           @lClearNonFin              smallint,
           @lUseMultCurr              smallint,
           @lAcctRefUsage             smallint,
           @lSourceModuleNo           smallint,
           @lDummyBatchKey            int,
           @lBatchPostDate            datetime,
           @lNullPostDateRows         int,
           @lFiscYear                 VARCHAR(5),
           @lFiscPer                  smallint,
           @lFiscPerStartDate         datetime,
           @lFiscPerEndDate           datetime,
           @lFiscYearPeriodStatus     smallint,
           @lFiscalYearPeriodRetVal   int,
           @lCreateTypeStandard       smallint,
           @lRowCount                 int,
           @lStartKey                 int,
           @lEndKey                   int,
           @lUpdAcctHistRetVal        int,
           @lUpdAcctHistCurrRetVal    int,
           @lUpdAcctHistRefRetVal     int,
           @lUpdFutBegBalRetVal       int,
           @lUpdFutBegBalCurrRetVal   int,
           @lUpdAcctHistBBRetVal      int,
           @lUpdFutBegBalBBRetVal     int,
           @lUpdAcctHistCurrBBRetVal  int,
           @lUpdFutBegBalCurrBBRetVal int,
           @lInsertGLTransRetVal      int,
           @lInsertGLTransBBRetVal    int

/* Assume an sp failure */
   SELECT @oRetVal = 0

/* Abort if no post to GL ************************************* */
   IF @iPostToGL = 0
   BEGIN
      SELECT @lGLPostingRetVal = 1
      GOTO FinishProc
   END

/* Initialize ************************************************ */
   SELECT @lGLPostingRetVal = 0,
          @lCreateTypeStandard = 1

/* Retrieve GL Options Info ********************************** */
   SELECT @lSuspenseAcctKey = SuspenseAcctKey,
          @lRetEarnGLAcctNo = RetainedEarnAcct,
          @lClearNonFin = ClearNonFin,
          @lUseMultCurr = UseMultCurr,
          @lAcctRefUsage = AcctRefUsage
   FROM tglOptions WITH (NOLOCK)
   WHERE CompanyID = @iCompanyID

   IF @@ROWCOUNT = 0
   BEGIN
      SELECT @oRetVal = 0
      RETURN
   END

/* Retrieve Batch Info ********************************** */
   SELECT @iBatchKey = b.BatchKey,
          @lSourceModuleNo = a.ModuleNo
      FROM tciBatchType a WITH (NOLOCK), 
           tciBatchLog b WITH (NOLOCK)
      WHERE a.BatchType = b.BatchType
      AND b.BatchKey = @iBatchKey

   IF @@ROWCOUNT = 0
   BEGIN
      SELECT @oRetVal = 0
      RETURN
   END

/* Check to see if there are any rows in tglPosting ********* */
   SELECT @lDummyBatchKey = BatchKey
      FROM tglPosting WITH (NOLOCK)
      WHERE BatchKey = @iBatchKey

/* Treat this situation like a success and exit the sp. */
   IF @@ROWCOUNT = 0
   BEGIN
      SELECT @oRetVal = 1
      RETURN
   END

/* Retrieve Batch PostDate ********************************** */
/* This query cannot be combined with the one above because of the MIN() */
   SELECT @lBatchPostDate = MIN(PostDate)
      FROM tglPosting WITH (NOLOCK)
      WHERE BatchKey = @iBatchKey

   IF @lBatchPostDate IS NULL
   BEGIN
      SELECT @oRetVal = 0
      RETURN
   END

/* Determine if there are any tglPosting rows with NULL Post Dates. */
   SELECT @lNullPostDateRows = 0
   SELECT @lNullPostDateRows = COUNT(*)
      FROM tglPosting WITH (NOLOCK)
      WHERE BatchKey = @iBatchKey
      AND COALESCE(DATALENGTH(LTRIM(RTRIM(PostDate))),0) = 0

   IF @lNullPostDateRows > 0
   BEGIN
      /* Yes, there ARE some tglPosting rows with NULL Post Dates, so fix them. */

      UPDATE tglPosting
      SET PostDate = @lBatchPostDate
      WHERE BatchKey = @iBatchKey
         AND COALESCE(DATALENGTH(LTRIM(RTRIM(PostDate))),0) = 0

   END /* End @lNullPostDateRows > 0 */

/* Retrieve Fiscal Year Info ********************************** */
   SELECT @lFiscYear = ''
   EXECUTE spglGetFiscalYearPeriod @iCompanyID,
                                   @lBatchPostDate,
                                   3,
                                   @lFiscYear               OUTPUT,
                                   @lFiscPer                OUTPUT,
                                   @lFiscPerStartDate       OUTPUT,
                                   @lFiscPerEndDate         OUTPUT,
                                   @lFiscYearPeriodStatus   OUTPUT,
                                   @lFiscalYearPeriodRetVal OUTPUT
 
   IF @lFiscalYearPeriodRetVal = 5
   BEGIN
      SELECT @lGLPostingRetVal = 2
      GOTO FinishProc
   END

   IF @lFiscYearPeriodStatus = 2
   BEGIN
      SELECT @lGLPostingRetVal = 3
      GOTO FinishProc
   END

/* Post the Transaction  ************************************* */

/* Determine how many non-beginning balance rows in tglPosting will go to tglTransaction. */
   SELECT @lRowCount = COUNT(1)
   FROM tglPosting WITH (NOLOCK)
   WHERE BatchKey = @iBatchKey
      AND NatCurrBegBal = 0
   
   IF @@ERROR <> 0 
   BEGIN
      SELECT @lGLPostingRetVal = 0
      GOTO FinishProc
   END
   
/* Do we have any non-beginning balance rows in tglPosting for this batch? */
   IF @lRowCount > 0
   BEGIN

/* Insert the rows from tglPosting into #tglTransaction, then into tglTransaction. */
      EXECUTE spglSetAPIInsertGLTrans @iBatchKey,
                                      @lBatchPostDate,
                                      @lFiscYear,
                                      @lFiscPer,
                                      @lSourceModuleNo,
                                      @lRowCount,
                                      @lInsertGLTransRetVal OUTPUT
      --SELECT 'Insert GL Trans Ret Val = ', @lInsertGLTransRetVal

/* Did any standard / transaction GL entries get inserted into tglTransaction? */
      IF @lInsertGLTransRetVal = 0
      BEGIN
         SELECT @oRetVal = 0
         RETURN
      END

      IF @lInsertGLTransRetVal <> 1
      BEGIN
         SELECT @lGLPostingRetVal = @lInsertGLTransRetVal
         GOTO FinishProc
      END

/* Update debit and credit amounts in tglAcctHist. */
      EXECUTE spglSetAPIUpdAcctHist @iCompanyID,
                                    @iBatchKey,
                                    @lFiscYear,
                                    @lFiscPer,
                                    @lUpdAcctHistRetVal OUTPUT

      IF @lUpdAcctHistRetVal = 0
      BEGIN
         SELECT @oRetVal = 0
         RETURN
      END

      IF @lUpdAcctHistRetVal <> 1
      BEGIN
         SELECT @lGLPostingRetVal = @lUpdAcctHistRetVal
         GOTO FinishProc
      END

/* Update beginning balances in future years in tglAcctHist. */
      EXECUTE spglSetAPIUpdFutBegBal @iCompanyID,
                                     @iBatchKey,
                                     @lFiscYear,
                                     @lClearNonFin,
                                     @lRetEarnGLAcctNo,
                                     @lUpdFutBegBalRetVal OUTPUT

      IF @lUpdFutBegBalRetVal = 0
      BEGIN
         SELECT @oRetVal = 0
         RETURN
      END

      IF @lUpdFutBegBalRetVal <> 1
      BEGIN
         SELECT @lGLPostingRetVal = @lUpdFutBegBalRetVal
         GOTO FinishProc
      END

/* Determine Multicurrency Use */
      IF @lUseMultCurr = 1
      BEGIN

/* Update debit and credit amounts in tglAcctHistCurr. */
         EXECUTE spglSetAPIUpdAcctHistCurr @iCompanyID,
                                           @iBatchKey,
                                           @lFiscYear,
                                           @lFiscPer,
                                           @lUpdAcctHistCurrRetVal OUTPUT

         IF @lUpdAcctHistCurrRetVal = 0
         BEGIN
            SELECT @oRetVal = 0
            RETURN
         END

         IF @lUpdAcctHistCurrRetVal <> 1
         BEGIN
            SELECT @lGLPostingRetVal = @lUpdAcctHistCurrRetVal
            GOTO FinishProc
         END

/* Update beginning balances in future years in tglAcctHistCurr. */
         EXECUTE spglSetAPIUpdFutBegBalCurr @iCompanyID,
                                            @iBatchKey,
                                            @lFiscYear,
                                            @lClearNonFin,
                                            @lUpdFutBegBalCurrRetVal OUTPUT

         IF @lUpdFutBegBalCurrRetVal = 0
         BEGIN
            SELECT @oRetVal = 0
            RETURN
         END

         IF @lUpdFutBegBalCurrRetVal <> 1
         BEGIN
            SELECT @lGLPostingRetVal = @lUpdFutBegBalCurrRetVal
            GOTO FinishProc
         END

      END /* End @lUseMultCurr = 1 [This Company DOES use Multicurrency.] */

/* Determine Account Reference Code Usage */
      IF (@lAcctRefUsage = 1 OR @lAcctRefUsage = 2)
      BEGIN

/* Update debit and credit amounts in tglAcctHistAcctRef. */
         EXECUTE spglSetAPIUpdAcctHistRef @iCompanyID,
                                          @iBatchKey,
                                          @lFiscYear,
                                          @lFiscPer,
                                          @lUpdAcctHistRefRetVal OUTPUT

         IF @lUpdAcctHistRefRetVal = 0
         BEGIN
            SELECT @oRetVal = 0
            RETURN
         END

         IF @lUpdAcctHistRefRetVal <> 1
         BEGIN
            SELECT @lGLPostingRetVal = @lUpdAcctHistRefRetVal
            GOTO FinishProc
         END

      END /* End @lAcctRefUsage = 1 OR @lAcctRefUsage = 2 */

   END /* End @lRowCount > 0 [We DO have some non-beginning balance rows in tglPosting.] */

/* Determine how many beginning balance rows in tglPosting will get updated in history. */
   SELECT @lRowCount = COUNT(1)
   FROM tglPosting WITH (NOLOCK)
   WHERE BatchKey = @iBatchKey
      AND NatCurrBegBal <> 0
   
   IF @@ERROR <> 0 
   BEGIN
      SELECT @lGLPostingRetVal = 0
      GOTO FinishProc
   END

/* Do we have any beginning balance rows in tglPosting for this batch? */
   IF @lRowCount > 0
   BEGIN

/* Insert the rows from tglPosting into #tglTransaction, then into tglTransaction, for BB transactions. */
      EXECUTE spglSetAPIInsertGLTransBB @iBatchKey,
                                        @lBatchPostDate,
                                        @lFiscYear,
                                        @lFiscPer,
                                        @lSourceModuleNo,
                                        @lRowCount,
                                        @lInsertGLTransBBRetVal OUTPUT
      --SELECT 'Insert GL Trans BB Ret Val = ', @lInsertGLTransBBRetVal

/* Did any beginning balance GL entries get inserted into tglTransaction? */
      IF @lInsertGLTransBBRetVal = 0
      BEGIN
         SELECT @oRetVal = 0
         RETURN
      END

      IF @lInsertGLTransBBRetVal <> 1
      BEGIN
         SELECT @lGLPostingRetVal = @lInsertGLTransBBRetVal
         GOTO FinishProc
      END

/* Update beginning balance amounts in tglAcctHist. */
      EXECUTE spglSetAPIUpdAcctHistBB @iCompanyID,
                                      @iBatchKey,
                                      @lFiscYear,
                                      @lUpdAcctHistBBRetVal OUTPUT

      IF @lUpdAcctHistBBRetVal = 0
      BEGIN
         SELECT @oRetVal = 0
         RETURN
      END

      IF @lUpdAcctHistBBRetVal <> 1
      BEGIN
         SELECT @lGLPostingRetVal = @lUpdAcctHistBBRetVal
         GOTO FinishProc
      END

/* Update beginning balances in future years in tglAcctHist. */
      EXECUTE spglSetAPIUpdFutBegBalBB @iCompanyID,
                                       @iBatchKey,
                                       @lFiscYear,
                                       @lClearNonFin,
                                       @lRetEarnGLAcctNo,
                                       @lUpdFutBegBalBBRetVal OUTPUT

      IF @lUpdFutBegBalBBRetVal = 0
      BEGIN
         SELECT @oRetVal = 0
         RETURN
      END

      IF @lUpdFutBegBalBBRetVal <> 1
      BEGIN
         SELECT @lGLPostingRetVal = @lUpdFutBegBalBBRetVal
         GOTO FinishProc
      END

/* Determine Multicurrency Use */
      IF @lUseMultCurr = 1
      BEGIN

/* Update beginning balance amounts in tglAcctHistCurr. */
         EXECUTE spglSetAPIUpdAcctHistCurrBB @iCompanyID,
                                             @iBatchKey,
                                             @lFiscYear,
                                             @lUpdAcctHistCurrBBRetVal OUTPUT

         IF @lUpdAcctHistCurrBBRetVal = 0
         BEGIN
            SELECT @oRetVal = 0
            RETURN
         END

         IF @lUpdAcctHistCurrBBRetVal <> 1
         BEGIN
            SELECT @lGLPostingRetVal = @lUpdAcctHistCurrBBRetVal
            GOTO FinishProc
         END

/* Update beginning balances in future years in tglAcctHistCurr. */
         EXECUTE spglSetAPIUpdFutBegBalCurrBB @iCompanyID,
                                              @iBatchKey,
                                              @lFiscYear,
                                              @lClearNonFin,
                                              @lUpdFutBegBalCurrBBRetVal OUTPUT

         IF @lUpdFutBegBalCurrBBRetVal = 0
         BEGIN
            SELECT @oRetVal = 0
            RETURN
         END

         IF @lUpdFutBegBalCurrBBRetVal <> 1
         BEGIN
            SELECT @lGLPostingRetVal = @lUpdFutBegBalCurrBBRetVal
            GOTO FinishProc
         END

      END /* End @lUseMultCurr = 1 [This Company DOES use Multicurrency.] */

   END /* End @lRowCount > 0 [Beginning balance rows exist in tglPosting for this batch.] */

/* Complete ********************************************* */
FinishProc:
   IF @lGLPostingRetVal = 0
      SELECT @oRetVal = 1
   ELSE
      SELECT @oRetVal = @lGLPostingRetVal

   --SELECT 'oRetVal = ', @oRetVal

   END /* End of the Stored Procedure */
