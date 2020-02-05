USE [MDCI_MAS500_APP]
GO
/****** Object:  StoredProcedure [dbo].[spglSetAPIInsertGLTrans]    Script Date: 8/9/2019 1:53:11 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/****************************************************************************************
Procedure Name: spglSetAPIInsertGLTrans
Author:         Chuck Lohr
Creation Date:  10/11/1999
Copyright:      Copyright (c) 1995-2001 Best Software, Inc.
                All Rights Reserved.

Description:    Inserts transactions into tglTransaction from
                entries in tglPosting for a given batch.

This stored procedure takes a set of GL accounts from a permanent
table called tglPosting, and posts them into the appropriate
rows into the permanent table tglTransaction using set operations.

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
   @iBatchKey         = [IN: Batch Key] 
   @iBatchPostDate    = [IN: Batch Post Date]
   @iFiscYear         = [IN: Fiscal Year for These GL Transactions]
   @iFiscPer          = [IN: Fiscal Period for These GL Transactions]
   @iSourceModuleNo   = [IN: Module Number That Is the Source for These GL Transactions]
   @iRowsToBeInserted = [IN: The Number of Rows to Be Inserted Into tglTransaction]

Output Parameters:
   @oRetVal = [OUT: Return flag indicating outcome of the procedure.]

   0 = Failure.  General SP Failure.
   1 = Successful.

Standard / Transaction Transactions from GL or Other Subsidiary Modules:
   4 = Failure.  The insert into #tglTransaction failed.
   5 = Failure.  Updating #tglTransaction surrogate keys failed.
   6 = Failure.  The insert into tglTransaction (from tglPosting) failed.

****************************************************************************************/

ALTER PROCEDURE [dbo].[spglSetAPIInsertGLTrans] (@iBatchKey         int,
                                          @iBatchPostDate    datetime,
                                          @iFiscYear         VARCHAR(5),
                                          @iFiscPer          smallint,
                                          @iSourceModuleNo   smallint,
                                          @iRowsToBeInserted int,
                                          @oRetVal           int OUTPUT)
AS

   BEGIN

/* Local Variables ***************************************** */
   DECLARE @lInsertGLTransRetVal  INTEGER,
           @lCreateTypeStandard   SMALLINT,
           @lStartKey             INTEGER,
           @lEndKey               INTEGER,
           @lTempTransRowCount    INTEGER,
           @lTempTransError       INTEGER,
           @lRetVal               INTEGER,
           @kPostingTypeFinancial SMALLINT,
           @kPA					  SMALLINT,
           @PAActive				  SMALLINT,
           @IntegrateWithAP		  SMALLINT,
           @kYes				  SMALLINT

/* Assume an sp failure */
   SELECT @oRetVal = 0

/* Initialize ************************************************ */
   SELECT @lInsertGLTransRetVal = 0,
          @lCreateTypeStandard = 1,
          @kPostingTypeFinancial = 1

/* Verify Parameters */
   IF @iBatchPostDate IS NULL
   BEGIN

      /* Get the Batch Post Date if not passed in. */
      SELECT @iBatchPostDate = MIN(PostDate)
      FROM tglPosting WITH (NOLOCK)
      WHERE BatchKey = @iBatchKey

   END /* End @iBatchPostDate IS NULL */

   IF @iRowsToBeInserted = 0
   BEGIN

      /* Get the number of rows to be inserted into tglTransaction if not passed in. */
      SELECT @iRowsToBeInserted = COUNT(1)
      FROM tglPosting WITH (NOLOCK)
      WHERE BatchKey = @iBatchKey
         AND NatCurrBegBal = 0

   END /* End @iRowsToBeInserted = 0 */

/* Create a temporary #tglTransaction table now. */
   CREATE TABLE #tglTransaction (glTranKey      int           NOT NULL,
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
                                 PostingKey     int           NOT NULL)

/* Insert rows from tglPosting into tglTransaction. */
/* NOTE: Make sure that PostAmt cannot be zero or an error will occur. */
/* Also, PostQty is intentionally set to zero if the GL Account's Posting Type is 'Financial Only'. */
   INSERT INTO #tglTransaction (glTranKey,
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
             a.AcctRefKey, /* AcctRefKey */
             a.BatchKey, /* BatchKey */
             @lCreateTypeStandard, /* CreateType */
             CASE
                WHEN a.PostAmt = 0 THEN 1 /* CurrExchRate */
                WHEN a.PostAmt <> 0 THEN a.PostAmtHC / a.PostAmt /* CurrExchRate */
             END,
             a.CurrID, /* CurrID */
             a.ExtCmnt, /* ExtCmnt */
             @iFiscPer, /* FiscPer */
             @iFiscYear, /* FiscYear */
             a.GLAcctKey, /* GLAcctKey */
             a.JrnlKey, /* JrnlKey */
             a.JrnlNo, /* JrnlNo */
             a.PostAmt, /* PostAmt */
             a.PostAmtHC, /* PostAmtHC */
             a.PostCmnt, /* PostCmnt */
             CASE 
                WHEN b.PostingType = @kPostingTypeFinancial THEN 0
                ELSE a.PostQty
             END, /* PostQty */
             @iSourceModuleNo, /* SourceModuleNo */
             a.TranDate, /* TranDate */
             a.TranKey, /* TranKey */
             a.TranNo, /* TranNo */
             a.TranType, /* TranType */
             a.PostingKey
         FROM tglPosting a WITH (NOLOCK),
              tglAccount b WITH (NOLOCK)
         WHERE a.GLAcctKey = b.GLAcctKey
         AND a.BatchKey = @iBatchKey
         AND a.NatCurrBegBal = 0

   SELECT @lTempTransRowCount = @@ROWCOUNT,
          @lTempTransError = @@ERROR
   --SELECT '#tglTransaction Row Count = ', @lTempTransRowCount
   --SELECT '#tglTransaction Error = ', @lTempTransError

/* Make sure that no error occurred in the insert to #tglTransaction. */
   IF @lTempTransError <> 0
   BEGIN
      SELECT @lInsertGLTransRetVal = 4
      DROP TABLE #tglTransaction
      GOTO FinishProc
   END /* End @lTempTransError <> 0 */

   /* Did any standard / transaction GL entries get inserted into tglTransaction? */
   IF @lTempTransRowCount > 0
   BEGIN

      IF OBJECT_ID('tempdb..#tglRsvpTransactionKeysWrk') IS NOT NULL
      SELECT 1 FROM #tglRsvpTransactionKeysWrk WHERE glTranKey <> 0
      IF @@ROWCOUNT <> 0
      BEGIN
         UPDATE #tglTransaction
            SET glTranKey = rsvp.glTranKey
         FROM #tglRsvpTransactionKeysWrk rsvp
         WHERE #tglTransaction.PostingKey = rsvp.PostingKey
      END

      /* Check if we still need to get more keys */
      SELECT @iRowsToBeInserted = COUNT(*) FROM #tglTransaction WHERE glTranKey = 0
      IF @iRowsToBeInserted <> 0
      BEGIN
         /* Generate the surrogate keys (glTranKey) needed for the insert above. */
         EXECUTE spGetNextBlockSurrogateKey 'tglTransaction',
                                            @iRowsToBeInserted,
                                            @lStartKey OUTPUT,
                                            @lEndKey   OUTPUT
         
         IF @@ERROR <> 0
         BEGIN
            SELECT @lInsertGLTransRetVal = 5
            DROP TABLE #tglTransaction
            GOTO FinishProc
         END
   
         /* Subtract 1 from the starting surrogate key so we don't waste one. */
         SELECT @lStartKey = @lStartKey - 1
   
         UPDATE #tglTransaction
            SET glTranKey = @lStartKey,
                @lStartKey = @lStartKey + 1
         WHERE glTranKey = 0
           
         IF @@ERROR <> 0
         BEGIN
            SELECT @lInsertGLTransRetVal = 5
            DROP TABLE #tglTransaction
            GOTO FinishProc
         END
      END

      /* Now transfer the rows into the permanent tglTransaction table. */
      INSERT INTO tglTransaction (glTranKey,
                                  AcctRefKey,
                                  BatchKey,
                                  CreateType,
                                  CreateDate,
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
                                  PostDate,
                                  PostQty,
                                  SourceModuleNo,
                                  TranDate,
                                  TranKey,
                                  TranNo,
                                  TranType)
         SELECT a.glTranKey, /* glTranKey */
                a.AcctRefKey, /* AcctRefKey */
                a.BatchKey, /* BatchKey */
                a.CreateType, /* CreateType */
                GETDATE(),
                a.CurrExchRate, /* CurrExchRate */
                a.CurrID, /* CurrID */
                a.ExtCmnt, /* ExtCmnt */
                a.FiscPer, /* FiscPer */
                a.FiscYear, /* FiscYear */
                a.GLAcctKey, /* GLAcctKey */
                a.JrnlKey, /* JrnlKey */
                a.JrnlNo, /* JrnlNo */
                a.PostAmt, /* PostAmt */
                a.PostAmtHC, /* PostAmtHC */
                a.PostCmnt, /* PostCmnt */
                @iBatchPostDate, /* PostDate */
                a.PostQty, /* PostQty */
                a.SourceModuleNo, /* SourceModuleNo */
                a.TranDate, /* TranDate */
                a.TranKey, /* TranKey */
                a.TranNo, /* TranNo */
                a.TranType /* TranType */
            FROM #tglTransaction a WITH (NOLOCK)

      IF @@ERROR <> 0
      BEGIN
         SELECT @lInsertGLTransRetVal = 6
         DROP TABLE #tglTransaction
         GOTO FinishProc
      END

	  --Intellisol Start
      SELECT  @kPA  = 19
      SELECT @kYes = 1
	
      SELECT @PAActive   = Active 
	   FROM tsmCompanyModule c
         INNER JOIN tciBatchLog b ON ( b.PostCompanyID = c.CompanyID )
      WHERE b.BatchKey = @iBatchKey
         AND c.ModuleNo  = @kPA
	

      IF @PAActive = @kYes
      BEGIN		
         EXEC spPAGLTranLink @iBatchKey, @lRetVal OUTPUT
         
         IF @lRetVal <> 0
         BEGIN
            SELECT @lInsertGLTransRetVal = @lRetVal
            DROP TABLE #tglTransaction
            GOTO FinishProc
         END
      END
	--Intellisol End


   END /* End @lTempTransRowCount > 0 [Some rows were inserted into #tglTransaction.] */

   DROP TABLE #tglTransaction

/* Complete ********************************************* */
FinishProc:
   IF @lInsertGLTransRetVal = 0
      SELECT @oRetVal = 1
   ELSE
      SELECT @oRetVal = @lInsertGLTransRetVal

   --SELECT 'oRetVal = ', @oRetVal

   END /* End of the Stored Procedure */
