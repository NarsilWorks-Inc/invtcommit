USE [MDCI_MAS500_APP]
GO
/****** Object:  StoredProcedure [dbo].[spglSummarizeBatchlessTglPosting]    Script Date: 7/9/2019 1:23:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[spglSummarizeBatchlessTglPosting] (@iCompanyID VARCHAR(3), 
                                              @iBatchKey INTEGER, 
                                              @oRetVal INTEGER OUTPUT, 
                                              @opt_UseTempTable SMALLINT = 0) -- Optional
AS
/****************************************************************************************************************
* Procedure Name:  spglSummarizeBatchlessTglPosting
* Creation date:   10/28/2004
* Author:          Gerard Tan-Torres
* Copyright:       Copyright (c) 1995-2004 Best Software, Inc.
* Description:     This SP designed to take a list of transaction keys and identify its corresponding
*                  GL posting records (tglPosting).  It will then look at the GL summarize options of 
*                  the Inventory and Sales Clearing account listings and summarize the GL posting
*                  records accordingly.  All other entries are posted in detail.  Next, it will replace
*                  the GL posting record's BatchKey with the one passed into this routine.
*
* Important:       The list of transaction keys (#tglPostingDetlTran.PostingDetlTranKey) should join 
*                  against tglPosting.TranKey.  This should represent the InvtTranKey of a shipment line.
*                  It is not the ShipKey or ShipLineKey.
*
* Assumptions:     This SP assumes that the #tglPostingDetlTran has been populated with a list of TranKeys
*                  found in tglPosting.
*                     CREATE TABLE #tglPostingDetlTran (
*                        PostingDetlTranKey INTEGER NOT NULL,
*                        TranType INTEGER NOT NULL)
*
*****************************************************************************************************************
* Parameters
*    INPUT:  @iCompanyID = Represents the CompanyID.
*            @iBatchKey = Represents GL batch key to be used to post the transactions to GL.
*            @opt_UseTempTable = (Optional).  Determines which table to use (tglPosting or #tglPostingRPT).
*   OUTPUT:  @oRetVal  = Return Value
*****************************************************************************************************************
*   RETURN Codes
*
*    0 - Unexpected Error (SP Failure)
*    1 - Successful
*****************************************************************************************************************/

BEGIN
   SET NOCOUNT ON

   SELECT * INTO #tglPostingTmp FROM tglPosting WHERE 1=2

   DECLARE @kSuccess INTEGER
   DECLARE @kFailure INTEGER
   DECLARE @kSummarizeInventory SMALLINT
   DECLARE @kSummarizeSalesClr SMALLINT

   DECLARE @lPostInDetl_Inventory SMALLINT
   DECLARE @lPostInDetl_SalesClearing SMALLINT
   DECLARE @lTranFlag SMALLINT
   DECLARE @lPostingTable VARCHAR(15)

   -- Set Local constants
   SELECT @kSuccess       = 1,
          @kFailure       = 0,
          @kSummarizeInventory = 709,
          @kSummarizeSalesClr = 800

   SELECT @oRetVal = @kFailure -- Default to failure.

   IF OBJECT_ID('tempdb..#tglPostingDetlTran') IS NULL
      RETURN -- #tglPostingDetlTran does not exist.

   IF @opt_UseTempTable = 0
   BEGIN
      IF NOT EXISTS (SELECT 1 FROM #tglPostingDetlTran tmp JOIN tglPosting gl ON tmp.TranType = gl.TranType AND tmp.PostingDetlTranKey = gl.TranKey)
      BEGIN      
         SELECT @oRetVal = @kSuccess -- Nothing to do.
         RETURN
      END
   END
   ELSE
   BEGIN
      IF NOT EXISTS (SELECT 1 FROM #tglPostingDetlTran tmp JOIN #tglPostingRPT gl ON tmp.TranType = gl.TranType AND tmp.PostingDetlTranKey = gl.TranKey)
      BEGIN      
         SELECT @oRetVal = @kSuccess -- Nothing to do.
         RETURN
      END
   END

   -- Get the account listing's posting options.
   SELECT @lPostInDetl_Inventory = im.PostInDetlInvt,
          @lPostInDetl_SalesClearing = so.PostInDetlSalesClr
   FROM timOptions im WITH (NOLOCK)
   JOIN tsoOptions so WITH (NOLOCK) ON im.CompanyID = so.CompanyID
   WHERE im.CompanyID = @iCompanyID

   IF @lPostInDetl_Inventory = 1 AND @lPostInDetl_SalesClearing = 1
   BEGIN      
      SELECT @oRetVal = @kSuccess -- Nothing to summarize.
      RETURN
   END

   -- Identify the GL posting records we are dealing with and store them off in a work table.
   -- Use ABS() funtion for the tglPosting.Summarize as it can be represented in a (+) or (-) 
   -- number depending if it is a DR or CR but... it is always the same number.
   -- IM Posting options:
   IF @lPostInDetl_Inventory = 1
   BEGIN
      IF @opt_UseTempTable = 0
      BEGIN
      INSERT #tglPostingTmp (
         AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              TranType)
         SELECT
         AcctRefKey,       @iBatchKey,          CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              gl.TranType
         FROM tglPosting gl WITH (NOLOCK)
         JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
         WHERE ABS(Summarize) = @kSummarizeInventory
      END
      ELSE
      BEGIN
      INSERT #tglPostingTmp (
         AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              TranType)
         SELECT
         AcctRefKey,       @iBatchKey,          CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              gl.TranType
         FROM #tglPostingRPT gl WITH (NOLOCK)
         JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
         WHERE ABS(Summarize) = @kSummarizeInventory         
      END
   END
   ELSE
   BEGIN
      IF @opt_UseTempTable = 0
      BEGIN
         INSERT #tglPostingTmp (
            AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
            GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
            PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
            PostDate,         Summarize,           TranDate,         SourceModuleNo,
            TranKey,          TranNo,              TranType)
            SELECT
            gl.AcctRefKey,    @iBatchKey,          gl.CurrID,        '',
            gl.GLAcctKey,     gl.JrnlKey,          gl.JrnlNo,        gl.NatCurrBegBal,
            SUM(gl.PostAmt),  SUM(gl.PostAmtHC),   SUM(gl.PostQty),  '',
            gl.PostDate,      gl.Summarize,        NULL,             MIN(gl.SourceModuleNo),
            NULL,             NULL,                NULL
            FROM tglPosting gl WITH (NOLOCK)
            JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
            WHERE ABS(gl.Summarize) = @kSummarizeInventory
            GROUP BY gl.JrnlKey, gl.JrnlNo, gl.GLAcctKey, gl.Summarize, gl.AcctRefKey, gl.CurrID, gl.NatCurrBegBal, gl.PostDate, gl.Summarize
      END
      ELSE
      BEGIN
         INSERT #tglPostingTmp (
            AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
            GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
            PostAmt,          PostAmtHC,           PostQty,          PostCmnt,
            PostDate,         Summarize,           TranDate,         SourceModuleNo,
            TranKey,          TranNo,              TranType)
            SELECT
            gl.AcctRefKey,    @iBatchKey,          gl.CurrID,        '',
            gl.GLAcctKey,     gl.JrnlKey,          gl.JrnlNo,        gl.NatCurrBegBal,
            SUM(gl.PostAmt),  SUM(gl.PostAmtHC),   SUM(gl.PostQty),  '',
            gl.PostDate,      gl.Summarize,        NULL,             MIN(gl.SourceModuleNo),
            NULL,             NULL,                NULL
            FROM #tglPostingRPT gl WITH (NOLOCK)
            JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
            WHERE ABS(gl.Summarize) = @kSummarizeInventory
            GROUP BY gl.JrnlKey, gl.JrnlNo, gl.GLAcctKey, gl.Summarize, gl.AcctRefKey, gl.CurrID, gl.NatCurrBegBal, gl.PostDate, gl.Summarize
      END
   END

   -- SO Posting options:
   IF @lPostInDetl_SalesClearing = 1
   BEGIN
      IF @opt_UseTempTable = 0
      BEGIN
      INSERT #tglPostingTmp (
         AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              TranType)
      SELECT
         AcctRefKey,       @iBatchKey,          CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              gl.TranType
      FROM tglPosting gl WITH (NOLOCK) 
      JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
      WHERE ABS(Summarize) = @kSummarizeSalesClr
      END
      ELSE
      BEGIN
      INSERT #tglPostingTmp (
         AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              TranType)
      SELECT
         AcctRefKey,       @iBatchKey,          CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              gl.TranType
      FROM #tglPostingRPT gl WITH (NOLOCK) 
      JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
      WHERE ABS(Summarize) = @kSummarizeSalesClr      
      END
   END
   ELSE
   BEGIN

      IF @opt_UseTempTable = 0
      BEGIN
      INSERT #tglPostingTmp (
         AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              TranType)
      SELECT 
         gl.AcctRefKey,    @iBatchKey,          gl.CurrID,        '',
         gl.GLAcctKey,     gl.JrnlKey,          gl.JrnlNo,        gl.NatCurrBegBal,
         SUM(gl.PostAmt),  SUM(gl.PostAmtHC),   SUM(gl.PostQty),  '',
         gl.PostDate,      gl.Summarize,        NULL,             MIN(gl.SourceModuleNo),
         NULL,             NULL,                NULL
      FROM tglPosting gl WITH (NOLOCK) 
      JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
      WHERE ABS(gl.Summarize) = @kSummarizeSalesClr
      GROUP BY gl.JrnlKey,
               gl.JrnlNo,
               gl.GLAcctKey,
               gl.Summarize,
               gl.AcctRefKey,
               gl.CurrID,
               gl.NatCurrBegBal,
               gl.PostDate,
               gl.Summarize
      END
      ELSE
      BEGIN
      INSERT #tglPostingTmp (
         AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
         GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
         PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
         PostDate,         Summarize,           TranDate,         SourceModuleNo,
         TranKey,          TranNo,              TranType)
      SELECT 
         gl.AcctRefKey,    @iBatchKey,          gl.CurrID,        '',
         gl.GLAcctKey,     gl.JrnlKey,          gl.JrnlNo,        gl.NatCurrBegBal,
         SUM(gl.PostAmt),  SUM(gl.PostAmtHC),   SUM(gl.PostQty),  '',
         gl.PostDate,      gl.Summarize,        NULL,             MIN(gl.SourceModuleNo),
         NULL,             NULL,                NULL
      FROM #tglPostingRPT gl WITH (NOLOCK) 
      JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
      WHERE ABS(gl.Summarize) = @kSummarizeSalesClr
      GROUP BY gl.JrnlKey,
               gl.JrnlNo,
               gl.GLAcctKey,
               gl.Summarize,
               gl.AcctRefKey,
               gl.CurrID,
               gl.NatCurrBegBal,
               gl.PostDate,
               gl.Summarize
      END
   END

   -- Get the rest of the GL Posting records that are not covered in the
   -- account listing posting options and store them in detail.
   IF @opt_UseTempTable = 0
   BEGIN
   INSERT #tglPostingTmp (
      AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
      GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
      PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
      PostDate,         Summarize,           TranDate,         SourceModuleNo,
      TranKey,          TranNo,              TranType)
   SELECT
      AcctRefKey,       @iBatchKey,          CurrID,           ExtCmnt,
      GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
      PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
      PostDate,         Summarize,           TranDate,         SourceModuleNo,
      TranKey,          TranNo,              gl.TranType
   FROM tglPosting gl WITH (NOLOCK) 
   JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
   WHERE ABS(Summarize) NOT IN (@kSummarizeInventory, @kSummarizeSalesClr)
   END
   ELSE
   BEGIN
   INSERT #tglPostingTmp (
      AcctRefKey,       BatchKey,            CurrID,           ExtCmnt,
      GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
      PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
      PostDate,         Summarize,           TranDate,         SourceModuleNo,
      TranKey,          TranNo,              TranType)
   SELECT
      AcctRefKey,       @iBatchKey,          CurrID,           ExtCmnt,
      GLAcctKey,        JrnlKey,             JrnlNo,           NatCurrBegBal,
      PostAmt,          PostAmtHC,           PostQty,          PostCmnt,    
      PostDate,         Summarize,           TranDate,         SourceModuleNo,
      TranKey,          TranNo,              gl.TranType
   FROM #tglPostingRPT gl WITH (NOLOCK) 
   JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
   WHERE ABS(Summarize) NOT IN (@kSummarizeInventory, @kSummarizeSalesClr)
   END

   -- See if there is anything to do.
   IF NOT EXISTS (SELECT 1 FROM #tglPostingTmp)
   BEGIN
      -- Nothing to do.  Expected rows.  Something went wrong.
      SELECT @oRetVal = @kFailure
   END
   ELSE
   BEGIN
      -- Create a transaction if needed.
      IF @@TRANCOUNT = 0 AND @opt_UseTempTable = 0
      BEGIN
         SELECT @lTranFlag = 1
         BEGIN TRAN
      END

      IF @opt_UseTempTable = 0
      BEGIN
         DELETE tglPosting FROM tglPosting gl
         JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
      END
      ELSE
      BEGIN
         DELETE #tglPostingRPT FROM #tglPostingRPT gl
         JOIN #tglPostingDetlTran detl ON gl.TranType = detl.TranType AND gl.TranKey = detl.PostingDetlTranKey
      END

      -- Re-Insert the records with the new BatchKey summarized according to the 
      -- account listing posting options.
      IF @opt_UseTempTable = 0
      BEGIN         
      INSERT tglPosting (
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
      FROM #tglPostingTmp
      END
      ELSE
      BEGIN
      INSERT #tglPostingRPT (
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
      FROM #tglPostingTmp
      END

      -- Check for errors.   
      IF @@ERROR <> 0
      BEGIN
         SELECT @oRetVal = @kFailure
         IF @lTranFlag = 1
            ROLLBACK TRANSACTION
      END
      ELSE
      BEGIN
         SELECT @oRetVal = @kSuccess
         IF @lTranFlag = 1
            COMMIT TRANSACTION
      END
   END

END
