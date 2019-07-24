USE [MDCI_MAS500_APP]
GO
/****** Object:  StoredProcedure [dbo].[spglSetAPIValidateAccount]    Script Date: 7/3/2019 8:08:45 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/********************************************************************
Procedure Name:   spglSetAPIValidateAccount
Author:           Chuck Lohr
Creation Date:    09/17/1999
Copyright:        Copyright (c) 1995-2001 Best Software, Inc.
                  All Rights Reserved.

Description:      Validates GL Accounts to be Posted to GL Module.

This stored procedure takes a set of GL accounts from a temporary
table called #tglValidateAcct and validates them in the same way that
the spglAPIValidateAccount sp validates GL accounts one at a time.
This sp replaces the spglAPIValidateAccount sp which only operated on
one row at a time (one GL Account) and was called repetitively by the 
spglAPIAcctPostRow sp in the subsidiary modules.
This new sp will only be called once by the spglSetAPIAcctPostRow sp.

This stored procedure ASSUMES:
      (1)  The existence of a temporary table called #tglValidateAcct.
      (2)  That #tglValidateAcct has been correctly populated with n rows
           of distinct combinations of GLAcctKey+AcctRefKey+CurrID.
      (3)  That all GLAcctKey's in #tglValidateAcct are only for @iCompanyID.
      (4)  That if a @iVerifyParams value other than one (1) is passed in,
           all parameter values in the NOTE below are guaranteed to be valid.
      (5)  The calling program is NOT relying on GL Accounts to be created
           if the AutoAcctAdd option is ON in tglOptions.  No GL Accounts
           are created when this sp is used for validation.
      (6)  The calling program is NOT relying on Account Reference Codes to
           be created if AcctRefUsage is set to '2' in tglOptions.  No Account
           Reference Codes are created when this sp is used for validation.
Use this sp with other Acuity API's that begin with spglSetAPI...

Input Parameters:
   @iCompanyID        = [IN: Valid Acuity Company; No Default]
   @iBatchKey         = [IN: Valid Batch Key or NULL; Default = NULL]
   @ioSessionID     = [IN/OUT: Valid No. or NULL; No Default]
   @iUserID           = [IN: Valid User or NULL; Default = spGetLoginName]
   @iLanguageID       = [IN: Valid Language ID or NULL; Default = NULL]
   @iHomeCurrID       = [IN: Valid Curr ID for @iCompanyID or NULL; Default = NULL]
   @iIsCurrIDUsed     = [IN: 0, 1 or NULL; Default = 0]
   @iAutoAcctAdd      = [IN: 0, 1 or NULL; Default = 0]
   @iUseMultCurr      = [IN: 0, 1 or NULL; Default = 0]
   @iGLAcctMask       = [IN: A Valid GL Account Mask or NULL; Default = NULL]
   @iAcctRefUsage     = [IN: 0, 1 or NULL; Default = 0]
   @iAllowWildCard    = [IN: 0, 1 or NULL; Default = 0]
   @iAllowActiveOnly  = [IN: 0, 1 or NULL; Default = 1]
   @iFinancial        = [IN: 0, 1 or NULL; Default = NULL]
   @iPostTypeFlag     = [IN: 0, 1 or NULL; Default = 1]
   @iPostingType      = [IN: 1, 2, 3 or NULL; Default = 3]
   @iEffectiveDate    = [IN: Effective Date or NULL]
   @iVerifyParams     = [IN: 0, 1 or NULL; Default = 1] 
   @iValidateGLAccts  = [IN: 0, 1 or NULL; Default = 1] 
   @iValidateAcctRefs = [IN: 0, 1 or NULL; Default = 1] 
   @iValidateCurrIDs  = [IN: 0, 1 or NULL; Default = 1] 

NOTE: The following parameters MUST be passed in with a valid value from the
calling stored procedure IF the @iVerifyParams parameter is passed in
with a value of anything OTHER THAN one (1):
   @iCompanyID
   @ioSessionID
   @iUserID
   @iLanguageID
   @iHomeCurrID
   @iIsCurrIDUsed
   @iAutoAcctAdd
   @iUseMultCurr
   @iGLAcctMask
   @iAcctRefUsage

Output Parameters:
   @ioSessionID = [IN/OUT: Valid No. or NULL; No Default]
   @oSeverity     = [OUT: 0=None, 1=Warning, 2=Fatal; Default=0]
   @oRetVal       = [OUT: return flag indicating outcome of the procedure]
          0 = Failure.  General SP Failure.
          1 = Successful.
          4 = Failure.  Masked GL account not allowed.
          9 = Failure.  Account doesn't exist not for the Company supplied (or at all).
         10 = Failure.  Account Key supplied does not exist.
         12 = Failure.  Account exists.  Failure of Active Account Restriction.
         13 = Failure.  Account exists.  Failure of Effective Dates Restriction.
         14 = Warning Only.  Account exists.  Failure of Home Currency Only Restriction.
         15 = Warning Only.  Account exists.  Failure of Specific Currency = Home Currency Restriction.
         16 = Failure.  Account Exists.  Failure of Currency not Specific Currency Restriction.
         17 = Failure.  Account exists.  Failure of Financial Type Restriction.
         19 = Failure.  Error Log Key not supplied and cannot be derived.
         20 = Failure.  Company ID not supplied.
         21 = Failure.  Company ID supplied does not exist or has no Home Currency ID.
         23 = Failure.  Currency ID for this Company exists but is not used in MC.
         24 = Failure.  GL Options row for this Company does not exist.
         25 = Failure.  Currency ID for this Company does not exist in MC.
         26 = Failure.  Multicurrency is not enabled for entered Company.
         27 = Failure.  Account Reference Key exists but not for the correct Company.
         30 = Failure.  Account Reference Key supplied does not exist.
         31 = Failure.  Failure of Account Reference Code Account Segments Restriction.
         32 = Failure.  Failure of Account Reference Code Effective Dates Restriction.
         33 = Failure.  User ID not supplied and cannot be derived.
         34 = Failure.  Language ID cannot be determined.
         37 = Failure.  Account Reference Code is not active.
         38 = Failure.  Accounts exists.  Failure of Posting Type Restriction.
         42 = Failure.  tglOptions.AcctRefUsage Flag not enabled.
         43 = Failure.  GL Account requires an Account Reference Code.
                                       
********************************************************************/

ALTER PROCEDURE [dbo].[spglSetAPIValidateAccount] (@iCompanyID        VARCHAR(3), 
                                            @iBatchKey         int,
                                            @ioSessionID       int      OUTPUT,
                                            @iUserID           VARCHAR(30),
                                            @iLanguageID       int,
                                            @iHomeCurrID       VARCHAR(3),
                                            @iIsCurrIDUsed     smallint,
                                            @iAutoAcctAdd      smallint,
                                            @iUseMultCurr      smallint,
                                            @iGLAcctMask       varchar(114),
                                            @iAcctRefUsage     smallint,
                                            @iAllowWildCard    smallint,
                                            @iAllowActiveOnly  smallint,
                                            @iFinancial        smallint,
                                            @iPostTypeFlag     smallint,
                                            @iPostingType      smallint,
                                            @iEffectiveDate    datetime,
                                            @iVerifyParams     smallint,
                                            @iValidateGLAccts  smallint,
                                            @iValidateAcctRefs smallint,
                                            @iValidateCurrIDs  smallint,
                                            @oSeverity         smallint OUTPUT,
                                            @oRetVal           int      OUTPUT)

AS

   BEGIN  
/* Create a temporary error log table to log errors into. */
   --SELECT 'Creating #tciErrorStg table'
   IF OBJECT_ID('tempdb..#tciErrorStg') IS NOT NULL
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
    		,Severity    smallint NOT NULL)

   IF OBJECT_ID('tempdb..#tciError') IS NOT NULL
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
    		,InvtTranKey int      NULL)

/* Create a temporary table to format gl accounts for error messages. */
   --SELECT 'Creating #tglAcctMask table'
   IF OBJECT_ID('tempdb..#tglAcctMask') IS NOT NULL
      TRUNCATE TABLE #tglAcctMask
   ELSE
      CREATE TABLE #tglAcctMask (
         GLAcctNo       varchar(100) NOT NULL,
         MaskedGLAcctNo varchar(114) NULL)

/* Local Variables ******************************************** */
   DECLARE @lAcctRefUsage            smallint,
           @lAutoAcctAdd             smallint,
           @lGLAcctMask              varchar(114),
           @lHomeCurrID              VARCHAR(3),
           @lLanguageID              int,
           @lValidateAcctRetVal      int,
           @lErrorLogRetVal          int,
           @lErrorNoLogRetVal        int,
           @lSetErrorLogRetVal       int,
           @lInterfaceError          smallint,
           @lFatalError              smallint,
           @lWarning                 smallint,
           @lUseMultCurr             smallint,
           @lInvalidCoID             int,
           @lInvalidCurr             int,
           @lInvalidGLOpts           int,
           @lNotUsedCurr             int,
           @lRequiredCoID            int,
           @lMultCurrError           int,
           @lMissingAcctKey          int,
           @lInvalidAcctCo           int,
           @lMaskedGLAcct            int,
           @lInactiveGLAcct          int,
           @lDeletedGLAcct           int,
           @lNonFinlGLAcct           int,
           @lFinlGLAcct              int,
           @lFinlPostType            int,
           @lStatPostType            int,
           @lInvalidHomeCurr         int,
           @lCurrIsHomeCurr          int,
           @lNotSpecificCurrency     int,
           @lGLAcctStartDateError    int,
           @lGLAcctEndDateError      int, 
           @lAcctRefSegs             int,
           @lIsCurrIDUsed            smallint,
           @lErrMsgNo                int,
           @lErrorsOccurred          smallint,
           @lValidateAcctRefSeverity smallint,
           @lValidateAcctRefRetVal   smallint,
           @lMaxAccountSegments      smallint,
           @lConvertToMMDDYYYYDate   smallint,
           @lAcctRefValFail          smallint

/* Initialize ************************************************* */
   SELECT @oSeverity = 0,    
          @oRetVal = 0,
          @lValidateAcctRetVal = 0,
          @lLanguageID = NULL,
          @lInterfaceError = 3,
          @lFatalError = 2,
          @lWarning = 1,
          @lInvalidCoID = 19102,
          @lInvalidCurr = 19103,
          @lInvalidGLOpts = 19105,
          @lNotUsedCurr = 19104,
          @lRequiredCoID = 19101,
          @lMultCurrError = 19112,
          @lMissingAcctKey = 19200,
          @lInvalidAcctCo = 19214,
          @lMaskedGLAcct = 19202,
          @lInactiveGLAcct = 19206,
          @lDeletedGLAcct = 19215,
          @lNonFinlGLAcct = 19207,
          @lFinlGLAcct = 19208,
          @lFinlPostType = 19240,
          @lStatPostType = 19241,
          @lInvalidHomeCurr = 19210,
          @lCurrIsHomeCurr = 19210,
          @lNotSpecificCurrency = 19216,
          @lGLAcctStartDateError = 19212,
          @lGLAcctEndDateError = 19213,
          @lAcctRefSegs = 19223,
          @lHomeCurrID = NULL,
          @lAutoAcctAdd = 0,    
          @lUseMultCurr = 0,
          @lGLAcctMask = NULL, 
          @lAcctRefUsage = 0,
          @lIsCurrIDUsed = 0,
          @lValidateAcctRefSeverity = 0,
          @lValidateAcctRefRetVal = 0,
          @lConvertToMMDDYYYYDate = 101,
          @lAcctRefValFail = 0

/* Verify Validate GL Accts Flag ****************************** */
   IF @iValidateGLAccts IS NULL
      SELECT @iValidateGLAccts = 1

   IF @iValidateGLAccts NOT IN (0,1)
      SELECT @iValidateGLAccts = 1

/* Verify Allow Wild Card Flag ******************************** */
   IF @iAllowWildCard IS NULL
      SELECT @iAllowWildCard = 0

   IF @iAllowWildCard NOT IN (0,1)
      SELECT @iAllowWildCard = 0

/* Verify Allow Active Only Accounts Flag ********************* */
   IF @iAllowActiveOnly IS NULL
      SELECT @iAllowActiveOnly = 1

   IF @iAllowActiveOnly NOT IN (0,1)
      SELECT @iAllowActiveOnly = 1

/* Verify Financial Flag ************************************** */
   IF @iFinancial NOT IN (0,1)
      SELECT @iFinancial = NULL

/* Verify Post Type Flag ************************************** */
   IF @iPostTypeFlag IS NOT NULL
      BEGIN
  
      IF @iPostTypeFlag NOT IN (0,1)
         SELECT @iPostTypeFlag = 1

      END /* End @iPostTypeFlag IS NOT NULL */

/* Verify Posting Type Flag *********************************** */
   IF @iPostingType IS NULL
      SELECT @iPostingType = 3

   IF @iPostingType NOT IN (1,2,3)
      SELECT @iPostingType = 3

/* Verify Validate Acct Refs Flag ***************************** */
   IF @iValidateAcctRefs IS NULL
      SELECT @iValidateAcctRefs = 1

   IF @iValidateAcctRefs NOT IN (0,1)
      SELECT @iValidateAcctRefs = 1

/* Verify Validate Curr IDs Flag ****************************** */
   IF @iValidateCurrIDs IS NULL
      SELECT @iValidateCurrIDs = 1

   IF @iValidateCurrIDs NOT IN (0,1)
      SELECT @iValidateCurrIDs = 1

/* Verify the Verify Params Flag ****************************** */
   IF @iVerifyParams IS NULL
      SELECT @iVerifyParams = 1

   IF @iVerifyParams NOT IN (0,1)
      SELECT @iVerifyParams = 1

/* Should we verify the parameters or has it already been done? */
   IF @iVerifyParams = 1
      BEGIN

	/* Yes, we are going to verify the input parameters. */
	/* Assign Default User ID if User ID is NULL */
		  IF COALESCE(DATALENGTH(LTRIM(RTRIM(@iUserID))), 0) = 0
			 EXEC spGetLoginName @iUserID OUTPUT

	/* Verify Error Log Key *************************************** */
		  IF @ioSessionID = 0
			 SELECT @ioSessionID = NULL

		  IF @ioSessionID IS NULL
			 BEGIN

			 EXECUTE spGetNextSurrogateKey 'tciErrorLog', 
										   @ioSessionID OUTPUT  

			 IF @ioSessionID IS NULL
				BEGIN

				SELECT @oSeverity = 2, 
					   @oRetVal = 19

	/* Rows cannot be written to tglPosting when this error occurs. */
				RETURN

				END /* End @ioSessionID IS NULL */

			 END /* @ioSessionID IS NULL */

	/* Verify User ID ********************************************* */
		  IF COALESCE(DATALENGTH(LTRIM(RTRIM(@iUserID))), 0) = 0
			 BEGIN
	  
			 SELECT @oSeverity = 2, 
					@oRetVal = 33

	/* Rows cannot be written to tglPosting when this error occurs. */
			 RETURN

			 END /* End COALESCE(DATALENGTH(LTRIM(RTRIM(@iUserID))), 0) = 0 */

		  IF COALESCE(DATALENGTH(LTRIM(RTRIM(@iUserID))), 0) > 0
			 BEGIN

			 SELECT @lLanguageID = MIN(LanguageID)
				FROM tsmUser WITH (NOLOCK)
				WHERE UserID = @iUserID

			 IF (@@ROWCOUNT = 0
			 OR COALESCE(@lLanguageID, 0) = 0)
				BEGIN

				SELECT @oSeverity = 2, 
					   @oRetVal = 34

	/* Rows cannot be written to tglPosting when this error occurs. */
				RETURN

				END /* End @@ROWCOUNT = 0 OR COALESCE(@lLanguageID, 0) = 0 */

			 END /* @iUserID > 0 */

	/* Verify Company ID ****************************************** */
		  IF COALESCE(DATALENGTH(LTRIM(RTRIM(@iCompanyID))), 0) = 0
			 BEGIN

			 SELECT @oSeverity = 2, 
					@oRetVal = 20,
					@lErrMsgNo = @lRequiredCoID

			 EXECUTE spciLogError @iBatchKey, 
								  NULL, 
								  @lErrMsgNo, 
								  '', '', '', '', '', 
								  3, 
								  @oSeverity, 
								  @lErrorLogRetVal OUTPUT,
								  @ioSessionID

	/* Rows cannot be written to tglPosting when this error occurs. */
			 RETURN

			 END /* @iCompanyID IS NULL */

	/* CompanyID must be valid (Get CurrID in the process) ******** */
		  SELECT @lHomeCurrID = CurrID
			 FROM tsmCompany WITH (NOLOCK)
			 WHERE CompanyID = @iCompanyID 

		  IF (@@ROWCOUNT = 0
		  OR COALESCE(DATALENGTH(LTRIM(RTRIM(@lHomeCurrID))), 0) = 0)
			 BEGIN

			 SELECT @oSeverity = 2, 
					@oRetVal = 21, 
					@lErrMsgNo = @lInvalidCoID

			 EXECUTE spciLogError @iBatchKey, 
								  NULL, 
								  @lErrMsgNo, 
								  @iCompanyID, 
								  '', '', '', '', 
								  3, 
								  @oSeverity, 
								  @lErrorLogRetVal OUTPUT,
								  @ioSessionID

	/* Rows cannot be written to tglPosting when this error occurs. */
			 RETURN

			 END /* @@ROWCOUNT = 0 [CompanyID doesn't exist in tsmCompany] */

	/* Does the Home Currency Exist? ****************************** */
		  SELECT @lIsCurrIDUsed = IsUsed
			 FROM tmcCurrency WITH (NOLOCK)
			 WHERE CurrID = @lHomeCurrID 

		  IF @@ROWCOUNT = 0
			 BEGIN

			 SELECT @oSeverity = 2, 
					@oRetVal = 25, 
					@lErrMsgNo = @lInvalidCurr

			 EXECUTE spciLogError @iBatchKey, 
								  NULL, 
								  @lErrMsgNo, 
								  @lHomeCurrID, 
								  '', '', '', '', 
								  3, 
								  @oSeverity, 
								  @lErrorLogRetVal OUTPUT,
								  @ioSessionID

	/* Rows cannot be written to tglPosting when this error occurs. */
			 RETURN

			 END /* @@ROWCOUNT = 0 [CurrID doesn't exist in tmcCurrency] */

	/* Is the Home Currency Used? ********************************* */
		  IF @lIsCurrIDUsed = 0
			 BEGIN

			 SELECT @oSeverity = 2, 
					@oRetVal = 23, 
					@lErrMsgNo = @lNotUsedCurr

			 EXECUTE spciLogError @iBatchKey, 
								  NULL, 
								  @lErrMsgNo, 
								  @lHomeCurrID, 
								  '', '', '', '', 
								  3, 
								  @oSeverity,
								  @lErrorLogRetVal OUTPUT,
								  @ioSessionID

	/* Rows cannot be written to tglPosting when this error occurs. */
			 RETURN
	 
			 END /* @lIsCurrIDUsed = 0 */

	/* Get the GL Options information  **************************** */
		  SELECT @lAutoAcctAdd = AutoAcctAdd,
				 @lUseMultCurr = UseMultCurr,
				 @lGLAcctMask = AcctMask,
				 @lAcctRefUsage = AcctRefUsage
			 FROM tglOptions WITH (NOLOCK)
			 WHERE CompanyID = @iCompanyID

		  IF @@ROWCOUNT = 0
			 BEGIN

			 SELECT @oSeverity = 2, 
					@oRetVal = 24, 
					@lErrMsgNo = @lInvalidGLOpts

			 EXECUTE spciLogError @iBatchKey, 
								  NULL, 
								  @lErrMsgNo,
								  @iCompanyID, 
								  '', '', '', '', 
								  3, 
								  @oSeverity, 
								  @lErrorLogRetVal OUTPUT,
								  @ioSessionID

	/* Rows cannot be written to tglPosting when this error occurs. */
			 RETURN

			 END /* @@ROWCOUNT = 0 [tglOptions row doesn't exist] */
      ELSE
/* @iVerifyParams <> 1 */
/* No, we are NOT going to verify the input parameters. */
         BEGIN

/* We simply transfer the passed in values to local variables. */
/* Obviously this simply ASSUMES that the values are valid. */
/* It is up to the application developer to make sure that is correct. */
         SELECT @lLanguageID = @iLanguageID
         SELECT @lHomeCurrID = @iHomeCurrID
         SELECT @lIsCurrIDUsed = @iIsCurrIDUsed
         SELECT @lAutoAcctAdd = @iAutoAcctAdd
         SELECT @lUseMultCurr = @iUseMultCurr
         SELECT @lGLAcctMask = @iGLAcctMask
         SELECT @lAcctRefUsage = @iAcctRefUsage

         END /* End @iVerifyParams <> 1 */

      END /* End @iVerifyParams = 1 */



/* Assume that no errors will occur. */
   SELECT @lErrorsOccurred = 0

/* Validate the GL accounts in #tglValidateAcct now */
--=============================================================================================================================================================================================================
	IF @iValidateGLAccts = 1
	BEGIN

		/* Make sure all GL accounts exist in tglAccount. */
		--SELECT 'Validating that GL Accounts exist in tglAccount'
		UPDATE #tglValidateAcct
        SET ValidationRetVal = 10,
             ErrorMsgNo = @lMissingAcctKey
        WHERE GLAcctKey NOT IN (SELECT GLAcctKey 
                                    FROM tglAccount WITH (NOLOCK))
         AND ValidationRetVal = 0
		OPTION (KEEP PLAN)

		/* Did a validation error occur above? */
		IF @@ROWCOUNT > 0
		BEGIN

			SELECT @lErrorsOccurred = 1
			--SELECT 'Error occurred validating that GL Accounts exist in tglAccount'
			SELECT @lValidateAcctRetVal = 10,
				@oSeverity = @lFatalError

			/* An error DID occur, so log it */
			INSERT #tciErrorStg (
				  GLAcctKey,   BatchKey,    ErrorType,   Severity, 
				  StringData1, StringData2, StringData3, StringData4, 
				  StringData5, StringNo)
			SELECT GLAcctKey,
				   @iBatchKey,       /* BatchKey */
				   @lInterfaceError, /* ErrorType */
				   @lFatalError,     /* ValidationSeverity */
				   GLAcctKey,        /* ErrorStrData1 */    
				   '',               /* ErrorStrData2 */
				   '',               /* ErrorStrData3 */
				   '',               /* ErrorStrData4 */
				   '',               /* ErrorStrData5 */
				   @lMissingAcctKey  /* ErrorMsgNo */
			   FROM #tglValidateAcct WITH (NOLOCK)
			   WHERE ValidationRetVal = 10
			   AND ErrorMsgNo = @lMissingAcctKey
				OPTION (KEEP PLAN)
				/* Rows cannot be written to tglPosting when this error occurs. */
			
			GOTO FinishProc

		END /* End @@ROWCOUNT > 0 [Error Occurred] */

		/* Make sure all GL accounts exist in tglAccount for this Company. */
		--SELECT 'Validating that GL Accounts exist in tglAccount for this Company'
		UPDATE #tglValidateAcct
        SET ValidationRetVal = 9,
             ErrorMsgNo = @lInvalidAcctCo
        WHERE GLAcctKey NOT IN (SELECT GLAcctKey 
                                    FROM tglAccount WITH (NOLOCK)
                                    WHERE CompanyID = @iCompanyID)
         AND ValidationRetVal = 0
			OPTION (KEEP PLAN)
			
		/* Did a validation error occur above? */
		IF @@ROWCOUNT > 0
        BEGIN

			SELECT @lErrorsOccurred = 1
			--SELECT 'Error occurred validating that GL Accounts exist for this Company'
			SELECT @lValidateAcctRetVal = 9,
                @oSeverity = @lFatalError

			/* An error DID occur, so log it */
			/* First, format the GL accounts used in the error message. */
			EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
			
			INSERT INTO #tglAcctMask
            SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
                            c.FormattedGLAcctNo  /* MaskedGLAcctNo */
               FROM #tglValidateAcct a WITH (NOLOCK),
                    tglAccount b WITH (NOLOCK),
                    vFormattedGLAcct c WITH (NOLOCK)
               WHERE a.GLAcctKey = b.GLAcctKey
               AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
               AND a.ValidationRetVal = 9
               AND a.ErrorMsgNo = @lInvalidAcctCo
			OPTION (KEEP PLAN)
         --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
         --EXECUTE spglSetAPIFormatAccount @iCompanyID

/* Then populate the temporary error log. */
			INSERT #tciErrorStg (
				GLAcctKey,   BatchKey,    ErrorType,   Severity, 
				StringData1, StringData2, StringData3, StringData4, 
				StringData5, StringNo)
			SELECT a.GLAcctKey,
				@iBatchKey,                          /* BatchKey */
				@lInterfaceError,                    /* ErrorType */
				@lFatalError,                        /* ValidationSeverity */
				CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */    
				@iCompanyID,                         /* ErrorStrData2 */
				'',                                  /* ErrorStrData3 */
				'',                                  /* ErrorStrData4 */
				'',                                  /* ErrorStrData5 */
				@lInvalidAcctCo                      /* ErrorMsgNo */
			FROM #tglValidateAcct a WITH (NOLOCK),
				tglAccount b WITH (NOLOCK),
				#tglAcctMask c WITH (NOLOCK)
			WHERE a.GLAcctKey = b.GLAcctKey
				AND b.GLAcctNo = c.GLAcctNo
				AND a.ValidationRetVal = 9
				AND a.ErrorMsgNo = @lInvalidAcctCo
			OPTION (KEEP PLAN)

        END /* End @@ROWCOUNT > 0 [Error Occurred] */

		/* Check for Mask Characters in the GL Account Number */
		IF @iAllowWildCard = 0
		BEGIN
			--SELECT 'Validating that no mask characters exist in GL Accounts'
			UPDATE #tglValidateAcct
				SET ValidationRetVal = 4,
					ErrorMsgNo = @lMaskedGLAcct
			WHERE GLAcctKey IN (SELECT GLAcctKey 
								FROM tglAccount WITH (NOLOCK)
								WHERE CHARINDEX('*', GLAcctNo) > 0)
				AND ValidationRetVal = 0
			OPTION (KEEP PLAN)

			/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
			BEGIN

				SELECT @lErrorsOccurred = 1
					--SELECT 'Error occurred validating that no mask characters exist in GL Accounts'
				SELECT @lValidateAcctRetVal = 4,
						@oSeverity = @lFatalError

				/* An error DID occur, so log it */
				/* First, format the GL accounts used in the error message. */
				EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
				INSERT INTO #tglAcctMask
				SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
					c.FormattedGLAcctNo  /* MaskedGLAcctNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK),
						vFormattedGLAcct c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
					AND a.ValidationRetVal = 4
					AND a.ErrorMsgNo = @lMaskedGLAcct
				OPTION (KEEP PLAN)
			--SELECT * FROM #tglAcctMask

	/* Second, execute the sp that formats the GL accounts. */
			--EXECUTE spglSetAPIFormatAccount @iCompanyID
			--SELECT * FROM #tglAcctMask

	/* Then populate the temporary error log. */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
				SELECT a.GLAcctKey,
					@iBatchKey,                          /* BatchKey */
					@lInterfaceError,                    /* ErrorType */
					@lFatalError,                        /* ValidationSeverity */
					CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
					'',                                  /* ErrorStrData2 */
					'',                                  /* ErrorStrData3 */
					'',                                  /* ErrorStrData4 */
					'',                                  /* ErrorStrData5 */
					@lMaskedGLAcct                       /* ErrorMsgNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					#tglAcctMask c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo
					AND a.ValidationRetVal = 4
					AND a.ErrorMsgNo = @lMaskedGLAcct
				OPTION (KEEP PLAN)

			END /* End @@ROWCOUNT > 0 [Error Occurred] */
		END /* End @iAllowWildCard = 0 [Wildcard characters are NOT allowed] */

		/* Active Account Validation */
		IF @iAllowActiveOnly = 1
		BEGIN

			/* First check for inactive GL accounts */
			--SELECT 'Validating that there are no inactive GL Accounts'
			UPDATE #tglValidateAcct
					SET ValidationRetVal = 12,
						ErrorMsgNo = @lInactiveGLAcct
			WHERE GLAcctKey IN (SELECT GLAcctKey 
								FROM tglAccount WITH (NOLOCK)
								WHERE Status = 2)
				AND ValidationRetVal = 0
			OPTION (KEEP PLAN)
 
/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
            BEGIN

				SELECT @lErrorsOccurred = 1
				--SELECT 'Error occurred validating that there are no inactive GL Accounts'
				SELECT @lValidateAcctRetVal = 12,
						@oSeverity = @lFatalError

				/* An error DID occur, so log it */
				/* First, format the GL accounts used in the error message. */
				EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
				INSERT INTO #tglAcctMask
				SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
					c.FormattedGLAcctNo  /* MaskedGLAcctNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					vFormattedGLAcct c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
					AND a.ValidationRetVal = 12
					AND a.ErrorMsgNo = @lInactiveGLAcct
				OPTION (KEEP PLAN)
				--SELECT * FROM #tglAcctMask

				/* Second, execute the sp that formats the GL accounts. */
				--EXECUTE spglSetAPIFormatAccount @iCompanyID
				--SELECT * FROM #tglAcctMask

				/* Then populate the temporary error log. */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
					SELECT a.GLAcctKey,
					@iBatchKey,                          /* BatchKey */
					@lInterfaceError,                    /* ErrorType */
					@lFatalError,                        /* ValidationSeverity */
					CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
					'',                                  /* ErrorStrData2 */
					'',                                  /* ErrorStrData3 */
					'',                                  /* ErrorStrData4 */
					'',                                  /* ErrorStrData5 */
					@lInactiveGLAcct                     /* ErrorMsgNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					#tglAcctMask c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo
					AND a.ValidationRetVal = 12
					AND a.ErrorMsgNo = @lInactiveGLAcct
				OPTION (KEEP PLAN)
            END /* End @@ROWCOUNT > 0 [Error Occurred] */

/* Then check for deleted GL accounts */
         --SELECT 'Validating that there are no deleted GL Accounts'
			UPDATE #tglValidateAcct
            SET ValidationRetVal = 12,
                ErrorMsgNo = @lDeletedGLAcct
            WHERE GLAcctKey IN (SELECT GLAcctKey 
                                   FROM tglAccount WITH (NOLOCK)
                                   WHERE Status = 3)
            AND ValidationRetVal = 0
			OPTION (KEEP PLAN)
/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
			BEGIN

				SELECT @lErrorsOccurred = 1
				--SELECT 'Error occurred validating that there are no deleted GL Accounts'
				SELECT @lValidateAcctRetVal = 12,
						@oSeverity = @lFatalError

				/* An error DID occur, so log it */
				/* First, format the GL accounts used in the error message. */
				EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
				INSERT INTO #tglAcctMask
				SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
					c.FormattedGLAcctNo  /* MaskedGLAcctNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					vFormattedGLAcct c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
					AND a.ValidationRetVal = 12
					AND a.ErrorMsgNo = @lDeletedGLAcct
				OPTION (KEEP PLAN)
				--SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
            --EXECUTE spglSetAPIFormatAccount @iCompanyID
            --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
               INSERT #tciErrorStg (
                  GLAcctKey,   BatchKey,    ErrorType,   Severity, 
                  StringData1, StringData2, StringData3, StringData4, 
                  StringData5, StringNo)
               SELECT a.GLAcctKey,
                      @iBatchKey,                          /* BatchKey */
                      @lInterfaceError,                    /* ErrorType */
                      @lFatalError,                        /* ValidationSeverity */
                      CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
                      '',                                  /* ErrorStrData2 */
                      '',                                  /* ErrorStrData3 */
                      '',                                  /* ErrorStrData4 */
                      '',                                  /* ErrorStrData5 */
                      @lDeletedGLAcct                      /* ErrorMsgNo */
                  FROM #tglValidateAcct a WITH (NOLOCK),
                       tglAccount b WITH (NOLOCK),
                       #tglAcctMask c WITH (NOLOCK)
                  WHERE a.GLAcctKey = b.GLAcctKey
                  AND b.GLAcctNo = c.GLAcctNo
                  AND a.ValidationRetVal = 12
                  AND a.ErrorMsgNo = @lDeletedGLAcct
				OPTION (KEEP PLAN)
            END /* End @@ROWCOUNT > 0 [Error Occurred] */
        END /* End @iAllowWildCard = 0 [Wildcard characters are NOT allowed] */

/* Financial Account Restriction */
		IF @iFinancial IS NOT NULL
		BEGIN

			/* Allow Financial Accounts Only */
			IF @iFinancial = 1
            BEGIN

				--SELECT 'Validating that there are no non-financial GL Accounts'
				UPDATE #tglValidateAcct
				   SET ValidationRetVal = 17,
					   ErrorMsgNo = @lNonFinlGLAcct
				   WHERE GLAcctKey IN (SELECT GLAcctKey 
										  FROM tglAccount a WITH (NOLOCK),
											   tglNaturalAcct b WITH (NOLOCK),
											   tglAcctType c WITH (NOLOCK)
										  WHERE a.NaturalAcctKey = b.NaturalAcctKey
										  AND b.AcctTypeKey = c.AcctTypeKey
										  AND a.CompanyID = @iCompanyID
										  AND c.AcctTypeID = 901)
				   AND ValidationRetVal = 0
					OPTION (KEEP PLAN)
/* Did a validation error occur above? */
				IF @@ROWCOUNT > 0
				BEGIN

					SELECT @lErrorsOccurred = 1
					--SELECT 'Error occurred validating that there are no non-financial GL Accounts'
					SELECT @lValidateAcctRetVal = 17,
							@oSeverity = @lFatalError

					/* An error DID occur, so log it */
					/* First, format the GL accounts used in the error message. */
					EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
					INSERT INTO #tglAcctMask
					SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
						c.FormattedGLAcctNo  /* MaskedGLAcctNo */
					FROM #tglValidateAcct a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK),
						vFormattedGLAcct c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
						AND a.ValidationRetVal = 17
						AND a.ErrorMsgNo = @lNonFinlGLAcct
					OPTION (KEEP PLAN)
               --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
               --EXECUTE spglSetAPIFormatAccount @iCompanyID
               --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
					INSERT #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT a.GLAcctKey,
						@iBatchKey,                          /* BatchKey */
						@lInterfaceError,                    /* ErrorType */
						@lFatalError,                        /* ValidationSeverity */
						CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
						'',                                  /* ErrorStrData2 */
						'',                                  /* ErrorStrData3 */
						'',                                  /* ErrorStrData4 */
						'',                                  /* ErrorStrData5 */
						@lNonFinlGLAcct                      /* ErrorMsgNo */
					FROM #tglValidateAcct a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK),
						#tglAcctMask c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo
						AND a.ValidationRetVal = 17
						AND a.ErrorMsgNo = @lNonFinlGLAcct
					OPTION (KEEP PLAN)
				END /* End @@ROWCOUNT > 0 [Error Occurred] */

			END /* End @iFinancial = 1 [Allow Financial Accounts Only] */

/* Allow Non-Financial Accounts Only */
			IF @iFinancial = 0
            BEGIN

            --SELECT 'Validating that there are no financial GL Accounts'
				UPDATE #tglValidateAcct
				SET ValidationRetVal = 17,
				   ErrorMsgNo = @lFinlGLAcct
				WHERE GLAcctKey IN (SELECT GLAcctKey 
									  FROM tglAccount a WITH (NOLOCK),
										   tglNaturalAcct b WITH (NOLOCK),
										   tglAcctType c WITH (NOLOCK)
									  WHERE a.NaturalAcctKey = b.NaturalAcctKey
									  AND b.AcctTypeKey = c.AcctTypeKey
									  AND a.CompanyID = @iCompanyID
									  AND c.AcctTypeID <> 901)
				AND ValidationRetVal = 0
				OPTION (KEEP PLAN)
/* Did a validation error occur above? */
				IF @@ROWCOUNT > 0
				BEGIN

					SELECT @lErrorsOccurred = 1
					--SELECT 'Error occurred validating that there are no financial GL Accounts'
					SELECT @lValidateAcctRetVal = 17,
							@oSeverity = @lFatalError

					/* An error DID occur, so log it */
					/* First, format the GL accounts used in the error message. */
					EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
					INSERT INTO #tglAcctMask
					SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
						c.FormattedGLAcctNo  /* MaskedGLAcctNo */
					FROM #tglValidateAcct a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK),
						vFormattedGLAcct c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
						AND a.ValidationRetVal = 17
						AND a.ErrorMsgNo = @lFinlGLAcct
					OPTION (KEEP PLAN)
               --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
               --EXECUTE spglSetAPIFormatAccount @iCompanyID
               --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
					INSERT #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT a.GLAcctKey,
						@iBatchKey,                          /* BatchKey */
						@lInterfaceError,                    /* ErrorType */
						@lFatalError,                        /* ValidationSeverity */
						CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
						'',                                  /* ErrorStrData2 */
						'',                                  /* ErrorStrData3 */
						'',                                  /* ErrorStrData4 */
						'',                                  /* ErrorStrData5 */
						@lFinlGLAcct                         /* ErrorMsgNo */
					FROM #tglValidateAcct a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK),
						#tglAcctMask c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo
						AND a.ValidationRetVal = 17
						AND a.ErrorMsgNo = @lFinlGLAcct
					OPTION (KEEP PLAN)
				END /* End @@ROWCOUNT > 0 [Error Occurred] */
			END /* End @iFinancial = 0 [Allow Non-Financial Accounts Only] */
		END /* End @iFinancial IS NOT NULL [We DO care about the Financial Acct. Restriction] */

/* Post Type Restriction */
		IF @iPostTypeFlag IS NOT NULL
		BEGIN

/* Financial Accounts Only */
			IF @iPostTypeFlag = 1 
			BEGIN

				--SELECT 'Validating that there are no statistical only GL Accounts'
				UPDATE #tglValidateAcct
				SET ValidationRetVal = 38,
					ErrorMsgNo = @lFinlPostType
				WHERE GLAcctKey IN (SELECT GLAcctKey 
									FROM tglAccount WITH (NOLOCK)
									WHERE CompanyID = @iCompanyID
										AND Status = 1
										AND PostingType NOT IN (1,3))
					AND ValidationRetVal = 0
				OPTION (KEEP PLAN)
/* Did a validation error occur above? */
				IF @@ROWCOUNT > 0
				BEGIN

					SELECT @lErrorsOccurred = 1
					--SELECT 'Error occurred validating that there are no statistical only GL Accounts'
					SELECT @lValidateAcctRetVal = 38,
							@oSeverity = @lFatalError

					/* An error DID occur, so log it */
					/* First, format the GL accounts used in the error message. */
					EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
					INSERT INTO #tglAcctMask
					SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
						c.FormattedGLAcctNo  /* MaskedGLAcctNo */
					FROM #tglValidateAcct a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK),
						vFormattedGLAcct c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
						AND a.ValidationRetVal = 38
						AND a.ErrorMsgNo = @lFinlPostType
					OPTION (KEEP PLAN)
               --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
               --EXECUTE spglSetAPIFormatAccount @iCompanyID
               --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
                  INSERT #tciErrorStg (
                     GLAcctKey,   BatchKey,    ErrorType,   Severity, 
                     StringData1, StringData2, StringData3, StringData4, 
                     StringData5, StringNo)
                  SELECT a.GLAcctKey,
                         @iBatchKey,                          /* BatchKey */
                         @lInterfaceError,                    /* ErrorType */
                         @lFatalError,                        /* ValidationSeverity */
                         CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
                         '',                                  /* ErrorStrData2 */
                         '',                                  /* ErrorStrData3 */
                         '',                                  /* ErrorStrData4 */
                         '',                                  /* ErrorStrData5 */
                         @lFinlPostType                       /* ErrorMsgNo */
                     FROM #tglValidateAcct a WITH (NOLOCK),
                          tglAccount b WITH (NOLOCK),
                          #tglAcctMask c WITH (NOLOCK)
                     WHERE a.GLAcctKey = b.GLAcctKey
                     AND b.GLAcctNo = c.GLAcctNo
                     AND a.ValidationRetVal = 38
                     AND a.ErrorMsgNo = @lFinlPostType
					OPTION (KEEP PLAN)
				END /* End @@ROWCOUNT > 0 [Error Occurred] */

			END /* End @iPostTypeFlag = 1 [Only Allow Financial GL Accounts] */

			/* Statistical Accounts Only */
			IF @iPostTypeFlag = 0
            BEGIN

				--SELECT 'Validating that there are no financial only GL Accounts'
				UPDATE #tglValidateAcct
				SET ValidationRetVal = 38,
					ErrorMsgNo = @lStatPostType
				WHERE GLAcctKey IN (SELECT GLAcctKey 
									FROM tglAccount WITH (NOLOCK)
									WHERE CompanyID = @iCompanyID
									AND Status = 1
									AND PostingType NOT IN (2,3))
					AND ValidationRetVal = 0
				OPTION (KEEP PLAN)
/* Did a validation error occur above? */
				IF @@ROWCOUNT > 0
				BEGIN

					SELECT @lErrorsOccurred = 1
					--SELECT 'Error occurred validating that there are no financial only GL Accounts'
					SELECT @lValidateAcctRetVal = 38,
							@oSeverity = @lFatalError

					/* An error DID occur, so log it */
					/* First, format the GL accounts used in the error message. */
					EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
					INSERT INTO #tglAcctMask
					SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
						c.FormattedGLAcctNo  /* MaskedGLAcctNo */
					FROM #tglValidateAcct a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK),
						vFormattedGLAcct c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
						AND a.ValidationRetVal = 38
						AND a.ErrorMsgNo = @lStatPostType
					OPTION (KEEP PLAN)
               --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
               --EXECUTE spglSetAPIFormatAccount @iCompanyID
               --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
                  INSERT #tciErrorStg (
                     GLAcctKey,   BatchKey,    ErrorType,   Severity, 
                     StringData1, StringData2, StringData3, StringData4, 
                     StringData5, StringNo)
                  SELECT a.GLAcctKey,
                         @iBatchKey,                          /* BatchKey */
                         @lInterfaceError,                    /* ErrorType */
                         @lFatalError,                        /* ValidationSeverity */
                         CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
                         '',                                  /* ErrorStrData2 */
                         '',                                  /* ErrorStrData3 */
                         '',                                  /* ErrorStrData4 */
                         '',                                  /* ErrorStrData5 */
                         @lStatPostType                       /* ErrorMsgNo */
                     FROM #tglValidateAcct a WITH (NOLOCK),
                          tglAccount b WITH (NOLOCK),
                          #tglAcctMask c WITH (NOLOCK)
                     WHERE a.GLAcctKey = b.GLAcctKey
                     AND b.GLAcctNo = c.GLAcctNo
                     AND a.ValidationRetVal = 38
                     AND a.ErrorMsgNo = @lStatPostType
					OPTION (KEEP PLAN)
				END /* End @@ROWCOUNT > 0 [Error Occurred] */

			END /* End @iPostTypeFlag = 0 [Only Allow Statistical Accounts] */

		END /* End @iPostTypeFlag IS NOT NULL */

/* Effective Date Restrictions */
		IF @iEffectiveDate IS NOT NULL
		BEGIN
			/* Check Effective Start Date */
			--SELECT 'Validating that there are no GL Account effective start date violations'
			UPDATE #tglValidateAcct
			SET ValidationRetVal = 13,
				ErrorMsgNo = @lGLAcctStartDateError
			WHERE GLAcctKey IN (SELECT GLAcctKey 
								FROM tglAccount WITH (NOLOCK)
								WHERE CompanyID = @iCompanyID
									AND Status = 1
									AND EffStartDate IS NOT NULL
									AND EffStartDate > @iEffectiveDate)
				AND ValidationRetVal = 0
			OPTION (KEEP PLAN)
/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
            BEGIN

				SELECT @lErrorsOccurred = 1
				--SELECT 'Error occurred validating GL Account effective start date violations'
				SELECT @lValidateAcctRetVal = 13,
					@oSeverity = @lFatalError

				/* An error DID occur, so log it */
				/* First, format the GL accounts used in the error message. */
				EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
				INSERT INTO #tglAcctMask
				SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
					c.FormattedGLAcctNo  /* MaskedGLAcctNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					vFormattedGLAcct c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
					AND a.ValidationRetVal = 13
					AND a.ErrorMsgNo = @lGLAcctStartDateError
				OPTION (KEEP PLAN)
            --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
            --EXECUTE spglSetAPIFormatAccount @iCompanyID
            --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
               INSERT #tciErrorStg (
                  GLAcctKey,   BatchKey,    ErrorType,   Severity, 
                  StringData1, StringData2, StringData3, StringData4, 
                  StringData5, StringNo)
               SELECT a.GLAcctKey,
                      @iBatchKey,                                                  /* BatchKey */
                      @lInterfaceError,                                            /* ErrorType */
                      @lFatalError,                                                /* ValidationSeverity */
                      CONVERT(VARCHAR(10), @iEffectiveDate, @lConvertToMMDDYYYYDate), /* ErrorStrData1 */
                      CONVERT(VARCHAR(30), c.MaskedGLAcctNo),                         /* ErrorStrData2 */
                      CONVERT(VARCHAR(10), b.EffStartDate, @lConvertToMMDDYYYYDate),  /* ErrorStrData3 */
                      '',                                                          /* ErrorStrData4 */
                      '',                                                          /* ErrorStrData5 */
                      @lGLAcctStartDateError                                       /* ErrorMsgNo */
                  FROM #tglValidateAcct a WITH (NOLOCK),
                       tglAccount b WITH (NOLOCK),
                       #tglAcctMask c WITH (NOLOCK)
                  WHERE a.GLAcctKey = b.GLAcctKey
                  AND b.GLAcctNo = c.GLAcctNo
                  AND a.ValidationRetVal = 13
                  AND a.ErrorMsgNo = @lGLAcctStartDateError
				OPTION (KEEP PLAN)
			END /* End @@ROWCOUNT > 0 [Error Occurred] */

			/* Check Effective End Date */
			--SELECT 'Validating that there are no GL Account effective end date violations'
			UPDATE #tglValidateAcct
			SET ValidationRetVal = 13,
			ErrorMsgNo = @lGLAcctEndDateError
			WHERE GLAcctKey IN (SELECT GLAcctKey 
								FROM tglAccount WITH (NOLOCK)
								WHERE CompanyID = @iCompanyID
									AND Status = 1
									AND EffEndDate IS NOT NULL
									AND EffEndDate < @iEffectiveDate)
				AND ValidationRetVal = 0
			OPTION (KEEP PLAN)
/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
            BEGIN

				SELECT @lErrorsOccurred = 1
				--SELECT 'Error occurred validating GL Account effective end date violations'
				SELECT @lValidateAcctRetVal = 13,
						@oSeverity = @lFatalError

				/* An error DID occur, so log it */
				/* First, format the GL accounts used in the error message. */
				EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
				INSERT INTO #tglAcctMask
				SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
						c.FormattedGLAcctNo  /* MaskedGLAcctNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
						tglAccount b WITH (NOLOCK),
						vFormattedGLAcct c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
					AND a.ValidationRetVal = 13
					AND a.ErrorMsgNo = @lGLAcctEndDateError
				OPTION (KEEP PLAN)
            --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
            --EXECUTE spglSetAPIFormatAccount @iCompanyID
            --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
				SELECT a.GLAcctKey,
					@iBatchKey,                                                  /* BatchKey */
					@lInterfaceError,                                            /* ErrorType */
					@lFatalError,                                                /* ValidationSeverity */
					CONVERT(VARCHAR(10), @iEffectiveDate, @lConvertToMMDDYYYYDate), /* ErrorStrData1 */
					CONVERT(VARCHAR(30), c.MaskedGLAcctNo),                         /* ErrorStrData2 */
					CONVERT(VARCHAR(10), b.EffEndDate, @lConvertToMMDDYYYYDate),    /* ErrorStrData3 */
					'',                                                          /* ErrorStrData4 */
					'',                                                          /* ErrorStrData5 */
					@lGLAcctEndDateError                                         /* ErrorMsgNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					#tglAcctMask c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo
					AND a.ValidationRetVal = 13
					AND a.ErrorMsgNo = @lGLAcctEndDateError
				OPTION (KEEP PLAN)

			END /* End @@ROWCOUNT > 0 [Error Occurred] */

		END /* End @iEffectiveDate IS NOT NULL [We DO care about GL account Effective Dates] */

	END /* @iValidateGLAccts = 1 [Validate GL accounts in #tglValidateAcct] */
--=============================================================================================================================================================================================================
/* Validate the Account Reference ID's in #tglValidateAcct now */
	IF (@iValidateAcctRefs = 1 AND @lAcctRefUsage <> 0)
	BEGIN

		EXECUTE spglSetAPIValidateAcctRef @iCompanyID,                      /* CompanyID */
										@iBatchKey,                       /* BatchKey */
										@ioSessionID            OUTPUT, /* ErrorLogKey */
										@iUserID,                         /* UserID */
										@lLanguageID,                     /* LanguageID */
										@lAcctRefUsage,                   /* AcctRefUsage */
										@iEffectiveDate,                  /* EffectiveDate */
										0,                                /* VerifyParams (No) */
										@lValidateAcctRefSeverity OUTPUT, /* Severity */
										@lValidateAcctRefRetVal   OUTPUT  /* RetVal */

		/* Did the Account Reference Code validation go OK? */
		--SELECT 'spglSetAPIValidateAcctRef RetVal = ', @lValidateAcctRefRetVal
		IF @lValidateAcctRefRetVal IN (19,20,21,23,24,25,30,33,34)
		BEGIN
			SELECT @oSeverity = 2, @lAcctRefValFail = 1,
			@oRetVal = @lValidateAcctRefRetVal
			/* Rows CANNOT be written to tglPosting in these cases. */
			GOTO FinishProc
		--RETURN --GERARD 
		END /* End @lValidateAcctRefRetVal IN (19,20,21,23,24,25,30,33,34) */

		IF @lValidateAcctRefRetVal NOT IN (0,1)
			SELECT @lValidateAcctRetVal = @lValidateAcctRefRetVal

      --SELECT 'spglSetAPIValidateAcctRef Severity = ', @lValidateAcctRefSeverity
      --SELECT 'oSeverity = ', @oSeverity
		IF (@lValidateAcctRefSeverity > 0 AND @oSeverity <> 2)
			SELECT @oSeverity = @lValidateAcctRefSeverity
       
/* In this case there is no need to update #tglPosting with validation */
/* error information from #tglValidateAcct.  All we need is the RetVal value. */
      --SELECT 'ValidateAccountRetVal after spglSetAPIValidateAcctRef = ', @lValidateAcctRetVal
      --SELECT 'Severity after spglSetAPIValidateAcctRef = ', @oSeverity

/* Verify that Account Reference Codes are valid for all Account Segments */
/* This validation is only done for the Validated ARC's Case */
		IF @lAcctRefUsage = 1
			BEGIN

/* Get Maximum GL Segments ************************************ */
				SELECT @lMaxAccountSegments = COUNT(SegmentKey)
				FROM tglSegment WITH (NOLOCK)
				WHERE CompanyID = @iCompanyID
         --SELECT 'Maximum Account Segments = ', @lMaxAccountSegments

/* Verify that Account Reference Codes are valid for all Account Segments */
				IF @lMaxAccountSegments > 0
				BEGIN

/* Uncomment this SQL Query to debug the UPDATE statement below it */
--         SELECT c.AcctRefKey
--            FROM tglAcctSegment a WITH (NOLOCK),
--                 tglAcctRefUsage b WITH (NOLOCK),
--                 tglAcctRef c WITH (NOLOCK),
--                 (SELECT DISTINCT AcctRefKey, GLAcctKey
--                     FROM #tglValidateAcct WITH (NOLOCK)) d
--            WHERE a.SegmentKey = b.SegmentKey
--            AND a.AcctSegValue = b.AcctSegValue
--            AND b.AcctRefGroupKey = c.AcctRefGroupKey
--            AND d.GLAcctKey = a.GLAcctKey
--            AND d.AcctRefKey = c.AcctRefKey
--            AND COALESCE(DATALENGTH(d.AcctRefKey), 0) > 0
--            GROUP BY d.GLAcctKey,
--                     c.AcctRefKey
--            HAVING COUNT(c.AcctRefKey) = @lMaxAccountSegments

            --SELECT 'Validating that the ARCs are valid for all Account Segments'
					UPDATE #tglValidateAcct
					   SET ValidationRetVal = 31,
						   ErrorMsgNo = @lAcctRefSegs
					   WHERE AcctRefKey NOT IN
						  (SELECT c.AcctRefKey
							  FROM tglAcctSegment a WITH (NOLOCK),
								   tglAcctRefUsage b WITH (NOLOCK),
								   tglAcctRef c WITH (NOLOCK),
								   (SELECT DISTINCT AcctRefKey, 
													GLAcctKey
									   FROM #tglValidateAcct WITH (NOLOCK)) d
							  WHERE a.SegmentKey = b.SegmentKey
							  AND a.AcctSegValue = b.AcctSegValue
							  AND b.AcctRefGroupKey = c.AcctRefGroupKey
							  AND d.GLAcctKey = a.GLAcctKey
							  AND d.AcctRefKey = c.AcctRefKey
							  GROUP BY d.GLAcctKey,
									   c.AcctRefKey
							  HAVING COUNT(c.AcctRefKey) = @lMaxAccountSegments)
					   AND COALESCE(DATALENGTH(AcctRefKey), 0) > 0
					   AND ValidationRetVal = 0
						OPTION (KEEP PLAN)
/* Did a validation error occur above? */
					IF @@ROWCOUNT > 0
					BEGIN

						SELECT @lErrorsOccurred = 1
						--SELECT 'Error occurred validating that ARCs are valid for all Account Segments'
						SELECT @lValidateAcctRetVal = 31,
								@oSeverity = @lFatalError

						/* An error DID occur, so log it */
						/* First, format the GL accounts used in the error message. */
						EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
						INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
								c.FormattedGLAcctNo  /* MaskedGLAcctNo */
						FROM #tglValidateAcct a WITH (NOLOCK),
							tglAccount b WITH (NOLOCK),
							vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
							AND a.ValidationRetVal = 31
							AND a.ErrorMsgNo = @lAcctRefSegs
						OPTION (KEEP PLAN)
               --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
               --EXECUTE spglSetAPIFormatAccount @iCompanyID
               --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
						INSERT #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,
							@iBatchKey,                          /* BatchKey */
							@lInterfaceError,                    /* ErrorType */
							@lFatalError,                        /* ValidationSeverity */
							CONVERT(VARCHAR(30), c.AcctRefCode),    /* ErrorStrData1 */
							CONVERT(VARCHAR(30), d.MaskedGLAcctNo), /* ErrorStrData2 */
							'',                                  /* ErrorStrData3 */
							'',                                  /* ErrorStrData4 */
							'',                                  /* ErrorStrData5 */
							@lAcctRefSegs                        /* ErrorMsgNo */
						FROM #tglValidateAcct a WITH (NOLOCK),
							tglAccount b WITH (NOLOCK),
							tglAcctRef c WITH (NOLOCK),
							#tglAcctMask d WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND a.AcctRefKey = c.AcctRefKey
							AND b.GLAcctNo = d.GLAcctNo
							AND a.ValidationRetVal = 31
							AND a.ErrorMsgNo = @lAcctRefSegs
						OPTION (KEEP PLAN)
					END /* End @@ROWCOUNT > 0 [Error Occurred] */

				END /* End @lMaxAccountSegments > 0 */

		END /* End @lAcctRefUsage = 1 [Validated Account Reference Codes] */

	END /* End @iValidateAcctRefs = 1 [Validate Account Reference Keys in #tglValidateAcct] */
--=============================================================================================================================================================================================================
/* Validate the Currency ID's in #tglValidateAcct now */
	IF @iValidateCurrIDs = 1
	BEGIN

		/* Validate that the Currency ID's exist in tmcCurrency. */
		--SELECT 'Validating that Currency IDs exist in tmcCurrency'
		UPDATE #tglValidateAcct
		SET ValidationRetVal = 25,
				ErrorMsgNo = @lInvalidCurr
		WHERE CurrID NOT IN (SELECT CurrID 
							FROM tmcCurrency WITH (NOLOCK))
							AND COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
			AND ValidationRetVal = 0
		OPTION (KEEP PLAN)
/* Did a validation error occur above? */
		IF @@ROWCOUNT > 0
		BEGIN

			SELECT @lErrorsOccurred = 1
			--SELECT 'Error occurred validating that Currency IDs exist in tmcCurrency'
			SELECT @lValidateAcctRetVal = 25,
			@oSeverity = @lFatalError

			/* An error DID occur, so log it */
			INSERT #tciErrorStg (
				GLAcctKey,   BatchKey,    ErrorType,   Severity, 
				StringData1, StringData2, StringData3, StringData4, 
				StringData5, StringNo)
			SELECT GLAcctKey,
				@iBatchKey,       /* BatchKey */
				@lInterfaceError, /* ErrorType */
				@lFatalError,     /* ValidationSeverity */
				CurrID,           /* ErrorStrData1 */
				'',               /* ErrorStrData2 */
				'',               /* ErrorStrData3 */
				'',               /* ErrorStrData4 */
				'',               /* ErrorStrData5 */
				@lInvalidCurr     /* ErrorMsgNo */
			FROM #tglValidateAcct WITH (NOLOCK)
			WHERE ValidationRetVal = 25
				AND ErrorMsgNo = @lInvalidCurr
			OPTION (KEEP PLAN)
			/* Rows cannot be written to tglPosting when this error occurs. */
			GOTO FinishProc

         END /* End @@ROWCOUNT > 0 [Error Occurred] */

		/* Validate that the Currency ID's are used in MC. */
		--SELECT 'Validating that Currency IDs are used in tmcCurrency'
		UPDATE #tglValidateAcct
		SET ValidationRetVal = 23,
		ErrorMsgNo = @lNotUsedCurr
		WHERE CurrID IN (SELECT CurrID 
			FROM tmcCurrency WITH (NOLOCK)
			WHERE IsUsed = 0)
		AND ValidationRetVal = 0
		OPTION (KEEP PLAN)
/* Did a validation error occur above? */
		IF @@ROWCOUNT > 0
		BEGIN

			SELECT @lErrorsOccurred = 1
			--SELECT 'Error occurred validating that Currency IDs are used in tmcCurrency'
			SELECT @lValidateAcctRetVal = 23,
			@oSeverity = @lFatalError

			/* An error DID occur, so log it */
			INSERT #tciErrorStg (
				GLAcctKey,   BatchKey,    ErrorType,   Severity, 
				StringData1, StringData2, StringData3, StringData4, 
				StringData5, StringNo)
			SELECT GLAcctKey,
				@iBatchKey,       /* BatchKey */
				@lInterfaceError, /* ErrorType */
				@lFatalError,     /* ValidationSeverity */
				CurrID,           /* ErrorStrData1 */
				'',               /* ErrorStrData2 */
				'',               /* ErrorStrData3 */
				'',               /* ErrorStrData4 */
				'',               /* ErrorStrData5 */
				@lNotUsedCurr     /* ErrorMsgNo */
			FROM #tglValidateAcct WITH (NOLOCK)
			WHERE ValidationRetVal = 23
				AND ErrorMsgNo = @lNotUsedCurr
			OPTION (KEEP PLAN)
		END /* End @@ROWCOUNT > 0 [Error Occurred] */

/* Make sure CurrID's are Home Currency IF Multicurrency is NOT used. */
		IF @lUseMultCurr <> 1
		BEGIN      
			/* Multicurrency is NOT used by this Acuity Company */
			--SELECT 'Validating that Curr IDs are Home Curr IDs (No MC)'
			UPDATE #tglValidateAcct
			SET ValidationRetVal = 26,
				ErrorMsgNo = @lMultCurrError
			WHERE CurrID <> @lHomeCurrID
			AND ValidationRetVal = 0
			OPTION (KEEP PLAN)
/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
            BEGIN

				SELECT @lErrorsOccurred = 1
				--SELECT 'Error occurred validating that Curr IDs are Home Curr IDs (No MC)'
				SELECT @lValidateAcctRetVal = 26,
				@oSeverity = @lFatalError

				/* An error DID occur, so log it */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
				SELECT GLAcctKey,
					@iBatchKey,       /* BatchKey */
					@lInterfaceError, /* ErrorType */
					@lFatalError,     /* ValidationSeverity */
					@iCompanyID,      /* ErrorStrData1 */
					'',               /* ErrorStrData2 */
					'',               /* ErrorStrData3 */
					'',               /* ErrorStrData4 */
					'',               /* ErrorStrData5 */
					@lMultCurrError   /* ErrorMsgNo */
				FROM #tglValidateAcct WITH (NOLOCK)
				WHERE ValidationRetVal = 26
					AND ErrorMsgNo = @lMultCurrError
				OPTION (KEEP PLAN)
			END /* End @@ROWCOUNT > 0 [Error Occurred] */

		END /* End @lUseMultCurr <> 1 [MC is NOT used by this Company] */

/* Multicurrency Restriction */
		IF @iUseMultCurr = 1
		BEGIN

/* Home Currency Only Restriction (Check Financial Accounts Only) */
         --SELECT 'Validating that GL Accounts don't violate Home Curr Only restriction'
			UPDATE #tglValidateAcct
            SET ValidationRetVal = 14,
                ErrorMsgNo = @lInvalidHomeCurr
            WHERE COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
            AND CurrID <> @lHomeCurrID
            AND GLAcctKey IN (SELECT GLAcctKey 
                                 FROM tglAccount a WITH (NOLOCK),
                                      tglNaturalAcct b WITH (NOLOCK),
                                      tglAcctType c WITH (NOLOCK)
                                 WHERE a.NaturalAcctKey = b.NaturalAcctKey
                                 AND b.AcctTypeKey = c.AcctTypeKey
                                 AND a.CompanyID = @iCompanyID
                                 AND a.CurrRestriction = 0
                                 AND c.AcctTypeID <> 901)
            AND ValidationRetVal = 0
			OPTION (KEEP PLAN)
/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
            BEGIN

				SELECT @lErrorsOccurred = 1
				--SELECT 'Error occurred validating that GL Accounts are Home Curr Only'
				SELECT @lValidateAcctRetVal = 14

				IF @oSeverity <> @lFatalError
				   SELECT @oSeverity = @lWarning

	/* An error DID occur, so log it */
	/* This is actually just a warning - sp continues executing */
	/* First, format the GL accounts used in the error message. */
				EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
				INSERT INTO #tglAcctMask
				SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
					c.FormattedGLAcctNo  /* MaskedGLAcctNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					vFormattedGLAcct c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
					AND a.ValidationRetVal = 14
					AND a.ErrorMsgNo = @lInvalidHomeCurr
				OPTION (KEEP PLAN)
            --SELECT * FROM #tglAcctMask

	/* Second, execute the sp that formats the GL accounts. */
				--EXECUTE spglSetAPIFormatAccount @iCompanyID
				--SELECT * FROM #tglAcctMask

	/* Then populate the temporary error log. */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
				SELECT a.GLAcctKey,
					@iBatchKey,                          /* BatchKey */
					@lInterfaceError,                    /* ErrorType */
					@lWarning,                           /* ValidationSeverity */
					CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
					a.CurrID,                            /* ErrorStrData2 */
					'Home Curr',                         /* ErrorStrData3 */
					@lHomeCurrID,                        /* ErrorStrData4 */
					@lHomeCurrID,                        /* ErrorStrData5 */
					@lInvalidHomeCurr                    /* ErrorMsgNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					#tglAcctMask c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo
					AND a.ValidationRetVal = 14
					AND a.ErrorMsgNo = @lInvalidHomeCurr
				OPTION (KEEP PLAN)
            END /* End @@ROWCOUNT > 0 [Error Occurred] */

/* Specific Foreign Currency Restriction #1 (Check Financial Accounts Only) */
         --SELECT 'Validating that GL Accounts don't violate Specific Foreign Curr restriction (#1)'
			UPDATE #tglValidateAcct
			SET ValidationRetVal = 15,
				ErrorMsgNo = @lCurrIsHomeCurr
			WHERE COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
				AND CurrID = @lHomeCurrID
				AND GLAcctKey IN (SELECT GLAcctKey 
									FROM tglAccount a WITH (NOLOCK),
										tglNaturalAcct b WITH (NOLOCK),
										tglAcctType c WITH (NOLOCK)
									WHERE a.NaturalAcctKey = b.NaturalAcctKey
										AND b.AcctTypeKey = c.AcctTypeKey
										AND a.CompanyID = @iCompanyID
										AND a.CurrRestriction = 1
										AND c.AcctTypeID <> 901)
				AND ValidationRetVal = 0
			OPTION (KEEP PLAN)
/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
            BEGIN

				SELECT @lErrorsOccurred = 1
				--SELECT 'Error occurred validating that GL Accounts have Specific Foreign Curr (#1)'
				SELECT @lValidateAcctRetVal = 15

				IF @oSeverity <> @lFatalError
					SELECT @oSeverity = @lWarning

/* An error DID occur, so log it */
/* This is actually just a warning - sp continues executing */
/* First, format the GL accounts used in the error message. */
				EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
				INSERT INTO #tglAcctMask
				SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
					c.FormattedGLAcctNo  /* MaskedGLAcctNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					vFormattedGLAcct c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
					AND a.ValidationRetVal = 15
					AND a.ErrorMsgNo = @lCurrIsHomeCurr
				OPTION (KEEP PLAN)
            --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
            --EXECUTE spglSetAPIFormatAccount @iCompanyID
            --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
				SELECT a.GLAcctKey,
					@iBatchKey,                          /* BatchKey */
					@lInterfaceError,                    /* ErrorType */
					@lWarning,                           /* ValidationSeverity */
					CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
					a.CurrID,                            /* ErrorStrData2 */
					'Specific Foreign Curr',             /* ErrorStrData3 */
					b.RestrictedCurrID,                  /* ErrorStrData4 */
					a.CurrID,                            /* ErrorStrData5 */
					@lCurrIsHomeCurr                     /* ErrorMsgNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					#tglAcctMask c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo
					AND a.ValidationRetVal = 15
					AND a.ErrorMsgNo = @lCurrIsHomeCurr
				OPTION (KEEP PLAN)
			END /* End @@ROWCOUNT > 0 [Error Occurred] */

/* Specific Foreign Currency Restriction #2 (Check Financial Accounts Only) */
         --SELECT 'Validating that GL Accounts don't violate Specific Foreign Curr restriction (#2)'
			UPDATE #tglValidateAcct
			SET #tglValidateAcct.ValidationRetVal = 16,
				#tglValidateAcct.ErrorMsgNo = @lNotSpecificCurrency
			FROM tglAccount a WITH (NOLOCK),
				tglNaturalAcct b WITH (NOLOCK),
				tglAcctType c WITH (NOLOCK)
			WHERE #tglValidateAcct.GLAcctKey = a.GLAcctKey
				AND a.NaturalAcctKey = b.NaturalAcctKey
				AND b.AcctTypeKey = c.AcctTypeKey
				AND a.CompanyID = @iCompanyID
				AND a.CurrRestriction = 1
				AND c.AcctTypeID <> 901
				AND COALESCE(DATALENGTH(LTRIM(RTRIM(#tglValidateAcct.CurrID))), 0) > 0
				AND #tglValidateAcct.CurrID <> @lHomeCurrID
				AND #tglValidateAcct.CurrID <> a.RestrictedCurrID
				AND #tglValidateAcct.ValidationRetVal = 0
			OPTION (KEEP PLAN)
/* Did a validation error occur above? */
			IF @@ROWCOUNT > 0
            BEGIN

				SELECT @lErrorsOccurred = 1
				--SELECT 'Error occurred validating that GL Accounts have Specific Foreign Curr (#2)'
				SELECT @lValidateAcctRetVal = 16,
					@oSeverity = @lFatalError

/* An error DID occur, so log it */
/* This is actually just a warning - sp continues executing */
/* First, format the GL accounts used in the error message. */
				EXECUTE sp_executesql N'TRUNCATE TABLE #tglAcctMask'
				INSERT INTO #tglAcctMask
				SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
					c.FormattedGLAcctNo  /* MaskedGLAcctNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					vFormattedGLAcct c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
					AND a.ValidationRetVal = 16
					AND a.ErrorMsgNo = @lNotSpecificCurrency
				OPTION (KEEP PLAN)
            --SELECT * FROM #tglAcctMask

/* Second, execute the sp that formats the GL accounts. */
            --EXECUTE spglSetAPIFormatAccount @iCompanyID
            --SELECT * FROM #tglAcctMask

/* Then populate the temporary error log. */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
				SELECT a.GLAcctKey,
					@iBatchKey,                          /* BatchKey */
					@lInterfaceError,                    /* ErrorType */
					@lFatalError,                        /* ValidationSeverity */
					CONVERT(VARCHAR(30), c.MaskedGLAcctNo), /* ErrorStrData1 */
					a.CurrID,                            /* ErrorStrData2 */
					'Specific Foreign Curr',             /* ErrorStrData3 */
					b.RestrictedCurrID,                  /* ErrorStrData4 */
					'',                                  /* ErrorStrData5 */
					@lNotSpecificCurrency                /* ErrorMsgNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAccount b WITH (NOLOCK),
					#tglAcctMask c WITH (NOLOCK)
				WHERE a.GLAcctKey = b.GLAcctKey
					AND b.GLAcctNo = c.GLAcctNo
					AND a.ValidationRetVal = 16
					AND a.ErrorMsgNo = @lNotSpecificCurrency
				OPTION (KEEP PLAN)
			END /* End @@ROWCOUNT > 0 [Error Occurred] */

		END /* End @iUseMultCurr = 1 [Company DOES use Multicurrency] */

	END /* End @iValidateCurrIDs = 1 [Validate Currency ID's in #tglValidateAcct] */

FinishProc:
/* Did any errors get written to #tciErrorStg? */
	IF @lErrorsOccurred = 1
	BEGIN

/* Transfer the errors in #tciErrorStg to #tciError table. Join back to #tglPosting
so we can link each GL error to its transaction line. */
		TRUNCATE TABLE #tciError
		INSERT #tciError
			(BatchKey,        StringNo,        ErrorType,       Severity, 
			StringData1,     StringData2,     StringData3,     StringData4, 
			StringData5,     TranType,        TranKey,         InvtTranKey)
		SELECT
			tmp.BatchKey,    tmp.StringNo,    tmp.ErrorType,   tmp.Severity, 
			tmp.StringData1, tmp.StringData2, tmp.StringData3, tmp.StringData4, 
			tmp.StringData5, gl.TranType,     NULL,            gl.TranKey
		FROM #tciErrorStg tmp
			JOIN #tglPosting gl ON tmp.GLAcctKey = gl.GLAcctKey

		--SELECT 'Executing spciLogErrors, ErrorLogKey = ', @ioSessionID
		EXECUTE spciLogErrors @iBatchKey, @lSetErrorLogRetVal, @ioSessionID
		--SELECT 'spciLogErrors RetVal = ', @lSetErrorLogRetVal

	END /* End @lErrorsOccurred = 1 */

   IF @lValidateAcctRetVal = 0
      SELECT @oRetVal = 1
   ELSE
      IF @lAcctRefValFail = 1
         SELECT @oRetVal = @lValidateAcctRefRetVal
      ELSE
         SELECT @oRetVal = @lValidateAcctRetVal

   --SELECT 'oRetVal = ', @oRetVal
END /* End of the Stored Procedure */
