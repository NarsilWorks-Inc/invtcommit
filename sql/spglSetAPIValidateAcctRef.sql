USE [MDCI_MAS500_APP]
GO
/****** Object:  StoredProcedure [dbo].[spglSetAPIValidateAcctRef]    Script Date: 7/3/2019 5:40:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/********************************************************************
Procedure Name:   spglSetAPIValidateAcctRef
Author:           Chuck Lohr
Creation Date:    09/22/1999
Copyright:        Copyright (c) 1995-2001 Best Software, Inc.
                  All Rights Reserved.

Description:      Validates Account Reference Codes to be Posted to GL.

This stored procedure takes a set of Account Reference Codes from a 
temporary table called #tglValidateAcct and validates them in the same
way that the spglAPIAcctRef sp validates Account Reference Codes one at
a time.  This sp replaces the spglAPIAcctRef sp which only operated on
one row at a time (one Account Reference Code) and was called repetitively
by the spglAPIAcctPostRow sp in the subsidiary modules.
This new sp will only be called once by the spglSetAPIValidateAcct sp.

This stored procedure ASSUMES:
      (1)  The existence of a temporary table called #tglValidateAcct.
      (2)  That #tglValidateAcct has been correctly populated with n rows
           of distinct combinations of GLAcctKey+AcctRefKey+CurrID.
      (3)  That all GLAcctKey's in #tglValidateAcct are only for @iCompanyID.
      (4)  That if a @iVerifyParams value other than one (1) is passed in,
           all parameter values in the NOTE below are guaranteed to be valid.
      (5)  The calling program is NOT relying on Account Reference Codes to
           be created if AcctRefUsage is set to '2' in tglOptions.  No Account
           Reference Codes are created when this sp is used for validation.

Use this sp with other Acuity API's that begin with spglSetAPI...

Input Parameters:
   @iCompanyID        = [IN: Valid Acuity Company; No Default]
   @iBatchKey         = [IN: Valid Batch Key or NULL; Default = NULL]
   @ioSessionID     = [IN/OUT: Valid No. or NULL; No Default]
   @iUserID           = [IN: Valid User or NULL; Default = spGetLoginName]
   @iLanguageID       = [IN: Valid Language ID or NULL; Default = NULL]
   @iAcctRefUsage     = [IN: 0, 1 or NULL; Default = 0]
   @iEffectiveDate    = [IN: Effective Date or NULL]
   @iVerifyParams     = [IN: 0, 1 or NULL; Default = 1] 

NOTE: The following parameters MUST be passed in with a valid value from the
calling stored procedure IF the @iVerifyParams parameter is passed in
with a value of anything OTHER THAN one (1):
   @iCompanyID
   @ioSessionID
   @iUserID
   @iLanguageID
   @iAcctRefUsage

Output Parameters:
   @ioSessionID = [IN/OUT: Valid No. or NULL; No Default]
   @oSeverity     = [OUT: 0=None, 1=Warning, 2=Fatal; Default=0]
   @oRetVal       = [OUT: return flag indicating outcome of the procedure]
          0 = Failure.  General SP Failure.
          1 = Successful.
         19 = Failure.  Error Log Key not supplied and cannot be derived.
         20 = Failure.  Company ID not supplied.
         21 = Failure.  Company ID supplied does not exist.
         24 = Failure.  GL Options row for this Company does not exist.
         27 = Failure.  Account Reference Key exists but not for the correct Company.
         30 = Failure.  Account Reference Key supplied does not exist.
         32 = Failure.  Failure of Account Reference Code Effective Dates Restriction.
         33 = Failure.  User ID not supplied and cannot be derived.
         34 = Failure.  Language ID cannot be determined.
         37 = Failure.  Account Reference Code is not active.
         42 = Failure.  tglOptions.AcctRefUsage Flag not enabled.
         43 = Failure.  GL Account requires an Account Reference Code.
                                       
********************************************************************/

ALTER PROCEDURE [dbo].[spglSetAPIValidateAcctRef] (@iCompanyID        VARCHAR(3), 
                                            @iBatchKey         int,
                                            @ioSessionID     int      OUTPUT,
                                            @iUserID           VARCHAR(30),
                                            @iLanguageID       int,
                                            @iAcctRefUsage     smallint,
                                            @iEffectiveDate    datetime,
                                            @iVerifyParams     smallint,
                                            @oSeverity         smallint OUTPUT,
                                            @oRetVal           int      OUTPUT)

AS

   BEGIN  
/* Create a temporary error log table to log errors into. */
   --SELECT 'Creating #tciErrorStg table'
   IF OBJECT_ID('tempdb..#tciErrorStg') IS NOT NULL
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
      CREATE TABLE #tglAcctMask (GLAcctNo       varchar(100) NOT NULL,
                                 MaskedGLAcctNo varchar(114) NULL)

/* Local Variables ******************************************** */
   DECLARE @lAcctRefUsage          smallint,
           @lValidateAcctRefRetVal int,
           @lErrorLogRetVal        int,
           @lSetErrorLogRetVal     int,
           @lInterfaceError        smallint,
           @lFatalError            smallint,
           @lLanguageID            int,
           @lRequiredCoID          int,
           @lInvalidCoID           int,
           @lInvalidGLOpts         int,
           @lAcctRefOption         int,
           @lAcctRefKeyReqd        int,
           @lAcctRefExist          int,
           @lAcctRefCo             int,
           @lAcctRefInactive       int,
           @lAcctRefStart          int,
           @lAcctRefEnd            int,
           @lErrMsgNo              int,
           @lErrorsOccurred        smallint

/* Initialize ************************************************* */
   SELECT @oSeverity = 0,    
          @oRetVal = 0,
          @lValidateAcctRefRetVal = 0,
          @lInterfaceError = 3,
          @lFatalError = 2,
          @lAcctRefUsage = 0,
          @lLanguageID = NULL,
          @lRequiredCoID = 19101,
          @lInvalidCoID = 19102,
          @lInvalidGLOpts = 19105,
          @lAcctRefOption = 19230,
          @lAcctRefKeyReqd = 19235,
          @lAcctRefExist = 19221,
          @lAcctRefCo = 19222,
          @lAcctRefInactive = 19227,
          @lAcctRefStart = 19224,
          @lAcctRefEnd = 19225

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

			EXECUTE spGetNextSurrogateKey 'tciErrorLog', @ioSessionID OUTPUT  

			IF @ioSessionID IS NULL
			BEGIN

				SELECT @oSeverity = 2, @oRetVal = 19

				/* Rows cannot be written to tglPosting when this error occurs. */
				RETURN

			END /* End @ioSessionID IS NULL */

		END /* @ioSessionID IS NULL */

/* Verify User ID ********************************************* */
		IF COALESCE(DATALENGTH(LTRIM(RTRIM(@iUserID))), 0) = 0
		BEGIN
  
			SELECT @oSeverity = 2, @oRetVal = 33

			/* Rows cannot be written to tglPosting when this error occurs. */
			RETURN

         END /* End COALESCE(DATALENGTH(LTRIM(RTRIM(@iUserID))), 0) = 0 */

		IF COALESCE(DATALENGTH(LTRIM(RTRIM(@iUserID))), 0) > 0
		BEGIN

			SELECT @lLanguageID = MIN(LanguageID)
			FROM tsmUser WITH (NOLOCK)
			WHERE UserID = @iUserID

			IF (@@ROWCOUNT = 0 OR COALESCE(@lLanguageID, 0) = 0)
			BEGIN

				SELECT @oSeverity = 2, @oRetVal = 34

				/* Rows cannot be written to tglPosting when this error occurs. */
				RETURN
			END/* End @@ROWCOUNT = 0 OR COALESCE(@lLanguageID, 0) = 0 */

		END /* End @iUserID > 0 */

/* Verify Company ID ****************************************** */
		IF COALESCE(DATALENGTH(LTRIM(RTRIM(@iCompanyID))), 0) = 0
		BEGIN

			SELECT @oSeverity = 2, @oRetVal = 20, @lErrMsgNo = @lRequiredCoID

			EXECUTE spciLogError @iBatchKey, NULL, @lErrMsgNo, 	'', '', '', '', '', 
			3, 
			@oSeverity, 
			@lErrorLogRetVal OUTPUT,
			@ioSessionID

			/* Rows cannot be written to tglPosting when this error occurs. */
			RETURN

         END /* End @iCompanyID IS NULL */

		/* Company ID must be valid ******** */
		SELECT CompanyName
		FROM tsmCompany WITH (NOLOCK)
		WHERE CompanyID = @iCompanyID 

		IF @@ROWCOUNT = 0
		BEGIN

			SELECT @oSeverity = 2, @oRetVal = 21, @lErrMsgNo = @lInvalidCoID

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

		END /* @@ROWCOUNT = 0 [Company ID doesn't exist in tsmCompany] */

		/* Get the GL Options information  **************************** */
		SELECT @lAcctRefUsage = AcctRefUsage
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

      END /* End @iVerifyParams = 1 */
	  ELSE
		/* @iVerifyParams <> 1 */
		/* No, we are NOT going to verify the input parameters. */
      BEGIN

			/* We simply transfer the passed in values to local variables. */
			/* Obviously this simply ASSUMES that the values are valid. */
			/* It is up to the application developer to make sure that is correct. */
		  SELECT @lLanguageID = @iLanguageID
		  SELECT @lAcctRefUsage = @iAcctRefUsage

      END /* End @iVerifyParams <> 1 */

	/* Are Account Reference Codes used by this Company? */
	IF @lAcctRefUsage = 0
	BEGIN

	/* No, Account Reference Codes are NOT used by this Company. */
		SELECT @oSeverity = 2, 
			 @oRetVal = 42, 
			 @lErrMsgNo = @lAcctRefOption

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

	END /* End @lAcctRefUsage = 0 [Account Reference Codes are not used.] */



/* Assume that no errors will occur. */
	SELECT @lErrorsOccurred = 0

/* Validate the required Account Reference ID's in #tglValidateAcct now */
/* This validation only applies when @lAcctRefUsage = 1 [Validated ARC's] */
	IF @lAcctRefUsage = 1
	BEGIN

		--SELECT 'Validating that required Account Reference Keys are NOT NULL'
		UPDATE #tglValidateAcct
		SET ValidationRetVal = 43,
			ErrorMsgNo = @lAcctRefKeyReqd
		WHERE GLAcctKey IN (SELECT GLAcctKey 
							FROM tglAccount a WITH (NOLOCK),
								tglNaturalAcct b WITH (NOLOCK)
							WHERE a.NaturalAcctKey = b.NaturalAcctKey
							AND b.ReqAcctRefCode = 1)
			AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) = 0
			AND ValidationRetVal = 0
		OPTION (KEEP PLAN)	-- Temp table changes won't change the query plan.

/* Did a validation error occur above? */
		IF @@ROWCOUNT > 0
		BEGIN

			SELECT @lErrorsOccurred = 1
			--SELECT 'Error occurred validating that required Account Reference Keys are NOT NULL'
			SELECT @lValidateAcctRefRetVal = 43, @oSeverity = @lFatalError

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
				AND a.ValidationRetVal = 43
				AND a.ErrorMsgNo = @lAcctRefKeyReqd
			OPTION (KEEP PLAN)	-- Temp table doesn't have enough rows to change performance due to query plan
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
				@lAcctRefKeyReqd                     /* ErrorMsgNo */
			FROM #tglValidateAcct a WITH (NOLOCK),
				tglAccount b WITH (NOLOCK),
				#tglAcctMask c WITH (NOLOCK)
			WHERE a.GLAcctKey = b.GLAcctKey
				AND b.GLAcctNo = c.GLAcctNo
				AND a.ValidationRetVal = 43
				AND a.ErrorMsgNo = @lAcctRefKeyReqd
			OPTION (KEEP PLAN)
		END /* End @@ROWCOUNT > 0 [Error Occurred] */

	END /* End @lAcctRefUsage = 1 [Validated Account Reference Codes] */

/* Do all the Reference Keys exist? */          
/* This validation applies when @lAcctRefUsage = 1 or 2 */
	IF (@lAcctRefUsage = 1 OR @lAcctRefUsage = 2) /* Validated Account Reference Codes */  /* Non-Validated Account Reference Codes */
	BEGIN
		--SELECT 'Validating that the Account Reference Keys exist in tglAcctRef'
		UPDATE #tglValidateAcct
		SET ValidationRetVal = 30,
			ErrorMsgNo = @lAcctRefExist
		WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
								FROM tglAcctRef WITH (NOLOCK))
			AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
			AND ValidationRetVal = 0
		OPTION (KEEP PLAN)
		/* Did a validation error occur above? */
		IF @@ROWCOUNT > 0
		BEGIN

			SELECT @lErrorsOccurred = 1
			--SELECT 'Error occurred validating that Account Reference Keys exist in tglAcctRef'
			SELECT @lValidateAcctRefRetVal = 30,
			@oSeverity = @lFatalError

			/* An error DID occur, so log it */
			INSERT #tciErrorStg (
			GLAcctKey,   BatchKey,    ErrorType,   Severity, 
			StringData1, StringData2, StringData3, StringData4, 
			StringData5, StringNo)
			SELECT GLAcctKey,
			@iBatchKey,                    /* BatchKey */
			@lInterfaceError,              /* ErrorType */
			@lFatalError,                  /* ValidationSeverity */
			CONVERT(VARCHAR(30), AcctRefKey), /* ErrorStrData1 */    
			'',                            /* ErrorStrData2 */
			'',                            /* ErrorStrData3 */
			'',                            /* ErrorStrData4 */
			'',                            /* ErrorStrData5 */
			@lAcctRefExist                 /* ErrorMsgNo */
			FROM #tglValidateAcct WITH (NOLOCK)
			WHERE ValidationRetVal = 30
			AND ErrorMsgNo = @lAcctRefExist
			OPTION (KEEP PLAN)
			/* Rows cannot be written to tglPosting when this error occurs. */
			GOTO FinishProc

		END /* End @@ROWCOUNT > 0 [Error Occurred] */

		/* Validating that all Account Reference Keys are for the correct Company */          
		/* This validation applies when @lAcctRefUsage = 1 or 2 */
				  --SELECT 'Validating that the Account Reference Keys are for the correct Company'
		UPDATE #tglValidateAcct
		SET ValidationRetVal = 27,
		ErrorMsgNo = @lAcctRefCo
		WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
							 FROM tglAcctRef WITH (NOLOCK)
							 WHERE CompanyID = @iCompanyID)
		AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
		AND ValidationRetVal = 0
		OPTION (KEEP PLAN)
		/* Did a validation error occur above? */
		IF @@ROWCOUNT > 0
		BEGIN

			SELECT @lErrorsOccurred = 1
			--SELECT 'Error occurred validating that Account Reference Keys exist in tglAcctRef'
			SELECT @lValidateAcctRefRetVal = 27,
			@oSeverity = @lFatalError

			/* An error DID occur, so log it */
			INSERT #tciErrorStg (
				GLAcctKey,   BatchKey,    ErrorType,   Severity, 
				StringData1, StringData2, StringData3, StringData4, 
				StringData5, StringNo)
			SELECT a.GLAcctKey,
				@iBatchKey,                       /* BatchKey */
				@lInterfaceError,                 /* ErrorType */
				@lFatalError,                     /* ValidationSeverity */
				CONVERT(VARCHAR(30), b.AcctRefCode), /* ErrorStrData1 */    
				@iCompanyID,                      /* ErrorStrData2 */
				'',                               /* ErrorStrData3 */
				'',                               /* ErrorStrData4 */
				'',                               /* ErrorStrData5 */
				@lAcctRefCo                       /* ErrorMsgNo */
			FROM #tglValidateAcct a WITH (NOLOCK),
				tglAcctRef b WITH (NOLOCK)
			WHERE a.AcctRefKey = b.AcctRefKey
				AND a.ValidationRetVal = 27
				AND a.ErrorMsgNo = @lAcctRefCo
			OPTION (KEEP PLAN)
		END /* End @@ROWCOUNT > 0 [Error Occurred] */

	END /* End @lAcctRefUsage = 1 OR 2 [Validated / Non-Validated ARC's] */

/* Validating that all Account Reference Keys have an active status */          
/* This validation applies when @lAcctRefUsage = 1 [Validated Account Reference Codes] */
	IF @lAcctRefUsage = 1
	BEGIN

		--SELECT 'Validating that the Account Reference Keys have an active status'
		UPDATE #tglValidateAcct
		SET ValidationRetVal = 37,
			ErrorMsgNo = @lAcctRefInactive
		WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
								FROM tglAcctRef WITH (NOLOCK)
							WHERE CompanyID = @iCompanyID
			AND Status = 1)
			AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
			AND ValidationRetVal = 0
		OPTION (KEEP PLAN)	-- Temp table doesn't have enough rows to change performance due to query plan
/* Did a validation error occur above? */
		IF @@ROWCOUNT > 0
		BEGIN

			SELECT @lErrorsOccurred = 1
			--SELECT 'Error occurred validating that Account Reference Keys have an active status'
			SELECT @lValidateAcctRefRetVal = 37,
			@oSeverity = @lFatalError

			/* An error DID occur, so log it */
			INSERT #tciErrorStg (
				GLAcctKey,   BatchKey,    ErrorType,   Severity, 
				StringData1, StringData2, StringData3, StringData4, 
				StringData5, StringNo)
			SELECT a.GLAcctKey,
				@iBatchKey,                       /* BatchKey */
				@lInterfaceError,                 /* ErrorType */
				@lFatalError,                     /* ValidationSeverity */
				CONVERT(VARCHAR(30), b.AcctRefCode), /* ErrorStrData1 */    
				'',                               /* ErrorStrData2 */
				'',                               /* ErrorStrData3 */
				'',                               /* ErrorStrData4 */
				'',                               /* ErrorStrData5 */
				@lAcctRefInactive                 /* ErrorMsgNo */
			FROM #tglValidateAcct a WITH (NOLOCK),
				tglAcctRef b WITH (NOLOCK)
			WHERE a.AcctRefKey = b.AcctRefKey
				AND a.ValidationRetVal = 37
				AND a.ErrorMsgNo = @lAcctRefInactive
			OPTION (KEEP PLAN)
		END /* End @@ROWCOUNT > 0 [Error Occurred] */

/* Reference Code Effective Date Restrictions */
		IF @iEffectiveDate IS NOT NULL
		BEGIN

			/* Check Effective Start Date */
			--SELECT 'Validating that there are no ARC effective start date violations'
			UPDATE #tglValidateAcct
			SET ValidationRetVal = 32,
			ErrorMsgNo = @lAcctRefStart
			WHERE AcctRefKey IN (SELECT AcctRefKey 
							FROM tglAcctRef WITH (NOLOCK)
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
				--SELECT 'Error occurred validating ARC effective start date violations'
				SELECT @lValidateAcctRefRetVal = 32,
					   @oSeverity = @lFatalError

				/* An error DID occur, so log it */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
				SELECT a.GLAcctKey,
					@iBatchKey,                       /* BatchKey */
					@lInterfaceError,                 /* ErrorType */
					@lFatalError,                     /* ValidationSeverity */
					@iEffectiveDate,                  /* ErrorStrData1 */
					CONVERT(VARCHAR(30), b.AcctRefCode), /* ErrorStrData2 */
					b.EffStartDate,                   /* ErrorStrData3 */
					'',                               /* ErrorStrData4 */
					'',                               /* ErrorStrData5 */
					@lAcctRefStart                    /* ErrorMsgNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
					tglAcctRef b WITH (NOLOCK)
				WHERE a.AcctRefKey = b.AcctRefKey
					AND a.ValidationRetVal = 32
					AND a.ErrorMsgNo = @lAcctRefStart
				OPTION (KEEP PLAN)
			END /* End @@ROWCOUNT > 0 [Error Occurred] */

/* Check Effective End Date */
         --SELECT 'Validating that there are no ARC effective end date violations'
			UPDATE #tglValidateAcct
			SET ValidationRetVal = 32,
			ErrorMsgNo = @lAcctRefEnd
			WHERE AcctRefKey IN (SELECT AcctRefKey 
								FROM tglAcctRef WITH (NOLOCK)
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
				--SELECT 'Error occurred validating ARC effective end date violations'
				SELECT @lValidateAcctRefRetVal = 32,
				@oSeverity = @lFatalError

				/* An error DID occur, so log it */
				INSERT #tciErrorStg (
					GLAcctKey,   BatchKey,    ErrorType,   Severity, 
					StringData1, StringData2, StringData3, StringData4, 
					StringData5, StringNo)
				SELECT a.GLAcctKey,
					@iBatchKey,                       /* BatchKey */
					@lInterfaceError,                 /* ErrorType */
					@lFatalError,                     /* ValidationSeverity */
					@iEffectiveDate,                  /* ErrorStrData1 */
					CONVERT(VARCHAR(30), b.AcctRefCode), /* ErrorStrData2 */
					b.EffEndDate,                     /* ErrorStrData3 */
					'',                               /* ErrorStrData4 */
					'',                               /* ErrorStrData5 */
					@lAcctRefEnd                      /* ErrorMsgNo */
				FROM #tglValidateAcct a WITH (NOLOCK),
				tglAcctRef b WITH (NOLOCK)
				WHERE a.AcctRefKey = b.AcctRefKey
					AND a.ValidationRetVal = 32
					AND a.ErrorMsgNo = @lAcctRefEnd
				OPTION (KEEP PLAN)
			END /* End @@ROWCOUNT > 0 [Error Occurred] */

		END /* End @iEffectiveDate IS NOT NULL [We DO care about ARC Effective Dates] */

	END /* End @lAcctRefUsage = 1 [Validated Account Reference Codes] */

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


	IF @lValidateAcctRefRetVal = 0
      SELECT @oRetVal = 1
   ELSE
      SELECT @oRetVal = @lValidateAcctRefRetVal

   --SELECT 'oRetVal = ', @oRetVal

END /* End of the Stored Procedure */