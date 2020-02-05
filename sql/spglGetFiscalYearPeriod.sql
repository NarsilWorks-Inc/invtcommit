USE [MDCI_MAS500_APP]
GO
/****** Object:  StoredProcedure [dbo].[spglGetFiscalYearPeriod]    Script Date: 7/24/2019 2:02:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[spglGetFiscalYearPeriod] (@iCompanyID  VARCHAR(3),
                                          @iDate       DATETIME,
                                          @iCreateFlag INT,
                                          @oFiscalYear VARCHAR(5) OUTPUT, 
                                          @oFiscalPer  SMALLINT   OUTPUT,
                                          @oStartDate  DATETIME   OUTPUT,  
                                          @oEndDate    DATETIME   OUTPUT, 
                                          @oStatus     INT        OUTPUT,
                                          @oRetVal     INT        OUTPUT)
AS

--  @iCreateFlag Values:
--   1 - Create Periods and Auto Commit  by Procedure
--   2 - Do not Create Periods
--   3 - Create Periods and Manual Commit
--
--  oRetVal Values:
--   1 - Existed
--   2 - Created/Written (New periods created)
--   3 - Calculated (New records not created) 
--   5 - Retained Earnings Account does not exist
--   6 - Could not Create/Calculate Prior	
--   7 - Could not Create/Calculate Future
--
--  oStatus Values:
--   0 - (Null) Invalid Value
--   1 - Open
--   2 - Closed
--
-- Special Notes ********************************************************
-- @oFiscalYear is an Output param. However, if we are creating a new
-- fiscal year, the SetUp Fiscal Year program is passing in the "Entered"
-- fiscal year. This means, that instead of calculating this "label", the
-- entered fiscal year will be used. This is necessary since tglAcctHist
-- and tglAcctHistCurr records were not being created. Also, Incorrect years
-- were being created and later removed/re-inserted due to this problem.
-- ************************************************************************

BEGIN

/* Variable Declarations *************************** */
   DECLARE @lCheckNextYearDate      DATETIME
   DECLARE @lCurrentFiscalYear      VARCHAR(5)
   DECLARE @lDate                   DATETIME
   DECLARE @lDeclaredFiscPerCursor  SMALLINT
   DECLARE @lEndDate                DATETIME
   DECLARE @lEndYearDate            DATETIME
   DECLARE @lFirstPerRecord         INT
   DECLARE @lFirstFiscalYear        VARCHAR(5)
   DECLARE @lFirstStartDate         DATETIME
   DECLARE @lFirstTime              INT
   DECLARE @lFiscPer                SMALLINT
   DECLARE @lInputYear              VARCHAR(5)
   DECLARE @lLastPerEndDate         DATETIME
   DECLARE @lMethod                 SMALLINT
           -- 1 = Normal Increment (Across Months)
           -- 2 = Using the Days Difference of each period
   DECLARE @lNextEndDate            DATETIME
   DECLARE @lNextStartDate          DATETIME
   DECLARE @lNoOfDays               INT
   DECLARE @lNoOfDaysinFiscYear     INT
   DECLARE @lOnceCreated            INT
   DECLARE @lPeriods                SMALLINT
   DECLARE @lPriorYearCreation      SMALLINT -- 0 = No, 1 = Yes
   DECLARE @lRetVal                 INT
   DECLARE @lStartDate              DATETIME
   DECLARE @lStartYearDate          DATETIME
   DECLARE @lTime                   DATETIME
   DECLARE @lYear                   VARCHAR(5)
   DECLARE @lEnteredFiscalYear      VARCHAR(5)
   DECLARE @lRetainedEarnAcct       VARCHAR(100)
   DECLARE @lSessionID              INT
   DECLARE @lUserID		            VARCHAR(30)

   /* Initialization *************************** */
   SELECT @lDeclaredFiscPerCursor = 0,
          @lPriorYearCreation = 0,
          @oRetVal = 0,
          @lRetVal = 0,
          @lSessionID = 0

   EXECUTE spGetLoginName @lUserID OUTPUT

/* Save the entered fiscal year, if applicable. */
   IF (COALESCE(DATALENGTH(LTRIM(RTRIM(@oFiscalYear))),0) <> 0)
   BEGIN
      SELECT @lEnteredFiscalYear = @oFiscalYear
   END
   ELSE
   BEGIN
      SELECT @lEnteredFiscalYear = ''
   END

   /* Begin Transaction */
   IF (@iCreateFlag = 1)
      BEGIN TRANSACTION

/* Main Loop ********************************* */
MainLoop:
   WHILE (1 = 1)
   BEGIN

      /* Initialization */
      SELECT @lPriorYearCreation = 0,
         @lFirstTime = 0,
         @oFiscalYear = '',
         @oFiscalPer = 0,
         @oStartDate = '',
         @oEndDate = ''

      /* Get Fiscal Info based upon entered date. */
      SELECT @oFiscalYear = FiscYear,
         @oFiscalPer = FiscPer,
         @oStartDate = StartDate, 
         @oEndDate = EndDate,
         @oStatus = Status
      FROM tglFiscalPeriod WITH (NOLOCK)
      WHERE CompanyID = @iCompanyID 
         AND @iDate BETWEEN StartDate AND EndDate

      /* Evaluate Query. */
      IF (@@ROWCOUNT = 0)
      BEGIN

         /* Fiscal Record not found */
         /* Get the latest Fiscal Year. */
         SELECT @lCurrentFiscalYear = MAX(FiscYear)
         FROM tglFiscalYear WITH (NOLOCK)
         WHERE CompanyID = @iCompanyID

         /* Get the First Fiscal Year. */
         SELECT @lFirstFiscalYear = MIN(FiscYear)
         FROM tglFiscalPeriod WITH (NOLOCK)
         WHERE CompanyID = @iCompanyID

         /* Get Start Date of First Fiscal Year. */
         SELECT @lFirstStartDate = StartDate
         FROM tglFiscalPeriod WITH (NOLOCK)
         WHERE CompanyID = @iCompanyID
            AND FiscYear = @lFirstFiscalYear
            AND FiscPer = (SELECT MIN(FiscPer)
                           FROM tglFiscalPeriod WITH (NOLOCK)
                           WHERE CompanyID = @iCompanyID 
                           AND FiscYear = @lFirstFiscalYear)

         IF (@iDate <= @lFirstStartDate)
         BEGIN
            /* Entered Date <= Start Date of First Fiscal Year */
            SELECT @lCurrentFiscalYear = @lFirstFiscalYear
            SELECT @lPriorYearCreation = 1
         END /* End @iDate <= @lFirstStartDate */
            
         /* Get No. of Periods for Oldest Fiscal Year. */
         SELECT @lPeriods = NoOfPeriods
         FROM tglFiscalYear WITH (NOLOCK)
         WHERE CompanyID = @iCompanyID
            AND FiscYear = @lCurrentFiscalYear

         /* Get Start and End Dates of Oldest Fiscal Year Period 1. */
         SELECT @lStartYearDate = StartDate,
               @lDate = EndDate 
         FROM tglFiscalPeriod WITH (NOLOCK)
         WHERE CompanyID = @iCompanyID
            AND FiscYear = @lCurrentFiscalYear
            AND FiscPer = (SELECT MIN(FiscPer)
                           FROM tglFiscalPeriod WITH (NOLOCK)
                           WHERE CompanyID = @iCompanyID 
                           AND FiscYear = @lCurrentFiscalYear)

         /* Get End Date of Oldest Fiscal Year. */
         SELECT @lEndYearDate = EndDate
         FROM tglFiscalPeriod WITH (NOLOCK)
         WHERE CompanyID = @iCompanyID
            AND FiscYear = @lCurrentFiscalYear
            AND FiscPer = (SELECT MAX(FiscPer)
                           FROM tglFiscalPeriod WITH (NOLOCK)
                           WHERE CompanyID = @iCompanyID 
                           AND FiscYear = @lCurrentFiscalYear)

         IF (@lPriorYearCreation = 0)
         BEGIN

            SELECT @lCheckNextYearDate = DATEADD(YEAR,1,@lDate)

            IF (@lCheckNextYearDate > @lEndYearDate)
            BEGIN
               SELECT @lMethod = 1 -- 1 - Normal Increment (Across Months)
            END
            ELSE  
            BEGIN
               SELECT @lMethod = 2 -- 2 - Using the Days Diff of each period
            END

            IF (COALESCE(DATALENGTH(LTRIM(RTRIM(@lEnteredFiscalYear))),0) <> 0)
            BEGIN
               SELECT @lYear = @lEnteredFiscalYear
            END
            ELSE
            BEGIN
               SELECT @lYear = CONVERT(VARCHAR(5),CONVERT(INT,SUBSTRING(@lCurrentFiscalYear,1,4)) + 1)
            END

         END /* End @lPriorYearCreation = 0 */
         ELSE         
         BEGIN /* @lPriorYearCreation <> 0 */

            SELECT @lMethod = 2 

            IF (COALESCE(DATALENGTH(LTRIM(RTRIM(@lEnteredFiscalYear))),0) <> 0)
            BEGIN
               SELECT @lYear = @lEnteredFiscalYear
            END
            ELSE
            BEGIN
               SELECT @lYear = CONVERT(VARCHAR(5),CONVERT(INT,SUBSTRING(@lCurrentFiscalYear,1,4)) - 1)
            END

            SELECT @lNoOfDaysinFiscYear = DATEDIFF(day, @lStartYearDate, @lEndYearDate) 

         END /* End @lPriorYearCreation <> 0 */

         SELECT @lFirstTime = 1
            
         IF (COALESCE(DATALENGTH(LTRIM(RTRIM(@lEnteredFiscalYear))),0) = 0)
         BEGIN
            SELECT @lYear = SUBSTRING(@lYear,1,4) + ' '
         END

         SELECT @lOnceCreated = 0,
            @lFirstPerRecord = 0,
            @lLastPerEndDate = @lEndYearDate,
            @lTime = getdate()

         IF @lDeclaredFiscPerCursor = 0
         BEGIN

            /* Declare Fiscal Period Cursor. */
            DECLARE FiscPerCursor INSENSITIVE CURSOR FOR
            SELECT FiscPer, StartDate, EndDate, 
                  DATEDIFF(day,StartDate,EndDate) NoOfDays
            FROM tglFiscalPeriod WITH (NOLOCK)
            WHERE CompanyID = @iCompanyID
               AND FiscYear = @lCurrentFiscalYear
            ORDER BY CompanyID, FiscYear, FiscPer

            SELECT @lDeclaredFiscPerCursor = 1

         END /* End @lDeclaredFiscPerCursor = 0 */

         OPEN FiscPerCursor

         FETCH NEXT FROM FiscPerCursor INTO @lFiscPer, @lStartDate, @lEndDate, @lNoOfDays

--FetchLoop:
         WHILE (@@FETCH_STATUS <> -1)
         BEGIN
            IF (@@FETCH_STATUS <> -2)
            BEGIN
               IF (@lFirstTime = 1)
               BEGIN
                  IF (@lMethod = 2)
                  BEGIN
                     SELECT @lNoOfDays = DATEDIFF(day,@lStartDate,@lEndDate)
                     IF (@lPriorYearCreation = 0)
                     BEGIN
                        SELECT @lStartDate = @lEndYearDate
                     END
                     ELSE
                     BEGIN
                        SELECT @lStartDate = DATEADD(DAY, -(@lNoOfDaysinFiscYear+2), @lStartDate)
                     END
                  END /* End @lMethod = 2 */
                  ELSE                  
                  BEGIN /* @lMethod <> 2 */
                     SELECT @lStartDate = @lEndYearDate
                  END /* End @lMethod <> 2 */

                  SELECT @lFirstTime = 0
               END /* End @lFirstTime = 1 */
               ELSE                  
               BEGIN /* @lFirstTime <> 1 */
                  SELECT @lStartDate = @lNextEndDate
               END /* End @lFirstTime <> 1 */

               EXECUTE spglGetNextYearPeriod 
                     @lStartDate, @lFiscPer,
		   				@lNoOfDays, @lEndDate, 
							@lEndYearDate, @lStartYearDate, @lMethod, 
							@lNextStartDate OUTPUT,
							@lNextEndDate OUTPUT

               SELECT @lLastPerEndDate = @lNextEndDate

               IF (@lPriorYearCreation = 1 AND @lFiscPer = @lPeriods)
               BEGIN
                  SELECT @lNextEndDate = DATEADD(DAY, -1, @lStartYearDate)
               END

               IF (@iCreateFlag IN (1,3))
               BEGIN
                  /* Create the New Records. */
                  IF (@lOnceCreated = 0)
                  BEGIN

                     INSERT INTO tglFiscalYear (CompanyID,
                        FiscYear,
                        NoOfPeriods)
                     VALUES (@iCompanyID,
                        @lYear,
                     @lPeriods)
   
                     SELECT @lOnceCreated = 1
               
                  END /* End @lOnceCreated = 0 */
    
                  IF (@lFirstPerRecord = 0)
                  BEGIN
                     SELECT @lFirstPerRecord = 1

                     EXECUTE spglInsertFiscalYearPer @iCompanyID, @lYear,
                                       @lFiscPer, @lNextStartDate,
                                       @lNextEndDate, 1 

                  END /* End @lFirstPerRecord = 0 */
                  ELSE               
                  BEGIN /* @lFirstPerPeriod <> 0 */

                     EXECUTE spglInsertFiscalYearPer @iCompanyID, @lYear,
                                          @lFiscPer, @lNextStartDate,
                                          @lNextEndDate, 1 

                  END /* End @lFirstPerPeriod <> 0 */
               END /* End Create the New Records */
               ELSE                   
               BEGIN

                  /* Create the New Temporary Records. */
                  IF (@lOnceCreated = 0)
                  BEGIN

                     INSERT INTO tciFiscYearWrk (CompanyID,
                                             FiscYear,
                                          NoOfPeriods, 
                                          DBUserID,
                                          TimeCreated)
                        VALUES (@iCompanyID,
                           @lYear,
                           @lPeriods,
                           @lUserID, 
                                 @lTime)

                     SELECT @lOnceCreated = 1

                  END /* End @lOnceCreated = 0 */
                       
                  IF (@lFirstPerRecord = 0)
                  BEGIN

                     SELECT @lFirstPerRecord = 1

                     EXECUTE spgltmpInsertFiscalYearPer @iCompanyID, @lYear,
							                            @lFiscPer, @lNextStartDate, 
									                    @lNextEndDate, 1, @lTime 

                  END /* End @lFirstPerRecord = 0 */
                  ELSE
                  BEGIN /* @lFirstPerRecord <> 0 */
                     EXECUTE spgltmpInsertFiscalYearPer @iCompanyID, @lYear,
                                          @lFiscPer, @lNextStartDate, 
                                          @lNextEndDate, 1, @lTime 
                  END /* End @lFirstPerRecord <> 0 */

               END /* End Create the New Temporary Records */

               FETCH NEXT FROM FiscPerCursor INTO @lFiscPer, @lStartDate, @lEndDate, @lNoOfDays

            END /* End Fetch Loop @@FETCH_STATUS <> -2 */

         END /* End Fetch Loop @@FETCH_STATUS <> -1 */

         CLOSE FiscPerCursor

      END /* End Fiscal Record Not Found */
      ELSE 
      BEGIN

         /* Fiscal Record Found */
         SELECT @oRetVal = 1

         /* Determine if Retained Earning Acct(s) exist only if creating fiscal year info. */
         IF (@iCreateFlag IN (1,3))
         BEGIN
 
            /* Get Retained Earning Acct from tglOptions. */
            SELECT @lRetainedEarnAcct = RetainedEarnAcct
            FROM tglOptions WITH (NOLOCK)
            WHERE CompanyID = @iCompanyID

            /* Will any Retained Earning Acct(s) need to be created? */
            EXECUTE spglRetEarnAcctCreate @iCompanyID,
                                             0,
                                             @lSessionID OUTPUT,
                                             0,
                                             @lRetainedEarnAcct,
                                             @lRetVal OUTPUT

            /* Evaluate Return: 0 = Not Masked: Needs Creation,
            1 = Masked: Needs Creation */
            IF (@lRetVal IN (0,1))
            BEGIN

               SELECT @oRetVal = 5

               /* Delete records in work table. */
               DELETE FROM tglRetEarnAcctWrk
               WHERE SessionID = @lSessionID
               AND CompanyID = @iCompanyID

            END /* End @lRetVal IN (0,1) */

         END /* End @iCreateFlag IN (1,3) */

         /* Break Main Loop */
         BREAK

      END /* End Fiscal Record Found */        

      /* Only attempt to get the fiscal information if we had to calculate/create fiscal information. */
      IF (@iCreateFlag IN (1,3))
      BEGIN

         /* New Records should have been written. */
         SELECT @oRetVal = 2,
                  @oFiscalYear = '',
                  @oFiscalPer = 0,
                  @oStartDate = '',
                  @oEndDate = '',
                  @oStatus = 0

         SELECT @oFiscalYear = FiscYear,
            @oFiscalPer = FiscPer, 
            @oStartDate = StartDate,
            @oEndDate = EndDate,
            @oStatus = Status
         FROM tglFiscalPeriod WITH (NOLOCK)
         WHERE CompanyID = @iCompanyID 
            AND @iDate BETWEEN StartDate AND EndDate

         IF (@@ROWCOUNT = 0)
         BEGIN

            IF (@lPriorYearCreation = 1)
            BEGIN
               SELECT @oRetVal = 6
            END
            ELSE
            BEGIN
               SELECT @oRetVal = 7
            END
         END /* End @@ROWCOUNT = 0 */

      END /* End @iCreateFlag IN (1,3) */
      ELSE      
      BEGIN /* @iCreateFlag NOT IN (1,3) */

         /* New Records not created, calculated only. */  
         SELECT @oRetVal = 3,
                @oFiscalYear = '',
                @oFiscalPer = 0,
                @oStartDate = '',
                @oEndDate = '',
                @oStatus = 0
               
         SELECT @oFiscalYear = FiscYear,
                @oFiscalPer = FiscPer, 
                @oStartDate = StartDate,
                @oEndDate = EndDate,
                @oStatus = Status
         FROM tciFiscPeriodWrk WITH (NOLOCK)
         WHERE CompanyID = @iCompanyID 
            AND @iDate BETWEEN StartDate AND EndDate

         IF (@@ROWCOUNT = 0)
         BEGIN

            IF (@lPriorYearCreation = 1)
            BEGIN
               SELECT @oRetVal = 6
            END
            ELSE
            BEGIN
               SELECT @oRetVal = 7
            END

         END /* End @@ROWCOUNT = 0 */

      END /* End @iCreateFlag NOT IN (1,3) */

      /* Calculate/Re-Calculate Beginning Balances. */
      IF (@iCreateFlag IN (1,3) AND @oRetVal = 2)
      BEGIN
         EXECUTE spglCalcBeginBalance @iCompanyID, @oFiscalYear, 0, @lRetVal OUTPUT

         IF (@lRetVal = 1)
         BEGIN
            /* Retained Earning Acct(s) not found. */
            SELECT @oRetVal = 5
         END /* End @lRetVal = 1 */

      END /* End @iCreateFlag IN (1,3) AND @oRetVal = 2 */

      /* Clean up tciFiscYearWrk Table. */
      DELETE FROM tciFiscYearWrk  
         WHERE DBUserID = @lUserID
         AND TimeCreated = @lTime

      /* Clean up tciFiscPeriodWrk. */
      DELETE FROM tciFiscPeriodWrk
         WHERE DBUserID = @lUserID
         AND TimeCreated = @lTime

      /* Break from the Main Loop. */
      BREAK
   
   END /* End of the Main Loop */

   IF (@lDeclaredFiscPerCursor = 1)
   BEGIN
      DEALLOCATE FiscPerCursor
   END

   IF (@iCreateFlag = 1) -- Create Periods and AutoCommit by Procedure.
   BEGIN

      IF @oRetVal IN (1,2,3) 
      BEGIN
         COMMIT TRANSACTION 
      END
      ELSE
      BEGIN
         ROLLBACK TRANSACTION 
      END

   END /* End @iCreateFlag = 1 */

END /* End of the Stored Procedure */
