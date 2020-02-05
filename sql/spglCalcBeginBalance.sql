USE [MDCI_MAS500_APP]
GO
/****** Object:  StoredProcedure [dbo].[spglCalcBeginBalance]    Script Date: 7/25/2019 4:18:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[spglCalcBeginBalance] 
    @iCompanyID  VARCHAR(3),
    @iFiscYear   VARCHAR(5),
    @iCommitFlag smallint,
    @oRetVal     int OUTPUT
AS

-- Return Values:
--  -1 - Unsuccessful
--   0 - Successful
--   1 - Retained Earnings Account does not exist.

BEGIN

    /* Variable Declarations *********************** */
    DECLARE @lAcctCatID                    smallint
    DECLARE @lBegBal                       decimal(18,3)
    DECLARE @lBegBalHC                     decimal(18,3)
    DECLARE @lBegBalNC                     decimal(18,3)
    DECLARE @lClearNonFin                  smallint
    DECLARE @lCurrID                       VARCHAR(3)
    DECLARE @lDeclaredGLAccountCursor      smallint
    DECLARE @lDeclaredGLAcctHistCurrCursor smallint
    DECLARE @lPriorFiscPer                 smallint
    DECLARE @lGLAcctKey                    int
    DECLARE @lGLAcctNo                     varchar(100)
    DECLARE @lPriorFiscYear                VARCHAR(5)
    DECLARE @lRetEarnSubAcctKey            int
    DECLARE @lRetEarnSubAcctNo             varchar(100)
    DECLARE @lRetainedEarnAcct             varchar(100)
    DECLARE @lStatBegBal                   decimal(16,4)
    DECLARE @lStatQtyBal                   decimal(16,4)
    DECLARE @lTotalBal                     decimal(18,3)
    DECLARE @lTotalBalHC                   decimal(18,3)
    DECLARE @lTotalBalNC                   decimal(18,3)
    DECLARE @lUseMultCurr                  smallint

    /* Begin Transaction ********************** */
    IF (@iCommitFlag = 1)
    BEGIN
        BEGIN TRANSACTION
    END

    /* Initialization ************************* */
    SELECT @oRetVal = -1,
        @lDeclaredGLAcctHistCurrCursor = 0,
        @lDeclaredGLAccountCursor = 0

    /* Get GL Options Information ************* */
    SELECT @lRetainedEarnAcct = RetainedEarnAcct,
        @lClearNonFin = ClearNonFin,
        @lUseMultCurr = UseMultCurr
    FROM tglOptions WITH (NOLOCK)
    WHERE CompanyID = @iCompanyID

    /* Get Prior Year Information ************* */
    EXECUTE spglFSGivePriorYearPeriod @iCompanyID,
        @iFiscYear,
        @lPriorFiscYear OUTPUT,
        @lPriorFiscPer  OUTPUT

    IF (COALESCE(DATALENGTH(LTRIM(RTRIM(@lPriorFiscYear))),0) > 0
    AND @lPriorFiscPer > 0)
    BEGIN

        /* Set Beginning Balances to zero in tglAcctHist. */
        UPDATE tglAcctHist
        SET BegBal = 0,
            StatBegBal = 0,
            UpdateCounter = UpdateCounter + 1
        WHERE FiscYear = @iFiscYear
            AND FiscPer = 1
            AND GLAcctKey IN (SELECT GLAcctKey
                            FROM tglAccount WITH (NOLOCK)
                            WHERE CompanyID = @iCompanyID)

        /* Set Beginning Balances HC/NC to zero in tglAcctHistCurr */
        IF (@lUseMultCurr = 1)
        BEGIN

            UPDATE tglAcctHistCurr
            SET BegBalHC = 0,
                BegBalNC = 0
            WHERE FiscYear = @iFiscYear
                AND FiscPer = 1
                AND GLAcctKey IN (SELECT GLAcctKey
                                    FROM tglAccount WITH (NOLOCK)
                                    WHERE CompanyID = @iCompanyID)

         END /* End @lUseMultCurr = 1 */

        /* Declare the GLAccount Cursor */
        IF (@lDeclaredGLAccountCursor = 0)
        BEGIN

            DECLARE GLAccountCursor INSENSITIVE CURSOR FOR
            SELECT a.GLAcctNo,
            a.GLAcctKey,
            d.AcctCatID
            FROM tglAccount a WITH (NOLOCK)
            INNER JOIN tglNaturalAcct b WITH (NOLOCK)
            ON (a.NaturalAcctKey = b.NaturalAcctKey)
            INNER JOIN tglAcctType c WITH (NOLOCK)
            ON (b.AcctTypeKey = c.AcctTypeKey)
            INNER JOIN tglAcctCategory d WITH (NOLOCK)
            ON (c.AcctCategoryKey = d.AcctCategoryKey)
            WHERE a.CompanyID = @iCompanyID

            SELECT @lDeclaredGLAccountCursor = 1

        END /* End @lDeclaredGLAccountCursor = 0 */

        /* Open the GLAccount Cursor */
        OPEN GLAccountCursor

        FETCH NEXT FROM GLAccountCursor INTO @lGLAcctNo,
            @lGLAcctKey,
            @lAcctCatID

        /* GL Account Loop ********************************** */
        GLAccountLoop:
        WHILE (@@FETCH_STATUS <> -1)
        BEGIN

            IF (@@FETCH_STATUS <> -2)
            BEGIN

                SELECT @lTotalBal = 0
                SELECT @lStatQtyBal = 0

                /* Get Total for Prior Year. */
                SELECT @lTotalBal = COALESCE(SUM(BegBal),0) + COALESCE(SUM(DebitAmt),0) - COALESCE(SUM(CreditAmt),0),
                    @lStatQtyBal = COALESCE(SUM(StatBegBal),0) + COALESCE(SUM(StatQty),0)
                FROM tglAcctHist WITH (NOLOCK)
                WHERE GLAcctKey = @lGLAcctKey
                    AND FiscYear = @lPriorFiscYear

                IF (@lAcctCatID IN (1,2,3) OR (@lAcctCatID = 9 AND @lClearNonFin = 0))
                BEGIN

                    /* Get Beginning Balance for Entered Year */
                    SELECT @lBegBal = BegBal,
                        @lStatBegBal = StatBegBal
                    FROM tglAcctHist WITH (NOLOCK)
                    WHERE GLAcctKey = @lGLAcctKey
                        AND FiscYear = @iFiscYear
                        AND FiscPer = 1

                    IF (@@ROWCOUNT = 0)
                    BEGIN
                        INSERT INTO tglAcctHist (BegBal,
                            CreditAmt,
                            DebitAmt,
                            FiscPer,
                            FiscYear,
                            GLAcctKey,
                            StatBegBal,
                            StatQty,
                            UpdateCounter)
                        VALUES (@lTotalBal,
                            0,
                            0,
                            1,
                            @iFiscYear,
                            @lGLAcctKey,
                            @lStatQtyBal,
                            0,
                            1)

                    END /* End @@ROWCOUNT = 0 [tglAcctHist row does NOT exist] */
                    ELSE                        
                    BEGIN /* @@ROWCOUNT > 0 [tglAcctHist row DOES exist] */

                        UPDATE tglAcctHist
                        SET BegBal = BegBal + @lTotalBal,
                        StatBegBal = StatBegBal + @lStatQtyBal,
                        UpdateCounter = UpdateCounter + 1
                        WHERE GLAcctKey = @lGLAcctKey
                        AND FiscYear = @iFiscYear
                        AND FiscPer = 1

                    END /* End @@ROWCOUNT > 0 [tglAcctHist row DOES exist] */

                END /* End CatID IN 1,2,3 ... */
                ELSE            
                BEGIN /* CatID NOT IN 1,2,3, ... */

                    IF (@lAcctCatID IN (4,5,6,7,8))
                    BEGIN

                        /* Does Retained Earnings Account exist? */
                        EXECUTE spglSubstAcct @lGLAcctNo,
                        @lRetainedEarnAcct,
                        @lRetEarnSubAcctNo OUTPUT

                        SELECT @lRetEarnSubAcctKey = GLAcctKey
                        FROM tglAccount WITH (NOLOCK)
                        WHERE CompanyID = @iCompanyID
                        AND GLAcctNo = @lRetEarnSubAcctNo

                        IF (@@ROWCOUNT = 0)
                        BEGIN

                            /* Retained Earnings Account does not exist. */
                            SELECT @oRetVal = 1
                            BREAK /* Break the AcctHist Loop */

                        END /* End @@ROWCOUNT = 0 [tglAccount row does NOT exist.] */

                        SELECT @lBegBal = BegBal,
                            @lStatBegBal = StatBegBal
                        FROM tglAcctHist WITH (NOLOCK)
                        WHERE GLAcctKey = @lRetEarnSubAcctKey
                            AND FiscYear = @iFiscYear
                            AND FiscPer = 1

                        IF (@@ROWCOUNT = 0)
                        BEGIN

                            INSERT INTO tglAcctHist (BegBal,
                            CreditAmt,
                            DebitAmt,
                            FiscPer,
                            FiscYear,
                            GLAcctKey,
                            StatBegBal,
                            StatQty,
                            UpdateCounter)
                            VALUES (@lTotalBal,
                            0,
                            0,
                            1,
                            @iFiscYear,
                            @lRetEarnSubAcctKey,
                            @lStatQtyBal,
                            0,
                            1)

                        END /* End @@ROWCOUNT = 0 [tglAcctHist row does NOT exist] */
                        ELSE                    
                        BEGIN /* @@ROWCOUNT > 0 [tglAcctHist row DOES exist] */

                            UPDATE tglAcctHist
                            SET BegBal = BegBal + @lTotalBal,
                            StatBegBal = StatBegBal + @lStatQtyBal,
                            UpdateCounter = UpdateCounter + 1
                            WHERE GLAcctKey = @lRetEarnSubAcctKey
                            AND FiscYear = @iFiscYear
                            AND FiscPer = 1

                        END /* End @@ROWCOUNT > 0 [tglAcctHist row DOES exist] */

                    END /* End CatID IN 4,5,6,7,8 */

                END /* End CatID NOT IN 1,2,3 ... */

                /* Get the next record */
                FETCH NEXT FROM GLAccountCursor INTO @lGLAcctNo,
                @lGLAcctKey,
                @lAcctCatID

            END /* End GLAccountLoop @@FETCH_STATUS <> -2 */

        END  /* End GLAccountLoop @@FETCH_STATUS <> -1 */

        /* Close and Deallocate the GLAccount Cursor */
        CLOSE GLAccountCursor

        IF (@lDeclaredGLAccountCursor = 1)
        BEGIN
            DEALLOCATE GLAccountCursor
            SELECT @lDeclaredGLAccountCursor = 0
        END /* End @lDeclaredGLAccountCursor = 1 */

        /* Multicurrency is Active */
        IF (@lUseMultCurr = 1)
        BEGIN

            IF @lDeclaredGLAcctHistCurrCursor = 0
            BEGIN

                /* Declare the GLAcctHistCurr Cursor */
                DECLARE GLAcctHistCurrCursor INSENSITIVE CURSOR FOR
                SELECT DISTINCT a.GLAcctKey,
                a.CurrID,
                b.GLAcctNo,
                e.AcctCatID
                FROM tglAcctHistCurr a WITH (NOLOCK)
                INNER JOIN tglAccount b WITH (NOLOCK)
                    ON (a.GLAcctKey = b.GLAcctKey)
                INNER JOIN tglNaturalAcct c WITH (NOLOCK)
                    ON (b.NaturalAcctKey = c.NaturalAcctKey)
                INNER JOIN tglAcctType d WITH (NOLOCK)
                    ON (c.AcctTypeKey = d.AcctTypeKey)
                INNER JOIN tglAcctCategory e WITH (NOLOCK)
                    ON (d.AcctCategoryKey = e.AcctCategoryKey)
                WHERE a.FiscYear = @lPriorFiscYear
                    AND b.CompanyID = @iCompanyID

                SELECT @lDeclaredGLAcctHistCurrCursor = 1

            END /* End @lDeclaredGLAcctHistCurrCursor = 0 */

            /* Open the GLAcctHistCurr Cursor */
            OPEN GLAcctHistCurrCursor

            FETCH NEXT FROM GLAcctHistCurrCursor INTO @lGLAcctKey,
                @lCurrID,
                @lGLAcctNo,
                @lAcctCatID

            /* GLAcctHistCurrLoop */
            GLAcctHistCurrLoop:
            WHILE (@@FETCH_STATUS <> -1)
            BEGIN

                IF (@@FETCH_STATUS <> -2)
                BEGIN

                    /* Get Total Balance HC for prior fiscal year */
                    SELECT @lTotalBalHC = 0
                    SELECT @lTotalBalHC = COALESCE(SUM(BegBalHC),0) + COALESCE(SUM(DebitAmtHC),0) - COALESCE(SUM(CreditAmtHC),0)
                    FROM tglAcctHistCurr WITH (NOLOCK)
                    WHERE GLAcctKey = @lGLAcctKey
                    AND FiscYear = @lPriorFiscYear
                    AND CurrID = @lCurrID

                    /* Get Total Balance NC for prior fiscal year */
                    SELECT @lTotalBalNC = 0
                    SELECT @lTotalBalNC = COALESCE(SUM(BegBalNC),0) + COALESCE(SUM(DebitAmtNC),0) - COALESCE(SUM(CreditAmtNC),0)
                    FROM tglAcctHistCurr WITH (NOLOCK)
                    WHERE GLAcctKey = @lGLAcctKey
                    AND FiscYear = @lPriorFiscYear
                    AND CurrID = @lCurrID

                    IF (@lAcctCatID IN (1,2,3)
                        OR (@lAcctCatID = 9 AND @lClearNonFin = 0))
                    BEGIN

                        /* Get Total Balance for entered year */
                        SELECT @lBegBalHC = BegBalHC,
                        @lBegBalNC = BegBalNC
                        FROM tglAcctHistCurr WITH (NOLOCK)
                        WHERE GLAcctKey = @lGLAcctKey
                        AND FiscYear = @iFiscYear
                        AND FiscPer = 1
                        AND CurrID = @lCurrID

                        IF (@@ROWCOUNT = 0)
                        BEGIN

                            INSERT INTO tglAcctHistCurr (BegBalHC,
                            BegBalNC,
                            CreditAmtHC,
                            CreditAmtNC,
                            CurrID,
                            DebitAmtHC,
                            DebitAmtNC,
                            FiscPer,
                            FiscYear,
                            GLAcctKey)
                            VALUES (@lTotalBalHC,
                            @lTotalBalNC,
                            0,
                            0,
                            @lCurrID,
                            0,
                            0,
                            1,
                            @iFiscYear,
                            @lGLAcctKey)

                        END /* End @@ROWCOUNT = 0 [tglAcctHistCurr row does NOT exist] */
                        ELSE                    
                        BEGIN /* @@ROWCOUNT > 0 [tglAcctHistCurr row DOES exist] */

                            UPDATE tglAcctHistCurr
                            SET BegBalHC = BegBalHC + @lTotalBalHC,
                            BegBalNC = BegBalNC + @lTotalBalNC
                            WHERE GLAcctKey = @lGLAcctKey
                            AND FiscYear = @iFiscYear
                            AND FiscPer = 1
                            AND CurrID = @lCurrID

                        END /* End @@ROWCOUNT > 0 [tglAcctHistCurr row DOES exist] */

                    END /* End CatID IN 1,2,3 ... */

                    /* Get the next record */
                    FETCH NEXT FROM GLAcctHistCurrCursor INTO @lGLAcctKey,
                    @lCurrID,
                    @lGLAcctNo,
                    @lAcctCatID

                END /* End GLAcctHistCurr FETCH_STATUS <> -2 */

            END /* End GLAcctHistCurr FETCH_STATUS <> -1 */

            /* Close and Deallocate the GLAcctHistCurr Cursor */
            CLOSE GLAcctHistCurrCursor

            IF (@lDeclaredGLAcctHistCurrCursor = 1)
            BEGIN
                DEALLOCATE GLAcctHistCurrCursor
                SELECT @lDeclaredGLAcctHistCurrCursor = 0
            END /* End @lDeclaredGLAcctHistCurrCursor = 1 */

        END /* End Multicurrency Active */

    END /* COALESCE(DATALENGTH(LTRIM(RTRIM(@lPriorFiscYear))),0) > 0 AND @lPriorFiscPer > 0 */

    IF (@oRetVal = -1)
    BEGIN
        SELECT @oRetVal = 0
    END /* End @oRetVal = -1 */

    IF (@iCommitFlag = 1)
    BEGIN

        IF (@oRetVal = 1)
        BEGIN
            ROLLBACK TRANSACTION
        END
        ELSE
        BEGIN
            COMMIT TRANSACTION
        END

    END /* End @iCommitFlag = 1 */

END /* End of the Stored Procedure */
