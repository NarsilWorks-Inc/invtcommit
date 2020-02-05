USE [MDCI_MAS500_APP]

GO

/****** Object:  StoredProcedure [dbo].[spglRetEarnAcctCreate]    Script Date: 7/29/2019 2:11:33 PM ******/
SET ANSI_NULLS ON

GO

SET QUOTED_IDENTIFIER ON

GO

ALTER PROCEDURE [dbo].[spglRetEarnAcctCreate] @iCompanyID        VARCHAR(3),
                                              @iSessionID        INT,
                                              @oSessionID        INT output,
                                              @iCommitFlag       SMALLINT,
                                              @iRetainedEarnAcct VARCHAR(100),
                                              @oRetVal           INT output
AS
  -- Return Values
  --      0 - Successful - No Asterisks in the Retained Earn Acct Mask
  --      1 - Successful - Asterisks in the Retained Earn Acct Mask
  --      2 - No Records Created in the process
  --     -1 - UnSuccessful
  BEGIN
      DECLARE @lDummy VARCHAR(3)
      DECLARE @lDeclaredGLCursor SMALLINT
      DECLARE @lGLAcctNo VARCHAR(100)
      DECLARE @lGLAcctKey INT
      DECLARE @lRetainedEarnAcct VARCHAR(100)
      DECLARE @lStarPos SMALLINT

      IF @iCommitFlag = 1
        BEGIN TRANSACTION

      SELECT @oRetVal = -1

      SELECT @lDeclaredGLCursor = 0

      MAINLOOP:

      WHILE ( 1 = 1 )
        BEGIN
            SELECT @lRetainedEarnAcct = @iRetainedEarnAcct

            IF @iSessionID = 0
              EXECUTE spGetNextSurrogateKey
                'tglRetEarnAcctWrk',
                @oSessionID output
            ELSE
              SELECT @oSessionID = @iSessionID

            SELECT @oRetVal = 2

            SELECT @lStarPos = Charindex('*', @lRetainedEarnAcct)

            IF @lStarPos = 0
            BEGIN
               SELECT @lGLAcctKey = GLAcctKey
               FROM   tglAccount
               WHERE  CompanyID = @iCompanyID
                  AND GLAcctNo = @lRetainedEarnAcct

               IF @@rowcount = 0
               BEGIN
                  SELECT @lDummy = CompanyID
                  FROM   tglRetEarnAcctWrk
                  WHERE  SessionID = @oSessionID
                        AND CompanyID = @iCompanyID
                        AND RetEarnGLAcctNo = @lRetainedEarnAcct

                  IF @@rowcount = 0
                  BEGIN
                     SELECT @oRetVal = 0

                     INSERT INTO tglRetEarnAcctWrk
                        (SessionID,
                        CompanyID,
                        RetEarnGLAcctNo)
                     VALUES      (@oSessionID,
                        @iCompanyID,
                        @lRetainedEarnAcct)
                  END
               END

               BREAK --  MainLoop
            END

            IF @lDeclaredGLCursor = 0
              BEGIN
                  DECLARE GLCursor insensitive CURSOR FOR
                    SELECT a.GLAcctNo
                    FROM   tglAccount a,
                           tglNaturalAcct b,
                           tglAcctType c,
                           tglAcctCategory d
                    WHERE  a.CompanyID = @iCompanyID
                           AND a.NaturalAcctKey = b.NaturalAcctKey
                           AND b.AcctTypeKey = c.AcctTypeKey
                           AND c.AcctCategoryKey = d.AcctCategoryKey
                           AND d.AcctCatID IN ( 4, 5, 6, 7,
                                                8, 9 )

                  SELECT @lDeclaredGLCursor = 1
              END

            OPEN GLCursor

            FETCH next FROM GLCursor INTO @lGLAcctNo

            GLLOOP:

            WHILE ( @@fetch_status <> -1 )
              BEGIN
                  IF ( @@fetch_status <> -2 )
                    BEGIN
                        EXECUTE spglSubstAcct
                          @lGLAcctNo,
                          @lRetainedEarnAcct,
                          @lGLAcctNo output

                        SELECT @lGLAcctKey = GLAcctKey
                        FROM   tglAccount
                        WHERE  CompanyID = @iCompanyID
                               AND GLAcctNo = @lGLAcctNo

                        IF @@rowcount = 0
                          BEGIN
                              SELECT @lDummy = CompanyID
                              FROM   tglRetEarnAcctWrk
                              WHERE  SessionID = @oSessionID
                                     AND CompanyID = @iCompanyID
                                     AND RetEarnGLAcctNo = @lGLAcctNo

                              IF @@rowcount = 0
                                BEGIN
                                    SELECT @oRetVal = 1

                                    INSERT INTO tglRetEarnAcctWrk
                                                (SessionID,
                                                 CompanyID,
                                                 RetEarnGLAcctNo)
                                    VALUES      (@oSessionID,
                                                 @iCompanyID,
                                                 @lGLAcctNo)
                                END
                          END

                        FETCH next FROM GLCursor INTO @lGLAcctNo
                    END
              END --  GLLoop

            CLOSE GLCursor

            BREAK --  MainLoop
        END --  MainLoop

      IF @lDeclaredGLCursor = 1
        DEALLOCATE GLCursor

      IF @iCommitFlag = 1
        BEGIN
            IF @oRetVal IN ( 0, 1 )
              COMMIT TRANSACTION
            ELSE
              ROLLBACK TRANSACTION
        END
  END 