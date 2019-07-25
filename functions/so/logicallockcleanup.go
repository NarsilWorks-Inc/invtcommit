package so

import (
	"gosqljobs/invtcommit/functions/constants"
	"strconv"
	"strings"

	du "github.com/eaglebush/datautils"
)

// LogicalLockCleanup - Removes logical locks for locks that do not have active SPIDs
// 			for the specified lock type.
//  PARAMETERS:
// 	@iLogicalLockType	Valid tsmLogicalLockType.LogicalLockType specified.
//  	@oRetVal	Return value of the lock processing.
// 				-1	Unexpected return.
// 				1 	Successful processing
// 				2	Invalid @LogicalLockType specified.
//  				When the stored procedure @oRetVal is > 1, then no logical
// 				locks were deleted.  When the stored procedure @oRetVal = 1,
// 				zero or more logical locks MAY have been deleted.  When an
// 				individual lock's cleanup procedure returns a non-successful
// 				status, that logical lock is not deleted, and that does not
// 				cause @oRetVal to be set to failure.  @oRetVal represents the
// 				status of spsmLogicalLockCleanup, not the status of any
// 				cleanup procedures called.
// ------------------------------------------------------------------------------
func LogicalLockCleanup(bq *du.BatchQuery, iLogicalLockType int) constants.ResultConstant {
	bq.ScopeName("LogicalLockCleanup")

	bq.Set(`IF OBJECT_ID('tempdb..#spLogicalLockList') IS NOT NULL
					TRUNCATE TABLE #spLogicalLockList
				ELSE BEGIN
					CREATE TABLE #spLogicalLockList
						(
							LogicalLockKey			INTEGER
							,LockCleanupProcedure	VARCHAR(255)	
							,LockCleanupParam1		VARCHAR(10)		
							,LockCleanupParam2		VARCHAR(10)		
							,LockCleanupParam3		VARCHAR(255)	
							,LockCleanupParam4		VARCHAR(255)	
							,LockCleanupParam5		VARCHAR(255)	
						)
					CREATE INDEX idx_spLogicalLockList ON #spLogicalLockList (LogicalLockKey)
				END;`)
	if !bq.OK() {
		return constants.ResultError
	}

	qr := bq.Get(`SELECT 1 FROM tsmLogicalLocktype WITH (NOLOCK) WHERE LogicalLockType=?;`, iLogicalLockType)
	if !qr.HasData {
		return constants.ResultFail
	}

	qr = bq.Set(`INSERT #spLogicalLockList
					 SELECT	L.LogicalLockKey
						   ,COALESCE(LT.LockCleanupProcedure,'')
						   ,CASE WHEN LockCleanupParam1 IS NULL THEN 'NULL'	
						    ELSE CONVERT(VARCHAR(10),L.LockCleanupParam1) END
						   ,CASE WHEN LockCleanupParam2 IS NULL THEN 'NULL'
							ELSE CONVERT(VARCHAR(10),L.LockCleanupParam2)
							END
						   ,COALESCE(L.LockCleanupParam3,'NULL')
						   ,COALESCE(L.LockCleanupParam4,'NULL')
						   ,COALESCE(L.LockCleanupParam5,'NULL')
					FROM tsmLogicalLock L WITH (NOLOCK)
						 INNER JOIN tsmLogicalLockType LT WITH (NOLOCK) ON LT.LogicalLockType=L.LogicalLockType
					WHERE L.LogicalLockType=?
						AND (L.HostID NOT IN (SELECT hostprocess FROM master..sysprocesses WITH (NOLOCK) WHERE hostprocess IS NOT NULL)
								OR 
							L.LogicalLockKey NOT IN (SELECT LL.LogicalLockKey 
													FROM tsmLogicalLock LL WITH (NOLOCK)
														INNER JOIN master..sysprocesses S WITH (NOLOCK) 
															ON LL.SpID = S.spid 
																AND RTRIM(LL.LoginTime) = RTRIM(S.LOGIN_TIME))
							);`, iLogicalLockType)
	if qr.HasAffectedRows {
		qr2 := bq.Get(`SELECT LogicalLockKey, LockCleanupProcedure, LockCleanupParam1, 
							LockCleanupParam2, LockCleanupParam3, LockCleanupParam4, LockCleanupParam5 
						FROM #spLogicalLockList;`)
		if qr2.HasData {
			clprocret := constants.ResultSuccess

			for _, v := range qr2.Data {
				logkey := int(v.ValueInt64Ord(0))
				clproc := strings.ToLower(v.ValueStringOrd(1))
				if clproc != "" {
					//param1 := v.ValueStringOrd(2)
					param2 := v.ValueStringOrd(3)
					param3 := v.ValueStringOrd(4)
					param4 := v.ValueStringOrd(5)
					param5 := v.ValueStringOrd(6)

					param2i, _ := strconv.Atoi(param2)
					switch clproc {
					case "spsopermanenthiddenbatchrecovery":
						clprocret = PermanentHiddenBatchRecovery(bq, logkey, param2i, param3, param4, param5)
					case "spglpermanenthiddenbatchrecovery":
						// no such stored proc
					case "spsopicklistrecovery":
						clprocret = PickListRecovery(bq, logkey, param2i, param3, param4, param5)
					case "spsodisposablebatchremover":
						clprocret = DisposableBatchRemover(bq, logkey, param2i, param3, param4, param5)
					}
				}

				if clprocret == constants.ResultSuccess {
					bq.Set(`DELETE FROM tsmLogicalLock WHERE LogicalLockKey=?;`, logkey)
				}
			}
		}
	}

	bq.Set(`DROP TABLE #spLogicalLockList;`)
	if !bq.OK() {
		return constants.ResultError
	}

	return constants.ResultSuccess
}
