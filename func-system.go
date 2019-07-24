package main

import (
	"strconv"
	"strings"
	"time"

	du "github.com/eaglebush/datautils"
)

// ResultConstant - result of evey processing
type ResultConstant int8

// ModuleConstant - module constants
type ModuleConstant int16

// LogicalLockResultConstant - Logical Lock Result
type LogicalLockResultConstant int16

//  Result Constants
const (
	ResultUnknown ResultConstant = -1
	ResultError   ResultConstant = 0
	ResultSuccess ResultConstant = 1
	ResultFail    ResultConstant = 2
)

// Module constants
const (
	ModuleAP ModuleConstant = 4
	ModuleAR ModuleConstant = 5
	ModuleIM ModuleConstant = 7
	ModuleSO ModuleConstant = 8
	ModuleCM ModuleConstant = 9
	ModuleMC ModuleConstant = 10
	ModulePO ModuleConstant = 11
	ModuleMF ModuleConstant = 12
)

// Logical Lock result constants
const (
	LogLockResultUnexpected          LogicalLockResultConstant = -1  // Unexpected return.
	LogLockResultCreated             LogicalLockResultConstant = 1   // SUCCESS.  Lock was created.
	LogLockResultNotFound            LogicalLockResultConstant = 2   // @iLogicalLockType not found in tsmLogicalLockType.
	LogLockResultLockTypeInvalid     LogicalLockResultConstant = 3   // @iLockType was not 1 or 2.
	LogLockResultSharedLockReqFailed LogicalLockResultConstant = 101 // Shared lock request failed, an exclusive lock exists.
	LogLockResultExclLockReqFailed   LogicalLockResultConstant = 102 // Exclusive lock request failed, a lock of some type exists.
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
func LogicalLockCleanup(bq *du.BatchQuery, iLogicalLockType int) ResultConstant {
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
		return ResultError
	}

	qr := bq.Get(`SELECT 1 FROM tsmLogicalLocktype WITH (NOLOCK) WHERE LogicalLockType=?;`, iLogicalLockType)
	if !qr.HasData {
		return ResultFail
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
			clprocret := ResultSuccess

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
						clprocret = SOPermanentHiddenBatchRecovery(bq, logkey, param2i, param3, param4, param5)
					case "spglpermanenthiddenbatchrecovery":
						// no such stored proc
					case "spsopicklistrecovery":
						clprocret = PickListRecovery(bq, logkey, param2i, param3, param4, param5)
					case "spsodisposablebatchremover":
						clprocret = SODisposableBatchRemover(bq, logkey, param2i, param3, param4, param5)
					}
				}

				if clprocret == ResultSuccess {
					bq.Set(`DELETE FROM tsmLogicalLock WHERE LogicalLockKey=?;`, logkey)
				}
			}
		}
	}

	bq.Set(`DROP TABLE #spLogicalLockList;`)
	if !bq.OK() {
		return ResultError
	}

	return ResultSuccess
}

// LogicalLockAddMultiple - Adds multiple logical locks.  Assumes HostName, HostID,
// 						    LoginTime, SpID of current connection.
//  PARAMETERS:
//  	@oRetVal	Return value of the lock processing.  This is not the success
// 				value of each lock requested
// 				-1	Unexpected return.
// 				1 	SUCCESS.  Lock processing did not encouunter any processing issues.  Locks may or may not have been	created.
// 				2	SUCCESS.  Locks processed, but at least one call to	spsmLogicalLockCleanup failed during cleanup procedure call.
// 				3	@iCleanupLocksFirst was not 0 or 1.  No locks were processed.
// 				4	#LogicalLocks table not found.  No locks were processed.
// 	@oLocksCreated
// 				The number of logical locks created.
// 	@oLocksRejected
// 				The number of logical locks requested by the caller but not
// 				created.
// 	@iCleanupLocksFirst
// 				0	Do not call spsmLogicalLockCleanup for each
// 					LogicalLockType
// 				1	Call spsmLogicalLockCleanup for each LogicalLockType.
// 	#LogicalLocks
// 				Temp table holding logical locks to be created.  This table
// 				should have the following format:
//  				CREATE TABLE #LogicalLocks
//  					(
//  					LogicalLockType			SMALLINT				-- value from tsmLogicalLockType (input)
//  					,LogicalLockID			VARCHAR(80)				-- User defined key (input)
// 					,UserKey				INTEGER			NULL	-- User defined key value.  Optional.  Only used for
// 																	-- the caller's reference.  Most likely a surrogate
// 																	-- key value for LogicalLockID.
//  					,LockType				SMALLINT				-- 1 = Shared, 2 = exclusive (input)
//  					,LogicalLockKey			INT				NULL	-- Logical LockKey returned (output, NULL to start)
// 					,Status					INTEGER			NULL	-- Logical lock creation status (output, NULL to start):
// 																	-- (Value returned from spsmLogicalLockAdd)
// 																	--	-1	Unexpected value
// 																	-- 	1	SUCCESS. Lock created.
// 																	--	2	Lock not created due to invalid LogicalLockType.
// 																	--	3	Lock not created due to invalid LockType.
// 																	--	101	Shared Lock not created due to existing
// 																	--		exclusive lock.
// 																	--	102	Exclusive lock not created due to existing
// 																	--		locks.
//  					,LockCleanupParam1		INTEGER			NULL	-- Param1 for cleanup procedure	(input, NULL if none)
//  					,LockCleanupParam2		INTEGER			NULL	-- Param2 for cleanup procedure	(input, NULL if none)
//  					,LockCleanupParam3		VARCHAR(255)	NULL	-- Param3 for cleanup procedure	(input, NULL if none)
//  					,LockCleanupParam4		VARCHAR(255)	NULL	-- Param4 for cleanup procedure	(input, NULL if none)
//  					,LockCleanupParam5		VARCHAR(255)	NULL	-- Param5 for cleanup procedure	(input, NULL if none)
//  					)
// -----------------------------------------------------------------------------
func LogicalLockAddMultiple(bq *du.BatchQuery, iCleanupLocksFirst bool, loginID string) (Result ResultConstant, LocksCreated int, LocksRejected int) {
	bq.ScopeName("LogicalLockAddMultiple")

	qr := bq.Get(`SELECT CASE WHEN OBJECT_ID('tempdb..#LogicalLocks') IS NULL THEN 0 ELSE 1 END;`)
	if qr.HasData {
		if qr.First().ValueInt64Ord(0) == 0 {
			return ResultConstant(4), 0, 0
		}
	}

	// Initialize temp table with starting values
	// where the LogicalLockKey has not yet to be defined.
	bq.Set(`UPDATE #LogicalLocks SET LogicalLockKey=NULL, Status=NULL WHERE (LogicalLockKey IS NULL OR LogicalLockKey=0);`)
	if !bq.OK() {
		return ResultFail, 0, 0
	}

	qr = bq.Get(`SELECT COUNT(*) FROM #LogicalLocks WHERE Status IS NULL;`)
	if qr.First().ValueInt64Ord(0) == 0 {
		return ResultSuccess, 0, 0
	}

	// Validate the LogicalLockType
	bq.Set(`UPDATE LL SET Status= 2
	     	 FROM #LogicalLocks LL 
			 WHERE Status IS NULL
					AND NOT EXISTS (
							SELECT LogicalLockType FROM tsmLogicalLockType LLT WITH (NOLOCK) WHERE LLT.LogicalLockType=LL.LogicalLockType
						);`)

	// Validate the LockType (shared/exclusive)
	bq.Set(`UPDATE #LogicalLocks SET Status=3 WHERE	Status IS NULL AND LockType NOT IN (1,2);`)

	// -----------------------------------------------------------------------
	// -- Call lock cleanup procedure for each logical lock type if requested.
	// -- Allows single cleanup call per lock type, rather than one call for
	// -- each row in #LogicalLocks.
	// -----------------------------------------------------------------------
	loopret := ResultUnknown
	if iCleanupLocksFirst {
		qr = bq.Get(`SELECT DISTINCT LogicalLockType FROM #LogicalLocks WHERE Status IS NULL ORDER BY LogicalLockType;`)
		if qr.HasData {
			//Loop through all LogicalLockTypes
			for _, v := range qr.Data {
				lr := LogicalLockCleanup(bq, int(v.ValueInt64Ord(0)))
				if lr != ResultSuccess {
					loopret := ResultFail
				}
			}
		}
	}

	// Set all lock keys to bad value
	bq.Set(`UPDATE #LogicalLocks SET LogicalLockKey=-1 WHERE Status IS NULL;`)

	loginTime := time.Now()
	qr = bq.Get(`SELECT	LOGIN_TIME FROM master..sysprocesses WITH (NOLOCK) WHERE spid=@@SPID;`)
	if qr.HasData {
		loginTime = qr.First().ValueTimeOrd(0)
	}

	qr = bq.Get(`SELECT	ID, LogicalLockType, LogicalLockID, LockType,
						LockCleanupParam1, LockCleanupParam2, LockCleanupParam3, LockCleanupParam4, LockCleanupParam5
				FROM #LogicalLocks WHERE Status IS NULL;`)
	for _, r := range qr.Data {
		llt := int(r.ValueInt64("LogicalLockType"))
		llid := r.ValueString("LogicalLockID")
		lt := int(r.ValueInt64("LockType"))
		lcp1, _ := strconv.Atoi(r.ValueString("LockCleanupParam1"))
		lcp2, _ := strconv.Atoi(r.ValueString("LockCleanupParam2"))
		lcp3 := r.ValueString("LockCleanupParam3")
		lcp4 := r.ValueString("LockCleanupParam4")
		lcp5 := r.ValueString("LockCleanupParam5")

		adres, lclogkey := LogicalLockAdd(bq, llt, llid, loginID, lt, false, lcp1, lcp2, lcp3, lcp4, lcp5)

		bq.Set(`UPDATE #LogicalLocks SET LogicalLockKey=?, Status=? WHERE ID=?;`, lclogkey, adres, r.ValueInt64("ID"))
	}

	lockscreated := 0
	locksrejected := 0
	qr = bq.Get("SELECT COUNT(*) FROM #LogicalLocks WHERE LogicalLockKey > 0 AND Status=1;")
	if qr.HasData {
		lockscreated = int(qr.First().ValueInt64Ord(0))
	}

	qr = bq.Get("SELECT COUNT(*) FROM #LogicalLocks")
	if qr.HasData {
		locksrejected = int(qr.First().ValueInt64Ord(0)) - lockscreated
	}

	if loopret != ResultFail {
		loopret = ResultSuccess
	}

	return loopret, lockscreated, locksrejected
}

// LogicalLockAdd - Adds a logical lock.  Assumes HostName, HostID, LoginTime,
// 				SpID of current connection.
// PARAMETERS:
//
// 	@iLogicalLockType
// 				tsmLogicalLockType.LogicalLockType for the desired type of
// 				lock.
//
//  	@iLogicalLockID
// 				User specified key.  Should have a unique meaning within
// 				@LogicalLockType.  There may be more than one record for
// 				a given @iLogicalLockID if the @iLockType is a shared lock.
//
//  	@oLogicalLockKey
// 				tsmLogicalLock.LogicalLockKey returned for the
// 				@LogicalLockType / @LogicalLockID.  This will be -1 if no
// 				lock was created (@oRetVal will be > 1).
//
//  	@oRetVal	Return value of the lock processing.
// 				-1	Unexpected return.
// 				1 	SUCCESS.  Lock was created.
// 				2	@iLogicalLockType not found in tsmLogicalLockType.
// 				3	@iLockType was not 1 or 2.
// 				4	@iCleanupLocksFirst was not 0 or 1.
// 				101	Shared lock request failed, an exclusive lock exists.
// 				102	Exclusive lock request failed, a lock of some type exists.
// 	@LockType
// 				Strength of lock to apply.
// 				1	Shared lock.  Allows other share locks as long as there
// 					is no exclusive lock for the LogicalLockType /
// 					LogicalLockID
// 				2	Exclusive lock.  Only one lock allowed for the given
// 					LogicalLockType / LogicalLockID.
// 	@iCleanupLocksFirst
// 				0	Do not call spsmLogicalLockCleanup.
// 				1	Call spsmLogicalLockCleanup first.
//
// 				Flag to indicate if spsmLogicalLockCleanup should be called
// 				prior to counting the locks.  Calling spsmLogicalLockCleanup
// 				first will slow down this procedure call, but will make sure
// 				the counts are up-to-date.  Not calling spsmLogicalLockCleanup
// 				may mean that logical locks have lost their connection but are
// 				still counted as valid.  It would be most accurate to call
// 				spsmLogicalLockCleanup first.  In cases where the caller
// 				essentially owns the lock type, and knows that a single
// 				connection will control all locks of that type, the caller
// 				may wish to call spsmLogicalLockCleanup directly at the start
// 				of its process and then set this flag to false when adding
// 				locks.
//  @LockCleanupParam1, @LockCleanupParam2, @LockCleanupParam3,
// 	@LockCleanupParam4, @LockCleanupParam5
// 				These parameters are not necessary in some cases.  They are
// 				required when the associated tsmLogicalLockType.LogicalLockType
// 				has a tsmLogicalLockType.LockCleanupProcedure that requires
// 				parameters.  When these parameters are required, the
// 				tsmLogicalLockType.LockCleanupProcedure will determine what
// 				should be in these parameters.
//
// 				These parameters are not required when the LogicalLockType
// 				does not have a LockCleanupProcedure, or when the
// 				LockCleanupProcedure does not need parameters.
// ------------------------------------------------------------------------------
func LogicalLockAdd(
	bq *du.BatchQuery,
	iLogicalLockType int,
	iLogicalLockID string,
	loginID string,
	iLockType int,
	iCleanupLocksFirst bool,
	iLockCleanupParam1 int,
	iLockCleanupParam2 int,
	iLockCleanupParam3 string,
	iLockCleanupParam4 string,
	iLockCleanupParam5 string) (Result LogicalLockResultConstant, LogicalLockKey int) {

	bq.ScopeName("LogicalLockAdd")

	exclusiveLock := 2

	if !(iLockType == 1 || iLockType == 2) {
		return LogLockResultLockTypeInvalid, 0
	}

	qr := bq.Get(`SELECT 1 FROM tsmLogicalLockType WITH (NOLOCK) WHERE LogicalLockType=?;`, iLogicalLockType)
	if !qr.HasData {
		return LogLockResultNotFound, 0
	}

	if iCleanupLocksFirst {
		LogicalLockCleanup(bq, iLockType)
	}

	// Exclusive lock requested.  Check for any lock.
	if iLockType == exclusiveLock {
		qr = bq.Get(`SELECT COUNT(LogicalLockID) FROM tsmLogicalLock WITH (NOLOCK) WHERE LogicalLockID=? AND LogicalLockType=?;`, iLogicalLockID, iLogicalLockType)
		if qr.HasData {
			if qr.First().ValueInt64Ord(0) > 0 {
				return LogLockResultExclLockReqFailed, 0
			}
		}
	}

	qr = bq.Get(`SELECT COUNT(LogicalLockID) 
				FROM tsmLogicalLock WITH (NOLOCK) 
				WHERE LogicalLockID=? AND LogicalLockType=? AND LockType=?;`, iLogicalLockID, iLogicalLockType, exclusiveLock)
	if qr.HasData {
		if qr.First().ValueInt64Ord(0) > 0 {
			return LogLockResultSharedLockReqFailed, 0
		}
	}

	loginTime := time.Now()
	qr = bq.Get(`SELECT	LOGIN_TIME FROM master..sysprocesses WITH (NOLOCK) WHERE spid=@@SPID;`)
	if qr.HasData {
		loginTime = qr.First().ValueTimeOrd(0)
	}

	// Turn off unnecessary counts
	bq.Set(`SET NOCOUNT ON;`)

	qr = bq.Get(`INSERT INTO tsmLogicalLock	(
					ActualUserID, HostName, HostID, LoginTime, SpID, LogicalLockID, LogicalLockType, LockType,
					LockCleanupParam1, LockCleanupParam2, LockCleanupParam3, LockCleanupParam4, LockCleanupParam5
				) VALUES (?,HOST_NAME(),HOST_ID(),?,@@SPID,?,?,?,?,?,?,?,?);
				SELECT SCOPE_IDENTITY();`, loginID, loginTime, iLogicalLockID, iLogicalLockType, iLockType,
		iLockCleanupParam1, iLockCleanupParam2, iLockCleanupParam3, iLockCleanupParam4, iLockCleanupParam5)

	if !bq.OK() {
		return LogLockResultUnexpected, 0
	}

	if !qr.HasData {
		return LogLockResultUnexpected, 0
	}

	return LogLockResultCreated, int(qr.First().ValueInt64Ord(0))
}

// LogicalLockRemoveMultiple - Removes multiple logical locks found in #LogicalLocks.  This
//				is the procedure that should be called by the caller who
//				originally added the locks.  Ideally, this procedure is called
//				once for every spsmLogicalLockAddMultiple procedure call.
//
//				NOTE:	This procdure will NOT cause the cleanup procedure to
//						execute.  The cleanup procedure is only executed for
//						unexpected situations such as a lost connection.
//
// PARAMETERS:
//
// 	@oRetVal	Return value of the lock processing.
//				-1	Unexpected return.
//				1 	SUCCESS.  Lock removal was processed.
//				2	#LogicalLocks table not found.
//
//	#LogicalLocks
//				Temp table holding logical locks to be deleted.  This
//				procedure will only delete those locks found to be valid.  It
//				is assumed that this table has been previosly processed by
//				spsmLogicalLockAddMultiple.
func LogicalLockRemoveMultiple(bq *du.BatchQuery) ResultConstant {
	bq.ScopeName("LogicalLockRemoveMultiple")

	qr := bq.Get(`SELECT ISNULL(OBJECT_ID('tempdb..#LogicalLocks'),0);`)
	if qr.HasData {
		if qr.First().ValueInt64Ord(0) == 0 {
			return ResultFail
		}
	}

	qr = bq.Set(`DELETE LL 
				FROM tsmLogicalLock LL 
					INNER JOIN #LogicalLocks T ON T.LogicalLockKey = LL.LogicalLockKey
				WHERE T.LogicalLockKey > 0 AND T.Status=1;`)
	if !bq.OK() {
		return ResultError
	}

	return ResultSuccess
}

// GetLanguage - Get Language code from tsmSiteProfile
func GetLanguage(bq *du.BatchQuery) int {
	qr := bq.Get(`SELECT LanguageID FROM tsmSiteProfile;`)
	if qr.HasData {
		return int(qr.First().ValueInt64Ord(0))
	}
	return 1033
}
