package sm

import (
	"gosqljobs/invtcommit/functions/constants"
	"strconv"

	du "github.com/eaglebush/datautils"
)

// LogicalLockAddMultiple - Adds multiple logical locks.  Assumes HostName, HostID,
// 						    LoginTime, SpID of current connection.
//
//
// NOTE: Before using this function, please read the lines with NOTES
//
//
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
func LogicalLockAddMultiple(bq *du.BatchQuery, iCleanupLocksFirst bool, loginID string) (Result constants.ResultConstant, LocksCreated int, LocksRejected int) {
	bq.ScopeName("LogicalLockAddMultiple")

	qr := bq.Get(`SELECT CASE WHEN OBJECT_ID('tempdb..#LogicalLocks') IS NULL THEN 0 ELSE 1 END;`)
	if qr.HasData {
		if qr.First().ValueInt64Ord(0) == 0 {
			return constants.ResultConstant(4), 0, 0
		}
	}

	// Initialize temp table with starting values
	// where the LogicalLockKey has not yet to be defined.
	bq.Set(`UPDATE #LogicalLocks SET LogicalLockKey=NULL, Status=NULL WHERE (LogicalLockKey IS NULL OR LogicalLockKey=0);`)
	if !bq.OK() {
		return constants.ResultFail, 0, 0
	}

	qr = bq.Get(`SELECT COUNT(*) FROM #LogicalLocks WHERE Status IS NULL;`)
	if qr.First().ValueInt64Ord(0) == 0 {
		return constants.ResultSuccess, 0, 0
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
	loopret := constants.ResultUnknown

	//=====================================================================================================================================================================================
	// NOTE: This condition must be put outside of this function. The logical lock cleanup must be executed before this function
	// if iCleanupLocksFirst {
	// 	qr = bq.Get(`SELECT DISTINCT LogicalLockType FROM #LogicalLocks WHERE Status IS NULL ORDER BY LogicalLockType;`)
	// 	if qr.HasData {
	// 		//Loop through all LogicalLockTypes
	// 		for _, v := range qr.Data {
	// 			lr := LogicalLockCleanup(bq, int(v.ValueInt64Ord(0)))
	// 			if lr != constants.ResultSuccess {
	// 				loopret = constants.ResultFail
	// 			}
	// 		}
	// 	}
	// }
	//=====================================================================================================================================================================================

	// Set all lock keys to bad value
	bq.Set(`UPDATE #LogicalLocks SET LogicalLockKey=-1 WHERE Status IS NULL;`)

	// loginTime := time.Now()
	// qr = bq.Get(`SELECT	LOGIN_TIME FROM master..sysprocesses WITH (NOLOCK) WHERE spid=@@SPID;`)
	// if qr.HasData {
	// 	loginTime = qr.First().ValueTimeOrd(0)
	// }

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

	if loopret != constants.ResultFail {
		loopret = constants.ResultSuccess
	}

	return loopret, lockscreated, locksrejected
}
