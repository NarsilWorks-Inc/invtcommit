package sm

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// LogicalLockAdd - Adds a logical lock.  Assumes HostName, HostID, LoginTime,
// 				SpID of current connection.
//
//
// NOTE: Before using this function, please read the lines with NOTES
//
//
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
	iLockCleanupParam5 string) (Result constants.LogicalLockResultConstant, LogicalLockKey int) {

	bq.ScopeName("LogicalLockAdd")

	exclusiveLock := 2

	if !(iLockType == 1 || iLockType == 2) {
		return constants.LogLockResultLockTypeInvalid, 0
	}

	qr := bq.Get(`SELECT 1 FROM tsmLogicalLockType WITH (NOLOCK) WHERE LogicalLockType=?;`, iLogicalLockType)
	if !qr.HasData {
		return constants.LogLockResultNotFound, 0
	}

	//=====================================================================================================================================================================================
	// NOTE: This part must be put outside of this function
	// if iCleanupLocksFirst {
	// 	LogicalLockCleanup(bq, iLockType)
	// }
	//=====================================================================================================================================================================================

	// Exclusive lock requested.  Check for any lock.
	if iLockType == exclusiveLock {
		qr = bq.Get(`SELECT COUNT(LogicalLockID) FROM tsmLogicalLock WITH (NOLOCK) WHERE LogicalLockID=? AND LogicalLockType=?;`, iLogicalLockID, iLogicalLockType)
		if qr.HasData {
			if qr.First().ValueInt64Ord(0) > 0 {
				return constants.LogLockResultExclLockReqFailed, 0
			}
		}
	}

	qr = bq.Get(`SELECT COUNT(LogicalLockID) 
				FROM tsmLogicalLock WITH (NOLOCK) 
				WHERE LogicalLockID=? AND LogicalLockType=? AND LockType=?;`, iLogicalLockID, iLogicalLockType, exclusiveLock)
	if qr.HasData {
		if qr.First().ValueInt64Ord(0) > 0 {
			return constants.LogLockResultSharedLockReqFailed, 0
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
		return constants.LogLockResultUnexpected, 0
	}

	if !qr.HasData {
		return constants.LogLockResultUnexpected, 0
	}

	return constants.LogLockResultCreated, int(qr.First().ValueInt64Ord(0))
}
