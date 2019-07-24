package sm

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

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
func LogicalLockRemoveMultiple(bq *du.BatchQuery) constants.ResultConstant {
	bq.ScopeName("LogicalLockRemoveMultiple")

	qr := bq.Get(`SELECT ISNULL(OBJECT_ID('tempdb..#LogicalLocks'),0);`)
	if qr.HasData {
		if qr.First().ValueInt64Ord(0) == 0 {
			return constants.ResultFail
		}
	}

	qr = bq.Set(`DELETE LL 
				FROM tsmLogicalLock LL 
					INNER JOIN #LogicalLocks T ON T.LogicalLockKey = LL.LogicalLockKey
				WHERE T.LogicalLockKey > 0 AND T.Status=1;`)
	if !bq.OK() {
		return constants.ResultError
	}

	return constants.ResultSuccess
}
