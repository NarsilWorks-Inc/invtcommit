package bat

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// UpdateBatchLogCmnt - created to fix the issue reported
// ---------------------------------------------------------------------
// This sp is created to fix the issue reported in Scopus 28653 -
//   Batch Comment is blank when Lookup is used in Batch Header.
//
//   The cause of the issue is the Lookup is reading data from tciBatchLog
//   instead of from the txxBatch table of its own Module in Batch Header.
//   When a Batch is created, Batch Comment is stored in the unposted batch table
//   (txxBatch, per each module).  The Batch Comment would not be written to
//   tciBatchLog untill the batch is posted.
//
//   This sp will copy the Batch Comment from the unposted batch table to
//   the tciBatchLog based on the BatchKey passed in.
//
//   Input Parameters:
//     	@_iBatchKey     Primary Surrogate Key to tciBatchLog
// 	@_iModuleNo	Optional, MoudleNo if available
//   Output Parameters:
//      	o_RetVal      	ReturnValue:
//                   	0 - Did not make it through the procedure
//                   	1 - Batch log record Updated
func UpdateBatchLogCmnt(bq *du.BatchQuery, iBatchKey int, iModuleNo constants.ModuleConstant) constants.BatchReturnConstant {
	bq.ScopeName("UpdateBatchLogCmnt")

	lModuleNo := iModuleNo
	if lModuleNo == 0 {
		qr := bq.Get(`SELECT b.ModuleNo
					  FROM tciBatchLog a WITH (NOLOCK)
						JOIN tciBatchType b WITH (NOLOCK) ON	a.BatchType = b.BatchType 
					  WHERE a.BatchKey=?`, iBatchKey)
		if qr.HasData {
			lModuleNo = constants.ModuleConstant(qr.Get(0).ValueInt64Ord(0))
		}
	}

	tbl := ""
	switch lModuleNo {
	case 3: // GL
		tbl = "tglBatch"
	case 4: // AP
		tbl = "tapBatch"
	case 5: // AR
		tbl = "tarBatch"
	case 7: // IM
		tbl = "timBatch"
	case 8: // SO
		tbl = "tsoBatch"
	case 9: // CM
		tbl = "tcmBatch"
	case 10: // MC
		tbl = "tmcBatch"
	case 11: // PO
		tbl = "tpoBatch"
	case 12: // MF
		tbl = "tmfBatch_HAI"
	case 19: // PA
		tbl = "tpaBatch"
	}

	if tbl == "" {
		return constants.BatchReturnFailed
	}

	qr := bq.Set(`UPDATE a
				SET	a.BatchCmnt = b.BatchCmnt,
					a.PostDate = b.PostDate
				FROM tciBatchLog a
					JOIN ` + tbl + ` b WITH (NOLOCK) ON a.BatchKey=b.BatchKey
				WHERE b.BatchKey=?;`)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return constants.BatchReturnFailed
		}
	}

	return constants.BatchReturnValid
}
