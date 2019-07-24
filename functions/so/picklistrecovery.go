package so

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// PickListRecovery - This will change the PickingComplete flag from False to True for the ShipLines in the
// 		specified pick list which had failed to be updated when exiting Create Pick process. This will
// 		make the specified pick list available to be edited by the Reprint Pick List, Process SO and
// 		Cancel Pick process.
//
//  Parameters
//     INPUT:  @iPickListKey
//
//    OUTPUT:  @oRetVal  = See Return codes
//
//    RETURN Codes
//     0 - Unexpected Error (SP Failure)
//     1 - Successful
func PickListRecovery(
	bq *du.BatchQuery,
	iPickListKey int,
	NonParam2 int,
	NonParam3 string,
	NonParam4 string,
	NonParam5 string) constants.ResultConstant {

	bq.ScopeName("PickListRecovery")

	//Delete SOLineDist lock keys
	bq.Set(`DELETE lck
			FROM tsoSOLineDistPick lck
				JOIN tsoShipLineDist sld WITH (NOLOCK) ON lck.SOLineDistKey=sld.SOLineDistKey
				JOIN tsoShipLine sl WITH (NOLOCK) ON sl.ShipLineKey=sld.ShipLineKey
			WHERE sl.PickListKey=?;`, iPickListKey)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`DELETE lck
			FROM timTrnsfrOrdLinePick lck
				JOIN tsoShipLine sl WITH (NOLOCK) ON lck.TrnsfrOrderLineKey = sl.TrnsfrOrderLineKey
			WHERE sl.PickListKey=?;`, iPickListKey)
	if !bq.OK() {
		return constants.ResultError
	}

	bq.Set(`UPDATE tsoShipLine
			SET	PickingComplete=1
			WHERE PickListKey=?
				AND	PickingComplete=0;`, iPickListKey)
	if !bq.OK() {
		return constants.ResultError
	}

	return constants.ResultSuccess
}
