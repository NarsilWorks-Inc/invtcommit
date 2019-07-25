package gl

import (
	"gosqljobs/invtcommit/functions/constants"

	du "github.com/eaglebush/datautils"
)

// PostAPIGLPosting - post api GL posting
func PostAPIGLPosting(bq *du.BatchQuery, iBatchKey int, iCompanyID string, iModuleNo int, iIntegrateWithGL bool) constants.ResultConstant {
	bq.ScopeName("PostAPIGLPosting")

	qr := bq.Get(`SELECT COUNT(*) FROM tglPosting WITH (NOLOCK) WHERE BatchKey=?;`, iBatchKey)
	lCount := qr.First().ValueInt64Ord(0)

	if lCount == 0 || !iIntegrateWithGL {
		return constants.ResultSuccess
	}

	res := SetAPIGLPosting(bq, iCompanyID, iBatchKey, iIntegrateWithGL)
	if res != constants.ResultSuccess {
		return constants.ResultError
	}

	bq.Set(`DELETE tglPosting WHERE BatchKey=?;`, iBatchKey)

	return constants.ResultSuccess
}
