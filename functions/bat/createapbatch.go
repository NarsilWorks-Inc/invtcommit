package bat

import (
	"gosqljobs/invtcommit/functions/constants"
	"time"

	du "github.com/eaglebush/datautils"
)

// CreateApBatch - Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//    @_iCompanyId       Current Company ID
//    @_iUserId          Acuity User Id
//    @_iDefBatchCmnt    Default batch comment (for tarBatch)
//    @_iBatchKey     Key of the batch to insert
// Output Parameters:
//    oNextBatch   Batch number
//    oRetVal      ReturnValue:
// 	  0 - Did not make it through the procedure
// 	  1 - Successfully created the tmcBatch record
// 	  5 - Failed to create mcBatch record
// 	  6 - McBatch record already exists for this batchkey
// Modifications:
// Date         SE          Description
// 04/07/98    Gil Leguen   Added @_iDfltVendPmtMethKey to support Multiple Payment Methods
// 07/06/99    AJS          defaulting of SC cash account only if allowed to print checks
func CreateApBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) constants.BatchReturnConstant {

	bq.ScopeName("CreateApBatch")

	var lDefCashKey int
	var iDfltVendPmtMethKey int
	var iBatchType int

	/* Get the Batch Type from the Batch Log */
	qr := bq.Get(`SELECT BatchType
				 FROM tciBatchLog WITH (NOLOCK)
					WHERE BatchKey=?;`, iBatchKey)
	if !qr.HasData {
		return constants.BatchReturnError
	}

	iBatchType = int(qr.Get(0).ValueInt64Ord(0))

	// Batch Type: APSC AP System Checks, get Payment Method from tapOptions
	if iBatchType == 403 {
		// Get default cash account & payment method
		qr = bq.Get(`SELECT DfltCashAcctKey ,DfltVendPmtMethKey
          			 FROM tapOptions WITH (NOLOCK)
					  WHERE CompanyID=?`, iCompanyID)
		if !qr.HasData {
			return constants.BatchReturnError
		}

		lDefCashKey = int(qr.Get(0).ValueInt64Ord(0))
		iDfltVendPmtMethKey = int(qr.Get(0).ValueInt64Ord(1))

		// Null the account if it is not capable of having checks printed
		qr = bq.Get(`SELECT 1 FROM tcmCashAcct WITH (NOLOCK) WHERE CashAcctKey=? AND PrintChks=1;`, lDefCashKey)
		if !qr.HasData {
			lDefCashKey = 0
		}
	}

	// Batch Type: APMC AP Manual Checks, get Payment Method from tapVendPmtMethod
	if iBatchType == 402 {
		qr = bq.Get(`SELECT DfltCashAcctKey FROM tapOptions WITH (NOLOCK) WHERE CompanyID=?`, iCompanyID)
		if !qr.HasData {
			return constants.BatchReturnError
		}
		lDefCashKey = int(qr.Get(0).ValueInt64Ord(0))

		qr = bq.Get(`SELECT VendPmtMethKey FROM tapVendPmtMethod WITH (NOLOCK) WHERE CompanyID=? AND TranType=411;`, iCompanyID)
		if !qr.HasData {
			return constants.BatchReturnError
		}
		iDfltVendPmtMethKey = int(qr.Get(0).ValueInt64Ord(0))
	}

	qr = bq.Set(` INSERT tapBatch
					(BatchKey,   
					Hold, 
					InterCompany, 
					BatchOvrdSegValue,
					CashAcctKey, 
					BatchCmnt, 
					OrigUserID, 
					PostDate, 
					Private, 
					TranCtrl,
					TranDate,
					VendPmtMethKey)
				VALUES(?,0,0,NULL,?,?,?,?,0,0,?,?);`, iBatchKey, lDefCashKey, iDefBatchCmnt, iDefBatchCmnt, iUserID, dPostDate, iDfltVendPmtMethKey)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return constants.BatchReturnFailed
		}
	}

	return constants.BatchReturnValid
}
