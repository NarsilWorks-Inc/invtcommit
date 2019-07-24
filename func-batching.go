package main

import (
	"fmt"
	"time"

	du "github.com/eaglebush/datautils"
)

// BatchReturnConstant - batch processing results
type BatchReturnConstant int8

// BatchStatusConstant - batch status
type BatchStatusConstant int8

// BatchPostStatusConstant - batch post status
type BatchPostStatusConstant int16

// BatchCreateTypeConstant - batch create post status
type BatchCreateTypeConstant int8

// BatchTranTypeConstant - batch tran type
type BatchTranTypeConstant int16

// Constant values of Batch Returns
const (
	BatchReturnError       BatchReturnConstant = 0
	BatchReturnValid       BatchReturnConstant = 1
	BatchReturnNoNum       BatchReturnConstant = 2
	BatchReturnNoRecord    BatchReturnConstant = 3
	BatchReturnNoLog       BatchReturnConstant = 4
	BatchReturnFailed      BatchReturnConstant = 5
	BatchReturnExists      BatchReturnConstant = 6
	BatchReturnInterrupted BatchReturnConstant = 7
)

// Constant values of batch status
const (
	BatchStatusUndefined   BatchStatusConstant = 0 // added only
	BatchStatusInUse       BatchStatusConstant = 1
	BatchStatusOnHold      BatchStatusConstant = 2
	BatchStatusOutOfBal    BatchStatusConstant = 3
	BatchStatusBalanced    BatchStatusConstant = 4
	BatchStatusPosting     BatchStatusConstant = 5
	BatchStatusPosted      BatchStatusConstant = 6
	BatchStatusInterrupted BatchStatusConstant = 7
)

// Constant values of batch post status
const (
	BatchPostStatusUndefined         BatchPostStatusConstant = -1 // added only
	BatchPostStatusOpen              BatchPostStatusConstant = 0
	BatchPostStatusDeleted           BatchPostStatusConstant = 999
	BatchPostStatusPrepStarted       BatchPostStatusConstant = 100
	BatchPostStatusPrepCompleted     BatchPostStatusConstant = 150
	BatchPostStatusModStarted        BatchPostStatusConstant = 200
	BatchPostStatusModCompleted      BatchPostStatusConstant = 250
	BatchPostStatusGLStarted         BatchPostStatusConstant = 300
	BatchPostStatusGLCompleted       BatchPostStatusConstant = 350
	BatchPostStatusModClnUpStarted   BatchPostStatusConstant = 400
	BatchPostStatusModClnUpCompleted BatchPostStatusConstant = 450
	BatchPostStatusCompleted         BatchPostStatusConstant = 500
)

// Constant valies of batch create type
const (
	BatchCreateTypeUnknown        BatchCreateTypeConstant = 0
	BatchCreateTypeStandard       BatchCreateTypeConstant = 1
	BatchCreateTypeImport         BatchCreateTypeConstant = 2
	BatchCreateTypeSeed           BatchCreateTypeConstant = 3
	BatchCreateTypeUpgrade        BatchCreateTypeConstant = 4
	BatchCreateTypeImportPendTran BatchCreateTypeConstant = 5
	BatchCreateTypeCompCopy       BatchCreateTypeConstant = 6
	BatchCreateTypeMigrate        BatchCreateTypeConstant = 7
	BatchCreateTypeWhseAuto       BatchCreateTypeConstant = 8
)

// Batch Tran Type Constants
const (
	// Common Information:
	BatchTranTypeSysInternal BatchTranTypeConstant = 201 // Internal System

	// General Ledger:
	BatchTranTypeGlGenJrnls       BatchTranTypeConstant = 301 // General Journals
	BatchTranTypeGlAllocs         BatchTranTypeConstant = 304 // Allocations
	BatchTranTypeGlInterCompJrnls BatchTranTypeConstant = 305 // Intercompany Journals
	BatchTranTypeGlReversal       BatchTranTypeConstant = 325 // Reversals

	// Accounts Payable:
	BatchTranTypeAPVouchers  BatchTranTypeConstant = 401 // Vouchers
	BatchTranTypeAPManChecks BatchTranTypeConstant = 402 // Manual Checks
	BatchTranTypeAPSysChecks BatchTranTypeConstant = 403 // System Checks
	BatchTranTypeAPPayApps   BatchTranTypeConstant = 404 // Payment Applications
	BatchTranTypeAPARSettle  BatchTranTypeConstant = 405 // AP-AR Settlement

	// Accounts Receivable:
	BatchTranTypeARInvoices  BatchTranTypeConstant = 501 // AR Invoices
	BatchTranTypeARFinChrgs  BatchTranTypeConstant = 502 // AR Finance Charges
	BatchTranTypeARCshRcpts  BatchTranTypeConstant = 503 // AR Cash Receipts
	BatchTranTypeARPayApps   BatchTranTypeConstant = 504 // AR Payment Applications
	BatchTranTypeARSalesComm BatchTranTypeConstant = 505 // AR Sales Commissions
	BatchTranTypeARWriteOffs BatchTranTypeConstant = 506 // AR Write Offs
	BatchTranTypeARBadDebts  BatchTranTypeConstant = 507 // AR Bad Debts
	BatchTranTypeARRefunds   BatchTranTypeConstant = 508 // AR Refunds

	// Inventory Management:
	BatchTranTypeIMProcInvTran     BatchTranTypeConstant = 701 // Process Inventory Transactions
	BatchTranTypeIMProcPhysInv     BatchTranTypeConstant = 702 // Process Physical Inventory
	BatchTranTypeIMProcCostTierAdj BatchTranTypeConstant = 703 // Process Cost Tier Adjustments
	BatchTranTypeIMProcKitAss      BatchTranTypeConstant = 704 // Process Kit Assembly
	BatchTranTypeIMTranDiscAdj     BatchTranTypeConstant = 705 // Transfer Discrepancy Adjustments

	// Sales Order:
	BatchTranTypeSOProcShip     BatchTranTypeConstant = 801 // Process Shipments
	BatchTranTypeSOProcCustRtrn BatchTranTypeConstant = 802 // Process Customer Returns

	// Cash Management:
	BatchTranTypeCMBankTran  BatchTranTypeConstant = 901 // CM Bank Transactions
	BatchTranTypeCMDeposits  BatchTranTypeConstant = 902 // CM Deposits
	BatchTranTypeCMBankRecon BatchTranTypeConstant = 903 // CM Bank Reconciliation

	// Multi-Currency:
	BatchTranTypeMCGlReval  BatchTranTypeConstant = 1001 // GL Revaluation
	BatchTranTypeMCAPReval  BatchTranTypeConstant = 1002 // AP Revaluation
	BatchTranTypeMCARReval  BatchTranTypeConstant = 1003 // AR Revaluation
	BatchTranTypeMCRevReval BatchTranTypeConstant = 1025 // Revaluation Reversal

	// Purchase Order:
	BatchTranTypePORcptGoods BatchTranTypeConstant = 1101 // Process Receipt Of Goods
	BatchTranTypePORcptInvc  BatchTranTypeConstant = 1102 // Process Receipt Of Invoice
	BatchTranTypePORtrnGoods BatchTranTypeConstant = 1103 // Process Return Of Goods

	// 2001-2099  Reserved

	// Manufacturing:
	BatchTranTypeMFMatWip     BatchTranTypeConstant = 9000 // MF-Material WIP
	BatchTranTypeMFLabWip     BatchTranTypeConstant = 9001 // MF-Labor WIP
	BatchTranTypeMFPrgWip     BatchTranTypeConstant = 9002 // MF-Progress WIP
	BatchTranTypeMFJobClsWip  BatchTranTypeConstant = 9003 // MF-Job Close WIP
	BatchTranTypeMFCostRollup BatchTranTypeConstant = 9004 // MF-Cost Rollup
)

// GetNextBatchNo - Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//     @_iCompanyID        Current Company ID
//     @_iBatchType        Type of Batch (Numeric Code)
//     @_iUserId           User Id
//     @_iModuleNo         Module No
//     @_optHiddenBatch    Optional parameter. Tells if batch being created is hidden,
//                         which results in a BatchNo = 0.
// Output Parameters:
//     @_oBatchKey         Batch Key
//     @_oNextBatch        Batch number
//     @_oRetVal           ReturnValue:
//                             0 - Did not make it through the procedure
//                             1 - Valid number found
//                             2 - No numbers could be found to use  (00001-99999)
//                             3 - No tciBatchTypCompany Record for the Batch/Co specified
//                             4 - Unable to create tciBatchLog record
// ---------------------------------------------------------------------
func GetNextBatchNo(
	bq *du.BatchQuery,
	iCompanyID string,
	iBatchType int,
	iUserID string,
	iModuleNo ModuleConstant,
	optHiddenBatch int) (Result BatchReturnConstant, BatchKey int, NextBatchNo int) {

	bq.ScopeName("GetNextBatchNo")

	var qr du.QueryResult

	lNextBatchNo := 0
	lBatchTypeID := ""
	lBatchKey := 0

	if optHiddenBatch == 0 {
		/* Override batch number for hidden batches.  Set them to zero. */
		qr = bq.Get(`SELECT nextbatchno, batchtypeid
					  FROM tciBatchTypCompany WITH (NOLOCK)
					  WHERE companyid=? AND batchtype=?;`, iCompanyID, iBatchType)
		if !qr.HasData {
			return BatchReturnNoRecord, 0, 0
		}
		lNextBatchNo = int(qr.Get(0).ValueInt64Ord(0))
		lBatchTypeID = qr.Get(0).ValueStringOrd(1)

		/* Lock the table for updates. Notice the UPDLOCK table hint */
		bq.Get(`SELECT nextbatchno
				FROM tciBatchTypCompany WITH (UPDLOCK)
				WHERE companyid=? AND batchtype=?;`, iCompanyID, iBatchType)
	}

	if lNextBatchNo > 999999 {
		lNextBatchNo = 1
	}

	/* Initialize loop starting number */
	lStartingNumber := lNextBatchNo
	lNoRecFound := false

	for {
		valid := BatchReturnError
		// Check if batch number exists
		qr = bq.Get(`SELECT TOP 1 batchno FROM tcibatchlog WITH (nolock) WHERE sourcecompanyid=? AND batchno=? AND batchtype=?;`, iCompanyID, lNextBatchNo, iBatchType)
		if !qr.HasData || optHiddenBatch != 0 {
			// If not found, create
			valid, lBatchKey = CreateBatchLog(bq, iCompanyID, iModuleNo, iBatchType, lBatchTypeID, lNextBatchNo, iUserID, 0)
			if valid != BatchReturnValid {
				return BatchReturnError, 0, 0
			}
			lNoRecFound = true
		}

		if optHiddenBatch == 0 {
			if lNextBatchNo > 999999 {
				lNextBatchNo = 1
			} else {
				lNextBatchNo++
			}

			if lNoRecFound {
				qr = bq.Set(`UPDATE tcibatchtypcompany SET nextbatchno=?
							 WHERE  companyid=?	AND batchtype=?;`, lNextBatchNo, iCompanyID, iBatchType)
				if qr.HasData {
					return BatchReturnValid, lBatchKey, lNextBatchNo
				}
			}
		}

		if optHiddenBatch != 0 {
			return BatchReturnValid, lBatchKey, lNextBatchNo
		}

		if lStartingNumber == lStartingNumber {
			return BatchReturnNoNum, lBatchKey, lNextBatchNo
		}
	}
}

// CreateBatchLog - Creates a batch log record
// ---------------------------------------------------------------------
// Input Parameters:
// @_iCompanyId        current Company ID
// @_iModuleNo         Module Number (5=AR,10=MC, etc..)
// @_iBatchType        Type of batch (Numeric Code)
// @_iBatchTypeID      Type of batch (ID)
// @_iBatchNo          batch Number
// @_iUserID           Acuity User Id
// @_iRevBatchKey      Key of the reverse batch
//
// Output Parameters:
// @_oBatchKey         Key of the batch added to batch log
// @_oRetVal           ReturnValue:
// 0 - Did not make it through the procedure
// 1 - batch log record created
// 4 - Unable to create tciBatchLog record
// Modified:
// MM/DD/YY 		BY		COMMENT
// 05/10/2007		JY		Added code to insert value into CreateDate, CreateType,
// 						UpdateCounter to avoid the trigger to be fired
// 						when insert data into tciBatchLog.
// ---------------------------------------------------------------------
func CreateBatchLog(
	bq *du.BatchQuery,
	iCompanyID string,
	iModuleNo ModuleConstant,
	iBatchType int,
	iBatchTypeID string,
	iBatchNo int,
	iUserID string,
	iRevBatchKey int) (Result BatchReturnConstant, BatchKey int) {

	bq.ScopeName("CreateBatchLog")

	qr := bq.Get(`SELECT ModuleID FROM tsmModuleDef WITH (NOLOCK) WHERE ModuleNo=?`, iModuleNo)
	if !qr.HasData {
		return BatchReturnError, 0
	}

	batchID := qr.Get(0).ValueStringOrd(0) + fmt.Sprintf("%0d", iBatchType) + `-` + fmt.Sprintf("%0d", iBatchNo)
	batchKey := GetNextSurrogateKey(bq, `tciBatchLog`)

	var rev interface{}
	if iRevBatchKey != 0 {
		rev = iRevBatchKey
	}

	qr = bq.Set(`INSERT INTO tciBatchLog (
					BatchKey,
					BatchID,
					BatchNo,
					Status,
					PostStatus,
					BatchType,
					OrigUserID,
					SourceCompanyID,
					PostCompanyID,
					RevrsBatchKey,
					CreateDate,
					CreateType,
					UpdateCounter
				) VALUES (?,?,?,?,?,?,?,?,?,?,GETDATE(),?,0);`,
		batchKey, batchID, iBatchNo, BatchStatusBalanced, BatchPostStatusOpen,
		iBatchType, iUserID, iCompanyID, iCompanyID, rev, BatchCreateTypeStandard)

	if qr.Get(0).ValueInt64("Affected") == 0 {
		return BatchReturnNoLog, 0
	}

	return BatchReturnValid, batchKey
}

// CreateMcBatch - Creates a batch log record
// ---------------------------------------------------------------------
// Input Parameters:
// @_iCompanyId   	current Company ID
// @_iUserID      	Acuity User Id
// @_iDefBatchCmnt	Default batch Comment (for tarBatch)
// @_iBatchKey     Key of the batch to insert
// Output Parameters:
// oNextBatch   batch Number
// oRetVal      ReturnValue:
//    0 - Did not make it through the procedure
//    1 - Successfully created the record
//    5 - Failed to create record
//    6 - McBatch record already exists for this BatchKey
func CreateMcBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	iBatchType int,
	iBatchKey int) BatchReturnConstant {

	bq.ScopeName("CreateMcBatch")

	qr := bq.Get(`SELECT BatchKey FROM tmcBatch WHERE BatchKey=?;`, iBatchKey)
	if qr.HasData {
		return BatchReturnExists
	}

	lRevaluationDate := time.Now().Format("2006-01-02")
	lReversalDate := lRevaluationDate
	lExchRateSchdKey := 0

	qr = bq.Get(`SELECT a.EndDate, 
						CASE 
							WHEN ? = 1001 THEN b.GLRevalExchSchdKey  /* GL Revaluation Gain */
							WHEN ? = 1002 THEN b.APRevalExchSchdKey  /* AP Revaluation Gain */
							WHEN ? = 1003 THEN b.ARRevalExchSchdKey  /* AR Revaluation Gain */ 
						END
				FROM tglFiscalPeriod a WITH (NOLOCK)
					INNER JOIN tmcOptions b WITH (NOLOCK) 
						ON a.FiscYear  = b.CurntFiscYear
							AND a.FiscPer   = b.CurntFiscPer
							AND a.CompanyID = b.CompanyID
				WHERE b.CompanyID=?;`, iBatchType, iBatchType, iBatchType, iCompanyID)
	if qr.HasData {
		lExchRateSchdKey = int(qr.Get(0).ValueInt64Ord(1))
		ed := qr.Get(0).ValueTimeOrd(0)
		lRevaluationDate = ed.Format("2006-01-02")
		lReversalDate = ed.Add(time.Hour * 24).Format("2006-01-02")
	}

	qr = bq.Set(`INSERT INTO tmcBatch
					(BatchKey,
					BatchCmnt, 
					Hold, 
					OrigUserID, 
					PostDate, 
					Private, 
					Processed, 
					ReversingDate,
					CurrExchSchdKey)
				VALUES (?,?,0,?,?,0,0,?,?);`, iBatchKey, iDefBatchCmnt, iUserID, lRevaluationDate, lReversalDate, lExchRateSchdKey)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

// CreateCmBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//    @_iCompanyId   	Current Company ID
//    @_iUserId      	User Id
//    @_iDefBatchCmnt	Default batch comment (for tcmBatch)
//    @_dPostDate     Post date
//    @_iBatchKey     Key of the batch to insert
// Output Parameters:
//    oNextBatch   Batch number
//    oRetVal      ReturnValue:
// 	  0 - Did not make it through the procedure
// 	  1 - Successfully created the record
// 	  5 - Failed to create record
// 	  6 - McBatch record already exists for this batchkey
func CreateCmBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) BatchReturnConstant {

	bq.ScopeName("CreateCmBatch")

	qr := bq.Set(`INSERT INTO tcmBatch (
					BatchKey,
					BankStmtKey,
					BatchCmnt,
					CashAcctKey,
					CurrExchSchdKey,
					DepSlipPrinted,
					Hold,
					InterCompany,
					InterCompBatchKey,
					OrigUserID,
					PostDate,
					Private,
					UpdateCounter
				) VALUES (?,NULL,?,0,NULL,0,0,0,NULL,?,?,0,0);`, iBatchKey, iDefBatchCmnt, iUserID, dPostDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

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
	iBatchKey int) BatchReturnConstant {

	bq.ScopeName("CreateApBatch")

	var lDefCashKey int
	var iDfltVendPmtMethKey int
	var iBatchType int

	/* Get the Batch Type from the Batch Log */
	qr := bq.Get(`SELECT BatchType
				 FROM tciBatchLog WITH (NOLOCK)
					WHERE BatchKey=?;`, iBatchKey)
	if !qr.HasData {
		return BatchReturnError
	}

	iBatchType = int(qr.Get(0).ValueInt64Ord(0))

	// Batch Type: APSC AP System Checks, get Payment Method from tapOptions
	if iBatchType == 403 {
		// Get default cash account & payment method
		qr = bq.Get(`SELECT DfltCashAcctKey ,DfltVendPmtMethKey
          			 FROM tapOptions WITH (NOLOCK)
					  WHERE CompanyID=?`, iCompanyID)
		if !qr.HasData {
			return BatchReturnError
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
			return BatchReturnError
		}
		lDefCashKey = int(qr.Get(0).ValueInt64Ord(0))

		qr = bq.Get(`SELECT VendPmtMethKey FROM tapVendPmtMethod WITH (NOLOCK) WHERE CompanyID=? AND TranType=411;`, iCompanyID)
		if !qr.HasData {
			return BatchReturnError
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
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

// CreateArBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//      @_iCompanyId      Current Company ID
//      @_iUserId         Acuity User Id
//      @_iDefBatchCmnt   Default batch comment (for tarBatch)
//    	@_dPostDate     Post date
//      @_iBatchKey     Key of the batch to insert
//   Output Parameters:
//      oNextBatch   Batch number
//      oRetVal      ReturnValue:
//         0 - Did not make it through the procedure
//         1 - Successfully created the record
//         5 - Failed to create record
//         6 - McBatch record already exists for this batchkey
func CreateArBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) BatchReturnConstant {

	bq.ScopeName("CreateArBatch")

	cashAcctKey := 0
	qr := bq.Get(`SELECT DfltCashAcctKey FROM tarOptions WHERE CompanyID=?`, iCompanyID)
	if qr.HasData {
		cashAcctKey = int(qr.Get(0).ValueInt64Ord(0))
	}

	qr = bq.Set(`INSERT INTO tarBatch (
					BatchKey, 
					BankDepAmt,  
					Hold, 
					InterCompany, 
					BatchOvrdSegValue,
					CashAcctKey, 
					BatchCmnt, 
					OrigUserID, 
					PostDate, 
					Private, 
					TranCtrl,
					SourceModule) VALUES (?,0,0,0,NULL,?,?,?,?,0,0,5);`, iBatchKey, cashAcctKey, iDefBatchCmnt, iUserID, dPostDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

// CreatePoBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//      @_iCompanyId      Current Company ID
//      @_iUserId         Acuity User Id
//      @_iDefBatchCmnt   Default batch comment (for tpoBatch)
//    	@_dPostDate     Post date
//      @_iBatchKey     Key of the batch to insert
//   Output Parameters:
//      oNextBatch   Batch number
//      oRetVal      ReturnValue:
//         0 - Did not make it through the procedure
//         1 - Successfully created the tmcBatch record
//         5 - Failed to create mcBatch record
//         6 - McBatch record already exists for this batchkey
func CreatePoBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) BatchReturnConstant {

	bq.ScopeName("CreatePoBatch")

	qr := bq.Set(`INSERT INTO tpoBatch ( 
					batchkey,
					batchcmnt,
					hold,
					intercompany,
					intercompbatchkey,
					origuserid,
					postdate,
					private,
					updatecounter,
					createdate)
				VALUES (?,?,0,0,NULL,?,?,0,0,GETDATE());`, iBatchKey, iDefBatchCmnt, iUserID, dPostDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

// CreateImBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//      @_iCompanyId      Current Company ID
//      @_iUserId         Acuity User Id
//      @_iDefBatchCmnt   Default batch comment (for timBatch)
//    	@_dPostDate     Post date
//      @_iBatchKey     Key of the batch to insert
//   Output Parameters:
//      oNextBatch   Batch number
//      oRetVal      ReturnValue:
//         0 - Did not make it through the procedure
//         1 - Successfully created the record
//         5 - Failed to create mcBatch record
//         6 - McBatch record already exists for this batchkey
func CreateImBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) BatchReturnConstant {

	bq.ScopeName("CreateImBatch")

	qr := bq.Set(`INSERT INTO timBatch
					(batchkey,
					batchcmnt,
					hold,
					origuserid,
					postdate,
					private,
					updatecounter,
					createdate)
				VALUES (?,?,0,?,?,0,0,GETDATE());`, iBatchKey, iDefBatchCmnt, iUserID, dPostDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

// CreateSoBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//      @_iCompanyId      Current Company ID
//      @_iUserId         Acuity User Id
//      @_iDefBatchCmnt   Default batch comment (for tsoBatch)
//    	@_dPostDate     Post date
//      @_iBatchKey     Key of the batch to insert
//   Output Parameters:
//      oNextBatch   Batch number
//      oRetVal      ReturnValue:
//         0 - Did not make it through the procedure
//         1 - Successfully created the record
//         5 - Failed to create record
//         6 - McBatch record already exists for this batchkey
func CreateSoBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int,
	optInvcDate *time.Time) BatchReturnConstant {

	bq.ScopeName("CreateSoBatch")

	invcDate := dPostDate
	if optInvcDate != nil {
		invcDate = *optInvcDate
	}

	qr := bq.Set(`INSERT INTO tsoBatch
					(batchkey,
					batchcmnt,
					hold,
					origuserid,
					postdate,
					invcdate,
					private,
					updatecounter,
					createdate)
				VALUES (?,?,0,?,?,?,0,0,GETDATE());`, iBatchKey, iDefBatchCmnt, iUserID, dPostDate, invcDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

// CreateMfBatch -  Create the next batch number
// ---------------------------------------------------------------------
// Input Parameters:
//      @_iCompanyId      Current Company ID
//      @_iUserId         Acuity User Id
//      @_iDefBatchCmnt   Default batch comment (for tmfBatch_HAI)
//    	@_dPostDate     Post date
//      @_iBatchKey     Key of the batch to insert
//   Output Parameters:
//      oNextBatch   Batch number
//      oRetVal      ReturnValue:
//         0 - Did not make it through the procedure
//         1 - Successfully created the record
//         5 - Failed to create mcBatch record
//         6 - McBatch record already exists for this batchkey
func CreateMfBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iUserID string,
	iDefBatchCmnt string,
	dPostDate time.Time,
	iBatchKey int) BatchReturnConstant {

	bq.ScopeName("CreateMfBatch")

	qr := bq.Set(`INSERT INTO tmfBatch_HAI (
					BatchKey,
					BatchCmnt,
					Hold,
					OrigUserID,
					PostDate,
					UpdateCounter,
					CreateDate)
				VALUES (?,?,0,?,?,0,GETDATE());`, iBatchKey, iDefBatchCmnt, iUserID, dPostDate)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

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
func UpdateBatchLogCmnt(bq *du.BatchQuery, iBatchKey int, iModuleNo ModuleConstant) BatchReturnConstant {
	bq.ScopeName("UpdateBatchLogCmnt")

	lModuleNo := iModuleNo
	if lModuleNo == 0 {
		qr := bq.Get(`SELECT b.ModuleNo
					  FROM tciBatchLog a WITH (NOLOCK)
						JOIN tciBatchType b WITH (NOLOCK) ON	a.BatchType = b.BatchType 
					  WHERE a.BatchKey=?`, iBatchKey)
		if qr.HasData {
			lModuleNo = ModuleConstant(qr.Get(0).ValueInt64Ord(0))
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
		return BatchReturnFailed
	}

	qr := bq.Set(`UPDATE a
				SET	a.BatchCmnt = b.BatchCmnt,
					a.PostDate = b.PostDate
				FROM tciBatchLog a
					JOIN ` + tbl + ` b WITH (NOLOCK) ON a.BatchKey=b.BatchKey
				WHERE b.BatchKey=?;`)
	if qr.HasData {
		if qr.Get(0).ValueInt64("Affected") == 0 {
			return BatchReturnFailed
		}
	}

	return BatchReturnValid
}

// GetNextBatch - Create the next batch Number
// ---------------------------------------------------------------------
// Input Parameters:
// @_iCompanyId   	current Company ID
// @_iBatchType   	Type of batch (Numeric Code)
// @_iUserId      	Acuity User Id
// @_iDefBatchCmnt	Default batch Comment (for tarBatch)
// @_iPostDate     Post Date used to create the batch.
// @_optHiddenBatch Optional parameter. Tells if batch being created is hidden,
// which results in a BatchNo = 0.
// Output Parameters:
// oNextBatch   batch Number
// oRetVal      ReturnValue:
// 0 - Did not make it through the procedure
// 1 - Valid Number found
// 2 - No numbers could be found to use  (0000001-9999999)
// 3 - No tciBatchTypCompany Record for the batch/Co specified
// 4 - Unable to create tciBatchLog record
// 5 - Unable to create tmcBatch record
// 6 - tmcBatch record already exists for that key
func GetNextBatch(
	bq *du.BatchQuery,
	iCompanyID string,
	iModuleNo ModuleConstant,
	iBatchType int,
	iUserID string,
	iDefBatchCmnt string,
	iPostDate time.Time,
	optHiddenBatch int,
	optInvcDate *time.Time) (Result BatchReturnConstant, BatchKey int, NextBatchNo int) {

	bq.ScopeName("GetNextBatch")

	nextBatchNo := 0
	batchKey := 0
	var res BatchReturnConstant

	if iModuleNo == 10 {
		qr := bq.Get(`SELECT batchno, batchkey
					  FROM tcibatchlog
					  WHERE  batchtype=?
							AND sourcecompanyid=?
							AND status <> 6
							AND poststatus <> 999;`, iBatchType, iCompanyID)
		if qr.HasData {
			nextBatchNo = int(qr.Get(0).ValueInt64Ord(0))
			batchKey = int(qr.Get(0).ValueInt64Ord(1))
			return BatchReturnInterrupted, batchKey, nextBatchNo
		}
	}

	// GetNextBatch will get a good batch Number,
	// create the tciBatchLog Record, increment the nextno
	// and write it back to tciBatchTypCompany.
	// (or it will return something other than 1).
	res, batchKey, nextBatchNo = GetNextBatchNo(bq, iCompanyID, iBatchType, iUserID, iModuleNo, optHiddenBatch)
	if res != BatchReturnValid {
		return res, 0, 0
	}

	switch iModuleNo {
	case ModuleAP: // AP
		res = CreateApBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case ModuleAR: // AR
		res = CreateArBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case ModuleIM: // IM
		res = CreateImBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case ModuleSO: // SO
		res = CreateSoBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey, optInvcDate)
	case ModuleCM: // CM
		res = CreateCmBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case ModuleMC: // MC
		res = CreateMcBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iBatchType, batchKey)
	case ModulePO: // PO
		res = CreatePoBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	case ModuleMF: // MF
		res = CreateMfBatch(bq, iCompanyID, iUserID, iDefBatchCmnt, iPostDate, batchKey)
	}

	ures := UpdateBatchLogCmnt(bq, batchKey, iModuleNo)
	if ures != BatchReturnValid {
		return ures, batchKey, nextBatchNo
	}

	return res, batchKey, nextBatchNo
}
