package constants

// ======================================================================== SYSTEM ===================================================================== //

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

// ======================================================================== BATCHING ===================================================================== //

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

// ======================================================================== SALESORDER ===================================================================== //

// SOTranTypeConstant - sales order tran types
type SOTranTypeConstant int16

// SOShipLogTranStatus - shipment log tran status
type SOShipLogTranStatus int8

// Sales Order Tran Type constants
const (
	SOTranTypeSalesOrder        SOTranTypeConstant = 801 // Sales Order (Standard)
	SOTranTypeSalesOrderBlanket SOTranTypeConstant = 802 // Sales Order (Blanket)
	SOTranTypeChangeOrder       SOTranTypeConstant = 804 // SO Change Order
	SOTranTypeCustShip          SOTranTypeConstant = 810 // Customer Shipment
	SOTranTypeCustRtrn          SOTranTypeConstant = 811 // Customer Return
	SOTranTypeTransShip         SOTranTypeConstant = 812 // Transfer Shipment
	SOTranTypeTransIn           SOTranTypeConstant = 813 // Transfer In (Transit Warehouse)
	SOTranTypeDropShip          SOTranTypeConstant = 814 // Drop Shipment
	SOTranTypePackList          SOTranTypeConstant = 820 // Packing List
	SOTranTypeBillLdng          SOTranTypeConstant = 830 // Bill of Lading
	SOTranTypeRMA               SOTranTypeConstant = 835 // RMA
	SOTranTypeQuote             SOTranTypeConstant = 840 // SO Quote
)

// shipment log tran status
const (
	SOShipLogIncomplete SOShipLogTranStatus = 1
	SOShipLogPending    SOShipLogTranStatus = 2
	SOShipLogPosted     SOShipLogTranStatus = 3
	SOShipLogPurged     SOShipLogTranStatus = 4
	SOShipLogVoid       SOShipLogTranStatus = 5
	SOShipLogCommitted  SOShipLogTranStatus = 6
)

// ======================================================================== INVENTORY ===================================================================== //

// InventoryActionConstant - action on the inventory
type InventoryActionConstant int8

// InventoryStatusConstant - inventory status
type InventoryStatusConstant int8

// InventoryTranTypeConstant - tran types
type InventoryTranTypeConstant int16

// Inventory action constants
const (
	InventoryIncrease InventoryActionConstant = 1
	InventoryDecrease InventoryActionConstant = -1
)

// Inventory status
const (
	InventoryStatusPending InventoryStatusConstant = 1
	InventoryStatusActive  InventoryStatusConstant = 2
	InventoryStatusClosed  InventoryStatusConstant = 3
)

// Inventory tran types
const (
	IMTranTypeSale           InventoryTranTypeConstant = 701 // IM Sale
	IMTranTypeSaleRtrn       InventoryTranTypeConstant = 702 // IM Sale Return
	IMTranTypePurchase       InventoryTranTypeConstant = 703 // IM Purchase
	IMTranTypePrchRtrn       InventoryTranTypeConstant = 704 // IM Purchase Return
	IMTranTypeTransfIn       InventoryTranTypeConstant = 705 // Transfer In
	IMTranTypeTransfOut      InventoryTranTypeConstant = 706 // Transfer Out
	IMTranTypeIssue          InventoryTranTypeConstant = 707 // Issue
	IMTranTypePhysCount      InventoryTranTypeConstant = 708 // Physical Count
	IMTranTypeCostTierAdj    InventoryTranTypeConstant = 709 // Cost Tier Adjustment
	IMTranTypeAdjustment     InventoryTranTypeConstant = 710 // Adjustment
	IMTranTypeKitAssembly    InventoryTranTypeConstant = 711 // Kit Assembly
	IMTranTypeKitAssComp     InventoryTranTypeConstant = 712 // Kit Assembly (Component)
	IMTranTypeKitDisassembly InventoryTranTypeConstant = 713 // Kit Disassembly
	IMTranTypeKitDisComp     InventoryTranTypeConstant = 714 // Kit Disassembly (Component)
	IMTranTypeThreeStepTrans InventoryTranTypeConstant = 715 // Three Step Transfer
	IMTranTypeBinTrans       InventoryTranTypeConstant = 716 // Bin Transfer
	IMTranTypeBegBal         InventoryTranTypeConstant = 749 // IM Beginning Balance
)

// ======================================================================== GENERAL LEDGER ===================================================================== //

// GLPostStatusConstant - General ledger post status constant
type GLPostStatusConstant int8

// GLErrorLevelConstant - error levels
type GLErrorLevelConstant int8

// GLPostStatusConstant - members of the constant
const (
	GLPostStatusDefault               GLPostStatusConstant = 0  // New Transaction, have not been processed (Default Value).
	GLPostStatusSuccess               GLPostStatusConstant = 1  // Posted successfully.
	GLPostStatusInvalid               GLPostStatusConstant = 2  // Invalid GL Account exists.  Not considered as a fatal error since it can be replaced with the suspense account.
	GLPostStatusTTypeNotSupported     GLPostStatusConstant = -1 // TranType not supported.
	GLPostStatusTranNotCommitted      GLPostStatusConstant = -2 // Transactions have not yet been committed.
	GLPostStatusPostBatchNotExist     GLPostStatusConstant = -3 // GL posting batch does not exists or is invalid.
	GLPostStatusPostingPriorSOPeriod  GLPostStatusConstant = -4 // Posting to a prior SO period.
	GLPostStatusPostingClosedGLPeriod GLPostStatusConstant = -5 // Posting to a closed GL period.
	GLPostStatusTranLockedByUser      GLPostStatusConstant = -6 // Transactions have been locked by another user.
	GLPostStatusDebitCreditNotEqual   GLPostStatusConstant = -7 // Debits and Credits do not equal.
)

// GLErrorLevelConstant - Error levels
const (
	GLErrorWarning GLErrorLevelConstant = 1
	GLErrorFatal   GLErrorLevelConstant = 2
)

// various constants
const lInterfaceError int = 3
const lFatalError int = 2
const lWarning int = 1

// ======================================================================== PURCHASE ORDER ===================================================================== //

// PurchaseOrderTranTypeConstants - purchase order tran type constants
type PurchaseOrderTranTypeConstants int16

// tran type constants
const (
	POTranTypeOrder        PurchaseOrderTranTypeConstants = 1101 // Purchase Order (Standard)
	POTranTypeBlanketOrder PurchaseOrderTranTypeConstants = 1102 // Purchase Order (Blanket)
	POTranTypeRcptVendor   PurchaseOrderTranTypeConstants = 1110 // Receipt from Vendor
	POTranTypeReturn       PurchaseOrderTranTypeConstants = 1111 // PO Return
	POTranTypeWarehouse    PurchaseOrderTranTypeConstants = 1112 // Receipt from Warehouse
	POTranTypeTransOut     PurchaseOrderTranTypeConstants = 1113 // Transfer Out (Transit Warehouse)
	POTranTypeVoucher      PurchaseOrderTranTypeConstants = 1120 // PO Voucher
	POTranTypeRequisition  PurchaseOrderTranTypeConstants = 1130 // Requisition
	POTranTypeChangeOrder  PurchaseOrderTranTypeConstants = 1150 // PO Change Order
)
