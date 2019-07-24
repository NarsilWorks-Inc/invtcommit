package main

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
