package gl

import (
	"gosqljobs/invtcommit/functions/sm"

	du "github.com/eaglebush/datautils"
)

// CalcBeginBalance - calculate beginning balance
// -- Return Values:
// --  -1 - Unsuccessful
// --   0 - Successful
// --   1 - Retained Earnings Account does not exist.
func CalcBeginBalance(
	bq *du.BatchQuery,
	iCompanyID string,
	iFiscYear string) int {

	bq.ScopeName("CalcBeginBalance")

	lRetainedEarnAcct := ""
	lUseMultCurr := false
	lClearNonFin := false
	qr := bq.Get(`SELECT RetainedEarnAcct, ClearNonFin, UseMultCurr
				  FROM tglOptions WITH (NOLOCK)
				  WHERE CompanyID=?`, iCompanyID)
	if qr.HasData {
		lRetainedEarnAcct = qr.First().ValueString("RetainedEarnAcct")
		lUseMultCurr = qr.First().ValueBool("ClearNonFin")
		lClearNonFin = qr.First().ValueBool("UseMultCurr")
	}

	lPriorFiscYear, lPriorFiscPer := FSGivePriorYearPeriod(bq, iCompanyID, iFiscYear)
	if lPriorFiscYear == "" && lPriorFiscPer == 0 {
		return -1
	}

	// Set Beginning Balances to zero in tglAcctHist.
	bq.Set(`UPDATE tglAcctHist
				SET BegBal = 0,
					StatBegBal = 0,
					UpdateCounter = UpdateCounter + 1
				WHERE FiscYear=?
					AND FiscPer=1
					AND GLAcctKey IN (SELECT GLAcctKey
										FROM tglAccount WITH (NOLOCK)
										WHERE CompanyID=?);`, iFiscYear, iCompanyID)

	if lUseMultCurr {
		bq.Set(`UPDATE tglAcctHistCurr
				SET BegBalHC = 0,
					BegBalNC = 0
				WHERE FiscYear=?
					AND FiscPer=1
					AND GLAcctKey IN (SELECT GLAcctKey
								FROM tglAccount WITH (NOLOCK)
								WHERE CompanyID=?);`, iFiscYear, iCompanyID)
	}

	var lRetainedEarnAcctExists bool

	qr = bq.Get(`SELECT a.GLAcctNo,
						a.GLAcctKey,
						d.AcctCatID
				FROM tglAccount a WITH (NOLOCK)
					INNER JOIN tglNaturalAcct b WITH (NOLOCK) ON (a.NaturalAcctKey = b.NaturalAcctKey)
					INNER JOIN tglAcctType c WITH (NOLOCK) ON (b.AcctTypeKey = c.AcctTypeKey)
					INNER JOIN tglAcctCategory d WITH (NOLOCK) ON (c.AcctCategoryKey = d.AcctCategoryKey)
				WHERE a.CompanyID=?;`, iCompanyID)

	for _, v := range qr.Data {

		lGLAcctNo := v.ValueString("GLAcctNo")
		lGLAcctKey := v.ValueInt64("GLAcctKey")
		lAcctCatID := v.ValueInt64("AcctCatID")

		lTotalBal := 0.0
		lStatQtyBal := 0.0
		lTempGLAcctKey := int64(0)
		lRetainedEarnAcctExists := true

		// Get Total for Prior Year
		qr2 := bq.Get(`SELECT COALESCE(SUM(BegBal),0) + COALESCE(SUM(DebitAmt),0) - COALESCE(SUM(CreditAmt),0),
							 COALESCE(SUM(StatBegBal),0) + COALESCE(SUM(StatQty),0)
						FROM tglAcctHist WITH (NOLOCK)
						WHERE GLAcctKey=?
							AND FiscYear=?;`, lGLAcctKey, lPriorFiscYear)
		if qr2.HasData {
			lTotalBal = qr2.First().ValueFloat64Ord(0)
			lStatQtyBal = qr2.First().ValueFloat64Ord(1)
		}

		if sm.InInt64Array(&[]int64{1, 2, 3}, lAcctCatID) || (lAcctCatID == 9 && lClearNonFin == false) {
			lTempGLAcctKey = lGLAcctKey
		}

		if sm.InInt64Array(&[]int64{4, 5, 6, 7, 8}, lAcctCatID) {

			// Does Retained Earnings Account exist?
			lRetEarnSubAcctNo := sm.SubstAcct(lGLAcctNo, lRetainedEarnAcct)
			qr2 = bq.Get(`SELECT GLAcctKey
							FROM tglAccount WITH (NOLOCK)
							WHERE CompanyID = ?
								AND GLAcctNo = ?;`, iCompanyID, lRetEarnSubAcctNo)
			if !qr2.HasData {
				lRetainedEarnAcctExists = false
			}

			lTempGLAcctKey = qr2.First().ValueInt64Ord(0)
		}

		if !lRetainedEarnAcctExists || lTempGLAcctKey == 0 {
			break
		}

		qr2 = bq.Get(`SELECT TOP 1 StatBegBal 
							FROM tglAcctHist WITH (NOLOCK)
							WHERE GLAcctKey=?
								AND FiscYear=?
								AND FiscPer=1;`, lTempGLAcctKey, iFiscYear)
		if !qr2.HasData {
			bq.Set(`INSERT INTO tglAcctHist (
							BegBal, CreditAmt, DebitAmt, FiscPer,
							FiscYear, GLAcctKey, StatBegBal, StatQty, UpdateCounter)
						VALUES (?, 0, 0, 1,
								?, ?, ?, 0, 1);`, lTotalBal,
				iFiscYear, lTempGLAcctKey, lStatQtyBal)
		} else {
			bq.Set(`UPDATE tglAcctHist
						SET BegBal = BegBal + ?,
							StatBegBal = StatBegBal + ?,
							UpdateCounter = UpdateCounter + 1
						WHERE GLAcctKey=?
							AND FiscYear=?
							AND FiscPer=1;`, lTotalBal, lStatQtyBal,
				lTempGLAcctKey, iFiscYear)
		}

	}

	// Multicurrency is Active
	if lUseMultCurr {

		qr = bq.Get(`SELECT DISTINCT a.GLAcctKey,
							a.CurrID,
							b.GLAcctNo,
							e.AcctCatID
					FROM tglAcctHistCurr a WITH (NOLOCK)
						INNER JOIN tglAccount b WITH (NOLOCK) ON (a.GLAcctKey = b.GLAcctKey)
						INNER JOIN tglNaturalAcct c WITH (NOLOCK) ON (b.NaturalAcctKey = c.NaturalAcctKey)
						INNER JOIN tglAcctType d WITH (NOLOCK) ON (c.AcctTypeKey = d.AcctTypeKey)
						INNER JOIN tglAcctCategory e WITH (NOLOCK) ON (d.AcctCategoryKey = e.AcctCategoryKey)
					WHERE a.FiscYear = ?
						AND b.CompanyID = ?;`, lPriorFiscYear, iCompanyID)

		for _, v := range qr.Data {

			lGLAcctNo := v.ValueString("GLAcctNo")
			lGLAcctKey := v.ValueInt64("GLAcctKey")
			lAcctCatID := v.ValueInt64("AcctCatID")
			lCurrID := v.ValueString("CurrID")

			lTotalBalHC := 0.0
			lTotalBalNC := 0.0

			qr2 := bq.Get(`SELECT 	COALESCE(SUM(BegBalHC),0) + COALESCE(SUM(DebitAmtHC),0) - COALESCE(SUM(CreditAmtHC),0),
									COALESCE(SUM(BegBalNC),0) + COALESCE(SUM(DebitAmtNC),0) - COALESCE(SUM(CreditAmtNC),0)
							FROM tglAcctHistCurr WITH (NOLOCK)
							WHERE GLAcctKey=?
								AND FiscYear=?
								AND CurrID=?;`, lGLAcctKey, lPriorFiscYear, lCurrID)
			if qr2.HasData {
				lTotalBalHC = qr2.First().ValueFloat64Ord(0)
				lTotalBalNC = qr2.First().ValueFloat64Ord(1)
			}

			if sm.InInt64Array(&[]int64{1, 2, 3}, lAcctCatID) || (lAcctCatID == 9 && lClearNonFin == false) {
				qr2 = bq.Get(`SELECT TOP 1 StatBegBal 
								FROM tglAcctHist WITH (NOLOCK)
								WHERE GLAcctKey=?
									AND FiscYear=?
									AND FiscPer=1;`, lGLAcctKey, iFiscYear)
				if !qr2.HasData {
					bq.Set(`INSERT INTO tglAcctHistCurr (
								BegBalHC, BegBalNC, CreditAmtHC, CreditAmtNC, CurrID, 
								DebitAmtHC, DebitAmtNC, FiscPer, FiscYear, GLAcctKey)
							VALUES (?, ?, 0, 0, ?,
									0, 0, 1, ?, ?);`, lTotalBalHC, lTotalBalNC, lCurrID,
						iFiscYear, lGLAcctKey)
				} else {
					bq.Set(`UPDATE tglAcctHistCurr
							SET BegBalHC = BegBalHC + ?,
								BegBalNC = BegBalNC + ?
							WHERE GLAcctKey = ?
								AND FiscYear = ?
								AND FiscPer = 1
								AND CurrID = ?;`, lTotalBalHC, lTotalBalNC, lGLAcctKey,
						iFiscYear, lCurrID)
				}
			}
		}
	}

	//
	if !lRetainedEarnAcctExists {
		return 1
	}

	return 0
}
