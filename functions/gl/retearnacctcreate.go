package gl

import (
	"gosqljobs/invtcommit/functions/sm"
	"strings"

	du "github.com/eaglebush/datautils"
)

//RetEarnAcctCreate - create retained earn account
func RetEarnAcctCreate(
	bq *du.BatchQuery,
	iCompanyID string,
	iSessionID int,
	iRetainedEarnAcct string) (Result int, SessionID int) {

	bq.ScopeName("RetEarnAcctCreate")

	// -- Return Values
	// --      0 - Successful - No Asterisks in the Retained Earn Acct Mask
	// --      1 - Successful - Asterisks in the Retained Earn Acct Mask
	// --      2 - No Records Created in the process
	// --     -1 - UnSuccessful
	lSessionID := iSessionID
	if lSessionID == 0 {
		lSessionID = sm.GetNextSurrogateKey(bq, `tglRetEarnAcctWrk`)
	}

	// check if the retained earn account number has asterisk (for wildcard )
	pos := strings.Index(iRetainedEarnAcct, "*")

	if pos == 0 {

		qr := bq.Get(`SELECT GLAcctKey FROM tglAccount WHERE CompanyID=? AND GLAcctNo = ?;`, iCompanyID, iRetainedEarnAcct)
		if !qr.HasData {
			qr2 := bq.Get(`SELECT CompanyID FROM tglRetEarnAcctWrk
							   WHERE SessionID = ?
									AND CompanyID = ?
									AND RetEarnGLAcctNo = ?;`, lSessionID, iCompanyID, iRetainedEarnAcct)
			if !qr2.HasData {
				rq := bq.Set(`INSERT INTO tglRetEarnAcctWrk (SessionID, CompanyID, RetEarnGLAcctNo)
									VALUES (?, ?, ?);`, lSessionID, iCompanyID, iRetainedEarnAcct)
				if rq.HasAffectedRows {
					return 0, lSessionID
				}
			}
		}

	}

	qr := bq.Get(`SELECT a.GLAcctNo
					FROM   tglAccount a,
						tglNaturalAcct b,
						tglAcctType c,
						tglAcctCategory d
					WHERE  a.CompanyID = ?
						AND a.NaturalAcctKey = b.NaturalAcctKey
						AND b.AcctTypeKey = c.AcctTypeKey
						AND c.AcctCategoryKey = d.AcctCategoryKey
						AND d.AcctCatID IN ( 4, 5, 6, 7, 8, 9 );`, iCompanyID)

	for _, v := range qr.Data {

		lGLAcctNo := v.ValueStringOrd(0)
		lGLAcctNo = sm.SubstAcct(lGLAcctNo, iRetainedEarnAcct)

		qr2 := bq.Get(`SELECT GLAcctKey FROM tglAccount
					   WHERE CompanyID = ?
							AND GLAcctNo = ?;`, iCompanyID, lGLAcctNo)
		if !qr2.HasData {

			qr2 := bq.Get(`SELECT CompanyID FROM tglRetEarnAcctWrk
							WHERE  SessionID = ?
								AND CompanyID =?
								AND RetEarnGLAcctNo = ?;`, lSessionID, iCompanyID, lGLAcctNo)

			if !qr2.HasData {

				rq := bq.Set(`INSERT INTO tglRetEarnAcctWrk (SessionID, CompanyID, RetEarnGLAcctNo)
								VALUES (?, ?, ?);`, lSessionID, iCompanyID, lGLAcctNo)

				if rq.HasAffectedRows {
					return 1, lSessionID
				}

			}
		}
	}

	return 2, iSessionID
}
