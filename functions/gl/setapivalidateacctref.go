package gl

import (
	"gosqljobs/invtcommit/functions/constants"
	"gosqljobs/invtcommit/functions/sm"
	"time"

	du "github.com/eaglebush/datautils"
)

// SetAPIValidateAcctRef - Validates Account Reference Codes to be Posted to GL.
//
// This stored procedure takes a set of Account Reference Codes from a
// temporary table called #tglValidateAcct and validates them in the same
// way that the spglAPIAcctRef sp validates Account Reference Codes one at
// a time.  This sp replaces the spglAPIAcctRef sp which only operated on
// one row at a time (one Account Reference Code) and was called repetitively
// by the spglAPIAcctPostRow sp in the subsidiary modules.
// This new sp will only be called once by the spglSetAPIValidateAcct sp.
//
// This stored procedure ASSUMES:
//       (1)  The existence of a temporary table called #tglValidateAcct.
//       (2)  That #tglValidateAcct has been correctly populated with n rows
//            of distinct combinations of GLAcctKey+AcctRefKey+CurrID.
//       (3)  That all GLAcctKey's in #tglValidateAcct are only for @iCompanyID.
//       (4)  That if a @iVerifyParams value other than one (1) is passed in,
//            all parameter values in the NOTE below are guaranteed to be valid.
//       (5)  The calling program is NOT relying on Account Reference Codes to
//            be created if AcctRefUsage is set to '2' in tglOptions.  No Account
//            Reference Codes are created when this sp is used for validation.
//
// Use this sp with other Acuity API's that begin with spglSetAPI...
//
// Input Parameters:
//    @iCompanyID        = [IN: Valid Acuity Company; No Default]
//    @iBatchKey         = [IN: Valid Batch Key or NULL; Default = NULL]
//    @ioSessionID     = [IN/OUT: Valid No. or NULL; No Default]
//    @iUserID           = [IN: Valid User or NULL; Default = spGetLoginName]
//    @iLanguageID       = [IN: Valid Language ID or NULL; Default = NULL]
//    @iAcctRefUsage     = [IN: 0, 1 or NULL; Default = 0]
//    @iEffectiveDate    = [IN: Effective Date or NULL]
//    @iVerifyParams     = [IN: 0, 1 or NULL; Default = 1]
//
// NOTE: The following parameters MUST be passed in with a valid value from the
// calling stored procedure IF the @iVerifyParams parameter is passed in
// with a value of anything OTHER THAN one (1):
//    @iCompanyID
//    @ioSessionID
//    @iUserID
//    @iLanguageID
//    @iAcctRefUsage
//
// Output Parameters:
//    @ioSessionID = [IN/OUT: Valid No. or NULL; No Default]
//    @oSeverity     = [OUT: 0=None, 1=Warning, 2=Fatal; Default=0]
//    @oRetVal       = [OUT: return flag indicating outcome of the procedure]
//           0 = Failure.  General SP Failure.
//           1 = Successful.
//          19 = Failure.  Error Log Key not supplied and cannot be derived.
//          20 = Failure.  Company ID not supplied.
//          21 = Failure.  Company ID supplied does not exist.
//          24 = Failure.  GL Options row for this Company does not exist.
//          27 = Failure.  Account Reference Key exists but not for the correct Company.
//          30 = Failure.  Account Reference Key supplied does not exist.
//          32 = Failure.  Failure of Account Reference Code Effective Dates Restriction.
//          33 = Failure.  User ID not supplied and cannot be derived.
//          34 = Failure.  Language ID cannot be determined.
//          37 = Failure.  Account Reference Code is not active.
//          42 = Failure.  tglOptions.AcctRefUsage Flag not enabled.
//          43 = Failure.  GL Account requires an Account Reference Code.
func SetAPIValidateAcctRef(bq *du.BatchQuery,
	iCompanyID string,
	iBatchKey int,
	iSessionID int,
	iUserID string,
	iLanguageID int,
	iAcctRefUsage int,
	iEffectiveDate *time.Time,
	iVerifyParams bool) (Result constants.ResultConstant, Severity int, SessionID int) {

	var qr du.QueryResult

	bq.ScopeName("SetAPIValidateAcctRef")

	createAPIValidationTempTables(bq)

	lLanguageID := iLanguageID
	lAcctRefUsage := iAcctRefUsage

	if iVerifyParams {

		if iSessionID == 0 {
			iSessionID = sm.GetNextSurrogateKey(bq, "tciErrorLog")
			if iSessionID == 0 {
				return constants.ResultConstant(19), 2, 0
			}
		}

		if iUserID != "" {
			qr = bq.Get(`SELECT MIN(LanguageID)	FROM tsmUser WITH (NOLOCK) WHERE UserID=?;`, iUserID)
			if qr.HasData {
				lLanguageID = int(qr.First().ValueInt64Ord(0))
			}

			if lLanguageID == 0 {
				return constants.ResultConstant(34), 2, 0
			}
		}

		if iCompanyID == "" {
			sm.LogError(bq, iBatchKey, 0, 19101, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return constants.ResultConstant(20), 2, 0
		}

		// CompanyID must be valid (Get CurrID in the process)
		qr = bq.Get(`SELECT CompanyName FROM tsmCompany WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
		if !qr.HasData {
			sm.LogError(bq, iBatchKey, 0, 19102, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return constants.ResultConstant(21), 2, 0
		}

		// Get the GL Options information. (Just check if this information exists on the company)
		qr = bq.Get(`SELECT AcctRefUsage FROM tglOptions WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
		if !qr.HasData {
			sm.LogError(bq, iBatchKey, 0, 19105, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return constants.ResultConstant(24), 2, 0
		}
		lAcctRefUsage = int(qr.First().ValueInt64Ord(0))

		if lAcctRefUsage == 0 {
			sm.LogError(bq, iBatchKey, 0, 19230, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return constants.ResultConstant(42), 2, 0
		}
	}

	lErrorsOccurred := false
	lValidateAcctRetVal := constants.ResultError
	oSeverity := 0

	const lAcctRefKeyReqd int = 19235
	const lAcctRefExist int = 19221
	const lAcctRefCo int = 19222
	const lAcctRefInactive int = 19227
	const lAcctRefStart int = 19224
	const lAcctRefEnd int = 19225

	// Validate the required Account Reference ID's in #tglValidateAcct now
	// This validation only applies when @lAcctRefUsage = 1 [Validated ARC's]
	if lAcctRefUsage == 0 {
		qr = bq.Set(`UPDATE #tglValidateAcct
					SET ValidationRetVal = 43,
						ErrorMsgNo = ?
					WHERE GLAcctKey IN (SELECT GLAcctKey 
										FROM tglAccount a WITH (NOLOCK),
											tglNaturalAcct b WITH (NOLOCK)
										WHERE a.NaturalAcctKey = b.NaturalAcctKey
										AND b.ReqAcctRefCode = 1)
						AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) = 0
						AND ValidationRetVal = 0;`, lAcctRefKeyReqd)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 43
			oSeverity = constants.FatalError

			bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

			bq.Set(`INSERT INTO #tglAcctMask
					SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
						c.FormattedGLAcctNo  /* MaskedGLAcctNo */
					FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
						AND a.ValidationRetVal = 43
						AND a.ErrorMsgNo = ?;`, iCompanyID, lAcctRefKeyReqd)

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), AcctRefKey), '', '', '', '', ?
					FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 30
						AND ErrorMsgNo=?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lAcctRefExist, lAcctRefExist)
		}
	}

	// Do all the Reference Keys exist? This validation applies when @lAcctRefUsage = 1 or 2
	if lAcctRefUsage == 1 || lAcctRefUsage == 2 {

		// Validating that the Account Reference Keys exist in tglAcctRef
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 30, ErrorMsgNo = ?
					 WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
											 FROM tglAcctRef WITH (NOLOCK))
											 AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
						AND ValidationRetVal = 0;`, lAcctRefExist)
		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 30
			oSeverity = constants.FatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), AcctRefKey), '', '', '', '', ?
					FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 30
						AND ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lAcctRefExist, lAcctRefExist)

			goto FinishFunc
		}

		// Validating that all Account Reference Keys are for the correct Company
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 27,	ErrorMsgNo = ?
					 WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
											FROM tglAcctRef WITH (NOLOCK)
											WHERE CompanyID = ?)
					 AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
					 AND ValidationRetVal=0;`, lAcctRefCo, iCompanyID)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 27
			oSeverity = constants.FatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), b.AcctRefCode), ?, '', '', '', ?
					FROM #tglValidateAcct a WITH (NOLOCK), tglAcctRef b WITH (NOLOCK)
					WHERE a.AcctRefKey = b.AcctRefKey
						AND a.ValidationRetVal = 27
						AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iCompanyID, lAcctRefCo, lAcctRefCo)
		}
	}

	if lAcctRefUsage == 1 {

		// Validating that the Account Reference Keys have an active status
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 37, ErrorMsgNo = ?
					 WHERE AcctRefKey NOT IN (SELECT AcctRefKey 
											FROM tglAcctRef WITH (NOLOCK)
										WHERE CompanyID =?
						AND Status = 1)
						AND COALESCE(DATALENGTH(LTRIM(RTRIM(AcctRefKey))), 0) > 0
						AND ValidationRetVal = 0;`, lAcctRefInactive, iCompanyID)
		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 37
			oSeverity = constants.FatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT a.GLAcctKey, ?,?,?, CONVERT(VARCHAR(30), b.AcctRefCode), '', '', '', '', ?
					FROM #tglValidateAcct a WITH (NOLOCK), tglAcctRef b WITH (NOLOCK)
					WHERE a.AcctRefKey = b.AcctRefKey
						AND a.ValidationRetVal = 37
						AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lAcctRefInactive, lAcctRefInactive)
		}

		// Reference Code Effective Date Restrictions
		if iEffectiveDate != nil {

			// Validating that there are no ARC effective start date violations
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 32, ErrorMsgNo = ?
						WHERE AcctRefKey IN (SELECT AcctRefKey 
											FROM tglAcctRef WITH (NOLOCK) 
											WHERE CompanyID = ? AND Status = 1 AND EffStartDate IS NOT NULL AND EffStartDate > '?')
						AND ValidationRetVal = 0;`, lAcctRefStart, iCompanyID, iEffectiveDate.Format(`2006-01-02`))

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 32
				oSeverity = constants.FatalError

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, '?', CONVERT(VARCHAR(30), b.AcctRefCode), b.EffStartDate, '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAcctRef b WITH (NOLOCK)
						WHERE a.AcctRefKey = b.AcctRefKey
							AND a.ValidationRetVal = 32
							AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iEffectiveDate.Format(`2006-01-02`), lAcctRefStart)

			}

			// Validating that there are no ARC effective end date violations
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 32, ErrorMsgNo = ?
						WHERE AcctRefKey IN (SELECT AcctRefKey 
											FROM tglAcctRef WITH (NOLOCK)
											WHERE CompanyID = ?
											AND Status = 1
											AND EffEndDate IS NOT NULL
											AND EffEndDate < '?')
						AND ValidationRetVal = 0;`, lAcctRefEnd, iCompanyID, iEffectiveDate.Format(`2006-01-02`))

			if qr.HasAffectedRows {
				lErrorsOccurred = true
				lValidateAcctRetVal = 32
				oSeverity = constants.FatalError

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,	?, ?, ?, '?', CONVERT(VARCHAR(30), b.AcctRefCode), b.EffEndDate, '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAcctRef b WITH (NOLOCK)
						WHERE a.AcctRefKey = b.AcctRefKey
							AND a.ValidationRetVal = 32
							AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iEffectiveDate.Format(`2006-01-02`), lAcctRefEnd, lAcctRefEnd)
			}
		}
	}

FinishFunc:

	if lErrorsOccurred {

		bq.Set(`TRUNCATE TABLE #tciError;`)

		bq.Set(`INSERT INTO #tciError
					(BatchKey,        StringNo,        ErrorType,       Severity, 
					StringData1,     StringData2,     StringData3,     StringData4, 
					StringData5,     TranType,        TranKey,         InvtTranKey)
				SELECT
					tmp.BatchKey,    tmp.StringNo,    tmp.ErrorType,   tmp.Severity, 
					tmp.StringData1, tmp.StringData2, tmp.StringData3, tmp.StringData4, 
					tmp.StringData5, gl.TranType,     NULL,            gl.TranKey
				FROM #tciErrorStg tmp
					JOIN #tglPosting gl ON tmp.GLAcctKey = gl.GLAcctKey;`)

		sm.LogErrors(bq, iBatchKey, iSessionID)
	}

	if lValidateAcctRetVal == 0 {
		lValidateAcctRetVal = constants.ResultSuccess
	}

	return lValidateAcctRetVal, oSeverity, iSessionID
}
