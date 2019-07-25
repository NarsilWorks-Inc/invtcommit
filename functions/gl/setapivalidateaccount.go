package gl

import (
	"gosqljobs/invtcommit/functions/constants"
	"gosqljobs/invtcommit/functions/sm"
	"time"

	du "github.com/eaglebush/datautils"
)

// SetAPIValidateAccount -Validates GL Accounts to be Posted to GL Module.
//
// This stored procedure takes a set of GL accounts from a temporary
// table called #tglValidateAcct and validates them in the same way that
// the spglAPIValidateAccount sp validates GL accounts one at a time.
// This sp replaces the spglAPIValidateAccount sp which only operated on
// one row at a time (one GL Account) and was called repetitively by the
// spglAPIAcctPostRow sp in the subsidiary modules.
// This new sp will only be called once by the spglSetAPIAcctPostRow sp.
//
// This stored procedure ASSUMES:
//       (1)  The existence of a temporary table called #tglValidateAcct.
//       (2)  That #tglValidateAcct has been correctly populated with n rows
//            of distinct combinations of GLAcctKey+AcctRefKey+CurrID.
//       (3)  That all GLAcctKey's in #tglValidateAcct are only for @iCompanyID.
//       (4)  That if a @iVerifyParams value other than one (1) is passed in,
//            all parameter values in the NOTE below are guaranteed to be valid.
//       (5)  The calling program is NOT relying on GL Accounts to be created
//            if the AutoAcctAdd option is ON in tglOptions.  No GL Accounts
//            are created when this sp is used for validation.
//       (6)  The calling program is NOT relying on Account Reference Codes to
//            be created if AcctRefUsage is set to '2' in tglOptions.  No Account
//            Reference Codes are created when this sp is used for validation.
// Use this sp with other Acuity API's that begin with spglSetAPI...
//
// Input Parameters:
//    @iCompanyID        = [IN: Valid Acuity Company; No Default]
//    @iBatchKey         = [IN: Valid Batch Key or NULL; Default = NULL]
//    @ioSessionID     = [IN/OUT: Valid No. or NULL; No Default]
//    @iUserID           = [IN: Valid User or NULL; Default = spGetLoginName]
//    @iLanguageID       = [IN: Valid Language ID or NULL; Default = NULL]
//    @iHomeCurrID       = [IN: Valid Curr ID for @iCompanyID or NULL; Default = NULL]
//    @iIsCurrIDUsed     = [IN: 0, 1 or NULL; Default = 0]
//    @iAutoAcctAdd      = [IN: 0, 1 or NULL; Default = 0]
//    @iUseMultCurr      = [IN: 0, 1 or NULL; Default = 0]
//    @iGLAcctMask       = [IN: A Valid GL Account Mask or NULL; Default = NULL]
//    @iAcctRefUsage     = [IN: 0, 1 or NULL; Default = 0]
//    @iAllowWildCard    = [IN: 0, 1 or NULL; Default = 0]
//    @iAllowActiveOnly  = [IN: 0, 1 or NULL; Default = 1]
//    @iFinancial        = [IN: 0, 1 or NULL; Default = NULL]
//    @iPostTypeFlag     = [IN: 0, 1 or NULL; Default = 1]
//    @iPostingType      = [IN: 1, 2, 3 or NULL; Default = 3]
//    @iEffectiveDate    = [IN: Effective Date or NULL]
//    @iVerifyParams     = [IN: 0, 1 or NULL; Default = 1]
//    @iValidateGLAccts  = [IN: 0, 1 or NULL; Default = 1]
//    @iValidateAcctRefs = [IN: 0, 1 or NULL; Default = 1]
//    @iValidateCurrIDs  = [IN: 0, 1 or NULL; Default = 1]
//
// NOTE: The following parameters MUST be passed in with a valid value from the
// calling stored procedure IF the @iVerifyParams parameter is passed in
// with a value of anything OTHER THAN one (1):
//    @iCompanyID
//    @ioSessionID
//    @iUserID
//    @iLanguageID
//    @iHomeCurrID
//    @iIsCurrIDUsed
//    @iAutoAcctAdd
//    @iUseMultCurr
//    @iGLAcctMask
//    @iAcctRefUsage
//
// Output Parameters:
//    @ioSessionID = [IN/OUT: Valid No. or NULL; No Default]
//    @oSeverity     = [OUT: 0=None, 1=Warning, 2=Fatal; Default=0]
//    @oRetVal       = [OUT: return flag indicating outcome of the procedure]
//           0 = Failure.  General SP Failure.
//           1 = Successful.
//           4 = Failure.  Masked GL account not allowed.
//           9 = Failure.  Account doesn't exist not for the Company supplied (or at all).
//          10 = Failure.  Account Key supplied does not exist.
//          12 = Failure.  Account exists.  Failure of Active Account Restriction.
//          13 = Failure.  Account exists.  Failure of Effective Dates Restriction.
//          14 = Warning Only.  Account exists.  Failure of Home Currency Only Restriction.
//          15 = Warning Only.  Account exists.  Failure of Specific Currency = Home Currency Restriction.
//          16 = Failure.  Account Exists.  Failure of Currency not Specific Currency Restriction.
//          17 = Failure.  Account exists.  Failure of Financial Type Restriction.
//          19 = Failure.  Error Log Key not supplied and cannot be derived.
//          20 = Failure.  Company ID not supplied.
//          21 = Failure.  Company ID supplied does not exist or has no Home Currency ID.
//          23 = Failure.  Currency ID for this Company exists but is not used in MC.
//          24 = Failure.  GL Options row for this Company does not exist.
//          25 = Failure.  Currency ID for this Company does not exist in MC.
//          26 = Failure.  Multicurrency is not enabled for entered Company.
//          27 = Failure.  Account Reference Key exists but not for the correct Company.
//          30 = Failure.  Account Reference Key supplied does not exist.
//          31 = Failure.  Failure of Account Reference Code Account Segments Restriction.
//          32 = Failure.  Failure of Account Reference Code Effective Dates Restriction.
//          33 = Failure.  User ID not supplied and cannot be derived.
//          34 = Failure.  Language ID cannot be determined.
//          37 = Failure.  Account Reference Code is not active.
//          38 = Failure.  Accounts exists.  Failure of Posting Type Restriction.
//          42 = Failure.  tglOptions.AcctRefUsage Flag not enabled.
//          43 = Failure.  GL Account requires an Account Reference Code.
func SetAPIValidateAccount(
	bq *du.BatchQuery,
	iCompanyID string,
	iBatchKey int,
	iSessionID int,
	iUserID string,
	iLanguageID int,
	iHomeCurrID string,
	iIsCurrIDUsed bool,
	iAutoAcctAdd bool,
	iUseMultCurr bool,
	iGLAcctMask string,
	iAcctRefUsage int,
	iAllowWildCard bool,
	iAllowActiveOnly bool,
	iFinancial int,
	iPostTypeFlag int,
	iPostingType int,
	iEffectiveDate *time.Time,
	iVerifyParams bool,
	iValidateGLAccts bool,
	iValidateAcctRefs bool,
	iValidateCurrIDs bool) (Result constants.ResultConstant, Severity int, SessionID int) {

	var qr du.QueryResult
	var lErrMsgNo int

	bq.ScopeName("SetAPIValidateAccount")

	createAPIValidationTempTables(bq)

	switch iFinancial {
	case 0, 1:
		break
	default:
		iFinancial = -1 // Default
	}

	if iPostTypeFlag != -1 {
		switch iPostTypeFlag {
		case 0, 1:
			break
		default:
			iPostTypeFlag = 1 // Default
		}
	}

	switch iPostingType {
	case 1, 2, 3:
		break
	default:
		iPostingType = 3
	}

	const lInvalidCurr int = 19103
	const lNotUsedCurr int = 19104

	lLanguageID := iLanguageID
	lIsCurrIDUsed := iIsCurrIDUsed
	lAcctRefUsage := iAcctRefUsage
	lAutoAcctAdd := iAutoAcctAdd
	lUseMultCurr := iUseMultCurr
	lGLAcctMask := iGLAcctMask
	lHomeCurrID := iHomeCurrID

	if iVerifyParams {

		if iSessionID == 0 {
			iSessionID = sm.GetNextSurrogateKey(bq, "tciErrorLog")
			if iSessionID == 0 {
				return constants.ResultConstant(19), 2, 0
			}
		}

		if iUserID == "" {
			return constants.ResultConstant(33), 2, 0
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

		qr = bq.Get(`SELECT CurrID FROM tsmCompany WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
		if !qr.HasData {
			sm.LogError(bq, iBatchKey, 0, 19102, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return constants.ResultConstant(21), 2, 0
		}

		// Does the Home Currency Exist?
		qr = bq.Get(`SELECT IsUsed FROM tmcCurrency WITH (NOLOCK) WHERE CurrID=?;`, iHomeCurrID)
		if !qr.HasData {
			sm.LogError(bq, iBatchKey, 0, lInvalidCurr, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return constants.ResultConstant(25), 2, 0
		}
		lIsCurrIDUsed = qr.First().ValueInt64Ord(0) == 1

		if !lIsCurrIDUsed {
			sm.LogError(bq, iBatchKey, 0, lNotUsedCurr, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return constants.ResultConstant(23), 2, 0
		}

		// Get the GL Options information. (Just check if this information exists on the company)
		qr = bq.Get(`SELECT  AutoAcctAdd, UseMultCurr, AcctMask, AcctRefUsage FROM tglOptions WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
		if !qr.HasData {
			sm.LogError(bq, iBatchKey, 0, 19105, iCompanyID, ``, ``, ``, ``, 3, 2, iSessionID, 0, 0, 0, 0)
			return constants.ResultConstant(24), 2, 0
		}
		lAutoAcctAdd = qr.First().ValueInt64Ord(0) == 1
		lUseMultCurr = qr.First().ValueInt64Ord(1) == 1
		lGLAcctMask = qr.First().ValueStringOrd(2)
		lAcctRefUsage = int(qr.First().ValueInt64Ord(3))
	}

	// Validate the GL accounts in #tglValidateAcct now
	lErrorsOccurred := false
	lValidateAcctRetVal := constants.ResultError
	lValidateAcctRefRetVal := constants.ResultError
	lValidateAcctRefSeverity := 0
	lMaxAccountSegments := 0
	lAcctRefValFail := 0
	oSeverity := 0

	const lMissingAcctKey int = 19200
	const lInvalidAcctCo int = 19214
	const lMaskedGLAcct int = 19202
	const lInactiveGLAcct int = 19206
	const lDeletedGLAcct int = 1921
	const lNonFinlGLAcct int = 19207
	const lFinlGLAcct int = 19208
	const lFinlPostType int = 19240
	const lStatPostType int = 19241
	const lGLAcctStartDateError int = 19212
	const lGLAcctEndDateError int = 19213
	const lAcctRefSegs int = 19223
	const lMultCurrError int = 19112
	const lInvalidHomeCurr int = 19210
	const lCurrIsHomeCurr int = 19210
	const lNotSpecificCurrency int = 19216
	const lConvertToMMDDYYYYDate int = 101

	if iValidateGLAccts {

		/* -------------- Make sure all GL accounts exist in tglAccount -------------- */
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal=25, ErrorMsgNo = @lInvalidCurr
					 WHERE CurrID NOT IN (SELECT CurrID 
										 FROM tmcCurrency WITH (NOLOCK))
										 AND COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
						AND ValidationRetVal=0;`)
		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 10
			oSeverity = constants.FatalError

			bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
					SELECT GLAcctKey, ?, ?,	?, GLAcctKey, '',  '', '', '', ?
					FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 10
						AND ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lMissingAcctKey, lMissingAcctKey)
			goto FinishFunc
		}

		/* -------------- Make sure all GL accounts exist in tglAccount for this Company. -------------- */
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 9, ErrorMsgNo=?
					 WHERE GLAcctKey NOT IN (SELECT GLAcctKey FROM tglAccount WITH (NOLOCK)	WHERE CompanyID = ?)
						 AND ValidationRetVal = 0;`, lInvalidAcctCo, iCompanyID)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 9
			oSeverity = constants.FatalError

			bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

			// Format the GL accounts used in the error message
			bq.Set(`INSERT INTO #tglAcctMask
					SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
					FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID=?
						AND a.ValidationRetVal = 9
						AND a.ErrorMsgNo=?;`, iCompanyID, lInvalidAcctCo)

			// Populate the temporary error log
			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), ?, '', '', '',	?
					FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
					WHERE a.GLAcctKey = b.GLAcctKey
						AND b.GLAcctNo = c.GLAcctNo
						AND a.ValidationRetVal = 9
						AND a.ErrorMsgNo=?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iCompanyID, lInvalidAcctCo, lInvalidAcctCo)
		}

		/* -------------- Check for Mask Characters in the GL Account Number -------------- */
		if iAllowWildCard {
			qr = bq.Set(`UPDATE #tglValidateAcct
							SET ValidationRetVal = 4, ErrorMsgNo=?
						WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE CHARINDEX('*', GLAcctNo) > 0)
							AND ValidationRetVal=0;`, lMaskedGLAcct)
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 4
				oSeverity = constants.FatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo,	c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK),	vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID=?
							AND a.ValidationRetVal = 4
							AND a.ErrorMsgNo=?;`, iCompanyID, lInvalidAcctCo)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), '', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 4
							AND a.ErrorMsgNo=?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lMaskedGLAcct, lMaskedGLAcct)
			}
		}

		/* -------------- Active Account Validation -------------- */
		if iAllowActiveOnly {
			qr = bq.Set(`UPDATE #tglValidateAcct
							SET ValidationRetVal = 12, ErrorMsgNo=?
						 WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE Status = 2)
							AND ValidationRetVal=0;`, lInactiveGLAcct)
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 12
				oSeverity = constants.FatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo 
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID=?
							AND a.ValidationRetVal = 12
							AND a.ErrorMsgNo = ?;`, iCompanyID, lInactiveGLAcct)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), '', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 12
							AND a.ErrorMsgNo=?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lInactiveGLAcct, lInactiveGLAcct)

			}

			// check for deleted GL accounts
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 12, ErrorMsgNo = ?
						WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE Status = 3)
						AND ValidationRetVal = 0;`, lDeletedGLAcct)
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 12
				oSeverity = constants.FatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo,	c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 12
							AND a.ErrorMsgNo = ?;`, iCompanyID, lDeletedGLAcct)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), '', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK),	#tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 12
							AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lDeletedGLAcct, lDeletedGLAcct)
			}
		}

		/* -------------- Financial Account Restriction -------------- */
		if iFinancial != -1 {

			iFinGL := 0
			iFinExpression := ``

			/* Allow Financial Accounts Only */
			if iFinancial == 1 {
				iFinGL = lNonFinlGLAcct
				iFinExpression = `=`
			}

			/* Allow Non-Financial Accounts Only */
			if iFinancial == 0 {
				iFinGL = lFinlGLAcct
				iFinExpression = `<>`
			}

			qr = bq.Set(`UPDATE #tglValidateAcct
							SET ValidationRetVal = 17, ErrorMsgNo = ?
							WHERE GLAcctKey IN (SELECT GLAcctKey 
												FROM tglAccount a WITH (NOLOCK),
														tglNaturalAcct b WITH (NOLOCK),
														tglAcctType c WITH (NOLOCK)
												WHERE a.NaturalAcctKey = b.NaturalAcctKey
												AND b.AcctTypeKey = c.AcctTypeKey
												AND a.CompanyID = ?
												AND c.AcctTypeID `+iFinExpression+` 901)
							AND ValidationRetVal = 0;`, iFinGL, iCompanyID)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 17
				oSeverity = constants.FatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo,	c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 17
							AND a.ErrorMsgNo = ?;`, iCompanyID, iFinGL)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,	?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), '', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 17
							AND a.ErrorMsgNo=?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iFinGL, iFinGL)
			}
		}

		/* -------------- Post Type Restriction  -------------- */
		if iPostTypeFlag != -1 {

			iPostTF := 0
			iPostExpression := ``

			/* Allow Financial Accounts Only */
			if iPostTypeFlag == 1 {
				iPostTF = lFinlPostType
				iPostExpression = `(1,3)`
			}

			/* Allow Non-Financial Accounts Only */
			if iPostTypeFlag == 0 {
				iPostTF = lStatPostType
				iPostExpression = `(2,3)`
			}

			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 38, ErrorMsgNo = ?
						WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE CompanyID = @iCompanyID
												AND Status = 1
												AND PostingType NOT IN `+iPostExpression+`)
							AND ValidationRetVal=0;`, iPostTF)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 38
				oSeverity = constants.FatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, /* GLAcctNo */
							c.FormattedGLAcctNo  /* MaskedGLAcctNo */
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
							AND a.ValidationRetVal = 38
							AND a.ErrorMsgNo = ?;`, iCompanyID, iPostTF)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo),'', '', '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 38
							AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iPostTF, iPostTF)
			}
		}

		/* -------------- Effective Date Restrictions -------------- */
		if iEffectiveDate != nil {

			// Check Effective Start Date
			qr = bq.Set(`UPDATE #tglValidateAcct
						 SET ValidationRetVal = 13, ErrorMsgNo = ?
						 WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE CompanyID =?
												AND Status = 1
												AND EffStartDate IS NOT NULL
												AND EffStartDate > '?')
							AND ValidationRetVal = 0;`, lGLAcctStartDateError, iCompanyID, iEffectiveDate.Format(`2006-01-02`))

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 13
				oSeverity = constants.FatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 13
							AND a.ErrorMsgNo = ?;`, iCompanyID, lGLAcctStartDateError)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(10), '?', ?), 
								CONVERT(VARCHAR(30), c.MaskedGLAcctNo),  
								CONVERT(VARCHAR(10), b.EffStartDate, ?), '', '', ?
							FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
							WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 13
							AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iEffectiveDate.Format(`2006-01-02`), lConvertToMMDDYYYYDate,
					lConvertToMMDDYYYYDate, lGLAcctStartDateError, lGLAcctStartDateError)

			}

			// Check Effective End Date
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 13, ErrorMsgNo = ?
						WHERE GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount WITH (NOLOCK)
											WHERE CompanyID =?
												AND Status = 1
												AND EffEndDate IS NOT NULL
												AND EffEndDate < '?')
							AND ValidationRetVal = 0;`, lGLAcctEndDateError, iCompanyID, iEffectiveDate.Format(`2006-01-02`))
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 13
				oSeverity = constants.FatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo,	c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 13
							AND a.ErrorMsgNo = ?;`, iCompanyID, lGLAcctEndDateError)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?,  CONVERT(VARCHAR(10), '?', ?),
							CONVERT(VARCHAR(30), c.MaskedGLAcctNo), 
							CONVERT(VARCHAR(10), b.EffEndDate, ?), '', '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 13
							AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iEffectiveDate.Format(`2006-01-02`), lConvertToMMDDYYYYDate,
					lConvertToMMDDYYYYDate, lGLAcctEndDateError, lGLAcctEndDateError)
			}
		}
	}

	// Validate the Account Reference ID's in #tglValidateAcct now
	if iValidateAcctRefs && iAcctRefUsage != 0 {

		lValidateAcctRefRetVal, lValidateAcctRefSeverity, iSessionID = SetAPIValidateAcctRef(bq, iCompanyID, iBatchKey, iSessionID, iUserID, iLanguageID, iAcctRefUsage, iEffectiveDate, false)

		/* Did the Account Reference Code validation go OK? */
		switch lValidateAcctRefRetVal {
		case 19, 20, 21, 23, 24, 25, 30, 33, 34:
			lAcctRefValFail = 1
			lValidateAcctRetVal = lValidateAcctRefRetVal

			goto FinishFunc
		}

		if lValidateAcctRefRetVal != 0 || lValidateAcctRefRetVal != 1 {
			lValidateAcctRetVal = lValidateAcctRefRetVal
		}

		if lValidateAcctRefSeverity > 0 && oSeverity != 2 {
			oSeverity = lValidateAcctRefSeverity
		}

		// Verify that Account Reference Codes are valid for all Account Segments
		if lAcctRefUsage == 1 {

			qr = bq.Get(`SELECT COUNT(SegmentKey) FROM tglSegment WITH (NOLOCK) WHERE CompanyID=?;`, iCompanyID)
			lMaxAccountSegments = int(qr.First().ValueInt64Ord(0))

			if lMaxAccountSegments > 0 {

				// Validating that the ARCs are valid for all Account Segments
				qr = bq.Set(`UPDATE #tglValidateAcct
							SET ValidationRetVal = 31, ErrorMsgNo = ?
							WHERE AcctRefKey NOT IN
							(SELECT c.AcctRefKey
								FROM tglAcctSegment a WITH (NOLOCK),
										tglAcctRefUsage b WITH (NOLOCK),
										tglAcctRef c WITH (NOLOCK),
										(SELECT DISTINCT AcctRefKey, GLAcctKey 
											FROM #tglValidateAcct WITH (NOLOCK)) d
											WHERE a.SegmentKey = b.SegmentKey
												AND a.AcctSegValue = b.AcctSegValue
												AND b.AcctRefGroupKey = c.AcctRefGroupKey
												AND d.GLAcctKey = a.GLAcctKey
												AND d.AcctRefKey = c.AcctRefKey
											GROUP BY d.GLAcctKey, c.AcctRefKey
											HAVING COUNT(c.AcctRefKey) = ?)
							AND COALESCE(DATALENGTH(AcctRefKey), 0) > 0
							AND ValidationRetVal = 0;`, lAcctRefSegs, lMaxAccountSegments)

				if qr.HasAffectedRows {

					lErrorsOccurred = true
					lValidateAcctRetVal = 31
					oSeverity = constants.FatalError

					bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

					bq.Set(`INSERT INTO #tglAcctMask
							SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
							FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
							WHERE a.GLAcctKey = b.GLAcctKey
								AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
								AND a.ValidationRetVal = 31
								AND a.ErrorMsgNo = ?;`, iCompanyID, lAcctRefSegs)

					bq.Set(`INSERT INTO #tciErrorStg (
								GLAcctKey,   BatchKey,    ErrorType,   Severity, 
								StringData1, StringData2, StringData3, StringData4, 
								StringData5, StringNo)
							SELECT a.GLAcctKey,	?, ?, ?, CONVERT(VARCHAR(30), c.AcctRefCode), CONVERT(VARCHAR(30), d.MaskedGLAcctNo), '', '', '', ?
							FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), tglAcctRef c WITH (NOLOCK), #tglAcctMask d WITH (NOLOCK)
							WHERE a.GLAcctKey = b.GLAcctKey
								AND a.AcctRefKey = c.AcctRefKey
								AND b.GLAcctNo = d.GLAcctNo
								AND a.ValidationRetVal = 31
								AND a.ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lAcctRefSegs, lAcctRefSegs)
				}
			}
		}
	}

	// Validate the Currency ID's in #tglValidateAcct now
	if iValidateCurrIDs {

		// Validating that Currency IDs exist in tmcCurrency
		qr = bq.Set(`UPDATE #tglValidateAcct
					 SET ValidationRetVal = 25,	ErrorMsgNo = ?
					 WHERE CurrID NOT IN (SELECT CurrID 
										FROM tmcCurrency WITH (NOLOCK))
										AND COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
						AND ValidationRetVal = 0;`, lInvalidCurr)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 25
			oSeverity = constants.FatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT GLAcctKey, ?, ?, ?, CurrID, '', '', '', '', FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 25 AND ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lInvalidCurr, lInvalidCurr)

			goto FinishFunc

		}

		// Validating that Currency IDs are used in tmcCurrency
		qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 23, ErrorMsgNo = ?
						WHERE CurrID IN (SELECT CurrID FROM tmcCurrency WITH (NOLOCK) WHERE IsUsed = 0)
						AND ValidationRetVal = 0;`, lNotUsedCurr)

		if qr.HasAffectedRows {

			lErrorsOccurred = true
			lValidateAcctRetVal = 23
			oSeverity = constants.FatalError

			bq.Set(`INSERT INTO #tciErrorStg (
						GLAcctKey,   BatchKey,    ErrorType,   Severity, 
						StringData1, StringData2, StringData3, StringData4, 
						StringData5, StringNo)
					SELECT GLAcctKey, ?, ?, ?, ?, CurrID, '', '', '', '', ?
					FROM #tglValidateAcct WITH (NOLOCK)
					WHERE ValidationRetVal = 23 AND ErrorMsgNo=?;`, iBatchKey, constants.InterfaceError, constants.FatalError, lNotUsedCurr, lNotUsedCurr)
		}

		//Make sure CurrID's are Home Currency IF Multicurrency is NOT used.
		if !lUseMultCurr {

			// Validating that Curr IDs are Home Curr IDs (No MC)
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 26, ErrorMsgNo=?
						WHERE CurrID <> ? AND ValidationRetVal = 0;`, lMultCurrError, lHomeCurrID)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 26
				oSeverity = constants.FatalError

				bq.Set(`INSERT #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT GLAcctKey, ?, ?, ?, ?, '', '', '', '', ?
						FROM #tglValidateAcct WITH (NOLOCK)
						WHERE ValidationRetVal = 26 AND ErrorMsgNo = ?;`, iBatchKey, constants.InterfaceError, constants.FatalError, iCompanyID, lMultCurrError, lMultCurrError)

			}

		}

		// Multicurrency Restriction
		if lUseMultCurr {

			// Validating that GL Accounts don't violate Home Curr Only restriction
			qr = bq.Set(`UPDATE #tglValidateAcct
						SET ValidationRetVal = 14, ErrorMsgNo = ?
						WHERE COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
						AND CurrID <> ?
						AND GLAcctKey IN (SELECT GLAcctKey 
											FROM tglAccount a WITH (NOLOCK),
												tglNaturalAcct b WITH (NOLOCK),
												tglAcctType c WITH (NOLOCK)
											WHERE a.NaturalAcctKey = b.NaturalAcctKey
											AND b.AcctTypeKey = c.AcctTypeKey
											AND a.CompanyID = ?
											AND a.CurrRestriction = 0
											AND c.AcctTypeID <> 901)
						AND ValidationRetVal = 0;`, lInvalidHomeCurr, lHomeCurrID, iCompanyID)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 14
				if oSeverity != constants.FatalError {
					oSeverity = constants.Warning
				}

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey	AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = ?
							AND a.ValidationRetVal = 14
							AND a.ErrorMsgNo = ?;`, iCompanyID, lInvalidHomeCurr)

				bq.Set(`INSERT #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey, ?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), a.CurrID, 'Specific Foreign Curr',	b.RestrictedCurrID,	a.CurrID, ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 15
							AND a.ErrorMsgNo=?;`, iBatchKey, constants.InterfaceError, constants.Warning, lCurrIsHomeCurr, lCurrIsHomeCurr)
			}

			// Specific Foreign Currency Restriction #1 (Check Financial Accounts Only)
			// Validating that GL Accounts don't violate Specific Foreign Curr restriction (#1)
			qr = bq.Set(`UPDATE #tglValidateAcct
						 SET ValidationRetVal = 15, ErrorMsgNo = ?
						 WHERE COALESCE(DATALENGTH(LTRIM(RTRIM(CurrID))), 0) > 0
							AND CurrID = ?
							AND GLAcctKey IN (SELECT GLAcctKey 
												FROM tglAccount a WITH (NOLOCK), tglNaturalAcct b WITH (NOLOCK), tglAcctType c WITH (NOLOCK)
												WHERE a.NaturalAcctKey = b.NaturalAcctKey
													AND b.AcctTypeKey = c.AcctTypeKey
													AND a.CompanyID = ?
													AND a.CurrRestriction = 1
													AND c.AcctTypeID <> 901)
							AND ValidationRetVal = 0;`, lCurrIsHomeCurr, lHomeCurrID, iCompanyID)
			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 15
				if oSeverity != constants.FatalError {
					oSeverity = constants.Warning
				}

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID = @iCompanyID
							AND a.ValidationRetVal = 15
							AND a.ErrorMsgNo=?;`, lCurrIsHomeCurr)

				bq.Set(`INSERT INTO #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,	?, ?, ?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), a.CurrID, 'Specific Foreign Curr', b.RestrictedCurrID, a.CurrID, ?
						FROM #tglValidateAcct a WITH (NOLOCK),	tglAccount b WITH (NOLOCK),	#tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 15
							AND a.ErrorMsgNo=?;`, iBatchKey, constants.InterfaceError, constants.Warning, lCurrIsHomeCurr, lCurrIsHomeCurr)
			}

			// Specific Foreign Currency Restriction #2 (Check Financial Accounts Only)
			// Validating that GL Accounts don't violate Specific Foreign Curr restriction (#2)
			qr = bq.Set(`UPDATE t
						SET #tglValidateAcct.ValidationRetVal = 16,	#tglValidateAcct.ErrorMsgNo=?
						FROM #tglValidateAcct t, tglAccount a WITH (NOLOCK), tglNaturalAcct b WITH (NOLOCK), tglAcctType c WITH (NOLOCK)
						WHERE t.GLAcctKey = a.GLAcctKey
							AND a.NaturalAcctKey = b.NaturalAcctKey
							AND b.AcctTypeKey = c.AcctTypeKey
							AND a.CompanyID = ?
							AND a.CurrRestriction = 1
							AND c.AcctTypeID <> 901
							AND COALESCE(DATALENGTH(LTRIM(RTRIM(t.CurrID))), 0) > 0
							AND t.CurrID <> ?
							AND t.CurrID <> a.RestrictedCurrID
							AND t.ValidationRetVal = 0;`, lNotSpecificCurrency, iCompanyID, lHomeCurrID)

			if qr.HasAffectedRows {

				lErrorsOccurred = true
				lValidateAcctRetVal = 16
				oSeverity = constants.FatalError

				bq.Set(`TRUNCATE TABLE #tglAcctMask;`)

				bq.Set(`INSERT INTO #tglAcctMask
						SELECT DISTINCT b.GLAcctNo, c.FormattedGLAcctNo
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), vFormattedGLAcct c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo AND c.CompanyID=?
							AND a.ValidationRetVal = 16
							AND a.ErrorMsgNo=?;`, iCompanyID, lNotSpecificCurrency)

				bq.Set(`INSERT #tciErrorStg (
							GLAcctKey,   BatchKey,    ErrorType,   Severity, 
							StringData1, StringData2, StringData3, StringData4, 
							StringData5, StringNo)
						SELECT a.GLAcctKey,?,?,?, CONVERT(VARCHAR(30), c.MaskedGLAcctNo), a.CurrID, 'Specific Foreign Curr', b.RestrictedCurrID, '', ?
						FROM #tglValidateAcct a WITH (NOLOCK), tglAccount b WITH (NOLOCK), #tglAcctMask c WITH (NOLOCK)
						WHERE a.GLAcctKey = b.GLAcctKey
							AND b.GLAcctNo = c.GLAcctNo
							AND a.ValidationRetVal = 16
							AND a.ErrorMsgNo = ?;`, lNotSpecificCurrency, lNotSpecificCurrency)

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
					JOIN #tglPosting gl ON tmp.GLAcctKey = gl.GLAcctKey`)

		sm.LogErrors(bq, iBatchKey, iSessionID)

		if lValidateAcctRetVal == 0 {
			lValidateAcctRetVal = constants.ResultSuccess
		} else {
			if lAcctRefValFail == 1 {
				lValidateAcctRetVal = lValidateAcctRefRetVal
			}
		}
	}

	return lValidateAcctRetVal, oSeverity, iSessionID
}
