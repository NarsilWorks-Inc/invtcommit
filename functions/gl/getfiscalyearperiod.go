package gl

import (
	"gosqljobs/invtcommit/functions/constants"
	"gosqljobs/invtcommit/functions/sm"
	"strconv"
	"strings"
	"time"

	du "github.com/eaglebush/datautils"
)

// GetFiscalYearPeriod - get fiscal year period
// --  @iCreateFlag Values:
// --   1 - Create Periods and Auto Commit  by Procedure
// --   2 - Do not Create Periods
// --   3 - Create Periods and Manual Commit
// --
// --  oRetVal Values:
// --   1 - Existed
// --   2 - Created/Written (New periods created)
// --   3 - Calculated (New records not created)
// --   5 - Retained Earnings Account does not exist
// --   6 - Could not Create/Calculate Prior
// --   7 - Could not Create/Calculate Future
// --
// --  oStatus Values:
// --   0 - (Null) Invalid Value
// --   1 - Open
// --   2 - Closed
// --
// -- Special Notes ********************************************************
// -- @oFiscalYear is an Output param. However, if we are creating a new
// -- fiscal year, the SetUp Fiscal Year program is passing in the "Entered"
// -- fiscal year. This means, that instead of calculating this "label", the
// -- entered fiscal year will be used. This is necessary since tglAcctHist
// -- and tglAcctHistCurr records were not being created. Also, Incorrect years
// -- were being created and later removed/re-inserted due to this problem.
// -- ************************************************************************
func GetFiscalYearPeriod(
	bq *du.BatchQuery,
	iCompanyID string,
	iDate time.Time,
	iCreateFlag int,
	iFiscalYear string,
	iUserID string) (
	Result constants.ResultConstant,
	Status int,
	FiscalYear string,
	FiscalPer int,
	StartDate time.Time,
	EndDate time.Time) {

	bq.ScopeName("GetFiscalYearPeriod")

	lEnteredFiscalYear := strings.TrimSpace(iFiscalYear)

	lIntFiscalYear := 0
	if lEnteredFiscalYear != "" {
		lIntFiscalYear, _ = strconv.Atoi(strings.TrimSpace(lEnteredFiscalYear))
	}

	oRetVal := 0
	oStatus := 0
	oFiscYear := ""
	oFiscPer := 0
	var oStartDate *time.Time
	var oEndDate *time.Time

	fiscexist := false

	qr := bq.Get(`SELECT FiscYear, FiscPer, StartDate, EndDate, Status
					FROM tglFiscalPeriod WITH (NOLOCK)
					WHERE CompanyID = ?
						AND ? BETWEEN StartDate AND EndDate;`, iCompanyID, iDate)

	if qr.HasData {
		oFiscYear = qr.First().ValueString("FiscYear")
		oFiscPer = int(qr.First().ValueInt64("FiscPer"))

		odt := qr.First().ValueTime("StartDate")
		oStartDate = &odt

		odt = qr.First().ValueTime("EndDate")
		oEndDate = &odt

		oStatus = int(qr.First().ValueInt64("Status"))

		fiscexist = true
	}

	lPriorYearCreation := false
	qr = bq.Get(`SELECT GETDATE();`)
	lTime := qr.First().ValueTimeOrd(0)

	// Fiscal Record not found, so we search for a suitable one
	if !fiscexist {

		lMethod := 0
		lCurrentFiscalYear := ""
		lFirstFiscalYear := ""
		lPeriods := int64(0)
		lNoOfDaysinFiscYear := 0.0

		var lFirstStartDate time.Time
		var lStartYearDate time.Time
		var lEndYearDate time.Time
		var lDate time.Time
		lYear := ""

		// Get the latest Fiscal Year.
		qr = bq.Get(`SELECT MAX(FiscYear)
						FROM tglFiscalYear WITH (NOLOCK)
					 WHERE CompanyID=?;`, iCompanyID)
		if qr.HasData {
			lCurrentFiscalYear = qr.First().ValueStringOrd(0)
		}

		// Get the First Fiscal Year.
		qr = bq.Get(`SELECT MIN(FiscYear)
						FROM tglFiscalPeriod WITH (NOLOCK)
					WHERE CompanyID=?;`, iCompanyID)
		if qr.HasData {
			lFirstFiscalYear = qr.First().ValueStringOrd(0)
		}

		// Get Start Date of First Fiscal Year
		qr = bq.Get(`SELECT StartDate
					FROM tglFiscalPeriod WITH (NOLOCK)
					WHERE CompanyID = ?
						AND FiscYear = ?
						AND FiscPer = (SELECT MIN(FiscPer)
										FROM tglFiscalPeriod WITH (NOLOCK)
										WHERE CompanyID = ?
										AND FiscYear = ?);`, iCompanyID, lFirstFiscalYear, iCompanyID, lFirstFiscalYear)
		if qr.HasData {
			lFirstStartDate = qr.First().ValueTimeOrd(0)
		}

		// Entered Date <= Start Date of First Fiscal Year
		if iDate.After(lFirstStartDate) || iDate.Equal(lFirstStartDate) {
			lCurrentFiscalYear = lFirstFiscalYear
			lPriorYearCreation = true
		}

		// Get No. of Periods for Oldest Fiscal Year
		qr = bq.Get(`SELECT NoOfPeriods
						FROM tglFiscalYear WITH (NOLOCK)
					WHERE CompanyID=?
						AND FiscYear=?;`, iCompanyID, lCurrentFiscalYear)
		if qr.HasData {
			lPeriods = qr.First().ValueInt64Ord(0)
		}

		// Get Start and End Dates of Oldest Fiscal Year Period 1.
		qr = bq.Get(`SELECT  StartDate, EndDate 
					FROM tglFiscalPeriod WITH (NOLOCK)
					WHERE CompanyID=?
						AND FiscYear = ?
						AND FiscPer = (SELECT MIN(FiscPer)
										FROM tglFiscalPeriod WITH (NOLOCK)
										WHERE CompanyID = ?
										AND FiscYear = ?);`, iCompanyID, lCurrentFiscalYear, iCompanyID, lCurrentFiscalYear)
		if qr.HasData {
			lStartYearDate = qr.First().ValueTimeOrd(0)
			lDate = qr.First().ValueTimeOrd(1)
		}

		// Get End Date of Oldest Fiscal Year.
		qr = bq.Get(`SELECT EndDate
					FROM tglFiscalPeriod WITH (NOLOCK)
					WHERE CompanyID = ?
						AND FiscYear = ?
						AND FiscPer = (SELECT MAX(FiscPer)
										FROM tglFiscalPeriod WITH (NOLOCK)
										WHERE CompanyID = ?
										AND FiscYear = ?);`, iCompanyID, lCurrentFiscalYear, iCompanyID, lCurrentFiscalYear)
		if qr.HasData {
			lEndYearDate = qr.First().ValueTimeOrd(0)
		}

		lMethod = 2 // Using the Days Diff of each period

		lyr, _ := strconv.Atoi(lCurrentFiscalYear)

		if lPriorYearCreation {
			lyr = lyr + 1

			lCheckNextYearDate := lDate.Add(time.Hour * 24)
			if lCheckNextYearDate.After(lEndYearDate) {
				lMethod = 1 // Normal Increment (Across Months)
			}
		} else {
			lyr = lyr - 1

			diff := lEndYearDate.Sub(lStartYearDate).Hours()
			lNoOfDaysinFiscYear := diff * 24.0
		}

		if lIntFiscalYear != 0 {
			lYear = lEnteredFiscalYear
		} else {
			lYear = strconv.Itoa(lyr) + " "
		}

		lFirstTime := true

		var lNextEndDate time.Time
		lOnceCreated := false
		lLastPerEndDate := lEndYearDate

		qr = bq.Get(`SELECT FiscPer, StartDate, EndDate, 
								DATEDIFF(day,StartDate,EndDate) NoOfDays
						FROM tglFiscalPeriod WITH (NOLOCK)
						WHERE CompanyID=?
							AND FiscYear=?
						ORDER BY CompanyID, FiscYear, FiscPer;`, iCompanyID, lCurrentFiscalYear)

		for _, v := range qr.Data {

			lFiscPer := v.ValueInt64("FiscPer")
			lStartDate := v.ValueTime("StartDate")
			lEndDate := v.ValueTime("EndDate")
			lNoOfDays := v.ValueFloat64("NoOfDays")

			if lFirstTime {
				lStartDate := lEndYearDate
				if lMethod == 2 {
					lNoOfDays = lEndDate.Sub(lStartDate).Hours() * 24.0

					if lPriorYearCreation {
						lDFY := -((lNoOfDaysinFiscYear + 2.0) / 24.0) // convert to hours, negative
						lStartDate = lStartDate.Add(time.Hour * time.Duration(lDFY))
					}

				}

				lFirstTime = false
			} else {
				lStartDate = lNextEndDate
			}

			lNextStartDate, lNextEndDate := sm.GetNextYearPeriod(lStartDate, int(lNoOfDays), lEndDate, lMethod)

			lLastPerEndDate = lNextEndDate

			if lPriorYearCreation && lFiscPer == lPeriods {
				lNextEndDate = lStartYearDate.Add(time.Hour * -1)
			}

			if sm.InIntArray(&[]int{1, 3}, iCreateFlag) {

				if !lOnceCreated {
					bq.Set(`INSERT INTO tglFiscalYear (CompanyID, FiscYear, NoOfPeriods)
							VALUES (?, ?, ?);`, iCompanyID, lYear, lPeriods)
					lOnceCreated = true
				}

				bq.Set(`INSERT INTO tglFiscalPeriod (
								CompanyID,
								FiscYear,
								FiscPer,
								EndDate,
								FiscYearPer,
								StartDate,
								Status)
							VALUES (?, ?, ?, ?,	' ', ?, ?);`, iCompanyID, lYear, lFiscPer, lNextEndDate, lNextStartDate, 1)

			} else {

				if !lOnceCreated {
					bq.Set(`INSERT INTO tciFiscYearWrk (
								CompanyID, 
								FiscYear,
								NoOfPeriods, 
								DBUserID,
								TimeCreated)
							VALUES (?, ?, ?, ?, ?)`, iCompanyID, lYear, lPeriods, iUserID, lTime)
					lOnceCreated = true
				}

				bq.Set(`INSERT INTO tciFiscPeriodWrk (
							CompanyID,
							FiscYear,
							FiscPer,
							EndDate,
							StartDate,
							Status,
							DBUserID,
							TimeCreated)
						VALUES (?, ?, ?, ?,	?, ?, ?, ?);`, iCompanyID, lYear, lFiscPer, lNextEndDate, lNextStartDate, 1, iUserID, lTime)

			}

		}

	}

	lRetVal := 0
	if fiscexist {

		oRetVal = 1

		// Determine if Retained Earning Acct(s) exist only if creating fiscal year info.
		if sm.InIntArray(&[]int{1, 3}, iCreateFlag) {
			lRetainedEarnAcct := ""
			lSessionID := 0

			// Get Retained Earning Acct from tglOptions.
			qr = bq.Get(`SELECT RetainedEarnAcct FROM tglOptions WITH (NOLOCK) WHERE CompanyID = ?;`, iCompanyID)
			if qr.HasData {
				lRetainedEarnAcct = qr.First().ValueStringOrd(0)
			}

			// Will any Retained Earning Acct(s) need to be created?
			lRetVal, lSessionID = RetEarnAcctCreate(bq, iCompanyID, lSessionID, lRetainedEarnAcct)

			//Evaluate Return:
			// 0 = Not Masked: Needs Creation,
			// 1 = Masked: Needs Creation
			if sm.InIntArray(&[]int{0, 1}, lRetVal) {
				oRetVal = 5
				bq.Set(`DELETE FROM tglRetEarnAcctWrk
						WHERE SessionID=? AND CompanyID=?;`, lSessionID, iCompanyID)
			}

			return constants.ResultConstant(oRetVal),
				oStatus,
				oFiscYear,
				oFiscPer,
				*oStartDate,
				*oEndDate

		}

	}

	oFiscYear = ""
	oFiscPer = 0
	oStartDate = nil
	oEndDate = nil
	oStatus = 0
	tbl := ""

	// Only attempt to get the fiscal information if we had to calculate/create fiscal information.
	if sm.InIntArray(&[]int{1, 3}, iCreateFlag) {
		// New Records should have been written.
		oRetVal = 2
		tbl = "tglFiscalPeriod"
	} else {
		// New Records not created, calculated only.
		oRetVal = 3
		tbl = "tciFiscPeriodWrk"
	}

	qr = bq.Get(`SELECT FiscYear, FiscPer, StartDate, EndDate, Status
					 FROM `+tbl+` WITH (NOLOCK)
					 WHERE CompanyID = ?
						AND ? BETWEEN StartDate AND EndDate;`, iCompanyID, iDate)

	if qr.HasData {
		oFiscYear = qr.First().ValueString("FiscYear")
		oFiscPer = int(qr.First().ValueInt64("FiscPer"))
		*oStartDate = qr.First().ValueTime("StartDate")
		*oEndDate = qr.First().ValueTime("EndDate")
		oStatus = int(qr.First().ValueInt64("Status"))
	} else {
		if lPriorYearCreation {
			oRetVal = 6
		} else {
			oRetVal = 7
		}
	}

	// Calculate/Re-Calculate Beginning Balances
	if sm.InIntArray(&[]int{1, 3}, iCreateFlag) && oRetVal == 2 {
		if lRetVal = CalcBeginBalance(bq, iCompanyID, oFiscYear); lRetVal == 1 {
			// Retained Earning Acct(s) not found.
			oRetVal = 5
		}
	}

	// Clean up tciFiscYearWrk Table
	bq.Set(`DELETE FROM tciFiscYearWrk  
			WHERE DBUserID = ?
			AND TimeCreated = ?;`, iUserID, lTime)

	bq.Set(`DELETE FROM tciFiscPeriodWrk  
			WHERE DBUserID = ?
			AND TimeCreated = ?;`, iUserID, lTime)

	return constants.ResultConstant(oRetVal),
		oStatus,
		oFiscYear,
		oFiscPer,
		*oStartDate,
		*oEndDate
}
