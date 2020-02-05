package sm

import (
	"math"
	"strings"
	"time"
)

// Common functions

// CheckLeapYear - returns a leap year end date if it is a leap year
func CheckLeapYear(iDate time.Time) time.Time {

	if iDate.Month() == 2 && (iDate.Day() == 28 || iDate.Day() == 29) {
		div := math.Mod(float64(iDate.Year()), 4)
		if div == 0 {
			return time.Date(iDate.Year(), iDate.Month(), 29, 0, 0, 0, 0, time.Local)
		}

		if div != 0 {
			// If year is divisible by 100, it is not leap year
			div = math.Mod(float64(iDate.Year()), 100)
			if div == 0 {
				// but if it is divisible by 400, it is leap year
				div = math.Mod(float64(iDate.Year()), 400)
				if div == 0 {
					return time.Date(iDate.Year(), iDate.Month(), 29, 0, 0, 0, 0, time.Local)
				}
			}
		}
	}

	return iDate
}

// GetNextYearPeriod - returns the next start date end next end date of a period
func GetNextYearPeriod(
	iStartDate time.Time,
	iNoOfDays int,
	iEndDate time.Time,
	iMethod int) (NextStartDate time.Time, NextEndDate time.Time) {

	lNextStartDate := iStartDate
	lNextEndDate := iEndDate

	if iMethod == 1 {
		lDate := iStartDate.AddDate(0, 0, 1)
		lNextStartDate = CheckLeapYear(lDate)

		lDate = iEndDate.AddDate(1, 0, 0)
		lNextEndDate = CheckLeapYear(lDate)
	}

	if iMethod == 2 {
		lNextStartDate = iStartDate.AddDate(0, 0, 1)
		lNextEndDate = lNextStartDate.AddDate(0, 0, iNoOfDays)
	}

	return lNextStartDate, lNextEndDate
}

// SubstAcct - substitute GL account to retaining account on wildcard (*) characters
func SubstAcct(iGlAcctNo string, iRetAcctNo string) string {
	iGLAcctLen := len(iGlAcctNo)

	for {
		pos := strings.Index(iRetAcctNo, "*")
		if pos == -1 || pos > iGLAcctLen {
			break
		}

		glchar := iGlAcctNo[pos : pos+1]
		iRetAcctNo = strings.Replace(iRetAcctNo, "*", glchar, 1)
	}

	return iRetAcctNo
}

// InStringArray - checks for the existence of value in a string array
func InStringArray(stack *[]string, value string) bool {
	value = strings.ToLower(value)
	for _, v := range *stack {
		if strings.ToLower(v) == value {
			return true
		}
	}
	return false
}

// InInt64Array - checks for the existence of value in a int64 array
func InInt64Array(stack *[]int64, value int64) bool {
	for _, v := range *stack {
		if v == value {
			return true
		}
	}
	return false
}

// InIntArray - checks for the existence of value in a int array
func InIntArray(stack *[]int, value int) bool {
	for _, v := range *stack {
		if v == value {
			return true
		}
	}
	return false
}
