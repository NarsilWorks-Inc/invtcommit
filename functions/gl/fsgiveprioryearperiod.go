package gl

import du "github.com/eaglebush/datautils"

// FSGivePriorYearPeriod - financial service give prior year period
func FSGivePriorYearPeriod(
	bq *du.BatchQuery,
	iCompanyID string,
	iFiscYear string) (FiscYear string, FiscPer int) {

	bq.ScopeName("FSGivePriorYearPeriod")

	qr := bq.Get(`SELECT TOP 1 fp.FiscYear, fp.FiscPer
				 FROM tglFiscalYear fy
					INNER JOIN tglFiscalPeriod fp ON fy.CompanyID=fp.CompanyID AND fy.FiscYear=fp.FiscYear
				 WHERE fy.CompanyID=? AND fy.FiscYear<?
				 ORDER BY fy.FiscYear DESC, fp.FiscPer DESC;`, iCompanyID, iFiscYear)
	if qr.HasData {
		return qr.First().ValueStringOrd(0), int(qr.First().ValueInt64Ord(1))
	}

	return ``, 0
}
