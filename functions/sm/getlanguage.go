package sm

import du "github.com/eaglebush/datautils"

// GetLanguage - Get Language code from tsmSiteProfile
func GetLanguage(bq *du.BatchQuery) int {
	qr := bq.Get(`SELECT LanguageID FROM tsmSiteProfile;`)
	if qr.HasData {
		return int(qr.First().ValueInt64Ord(0))
	}
	return 1033
}
