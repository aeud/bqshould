package bqshould

import "testing"

func TestLegacySQL(t *testing.T) {
	set := Query("SELECT COUNT(*) FROM [bigquery-public-data:hacker_news.comments] LIMIT 1000")
	set.Should(0, 0).BeEqual(8399417)
	if valid, errors := set.Test(); !valid {
		t.Error(errors)
	}
}

func TestStandardSQL(t *testing.T) {
	set := Query("#standardSQL\nSELECT COUNT(*) FROM `bigquery-public-data.hacker_news.comments` LIMIT 1000")
	set.Should(0, 0).BeEqual(8399417)
	if valid, errors := set.Test(); !valid {
		t.Error(errors)
	}
}
