package bqshould

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	bigquery "google.golang.org/api/bigquery/v2"
)

// SetConstants will apply the 2 main constants as environment variables
func SetConstants(googleKeyPath, projectID string) {
	os.Setenv("GOOGLE_KEY_PATH", googleKeyPath)
	os.Setenv("GOOGLE_PROJECT_ID", projectID)
}

// Condition represents a single test to apply on the query result
type Condition struct {
	row   int
	col   int
	value interface{}
	label string
}

// TestSet is a set of conditions (tests)
type TestSet struct {
	query      string
	conditions []*Condition
	resp       *bigquery.GetQueryResultsResponse
}

// Query creates a new TestSet
func Query(sql string) *TestSet {
	return &TestSet{
		query:      sql,
		conditions: make([]*Condition, 0),
	}
}

// Should creates a new condition on a row, col combination
func (s *TestSet) Should(row, col int) (c *Condition) {
	c = &Condition{
		row: row,
		col: col,
	}
	s.conditions = append(s.conditions, c)
	return
}

// BeEqual gives the test supposed value
func (c *Condition) BeEqual(v interface{}) *Condition {
	c.value = fmt.Sprintf("%v", v)
	return c
}

// Label gives a label to the test
func (c *Condition) Label(s string) *Condition {
	c.label = s
	return c
}

// Test runs the query and all the condition validations
func (s *TestSet) Test() (valid bool, errors []error) {
	if err := s.runQuery(); err != nil {
		errors = append(errors, err)
		return
	}
	valid = true
	errors = make([]error, len(s.conditions))
	for i, c := range s.conditions {
		if r := s.resp.Rows[c.row].F[c.col].V; r.(string) != c.value.(string) {
			errors[i] = fmt.Errorf("%v: (%v,%v) is %v, should be %v", c.label, c.row, c.col, r, c.value)
			valid = false
		}
	}
	return
}

// TestLog will run the Test() and log the errors if there is any
func (s *TestSet) TestLog() {
	if v, errors := s.Test(); !v {
		for _, err := range errors {
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (s *TestSet) runQuery() error {
	data, err := ioutil.ReadFile(os.Getenv("GOOGLE_KEY_PATH"))
	if err != nil {
		return err
	}
	conf, err := google.JWTConfigFromJSON(data, []string{bigquery.BigqueryScope}...)
	if err != nil {
		return err
	}
	service, err := bigquery.New(conf.Client(oauth2.NoContext))
	if err != nil {
		return err
	}
	jobsService := bigquery.NewJobsService(service)
	job, err := jobsService.Insert(os.Getenv("GOOGLE_PROJECT_ID"), &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Query: &bigquery.JobConfigurationQuery{
				Query: s.query,
			},
		},
	}).Do()
	if err != nil {
		return err
	}
	resp, err := jobsService.GetQueryResults(job.JobReference.ProjectId, job.JobReference.JobId).Do()
	if err != nil {
		return err
	}
	s.resp = resp
	return nil
}
