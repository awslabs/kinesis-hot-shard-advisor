// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package analyse

import (
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"time"

	"github.com/fatih/color"
)

type HTMLReporter struct {
	fname string
}

func (rp *HTMLReporter) Report(start time.Time, stats map[string]map[string]interface{}, limit int) error {
	type shardStats struct {
		ShardID string                 `json:"shardId"`
		Stats   map[string]interface{} `json:"stats"`
	}
	type report struct {
		From   int          `json:"from"`
		Shards []shardStats `json:"shards"`
	}
	data := make([]shardStats, 0)
	for sid, v := range stats {
		data = append(data, shardStats{sid, v})
	}
	r := &report{
		From:   int(start.Unix()),
		Shards: data,
	}
	buf, err := json.Marshal(r)
	if err != nil {
		return err
	}
	t, err := template.New("output").Funcs(template.FuncMap{
		"trustedJS": func(s string) template.JS {
			return template.JS(s)
		},
		"trustedHTML": func(s string) template.HTML {
			return template.HTML(s)
		},
	}).Parse(outputTemplate)
	if err != nil {
		return err
	}
	fname := rp.fname
	file, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Sync()
	defer file.Close()
	err = t.Execute(file, map[string]interface{}{
		"Date":   time.Now(),
		"Limit":  limit,
		"Report": template.JS(string(buf)),
	})
	if err != nil {
		return err
	}
	fmt.Print(color.YellowString(": %s ", fname))
	return nil
}

func NewHTMLReporter(fname string) *HTMLReporter {
	return &HTMLReporter{
		fname: fname,
	}
}
