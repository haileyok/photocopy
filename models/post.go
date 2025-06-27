package models

import "time"

type Post struct {
	Uri       string    `ch:"uri"`
	Did       string    `ch:"did"`
	Rkey      string    `ch:"rkey"`
	CreatedAt time.Time `ch:"created_at"`
	IndexedAt time.Time `ch:"indexed_at"`
	RootUri   string    `ch:"root_uri"`
	RootDid   string    `ch:"root_did"`
	ParentUri string    `ch:"parent_uri"`
	ParentDid string    `ch:"parent_did"`
	QuoteUri  string    `ch:"quote_uri"`
	QuoteDid  string    `ch:"quote_did"`
}
