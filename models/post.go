package models

import "time"

type Post struct {
	Uri       string    `ch:"uri"`
	Did       string    `ch:"did"`
	Rkey      string    `ch:"rkey"`
	CreatedAt time.Time `ch:"created_at"`
	IndexedAt time.Time `ch:"indexed_at"`
	RootUri   string    `ch:"root"`
	RootDid   string    `ch:"root_did"`
	ParentUri string    `ch:"reply"`
	ParentDid string    `ch:"reply_did"`
	QuoteUri  string    `ch:"quote_uri"`
	QuoteDid  string    `ch:"quote_did"`
}
