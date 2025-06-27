package models

import "time"

type Follow struct {
	Uri       string    `ch:"uri"`
	Did       string    `ch:"did"`
	Rkey      string    `ch:"rkey"`
	CreatedAt time.Time `ch:"created_at"`
	IndexedAt time.Time `ch:"indexed_at"`
	Subject   string    `ch:"subject"`
}
