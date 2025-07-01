package models

import "time"

type Record struct {
	Did        string    `ch:"did"`
	Rkey       string    `ch:"rkey"`
	Collection string    `ch:"collection"`
	Cid        string    `ch:"cid"`
	Seq        string    `ch:"seq"`
	Raw        string    `ch:"raw"`
	CreatedAt  time.Time `ch:"created_at"`
}
