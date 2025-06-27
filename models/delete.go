package models

import "time"

type Delete struct {
	Did       string    `ch:"did"`
	Rkey      string    `ch:"rkey"`
	CreatedAt time.Time `ch:"created_at"`
}
