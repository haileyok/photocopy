package models

import "time"

type PostLabel struct {
	Did         string    `ch:"did"`
	Rkey        string    `ch:"rkey"`
	CreatedAt   time.Time `ch:"created_at"`
	Text        string    `ch:"text"`
	Label       string    `ch:"label"`
	EntityId    string    `ch:"entity_id"`
	Description string    `ch:"description"`
	Topic       string    `ch:"topic"`
}
