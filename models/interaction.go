package models

import "time"

type Interaction struct {
	Uri        string    `ch:"like"`
	Did        string    `ch:"did"`
	Rkey       string    `ch:"rkey"`
	Kind       string    `ch:"kind"`
	CreatedAt  time.Time `ch:"created_at"`
	IndexedAt  time.Time `ch:"indexed_at"`
	SubjectUri string    `ch:"subject_uri"`
	SubjectDid string    `ch:"subject_did"`
}
