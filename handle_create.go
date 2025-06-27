package photocopy

import (
	"context"
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
)

func (p *Photocopy) handleCreate(ctx context.Context, recb []byte, indexedAt, rev, did, collection, rkey, cid string) error {
	_, err := dateparse.ParseAny(indexedAt)
	if err != nil {
		return err
	}

	switch collection {
	}

	return nil
}

// func (p *Photocopy) handleCreatePost(ctx context.Context, rev string, recb []byte, uri, did, collection, rkey, cid string, indexedAt time.Time) error {
// 	var rec bsky.FeedPost
// 	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
// 		return err
// 	}
//
// 	jb, err := json.Marshal(rec)
// 	if err != nil {
// 		return err
// 	}
//
// 	cat, err := parseTimeFromRecord(rec, rkey)
// 	if err != nil {
// 		return err
// 	}
//
// 	bqrec := &BigQueryRecord{
// 		Uri:        uri,
// 		AuthorDid:  did,
// 		Collection: collection,
// 		Rkey:       rkey,
// 		CreatedAt:  *cat,
// 		IndexedAt:  indexedAt,
// 		Raw:        recb,
// 		Json:       string(jb),
// 		Cid:        cid,
// 		Rev:        rev,
// 	}
//
// 	if rec.Reply != nil {
// 		if rec.Reply.Parent != nil {
// 			bqrec.ReplyToUri = rec.Reply.Parent.Uri
// 		}
// 		if rec.Reply.Root != nil {
// 			bqrec.InThreadUri = rec.Reply.Root.Uri
// 		}
// 	}
//
// 	if err := p.inserters.genericInserter.Insert(ctx, bqrec); err != nil {
// 		return err
// 	}
//
// 	return nil
// }

// func (p *Photocopy) handleCreateFollow(ctx context.Context, recb []byte, uri, did, rkey string, indexedAt time.Time) error {
// 	var rec bsky.GraphFollow
// 	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
// 		return err
// 	}
//
// 	cat, err := parseTimeFromRecord(rec, rkey)
// 	if err != nil {
// 		return err
// 	}
//
// 	bqrec := &BigQueryFollow{
// 		Uri:        uri,
// 		AuthorDid:  did,
// 		Rkey:       rkey,
// 		CreatedAt:  *cat,
// 		IndexedAt:  indexedAt,
// 		SubjectDid: rec.Subject,
// 	}
//
// 	if err := p.inserters.followsInserter.Insert(ctx, bqrec); err != nil {
// 		return err
// 	}
//
// 	return nil
// }
//
// func (p *Photocopy) handleCreateInteraction(ctx context.Context, recb []byte, uri, did, collection, rkey string, indexedAt time.Time) error {
// 	colPts := strings.Split(collection, ".")
// 	if len(colPts) < 4 {
// 		return fmt.Errorf("invalid collection type %s", collection)
// 	}
//
// 	bqi := &BigQueryInteraction{
// 		Uri:       uri,
// 		Kind:      colPts[3],
// 		AuthorDid: did,
// 		Rkey:      rkey,
// 		IndexedAt: indexedAt,
// 	}
//
// 	switch collection {
// 	case "app.bsky.feed.like":
// 		var rec bsky.FeedLike
// 		if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
// 			return err
// 		}
//
// 		cat, err := parseTimeFromRecord(rec, rkey)
// 		if err != nil {
// 			return err
// 		}
//
// 		if rec.Subject == nil {
// 			return fmt.Errorf("invalid subject in like")
// 		}
//
// 		bqi.SubjectUri = rec.Subject.Uri
// 		bqi.CreatedAt = *cat
// 	case "app.bsky.feed.repost":
// 		var rec bsky.FeedRepost
// 		if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
// 			return err
// 		}
//
// 		cat, err := parseTimeFromRecord(rec, rkey)
// 		if err != nil {
// 			return err
// 		}
//
// 		if rec.Subject == nil {
// 			return fmt.Errorf("invalid subject in repost")
// 		}
//
// 		bqi.SubjectUri = rec.Subject.Uri
// 		bqi.CreatedAt = *cat
// 	}
//
// 	if err := p.inserters.interactionsInserter.Insert(ctx, bqi); err != nil {
// 		return err
// 	}
//
// 	return nil
// }

func parseTimeFromRecord(rec any, rkey string) (*time.Time, error) {
	var rkeyTime time.Time
	if rkey != "self" {
		rt, err := syntax.ParseTID(rkey)
		if err == nil {
			rkeyTime = rt.Time()
		}
	}
	switch rec := rec.(type) {
	case *bsky.FeedPost:
		t, err := dateparse.ParseAny(rec.CreatedAt)
		if err != nil {
			return nil, err
		}

		if inRange(t) {
			return &t, nil
		}

		if rkeyTime.IsZero() || !inRange(rkeyTime) {
			return timePtr(time.Now()), nil
		}

		return &rkeyTime, nil
	case *bsky.FeedRepost:
		t, err := dateparse.ParseAny(rec.CreatedAt)
		if err != nil {
			return nil, err
		}

		if inRange(t) {
			return timePtr(t), nil
		}

		if rkeyTime.IsZero() {
			return nil, fmt.Errorf("failed to get a useful timestamp from record")
		}

		return &rkeyTime, nil
	case *bsky.FeedLike:
		t, err := dateparse.ParseAny(rec.CreatedAt)
		if err != nil {
			return nil, err
		}

		if inRange(t) {
			return timePtr(t), nil
		}

		if rkeyTime.IsZero() {
			return nil, fmt.Errorf("failed to get a useful timestamp from record")
		}

		return &rkeyTime, nil
	case *bsky.ActorProfile:
		// We can't really trust the createdat in the profile record anyway, and its very possible its missing. just use iat for this one
		return timePtr(time.Now()), nil
	case *bsky.FeedGenerator:
		if !rkeyTime.IsZero() && inRange(rkeyTime) {
			return &rkeyTime, nil
		}
		return timePtr(time.Now()), nil
	default:
		if !rkeyTime.IsZero() && inRange(rkeyTime) {
			return &rkeyTime, nil
		}
		return timePtr(time.Now()), nil
	}
}

func inRange(t time.Time) bool {
	now := time.Now()
	if t.Before(now) {
		return now.Sub(t) <= time.Hour*24*365*5
	}
	return t.Sub(now) <= time.Hour*24*200
}

func timePtr(t time.Time) *time.Time {
	return &t
}
