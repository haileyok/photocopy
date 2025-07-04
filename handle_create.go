package photocopy

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/haileyok/photocopy/models"
)

func (p *Photocopy) handleCreate(ctx context.Context, recb []byte, indexedAt, rev, did, collection, rkey, cid, seq string) error {
	iat, err := dateparse.ParseAny(indexedAt)
	if err != nil {
		return err
	}

	if err := p.handleCreateRecord(ctx, did, rkey, collection, cid, recb, seq); err != nil {
		p.logger.Error("error creating record", "error", err)
	}

	switch collection {
	case "app.bsky.feed.post":
		return p.handleCreatePost(ctx, rev, recb, uriFromParts(did, collection, rkey), did, collection, rkey, cid, iat)
	case "app.bsky.graph.follow":
		return p.handleCreateFollow(ctx, recb, uriFromParts(did, collection, rkey), did, rkey, iat)
	case "app.bsky.feed.like", "app.bsky.feed.repost":
		return p.handleCreateInteraction(ctx, recb, uriFromParts(did, collection, rkey), did, collection, rkey, iat)
	default:
		return nil
	}
}

func (p *Photocopy) handleCreateRecord(ctx context.Context, did, rkey, collection, cid string, raw []byte, seq string) error {
	var cat time.Time
	prkey, err := syntax.ParseTID(rkey)
	if err == nil {
		cat = prkey.Time()
	} else {
		cat = time.Now()
	}

	rec := models.Record{
		Did:        did,
		Rkey:       rkey,
		Collection: collection,
		Cid:        cid,
		Seq:        seq,
		Raw:        string(raw),
		CreatedAt:  cat,
	}

	if err := p.inserters.recordsInserter.Insert(ctx, rec); err != nil {
		return err
	}

	return nil
}

func (p *Photocopy) handleCreatePost(ctx context.Context, rev string, recb []byte, uri, did, collection, rkey, cid string, indexedAt time.Time) error {
	var rec bsky.FeedPost
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	cat, err := parseTimeFromRecord(rec, rkey)
	if err != nil {
		return err
	}

	lang := ""
	if len(rec.Langs) != 0 {
		lang = rec.Langs[0]
	}

	post := models.Post{
		Uri:       uri,
		Rkey:      rkey,
		CreatedAt: *cat,
		IndexedAt: indexedAt,
		Did:       did,
		Lang:      lang,
		Text:      rec.Text,
	}

	if rec.Reply != nil {
		if rec.Reply.Parent != nil {
			aturi, err := syntax.ParseATURI(rec.Reply.Parent.Uri)
			if err != nil {
				return fmt.Errorf("error parsing at-uri: %w", err)

			}
			post.ParentDid = aturi.Authority().String()
			post.ParentUri = rec.Reply.Parent.Uri
		}
		if rec.Reply.Root != nil {
			aturi, err := syntax.ParseATURI(rec.Reply.Root.Uri)
			if err != nil {
				return fmt.Errorf("error parsing at-uri: %w", err)

			}
			post.RootDid = aturi.Authority().String()
			post.RootUri = rec.Reply.Root.Uri
		}
	}

	if rec.Embed != nil && rec.Embed.EmbedRecord != nil && rec.Embed.EmbedRecord.Record != nil {
		aturi, err := syntax.ParseATURI(rec.Embed.EmbedRecord.Record.Uri)
		if err != nil {
			return fmt.Errorf("error parsing at-uri: %w", err)

		}
		post.QuoteDid = aturi.Authority().String()
		post.QuoteUri = rec.Embed.EmbedRecord.Record.Uri
	} else if rec.Embed != nil && rec.Embed.EmbedRecordWithMedia != nil && rec.Embed.EmbedRecordWithMedia.Record != nil && rec.Embed.EmbedRecordWithMedia.Record.Record != nil {
		aturi, err := syntax.ParseATURI(rec.Embed.EmbedRecordWithMedia.Record.Record.Uri)
		if err != nil {
			return fmt.Errorf("error parsing at-uri: %w", err)

		}
		post.QuoteDid = aturi.Authority().String()
		post.QuoteUri = rec.Embed.EmbedRecordWithMedia.Record.Record.Uri
	}

	if err := p.inserters.postsInserter.Insert(ctx, post); err != nil {
		return err
	}

	if rec.Text != "" {
		go func(ctx context.Context, rec bsky.FeedPost, did, rkey string) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			nervanaItems, err := p.makeNervanaRequest(ctx, rec.Text)
			if err != nil {
				p.logger.Error("error making nervana items request", "error", err)
				return
			}

			for _, ni := range nervanaItems {
				postLabel := models.PostLabel{
					Did:         did,
					Rkey:        rkey,
					Text:        ni.Text,
					Label:       ni.Label,
					EntityId:    ni.EntityId,
					Description: ni.Description,
					Topic:       "",
				}
				p.inserters.labelsInserter.Insert(ctx, postLabel)
			}
		}(ctx, rec, did, rkey)
	}

	return nil
}

func (p *Photocopy) handleCreateFollow(ctx context.Context, recb []byte, uri, did, rkey string, indexedAt time.Time) error {
	var rec bsky.GraphFollow
	if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
		return err
	}

	cat, err := parseTimeFromRecord(rec, rkey)
	if err != nil {
		return err
	}

	follow := models.Follow{
		Uri:       uri,
		Did:       did,
		Rkey:      rkey,
		CreatedAt: *cat,
		IndexedAt: indexedAt,
		Subject:   rec.Subject,
	}

	if err := p.inserters.followsInserter.Insert(ctx, follow); err != nil {
		return err
	}

	return nil
}

func (p *Photocopy) handleCreateInteraction(ctx context.Context, recb []byte, uri, did, collection, rkey string, indexedAt time.Time) error {
	colPts := strings.Split(collection, ".")
	if len(colPts) < 4 {
		return fmt.Errorf("invalid collection type %s", collection)
	}

	interaction := models.Interaction{
		Uri:        uri,
		Kind:       colPts[3],
		Rkey:       rkey,
		IndexedAt:  indexedAt,
		Did:        did,
		SubjectUri: uri,
		SubjectDid: did,
	}

	switch collection {
	case "app.bsky.feed.like":
		var rec bsky.FeedLike
		if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
			return err
		}

		cat, err := parseTimeFromRecord(rec, rkey)
		if err != nil {
			return err
		}

		if rec.Subject == nil {
			return fmt.Errorf("invalid subject in like")
		}

		aturi, err := syntax.ParseATURI(rec.Subject.Uri)
		if err != nil {
			return fmt.Errorf("error parsing at-uri: %w", err)

		}

		interaction.SubjectDid = aturi.Authority().String()
		interaction.SubjectUri = rec.Subject.Uri
		interaction.CreatedAt = *cat
	case "app.bsky.feed.repost":
		var rec bsky.FeedRepost
		if err := rec.UnmarshalCBOR(bytes.NewReader(recb)); err != nil {
			return err
		}

		cat, err := parseTimeFromRecord(rec, rkey)
		if err != nil {
			return err
		}

		if rec.Subject == nil {
			return fmt.Errorf("invalid subject in repost")
		}

		aturi, err := syntax.ParseATURI(rec.Subject.Uri)
		if err != nil {
			return fmt.Errorf("error parsing at-uri: %w", err)

		}

		interaction.SubjectDid = aturi.Authority().String()
		interaction.SubjectUri = rec.Subject.Uri
		interaction.CreatedAt = *cat
	}

	if err := p.inserters.interactionsInserter.Insert(ctx, interaction); err != nil {
		return err
	}

	return nil
}

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
