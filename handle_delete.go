package photocopy

import (
	"context"
	"time"

	"github.com/haileyok/photocopy/models"
)

func (p *Photocopy) handleDelete(ctx context.Context, did, collection, rkey string) error {
	del := models.Delete{
		Did:       did,
		Rkey:      rkey,
		CreatedAt: time.Now(),
	}

	if err := p.inserters.deletesInserter.Insert(ctx, del); err != nil {
		return err
	}

	return nil
}
