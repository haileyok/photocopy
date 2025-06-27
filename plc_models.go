package photocopy

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
)

type PLCEntry struct {
	Did       string           `json:"did" bigquery:"did"`
	Operation PLCOperationType `json:"operation" bigquery:"operation"`
	Cid       string           `json:"cid" bigquery:"cid"`
	Nullified bool             `json:"nullified" bigquery:"nullified"`
	CreatedAt time.Time        `json:"createdAt" bigquery:"created_at"`
}

type PLCOperation struct {
	Sig                     string                `json:"sig" bigquery:"sig"`
	Prev                    bigquery.NullString   `json:"prev" bigquery:"prev"`
	Type                    string                `json:"type" bigquery:"type"`
	Services                map[string]PLCService `json:"services" bigquery:"-"`
	ServicesJSON            string                `json:"-" bigquery:"services"`
	AlsoKnownAs             []string              `json:"alsoKnownAs" bigquery:"also_known_as"`
	RotationKeys            []string              `json:"rotationKeys" bigquery:"rotation_keys"`
	VerificationMethods     map[string]string     `json:"verificationMethods" bigquery:"-"`
	VerificationMethodsJSON string                `json:"-" bigquery:"verification_methods"`
}

type PLCTombstone struct {
	Sig  string `json:"sig"`
	Prev string `json:"prev"`
	Type string `json:"type"`
}

type PLCService struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

type LegacyPLCOperation struct {
	Sig         string `json:"sig"`
	Prev        string `json:"prev"`
	Type        string `json:"type"`
	Handle      string `json:"handle"`
	Service     string `json:"service"`
	SigningKey  string `json:"signingKey"`
	RecoveryKey string `json:"recoveryKey"`
}

type PLCOperationType struct {
	OperationType      string              `json:"-" bigquery:"operation_type"`
	PLCOperation       *PLCOperation       `json:"-" bigquery:"plc_operation"`
	PLCTombstone       *PLCTombstone       `json:"-" bigquery:"plc_tombstone"`
	LegacyPLCOperation *LegacyPLCOperation `json:"-" bigquery:"legacy_plc_operation"`
}

func (o *PLCOperationType) UnmarshalJSON(data []byte) error {
	type Base struct {
		PLCOperation       *PLCOperation
		PLCTombstone       *PLCTombstone
		LegacyPLCOperation *LegacyPLCOperation
		Type               string `json:"type"`
	}

	var base Base
	if err := json.Unmarshal(data, &base); err != nil {
		return err
	}

	switch base.Type {
	case "plc_operation":
		var op PLCOperation
		if err := json.Unmarshal(data, &op); err != nil {
			return err
		}
		o.PLCOperation = &op
		o.OperationType = "plc_operation"
	case "plc_tombstone":
		var op PLCTombstone
		if err := json.Unmarshal(data, &op); err != nil {
			return err
		}
		o.PLCTombstone = &op
		o.OperationType = "plc_tombstone"
	case "create":
		var op LegacyPLCOperation
		if err := json.Unmarshal(data, &op); err != nil {
			return err
		}
		o.LegacyPLCOperation = &op
		o.OperationType = "legacy_plc_operation"
	default:
		if base.PLCOperation != nil || base.PLCTombstone != nil || base.LegacyPLCOperation != nil {
			o.PLCOperation = base.PLCOperation
			o.PLCTombstone = base.PLCTombstone
			o.LegacyPLCOperation = base.LegacyPLCOperation
		} else {
			return fmt.Errorf("invalid operation type %s", base.Type)
		}
	}

	return nil
}

func (o *PLCOperationType) MarshalJSON() ([]byte, error) {
	if o.PLCOperation != nil {
		return json.Marshal(o.PLCOperation)
	}
	if o.PLCTombstone != nil {
		return json.Marshal(o.PLCTombstone)
	}
	if o.LegacyPLCOperation != nil {
		return json.Marshal(o.LegacyPLCOperation)
	}
	return nil, errors.New("no valid operation type found")
}

func (o *PLCOperationType) Value() (any, error) {
	return json.Marshal(o)
}

func (o *PLCOperationType) Scan(value any) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("could not scan PLCOperationType")
	}
	return json.Unmarshal(bytes, o)
}

func (e *PLCEntry) prepareForBigQuery() (map[string]bigquery.Value, string, error) {
	if e.Operation.PLCOperation != nil {
		if e.Operation.PLCOperation.Services != nil {
			b, err := json.Marshal(e.Operation.PLCOperation.Services)
			if err != nil {
				return nil, "", fmt.Errorf("error marshaling services: %w", err)
			}
			e.Operation.PLCOperation.ServicesJSON = string(b)
		}
		if e.Operation.PLCOperation.VerificationMethods != nil {
			b, err := json.Marshal(e.Operation.PLCOperation.VerificationMethods)
			if err != nil {
				return nil, "", fmt.Errorf("error marshaling verification methods: %w", err)
			}
			e.Operation.PLCOperation.VerificationMethodsJSON = string(b)
		}
	}
	return nil, "", nil
}
