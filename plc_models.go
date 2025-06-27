package photocopy

import (
	"encoding/json"
	"fmt"
	"time"
)

type PLCEntry struct {
	Did       string           `json:"did"`
	Operation PLCOperationType `json:"operation"`
	Cid       string           `json:"cid"`
	Nullified bool             `json:"nullified"`
	CreatedAt time.Time        `json:"createdAt"`
}

type PLCOperation struct {
	Sig                 string                `json:"sig" `
	Prev                *string               `json:"prev"`
	Type                string                `json:"type"`
	Services            map[string]PLCService `json:"services"`
	AlsoKnownAs         []string              `json:"alsoKnownAs"`
	RotationKeys        []string              `json:"rotationKeys"`
	VerificationMethods map[string]string     `json:"verificationMethods"`
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
	OperationType      string
	PLCOperation       *PLCOperation
	PLCTombstone       *PLCTombstone
	LegacyPLCOperation *LegacyPLCOperation
}

type ClickhousePLCEntry struct {
	Did                 string    `ch:"did"`
	Cid                 string    `ch:"cid"`
	Nullified           bool      `ch:"nullified"`
	CreatedAt           time.Time `ch:"created_at"`
	PlcOpSig            string    `ch:"plc_op_sig"`
	PlcOpPrev           string    `ch:"plc_op_prev"`
	PlcOpType           string    `ch:"plc_op_type"`
	PlcOpServices       []string  `ch:"plc_op_services"`
	PlcOpAlsoKnownAs    []string  `ch:"plc_op_also_known_as"`
	PlcOpRotationKeys   []string  `ch:"plc_op_rotation_keys"`
	PlcTombSig          string    `ch:"plc_tomb_sig"`
	PlcTombPrev         string    `ch:"plc_tomb_prev"`
	PlcTombType         string    `ch:"plc_tomb_type"`
	LegacyOpSig         string    `ch:"legacy_op_sig"`
	LegacyOpPrev        string    `ch:"legacy_op_prev"`
	LegacyOpType        string    `ch:"legacy_op_type"`
	LegacyOpHandle      string    `ch:"legacy_op_handle"`
	LegacyOpService     string    `ch:"legacy_op_service"`
	LegacyOpSigningKey  string    `ch:"legacy_op_signing_key"`
	LegacyOpRecoveryKey string    `ch:"legacy_op_recovery_key"`
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

func (e *PLCEntry) prepareForClickhouse() (*ClickhousePLCEntry, error) {
	che := &ClickhousePLCEntry{
		Did:       e.Did,
		Cid:       e.Cid,
		Nullified: e.Nullified,
		CreatedAt: e.CreatedAt,
	}
	if e.Operation.PLCOperation != nil {
		pop := e.Operation.PLCOperation
		che.PlcOpSig = pop.Sig
		if pop.Prev != nil {
			che.PlcOpPrev = *pop.Prev
		}
		che.PlcOpType = pop.Type
		che.PlcOpAlsoKnownAs = pop.AlsoKnownAs
		che.PlcOpRotationKeys = pop.RotationKeys
		if e.Operation.PLCOperation.Services == nil {
			for _, s := range e.Operation.PLCOperation.Services {
				che.PlcOpServices = append(che.PlcOpServices, s.Endpoint)
			}
		}
		return che, nil
	} else if e.Operation.PLCTombstone != nil {
		che.PlcTombSig = e.Operation.PLCTombstone.Sig
		che.PlcTombPrev = e.Operation.PLCTombstone.Prev
		che.PlcTombType = e.Operation.PLCTombstone.Type
		return che, nil
	} else if e.Operation.LegacyPLCOperation != nil {
		lop := e.Operation.LegacyPLCOperation
		che.LegacyOpSig = lop.Sig
		che.LegacyOpPrev = lop.Prev
		che.LegacyOpType = lop.Type
		che.LegacyOpService = lop.Service
		che.LegacyOpHandle = lop.Handle
		che.LegacyOpSigningKey = lop.SigningKey
		che.LegacyOpRecoveryKey = lop.RecoveryKey
		return che, nil
	}

	return nil, fmt.Errorf("no valid plc operation type")
}
