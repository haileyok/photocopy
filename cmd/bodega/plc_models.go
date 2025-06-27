package main

import (
	"time"
)

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
