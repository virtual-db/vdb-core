// Package types — json.go
//
// The JSON form of a Value (doc 06 §2.2): a string when its bytes are valid
// UTF-8, null when NULL, otherwise {"base64": "..."}.
package types

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// MarshalJSON implements json.Marshaler.
func (v Value) MarshalJSON() ([]byte, error) {
	if v.Null {
		return []byte("null"), nil
	}
	if utf8.Valid(v.B) {
		return json.Marshal(string(v.B))
	}
	return json.Marshal(map[string]string{"base64": base64.StdEncoding.EncodeToString(v.B)})
}

// UnmarshalJSON implements json.Unmarshaler.
func (v *Value) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*v = Value{Null: true}
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*v = Value{B: []byte(s)}
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return fmt.Errorf("types.Value: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(m["base64"])
	if err != nil {
		return fmt.Errorf("types.Value: base64: %w", err)
	}
	*v = Value{B: raw}
	return nil
}
