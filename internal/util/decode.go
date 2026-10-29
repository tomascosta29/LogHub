package util

import "github.com/mitchellh/mapstructure"

// DecodeProperties decodes a map of properties into a target struct using
// mapstructure tags. If no mapstructure tag is present, it falls back to the
// field name.
func DecodeProperties(props map[string]interface{}, target interface{}) error {
	if err := decodeWithTag(props, target, "mapstructure"); err != nil {
		return err
	}
	return decodeWithTag(props, target, "json")
}

func decodeWithTag(props map[string]interface{}, target interface{}, tagName string) error {
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           target,
		TagName:          tagName,
		WeaklyTypedInput: true,
	})
	if err != nil {
		return err
	}
	return decoder.Decode(props)
}
