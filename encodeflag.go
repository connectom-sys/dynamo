package dynamo

import (
	"reflect"
)

type encodeFlags uint

const (
	flagSet encodeFlags = 1 << iota
	flagOmitEmpty
	flagOmitEmptyElem
	flagAllowEmpty
	flagAllowEmptyElem
	flagNull
	flagUnixTime
	flagString

	flagNone encodeFlags = 0
)

func fieldInfo(field reflect.StructField) (name string, tagName string, flags encodeFlags) {
	tagName = "dynamo"
	tag := field.Tag.Get(tagName)
	if tag == "" {
		// dynamo tagが無い場合は、json tagを見る
		tagName = "json"
		tag = field.Tag.Get(tagName)
		if tag == "" {
			return field.Name, "", flagNone
		}
	}

	begin := 0
	for i := 0; i <= len(tag); i++ {
		if !(i == len(tag) || tag[i] == ',') {
			continue
		}
		part := tag[begin:i]
		begin = i + 1

		if name == "" {
			if part == "" {
				name = field.Name
			} else {
				name = part
			}
			continue
		}

		switch part {
		case "set":
			flags |= flagSet
		case "omitempty":
			flags |= flagOmitEmpty
		case "omitemptyelem":
			flags |= flagOmitEmptyElem
		case "allowempty":
			flags |= flagAllowEmpty
		case "allowemptyelem":
			flags |= flagAllowEmptyElem
		case "null":
			flags |= flagNull
		case "unixtime":
			flags |= flagUnixTime
		case "string": // json tagのstring
			flags |= flagString
		}
	}

	return
}
