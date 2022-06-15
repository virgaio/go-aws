package dynamodb

import (
	"net/url"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type RecordParamHelper struct {
	Params  url.Values
	Builder *expression.UpdateBuilder
	Changed bool
}

func (rph *RecordParamHelper) AddToBuilder(key, val string) {
	if rph.Builder == nil {
		rph.Builder = &expression.UpdateBuilder{}
	}
	b := rph.Builder.Set(expression.Name(key), expression.Value(val))
	rph.Builder = &b
	rph.Changed = true
}

func (rph *RecordParamHelper) SetStringFromParam(loc *string, key string) {
	if loc == nil || key == "" || rph == nil || rph.Params == nil {
		return
	}
	if _, ok := rph.Params[key]; ok {
		val := rph.Params.Get(key)
		*loc = val
		if rph.Builder == nil {
			rph.Builder = &expression.UpdateBuilder{}
		}
		b := rph.Builder.Set(expression.Name(key), expression.Value(val))
		rph.Builder = &b
		rph.Changed = true
	}
}

func (rph *RecordParamHelper) SetFloatFromParam(loc *float64, key string) {
	if loc == nil || key == "" || rph == nil || rph.Params == nil {
		return
	}
	if _, ok := rph.Params[key]; ok {
		val := rph.Params.Get(key)
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return
		}
		*loc = f
		if rph.Builder == nil {
			rph.Builder = &expression.UpdateBuilder{}
		}
		b := rph.Builder.Set(expression.Name(key), expression.Value(f))
		rph.Builder = &b
		rph.Changed = true
	}
}

func (rph *RecordParamHelper) SetIntFromParam(loc *int, key string) {
	if loc == nil || key == "" || rph == nil || rph.Params == nil {
		return
	}
	if _, ok := rph.Params[key]; ok {
		val := rph.Params.Get(key)
		if val == "" {
			b := rph.Builder.Set(expression.Name(key), expression.Value(val))
			rph.Builder = &b
			rph.Changed = true
			return
		}
		i, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return
		}
		*loc = int(i)
		if rph.Builder == nil {
			rph.Builder = &expression.UpdateBuilder{}
		}
		b := rph.Builder.Set(expression.Name(key), expression.Value(i))
		rph.Builder = &b
		rph.Changed = true
	}
}

func (rph *RecordParamHelper) SetInt64FromParam(loc *int64, key string) {
	if loc == nil || key == "" || rph == nil || rph.Params == nil {
		return
	}
	if _, ok := rph.Params[key]; ok {
		val := rph.Params.Get(key)
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return
		}
		*loc = i
		if rph.Builder == nil {
			rph.Builder = &expression.UpdateBuilder{}
		}
		b := rph.Builder.Set(expression.Name(key), expression.Value(i))
		rph.Builder = &b
		rph.Changed = true
	}
}

func (rph *RecordParamHelper) SetBoolFromParam(loc *bool, key string) {
	if loc == nil || key == "" || rph == nil || rph.Params == nil {
		return
	}
	if _, ok := rph.Params[key]; ok {
		val := rph.Params.Get(key)
		b, err := strconv.ParseBool(val)
		if err != nil {
			return
		}
		*loc = b
		if rph.Builder == nil {
			rph.Builder = &expression.UpdateBuilder{}
		}
		bldr := rph.Builder.Set(expression.Name(key), expression.Value(b))
		rph.Builder = &bldr
		rph.Changed = true
	}
}
