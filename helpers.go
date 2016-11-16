package pilosago

import (
	"github.com/umbel/pilosa/pql"
	"time"
)

// convenience wrappers around pql types
func ClearBit(id uint64, frame string, profileID uint64) *pql.ClearBit {
	return &pql.ClearBit{
		ID:        id,
		Frame:     frame,
		ProfileID: profileID,
	}
}

func Count(bm pql.BitmapCall) *pql.Count {
	return &pql.Count{
		Input: bm,
	}
}

func Profile(id uint64) *pql.Profile {
	return &pql.Profile{
		ID: id,
	}
}

func SetBit(id uint64, frame string, profileID uint64) *pql.SetBit {
	return &pql.SetBit{
		ID:        id,
		Frame:     frame,
		ProfileID: profileID,
	}
}

func SetBitmapAttrs(id uint64, frame string, attrs map[string]interface{}) *pql.SetBitmapAttrs {
	return &pql.SetBitmapAttrs{
		ID:    id,
		Frame: frame,
		Attrs: attrs,
	}
}

func SetProfileAttrs(id uint64, attrs map[string]interface{}) *pql.SetProfileAttrs {
	return &pql.SetProfileAttrs{
		ID:    id,
		Attrs: attrs,
	}
}

func TopN(frame string, n int, src pql.BitmapCall, bmids []uint64, field string, filters []interface{}) *pql.TopN {
	return &pql.TopN{
		Frame:     frame,
		N:         n,
		Src:       src,
		BitmapIDs: bmids,
		Field:     field,
		Filters:   filters,
	}
}

func Difference(bms ...pql.BitmapCall) *pql.Difference {
	// TODO does this need to be limited to two inputs?
	return &pql.Difference{
		Inputs: bms,
	}
}

func Intersect(bms ...pql.BitmapCall) *pql.Intersect {
	return &pql.Intersect{
		Inputs: bms,
	}
}

func Union(bms ...pql.BitmapCall) *pql.Union {
	return &pql.Union{
		Inputs: bms,
	}
}

func Bitmap(id uint64, frame string) *pql.Bitmap {
	return &pql.Bitmap{
		ID:    id,
		Frame: frame,
	}
}

func Range(id uint64, frame string, start time.Time, end time.Time) *pql.Range {
	return &pql.Range{
		ID:        id,
		Frame:     frame,
		StartTime: start,
		EndTime:   end,
	}
}
