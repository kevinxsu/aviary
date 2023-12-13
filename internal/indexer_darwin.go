package aviary

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
)

func IMap(document string, value string) (res []KeyValue) {
	m := make(map[string]bool)
	words := strings.FieldsFunc(value, func(x rune) bool { return !unicode.IsLetter(x) })
	for _, w := range words {
		m[w] = true
	}
	for w := range m {
		kv := KeyValue{w, document}
		res = append(res, kv)
	}
	return
}

func IReduce(key string, values []string) string {
	sort.Strings(values)
	return fmt.Sprintf("%d %s", len(values), strings.Join(values, ","))
}
