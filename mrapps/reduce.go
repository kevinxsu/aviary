import "strconv"

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
