package udpcloner

func copySlice[T any](in []T) []T {
	result := make([]T, 0, len(in))
	result = append(result, in...)
	return result
}

func copyMap[K comparable, V any](in map[K]V) map[K]V {
	result := make(map[K]V, len(in))
	for k, v := range in {
		result[k] = v
	}
	return result
}
