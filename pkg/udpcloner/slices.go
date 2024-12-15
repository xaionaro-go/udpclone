package udpcloner

func copySlice[T any](in []T) []T {
	result := make([]T, 0, len(in))
	result = append(result, in...)
	return result
}
