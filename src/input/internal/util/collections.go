package util

// SelectDeref returns a collection of item pointers as a collection of their dereferenced values
func SelectDeref[Tin any](in []*Tin) []Tin {
	return Select(in, func(v *Tin) Tin {
		return *v
	})
}

// Select projects a collection of items into another form, determined by the provided function
func Select[Tin any, Tout any](in []Tin, fn func(Tin) Tout) []Tout {
	out := make([]Tout, 0, len(in))
	for i, v := range in {
		out[i] = fn(v)
	}

	return out
}
