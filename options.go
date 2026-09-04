package processor

import "context"

func OnlyErrorWait(f func(ctx context.Context) error) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		err := f(ctx)
		if err != nil {
			return true, err
		}

		return false, nil
	}
}

func AlwaysWait(f func(ctx context.Context) error) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		return true, f(ctx)
	}
}

func SkipWait(f func(ctx context.Context) error) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		return false, f(ctx)
	}
}
