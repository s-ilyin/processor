package processor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnlyErrorWait(t *testing.T) {
	t.Run("success without error", func(t *testing.T) {
		f := func(ctx context.Context) error {
			return nil
		}
		wrapped := OnlyErrorWait(f)
		ctx := context.Background()
		wait, err := wrapped(ctx)
		require.NoError(t, err)
		require.False(t, wait)
	})

	t.Run("error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		f := func(ctx context.Context) error {
			return expectedErr
		}
		wrapped := OnlyErrorWait(f)
		ctx := context.Background()
		wait, err := wrapped(ctx)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.True(t, wait)
	})
}

func TestAlwaysWait(t *testing.T) {
	t.Run("success without error", func(t *testing.T) {
		f := func(ctx context.Context) error {
			return nil
		}
		wrapped := AlwaysWait(f)
		ctx := context.Background()
		wait, err := wrapped(ctx)
		require.NoError(t, err)
		require.True(t, wait)
	})

	t.Run("error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		f := func(ctx context.Context) error {
			return expectedErr
		}
		wrapped := AlwaysWait(f)
		ctx := context.Background()
		wait, err := wrapped(ctx)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.True(t, wait)
	})
}

func TestSkipWait(t *testing.T) {
	t.Run("success without error", func(t *testing.T) {
		f := func(ctx context.Context) error {
			return nil
		}
		wrapped := SkipWait(f)
		ctx := context.Background()
		wait, err := wrapped(ctx)
		require.NoError(t, err)
		require.False(t, wait)
	})

	t.Run("error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		f := func(ctx context.Context) error {
			return expectedErr
		}
		wrapped := SkipWait(f)
		ctx := context.Background()
		wait, err := wrapped(ctx)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.False(t, wait)
	})
}

func TestContextPropagation(t *testing.T) {
	type testKey string

	key := testKey("key")

	testFunc := func(wrapper func(func(context.Context) error) func(context.Context) (bool, error), name string) {
		t.Run(name, func(t *testing.T) {
			f := func(ctx context.Context) error {
				if ctx.Value(key) == nil {
					return errors.New("context value not found")
				}
				return nil
			}

			wrapped := wrapper(f)
			ctx := context.WithValue(context.Background(), key, "value")
			_, err := wrapped(ctx)
			require.NoError(t, err)
		})
	}

	testFunc(OnlyErrorWait, "OnlyErrorWait")
	testFunc(AlwaysWait, "AlwaysWait")
	testFunc(SkipWait, "SkipWait")
}

func TestPanicPropagation(t *testing.T) {
	testFunc := func(wrapper func(func(context.Context) error) func(context.Context) (bool, error), name string) {
		t.Run(name, func(t *testing.T) {
			f := func(ctx context.Context) error {
				panic("test panic")
			}
			wrapped := wrapper(f)
			ctx := context.Background()
			require.Panics(t, func() { wrapped(ctx) })
		})
	}

	testFunc(OnlyErrorWait, "OnlyErrorWait")
	testFunc(AlwaysWait, "AlwaysWait")
	testFunc(SkipWait, "SkipWait")
}
