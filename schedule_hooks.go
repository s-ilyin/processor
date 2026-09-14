package processor

import (
	"context"
	"log/slog"
)

func (p *Processor) onStart(ctx context.Context) {
	if p.hooks.onStart == nil {
		return
	}
	p.safeHook("on_start", func() { p.hooks.onStart(ctx) })
}

func (p *Processor) onStop(ctx context.Context) {
	if p.hooks.onStop == nil {
		return
	}
	p.safeHook("on_stop", func() { p.hooks.onStop(ctx) })
}

func (p *Processor) onError(ctx context.Context, err error) {
	if p.hooks.onError == nil {
		return
	}
	p.safeHook("on_error", func() { p.hooks.onError(ctx, err) })
}

func (p *Processor) onPanic(ctx context.Context, rec any) {
	if p.hooks.onPanic == nil {
		return
	}
	p.safeHook("on_panic", func() { p.hooks.onPanic(ctx, rec) })
}

func (p *Processor) safeHook(name string, fn func()) {
	if fn == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("hook panic",
				slog.String("processor", p.config.Name),
				slog.String("hook", name),
				slog.Any("recover", r),
			)
		}
	}()
	fn()
}
