package repositorx

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type Core[T any] struct {
	db *sqlx.DB
}

func NewCore[T any](db *sqlx.DB) Core[T] {
	return Core[T]{
		db: db,
	}
}

func (c Core[T]) Delete(query string, args ...any) error {
	return c.DeleteWithContext(context.Background(), query, args...)
}

func (c Core[T]) DeleteWithContext(ctx context.Context, query string, args ...any) error {
	_, err := c.db.ExecContext(ctx, query, args...)
	return err
}

func (c Core[T]) Find(query string, args ...any) (T, error) {
	return c.FindWithContext(context.Background(), query, args...)
}

func (c Core[T]) FindWithContext(ctx context.Context, query string, args ...any) (T, error) {
	var t T
	err := c.db.GetContext(ctx, &t, query, args...)
	return t, err
}

func (c Core[T]) Insert(query string, args ...any) error {
	return c.InsertWithContext(context.Background(), query, args...)
}

func (c Core[T]) InsertWithContext(ctx context.Context, query string, args ...any) error {
	res, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		return fmt.Errorf("expected to affect 1 row, affected %d", rows)
	}

	return nil
}

func (c Core[T]) List(query string, args ...any) ([]T, error) {
	return c.ListWithContext(context.Background(), query, args...)
}

func (c Core[T]) ListWithContext(ctx context.Context, query string, args ...any) ([]T, error) {
	var ts []T
	err := c.db.SelectContext(ctx, &ts, query, args...)
	return ts, err
}

func (c Core[T]) Update(query string, args ...any) error {
	return c.UpdateWithContext(context.Background(), query, args...)
}

func (c Core[T]) UpdateWithContext(ctx context.Context, query string, args ...any) error {
	_, err := c.db.ExecContext(ctx, query, args...)
	return err
}
