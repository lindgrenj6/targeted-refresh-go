package db

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

func Connect() (*pgx.Conn, error) {
	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		os.Getenv("DATABASE_USER"),
		os.Getenv("DATABASE_PASSWORD"),
		os.Getenv("DATABASE_HOST"),
		os.Getenv("DATABASE_PORT"),
		os.Getenv("DATABASE_NAME"),
	)

	return pgx.Connect(context.Background(), url)
}
