package main

import (
	"context"
	"fmt"
	"os"

	"targeted_refresh/db"
)

var query string = `
select
tasks.id,
	tasks.source_id,
	tasks.target_source_ref,
	tasks.forwardable_headers,
	sources.uid as source_uid
from
	"tasks"
inner join "sources" on
	"sources"."id" = "tasks"."source_id"
where
	"tasks"."state" = 'running'
	and "tasks"."target_type" = 'ServiceInstance'
order by
	source_id;
`

func main() {
	fmt.Println("Starting Targeted Refresh...")

	// conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	conn, err := db.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var id, source_id int32
	var target_source_ref, forwardable_headers, source_uid string
	for rows.Next() {
		rows.Scan(&id, &source_id, &target_source_ref, &forwardable_headers, &source_uid)
		// TODO: Scan everything, then put stuff on the queue.
		fmt.Println(id, target_source_ref)
	}

	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "Error handling running task: %v\n", rows.Err())
		os.Exit(1)
	}

	fmt.Println("Targeted Refresh Successfully Completed.")
}
