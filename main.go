package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"targeted_refresh/db"
	"targeted_refresh/kafka"
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

type TargetedTaskUpdate struct {
	source_id  string
	source_uid string
	params     []map[string]string
}

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

	kafka := kafka.Producer()
	defer kafka.Close()

	var task_id, source_id int
	var target_source_ref, forwardable_headers, source_uid string
	for rows.Next() {
		params := make(map[string]string)
		rows.Scan(&task_id, &source_id, &target_source_ref, &forwardable_headers, &source_uid)
		params["task_id"] = strconv.Itoa(task_id)
		params["source_ref"] = target_source_ref
		params["request_context"] = forwardable_headers

		fmt.Fprintf(os.Stdout, "params: %s\n", params)

		t := TargetedTaskUpdate{strconv.Itoa(source_id), source_uid, []map[string]string{params}}
		fmt.Println(t.source_uid)
		msg, err := json.Marshal(t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling json: %v\n", err)
		}

		fmt.Fprintf(os.Stdout, "json source: %s\n", msg)
	}
	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "Error handling running task: %v\n", rows.Err())
		os.Exit(1)
	}

	fmt.Println("Targeted Refresh Successfully Completed.")
}
