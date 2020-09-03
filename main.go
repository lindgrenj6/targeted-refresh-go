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
	SourceId  string                 `json:"source_id"`
	SourceUid string                 `json:"source_uid"`
	Params    map[string]interface{} `json:"params"`
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
		// scan the values from the row into our vars
		rows.Scan(&task_id, &source_id, &target_source_ref, &forwardable_headers, &source_uid)

		// make the params hash
		params := make(map[string]interface{})
		params["task_id"] = strconv.Itoa(task_id)
		params["source_ref"] = target_source_ref
		params["request_context"] = forwardable_headers
		fmt.Fprintf(os.Stdout, "params: %s\n", params)

		// Create the struct to marshal into JSON
		t := TargetedTaskUpdate{strconv.Itoa(source_id), source_uid, params}
		msg, err := json.Marshal(t)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling json: %v\n", err)
		}

		// TODO: remove, this is just validating the msg/struct marshaling
		fmt.Fprintf(os.Stdout, "json source: %s\n", msg)
		var tu TargetedTaskUpdate
		err = json.Unmarshal(msg, &tu)
		fmt.Fprintf(os.Stdout, "struct unmarshaled: %s\n", tu)
	}
	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "Error handling running task: %v\n", rows.Err())
		os.Exit(1)
	}

	fmt.Println("Targeted Refresh Successfully Completed.")
}
