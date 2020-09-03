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

	producer := kafka.Producer()
	defer producer.Close()

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

		// Write the message out to kafka
		producer.WriteMessages(context.TODO(), kafka.Message("ServiceInstance.Refresh", string(msg)))
	}

	if rows.Err() != nil {
		fmt.Fprintf(os.Stderr, "Error handling running tasks: %v\n", rows.Err())
		os.Exit(1)
	}

	fmt.Println("Targeted Refresh Successfully Completed.")
}
