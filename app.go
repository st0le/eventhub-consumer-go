package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go"
)

func main() {

	connStr := flag.String("connection-string", "", "connection string of eventhub")
	flag.Parse()

	envVar := os.Getenv("EVENTHUB_CONNECTION_STRING")
	if *connStr == "" && envVar != "" {
		connStr = &envVar
	}

	hub, err := eventhub.NewHubFromConnectionString(*connStr)

	if err != nil {
		fmt.Println(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	handler := func(c context.Context, event *eventhub.Event) error {
		fmt.Println(string(event.Data))
		return nil
	}

	// listen to each partition of the Event Hub
	runtimeInfo, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, partitionID := range runtimeInfo.PartitionIDs {
		_, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithPrefetchCount(10000), eventhub.ReceiveWithLatestOffset())
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan

	hub.Close(context.Background())

}
