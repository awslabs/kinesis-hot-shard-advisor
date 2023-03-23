// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package analyse

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/kinesis-hot-shard-advisor/analyse/service"
	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
)

const efoConsumerName string = "khs-062DE8C182964A218E936DF0938F48B3"

// CMD represents the cli command for analysing kinesis data streams.
type CMD struct {
	streamName string
	shardIDs   []string
	start      time.Time
	end        time.Time
	discover   discover
	efo        efo
	output     output
	processor  shardProcessor
}

func NewCMD(streamName string, kds service.KDS, reporter service.Reporter, aggregatorBuilder service.AggregatorBuilder, limit, top int, start, end time.Time, shardIDs []string) *CMD {
	return newCMD(
		streamName,
		shardIDs,
		service.NewDiscover(streamName, kds),
		service.NewEFO(streamName, efoConsumerName, kds),
		service.NewOutput(start, limit, top, reporter),
		service.NewShardProcessor(kds, aggregatorBuilder, start, end),
	)
}

func newCMD(streamName string, shardIDs []string, discover discover, efo efo, output output, processor shardProcessor) *CMD {
	return &CMD{
		streamName: streamName,
		shardIDs:   shardIDs,
		discover:   discover,
		efo:        efo,
		output:     output,
		processor:  processor,
	}
}

// Start starts the execution of stream analysis workflow outlined below.
//   - Create a new EFO consumer
//   - Read all shards in the stream
//     Shards are read concurrently without loosing the order of messages
//   - Generate the output
//   - Delete EFO consumer
func (c *CMD) Start(ctx context.Context) error {
	var bar *pb.ProgressBar
	color.Green("Stream: %s\nFrom: %v\nTo: %v", c.streamName, c.start, c.end)
	fmt.Print(color.YellowString("Creating an EFO consumer..."))
	streamArn, consumerArn, err := c.efo.EnsureEFOConsumer(ctx)
	if err != nil {
		return err
	}
	defer c.efo.DeregisterConsumer(streamArn, consumerArn)
	color.Yellow(": %s OK!\n", *consumerArn)
	output := make([]*service.ProcessOutput, 0)
	if len(c.shardIDs) > 0 {
		bar = pb.StartNew(len(c.shardIDs))
		output, err = c.processor.Process(ctx, *consumerArn, c.shardIDs, false, func() { bar.Increment() })
		if err != nil {
			return err
		}
	} else {
		fmt.Print(color.YellowString("Listing shards for stream %s...", c.streamName))
		shardIDs, l, err := c.discover.ParentShards(ctx)
		if err != nil {
			return err
		}
		color.Yellow(" OK!")
		bar = pb.StartNew(l)
		output, err = c.processor.Process(ctx, *consumerArn, shardIDs, true, func() { bar.Increment() })
		if err != nil {
			return err
		}
	}
	bar.Finish()

	fmt.Print(color.YellowString("Generating output..."))
	err = c.output.Write(output)
	if err != nil {
		return err
	}
	color.Yellow("OK!")
	return nil
}

func (c *CMD) deregisterConsumer(streamArn, consumerArn *string) {
	err := c.efo.DeregisterConsumer(streamArn, consumerArn)
	if err != nil {
		color.Cyan("FAILED!")
		color.Red("%v", err)
	} else {
		color.Yellow("OK!")
	}
}

type discover interface {
	ParentShards(ctx context.Context) ([]string, int, error)
}

type efo interface {
	EnsureEFOConsumer(ctx context.Context) (*string, *string, error)
	DeregisterConsumer(streamArn, consumerArn *string) error
}

type output interface {
	Write(aggregatedShards []*service.ProcessOutput) error
}

type shardProcessor interface {
	Process(ctx context.Context, consumerArn string, parentShardIDs []string, children bool, progress func()) ([]*service.ProcessOutput, error)
}
