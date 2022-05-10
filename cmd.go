package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"os"
	"time"

	_ "embed"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	smithy "github.com/aws/smithy-go"
	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
)

const efoConsumerName string = "khs-062DE8C182964A218E936DF0938F48B3"

//go:embed template.html
var outputTemplate string

type Aggregator interface {
	Name() string
	Aggregate(record *types.Record)
	Result() interface{}
}

type aggregatedResult struct {
	ShardID     string
	ChildShards []types.ChildShard
	Error       error
	Result      map[string]interface{}
}

// cmd Implements the main workflow for hot shard analysis.
// It first creates an EFO consumer. Then it enumerate shards
// and for each top level shard to create an aggregator pipeline
// to analyse usage stats.
// An aggregation pipeline will return shard stats an any child
// shards that should be processed.
// cmd main workflow will continue to process those child shards
// until all aggregation pipelines return an empty slice of children.
type cmd struct {
	streamName             string
	kds                    kds
	aggregators            []Aggregator
	shardTree              map[string][]string
	limit                  int
	period                 *period
	aggregatorStackBuilder func() []Aggregator
}

func (c *cmd) Start(ctx context.Context) error {
	color.Yellow("Creating an EFO consumer...")
	streamArn, consumerArn, err := c.ensureEFOConsumer(ctx)
	if err != nil {
		return err
	}
	defer c.deregisterConsumer(ctx, streamArn, consumerArn)
	fmt.Print(color.YellowString("Consumer ARN %s OK!", consumerArn))
	fmt.Print(color.YellowString("Listing shards for stream %s...", c.streamName))
	shards, err := c.listShards(ctx, c.streamName)
	if err != nil {
		return err
	}
	color.Yellow("OK!")
	// Store all shard ids in a map so that we can
	// quickly check if a given shard id exists in the
	// list or not.
	shardsSet := make(map[string]bool)
	for _, shard := range shards {
		shardsSet[*shard.ShardId] = true
	}
	// Hold results from each enumeration in a map
	// keyed by shard ID.
	results := make(map[string]map[string]interface{})
	resultsChan := make(chan *aggregatedResult)
	bar := pb.StartNew(len(shards))
	pendingEnumerations := 0
	for _, shard := range shards {
		hasActiveParent := false
		if shard.ParentShardId != nil {
			_, hasActiveParent = shardsSet[*shard.ParentShardId]
		}
		if !hasActiveParent {
			pendingEnumerations++
			go c.aggregateAndReport(ctx, resultsChan, *shard.ShardId, *consumerArn)
		}
	}
	for pendingEnumerations > 0 {
		select {
		case r := <-resultsChan:
			pendingEnumerations--
			bar.Increment()
			results[r.ShardID] = r.Result
			for _, cs := range r.ChildShards {
				pendingEnumerations++
				go c.aggregateAndReport(ctx, resultsChan, *cs.ShardId, *consumerArn)
			}
		case <-ctx.Done():
			break
		}
	}
	close(resultsChan)
	bar.Finish()
	color.Yellow("Generating output...")
	c.generateReport(results)
	color.Yellow("Done")
	return nil
}

func (c *cmd) aggregateAndReport(ctx context.Context, resultsChan chan<- *aggregatedResult, shardID, consumerArn string) {
	result := c.aggregateShard(ctx, shardID, consumerArn)
	if result != nil {
		select {
		case resultsChan <- result:
		case <-ctx.Done():
		}
	}
}

func (c *cmd) aggregateShard(ctx context.Context, shardID string, consumerArn string) *aggregatedResult {
	aggregators := c.aggregatorStackBuilder()
	for {
		subscription, err := c.kds.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN: &consumerArn,
			ShardId:     &shardID,
			StartingPosition: &types.StartingPosition{
				Type:      types.ShardIteratorTypeAtTimestamp,
				Timestamp: &c.period.start,
			},
		})
		if err != nil {
			return &aggregatedResult{Error: err}
		}
		subscribed := true
		for subscribed {
			select {
			case event, ok := <-subscription.GetStream().Events():
				if !ok {
					subscribed = false
				} else {
					if tevent, ok := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent); ok {
						value := tevent.Value
						stop := false
						for _, r := range value.Records {
							if c.period.end.Sub(*r.ApproximateArrivalTimestamp) > 0 {
								for _, a := range aggregators {
									a.Aggregate(&r)
								}
							} else {
								stop = true
								break
							}
						}
						if *value.MillisBehindLatest == 0 || stop {
							r := make(map[string]interface{})
							for _, a := range c.aggregators {
								r[a.Name()] = a.Result()
							}
							return &aggregatedResult{
								ShardID:     shardID,
								ChildShards: value.ChildShards,
								Result:      r,
							}
						}
					}
				}
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (i *cmd) listShards(ctx context.Context, streamName string) ([]types.Shard, error) {
	var (
		ntoken *string
	)
	r := make([]types.Shard, 0)
	for {
		lso, err := i.kds.ListShards(ctx, &kinesis.ListShardsInput{
			StreamName: &streamName,
			NextToken:  ntoken,
		})
		if err != nil {
			return nil, err
		}
		r = append(r, lso.Shards...)
		ntoken = lso.NextToken
		if ntoken == nil {
			break
		}
	}
	return r, nil
}

func (i *cmd) generateReport(stats map[string]map[string]interface{}) {
	type shardStats struct {
		ShardID string                 `json:"shardId"`
		Stats   map[string]interface{} `json:"stats"`
	}
	type report struct {
		From   int          `json:"from"`
		Shards []shardStats `json:"shards"`
	}
	data := make([]shardStats, 0)
	for sid, v := range stats {
		data = append(data, shardStats{sid, v})
	}
	r := &report{
		From:   int(i.period.start.Unix()),
		Shards: data,
	}
	buf, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	t, err := template.New("output").Funcs(template.FuncMap{
		"trustedJS": func(s string) template.JS {
			return template.JS(s)
		},
		"trustedHTML": func(s string) template.HTML {
			return template.HTML(s)
		},
	}).Parse(outputTemplate)
	if err != nil {
		panic(err)
	}
	fname := "out.html"
	file, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Sync()
	defer file.Close()
	err = t.Execute(file, map[string]interface{}{
		"Date":   time.Now(),
		"Limit":  i.limit,
		"Report": template.JS(string(buf)),
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("output is written to %s\n", fname)
}

func (c *cmd) ensureEFOConsumer(ctx context.Context) (*string, *string, error) {
	stream, err := c.kds.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: &c.streamName,
	})
	if err != nil {
		return nil, nil, err
	}
	consumer, err := c.kds.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
		ConsumerName: aws.String(efoConsumerName),
		StreamARN:    stream.StreamDescriptionSummary.StreamARN,
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if _, ok := apiErr.(*types.ResourceNotFoundException); ok {
				c, err := c.kds.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
					ConsumerName: aws.String(efoConsumerName),
					StreamARN:    stream.StreamDescriptionSummary.StreamARN,
				})
				if err == nil {
					return stream.StreamDescriptionSummary.StreamARN, c.Consumer.ConsumerARN, nil
				}
			}
		}
		return nil, nil, err
	}
	return stream.StreamDescriptionSummary.StreamARN, consumer.ConsumerDescription.ConsumerARN, nil
}

func (c *cmd) deregisterConsumer(ctx context.Context, streamArn, consumerArn *string) error {
	_, err := c.kds.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
		StreamARN:   streamArn,
		ConsumerARN: consumerArn,
	})
	return err
}

func newCMD(streamName string, kds kds, aggregators []Aggregator, limit int, p *period) *cmd {
	return &cmd{
		kds:         kds,
		streamName:  streamName,
		aggregators: aggregators,
		limit:       limit,
		period:      p,
		shardTree:   make(map[string][]string),
	}
}
