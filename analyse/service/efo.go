// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/fatih/color"
)

// EFO Service is used to reliably create and delete
// an EFO consumer for processing the stream.
type EFO struct {
	stream          string
	efoConsumerName string
	kds             KDS
}

func NewEFO(stream, efoConsumerName string, kds KDS) *EFO {
	return &EFO{
		stream:          stream,
		efoConsumerName: efoConsumerName,
		kds:             kds,
	}
}

func (e *EFO) EnsureEFOConsumer(ctx context.Context) (*string, *string, error) {
	stream, err := e.kds.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: &e.stream,
	})
	if err != nil {
		return nil, nil, err
	}
	for {
		consumer, err := e.kds.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
			ConsumerName: aws.String(e.efoConsumerName),
			StreamARN:    stream.StreamDescriptionSummary.StreamARN,
		})
		if err != nil {
			var rnf *types.ResourceNotFoundException
			if !errors.As(err, &rnf) {
				return nil, nil, err
			}
			_, err := e.kds.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
				ConsumerName: aws.String(e.efoConsumerName),
				StreamARN:    stream.StreamDescriptionSummary.StreamARN,
			})
			if err != nil {
				return nil, nil, err
			}
		} else {
			// Consumer is not available for servicing until after sometime they are created.
			// Therefore we wait until its status is Active.
			if consumer.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
				return stream.StreamDescriptionSummary.StreamARN, consumer.ConsumerDescription.ConsumerARN, nil
			}
			time.Sleep(time.Second * 5)
		}
	}
}

func (c *EFO) DeregisterConsumer(streamArn, consumerArn *string) error {
	fmt.Print(color.YellowString("Deleting EFO Consumer..."))
	_, err := c.kds.DeregisterStreamConsumer(context.Background(), &kinesis.DeregisterStreamConsumerInput{
		StreamARN:   streamArn,
		ConsumerARN: consumerArn,
	})
	return err
}
