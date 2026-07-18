package sns

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"samarth/payment-service/internal/ports"
)

type PublishAPI interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type Publisher struct {
	client               PublishAPI
	topicARN             string
	log                  ports.Logger
	aggregateVersionAttr bool
}

func NewPublisher(client PublishAPI, topicARN string, aggregateVersionAttr bool, log ports.Logger) *Publisher {
	return &Publisher{client: client, topicARN: topicARN, aggregateVersionAttr: aggregateVersionAttr, log: log}
}

func NewPublisherFromConfig(ctx context.Context, region, topicARN string, aggregateVersionAttr bool, log ports.Logger) (*Publisher, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("sns: load aws config: %w", err)
	}
	return NewPublisher(sns.NewFromConfig(awsCfg), topicARN, aggregateVersionAttr, log), nil
}

func (p *Publisher) Publish(ctx context.Context, event ports.PendingEvent) error {
	out, err := p.client.Publish(ctx, &sns.PublishInput{
		TopicArn:          aws.String(p.topicARN),
		Message:           aws.String(string(event.Payload)),
		MessageAttributes: p.attributes(event),
	})
	if err != nil {
		return fmt.Errorf("sns: publish %s (%s): %w", event.ID, event.EventType, err)
	}

	messageID := ""
	if out != nil && out.MessageId != nil {
		messageID = *out.MessageId
	}
	p.log.Info(ports.LogEventOutboxPublish, map[string]any{
		"event_type":     event.EventType,
		"aggregate_id":   event.AggregateID.String(),
		"event_id":       event.ID.String(),
		"event_version":  event.EventVersion,
		"sns_message_id": messageID,
		"sink":           "sns",
	})
	return nil
}

func (p *Publisher) attributes(event ports.PendingEvent) map[string]types.MessageAttributeValue {
	str := func(v string) types.MessageAttributeValue {
		return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	attrs := map[string]types.MessageAttributeValue{
		"event_id":       str(event.ID.String()),
		"event_type":     str(event.EventType),
		"aggregate_type": str(event.AggregateType),
		"aggregate_id":   str(event.AggregateID.String()),
		"event_version":  str(strconv.Itoa(event.EventVersion)),
	}
	if p.aggregateVersionAttr {
		attrs["aggregate_version"] = types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(strconv.Itoa(event.AggregateVersion)),
		}
	}
	return attrs
}
