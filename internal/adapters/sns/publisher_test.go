package sns

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/ports"
)

type fakeSNS struct {
	input *awssns.PublishInput
	out   *awssns.PublishOutput
	err   error
}

func (f *fakeSNS) Publish(_ context.Context, in *awssns.PublishInput, _ ...func(*awssns.Options)) (*awssns.PublishOutput, error) {
	f.input = in
	return f.out, f.err
}

func discardLogger() ports.Logger {
	return observability.NewSlogLoggerFromHandler(slog.NewJSONHandler(io.Discard, nil))
}

func sampleEvent() ports.PendingEvent {
	return ports.PendingEvent{
		ID:            uuid.New(),
		AggregateID:   uuid.New(),
		AggregateType: "transaction",
		EventType:     ports.EventTypePaymentSucceeded,
		Payload:       []byte(`{"transaction_id":"abc"}`),
		EventVersion:  1,
		CreatedAt:     time.Now(),
	}
}

func TestPublisher_PublishesToTopicWithAttributes(t *testing.T) {
	fake := &fakeSNS{out: &awssns.PublishOutput{MessageId: aws.String("msg-1")}}
	p := NewPublisher(fake, "arn:aws:sns:us-east-1:123:payment-events", discardLogger())
	ev := sampleEvent()

	if err := p.Publish(context.Background(), ev); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fake.input == nil {
		t.Fatal("Publish was not called")
	}
	if got := aws.ToString(fake.input.TopicArn); got != "arn:aws:sns:us-east-1:123:payment-events" {
		t.Errorf("wrong topic ARN: %s", got)
	}
	if got := aws.ToString(fake.input.Message); got != string(ev.Payload) {
		t.Errorf("message should be the raw payload, got %s", got)
	}

	// The dedup key (outbox event id) and routing type must ride as attributes,
	// so consumers can de-duplicate at-least-once redeliveries and filter.
	attrs := fake.input.MessageAttributes
	if got := aws.ToString(attrs["event_id"].StringValue); got != ev.ID.String() {
		t.Errorf("event_id attribute = %s, want %s", got, ev.ID)
	}
	if got := aws.ToString(attrs["event_type"].StringValue); got != ev.EventType {
		t.Errorf("event_type attribute = %s, want %s", got, ev.EventType)
	}
	if got := aws.ToString(attrs["aggregate_type"].StringValue); got != ev.AggregateType {
		t.Errorf("aggregate_type attribute = %s, want %s", got, ev.AggregateType)
	}
}

func TestPublisher_PropagatesPublishError(t *testing.T) {
	fake := &fakeSNS{err: errors.New("throttled")}
	p := NewPublisher(fake, "arn:topic", discardLogger())

	err := p.Publish(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("expected error to propagate so the relay retries/dead-letters")
	}
}
