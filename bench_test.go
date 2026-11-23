package limiter

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// stubDDB always allows (idle path) and reports the id as registered.
type stubDDB struct{ meta *dynamodb.GetItemOutput }

func (s *stubDDB) GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return s.meta, nil
}

func (s *stubDDB) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (s *stubDDB) BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (s *stubDDB) DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

func newStubDDB(t testing.TB) *stubDDB {
	meta, err := attributevalue.MarshalMap(&registrationItem{BucketIDShardID: "k" + metaSuffix, RegisteredAt: 1})
	if err != nil {
		t.Fatal(err)
	}
	return &stubDDB{meta: &dynamodb.GetItemOutput{Item: meta}}
}

func BenchmarkAllow(b *testing.B) {
	rl, err := New(testConfig(), newStubDDB(b))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := rl.Allow(ctx, "k"); err != nil {
			b.Fatal(err)
		}
	}
}
