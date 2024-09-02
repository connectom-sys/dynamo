package dynamo

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go"
)

type ServerResponse struct {
	EmbeddedID int64 `json:"embeddedId,omitempty,string"`
}

type TestAddition struct {
	Data           int64 `json:"data,omitempty,string"`
	ServerResponse `json:"-"`
}

type TestItem struct {
	ID       int          `dynamo:"id,hash" json:"id"`
	Name     string       `dynamo:"name,range" json:"name"`
	Addition TestAddition `dynamo:"addition,omitempty"`
}

func isTableExists(err error) bool {
	var aerr smithy.APIError
	if errors.As(err, &aerr) {
		if aerr.ErrorCode() == "ResourceInUseException" {
			return true
		}
	}
	return false
}

func TestTableAccessAWS(t *testing.T) {
	if os.Getenv("DYNAMO_TEST_REGION") != "" {
		t.Skip("online test skipped")
	}
	os.Setenv("AWS_REGION", "ap-northeast-1")
	os.Setenv("AWS_PROFILE", "media-dev")

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(os.Getenv("AWS_REGION")),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(RetryTxConflicts)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	db := New(cfg)
	tableName := "karasawa_ls_test_table"
	if err := db.CreateTable(tableName, TestItem{}).OnDemand(true).Run(ctx); err != nil && !isTableExists(err) {
		t.Fatal(err)
	}
	tbl := db.Table(tableName)
	if err := tbl.Put(TestItem{ID: 1, Name: "test", Addition: TestAddition{
		10,
		ServerResponse{EmbeddedID: 100},
	}}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	var item TestItem
	if err := tbl.Get("id", 1).Range("name", Equal, "test").One(ctx, &item); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("%+v", item)
	}
}

type v1DataItem struct {
	ID   string `dynamo:"id,hash" index:"migrate_test_index,hash"`
	Name string `dynamo:"name,range"`
	Code int64  `json:"code,omitempty,string" index:"migrate_test_index,range"`
}

func TestTableCreate(t *testing.T) {
	if os.Getenv("DYNAMO_TEST_REGION") != "" {
		t.Skip("online test skipped")
	}
	os.Setenv("AWS_REGION", "ap-northeast-1")
	os.Setenv("AWS_PROFILE", "media-dev")

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(os.Getenv("AWS_REGION")),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(RetryTxConflicts)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	db := New(cfg)
	tableName := "karasawa_ls_create_table"
	if err := db.CreateTable(tableName, v1DataItem{}).OnDemand(true).Wait(ctx); err != nil && !isTableExists(err) {
		t.Fatal(err)
	}
	tbl := db.Table(tableName)
	if err := tbl.DeleteTable().Run(ctx); err != nil {
		t.Fatal(err)
	}
}
