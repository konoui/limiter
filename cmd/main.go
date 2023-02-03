package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/konoui/limiter"
	"github.com/spf13/cobra"
)

func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:  "limiter",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		SilenceErrors:      true,
		DisableSuggestions: true,
	}
	return rootCmd
}

func NewCreateTableCmd(c *dynamodb.Client) *cobra.Command {
	var tableName string
	cmd := &cobra.Command{
		Use:  "create-table",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := limiter.CreateTable(cmd.Context(), tableName, c); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "new table: %s", tableName)
			return nil
		},
		SilenceUsage:       true,
		DisableSuggestions: true,
	}
	cmd.PersistentFlags().StringVar(&tableName, "table-name", "", "dynamodb table name")
	_ = cmd.MarkPersistentFlagRequired("table-name")
	return cmd
}

func NewCreateToken(c *dynamodb.Client) *cobra.Command {
	var bucketSize int64 = -1
	var rateLimit int64
	var interval int
	var tableName string
	cmd := &cobra.Command{
		Use:  "create-token",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, err := limiter.NewTokenBucket(
				rateLimit,
				bucketSize,
				limiter.WithInterval(time.Duration(interval)*time.Second),
			)
			if err != nil {
				return err
			}

			if bucketSize != -1 {
				bucketSize = rateLimit * 2
			}

			rl := limiter.New(tableName, bucket, c)
			uid, err := uuid.NewRandom()
			if err != nil {
				return err
			}

			if err := rl.PrepareTokens(cmd.Context(), uid.String()); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "new token: %s", uid.String())
			return nil
		},
	}
	cmd.PersistentFlags().StringVar(&tableName, "table-name", "", "dynamodb table name")
	cmd.PersistentFlags().Int64Var(&rateLimit, "rate-limit", 0, "token bucket size")
	cmd.PersistentFlags().IntVar(&interval, "interval", limiter.DefaultInterval, "interval to add tokens to a bucket")
	cmd.PersistentFlags().Int64Var(&bucketSize, "bucket-size", bucketSize, "token bucket burst size")
	_ = cmd.MarkPersistentFlagRequired("table-name")
	_ = cmd.MarkPersistentFlagRequired("rate-limit")
	return cmd
}

func NewStartServer(c *dynamodb.Client) *cobra.Command {
	var bucketSize int64 = -1
	var rateLimit int64
	var interval int
	var tableName string
	cmd := &cobra.Command{
		Use:  "start-server",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket, err := limiter.NewTokenBucket(
				rateLimit,
				bucketSize,
				limiter.WithInterval(time.Duration(interval)*time.Second),
			)
			if err != nil {
				return err
			}

			if bucketSize != -1 {
				bucketSize = rateLimit * 2
			}

			rl := limiter.New(tableName, bucket, c)
			return start(":8080", rl, "x-api-key")
		},
		SilenceUsage:       true,
		DisableSuggestions: true,
	}

	cmd.PersistentFlags().StringVar(&tableName, "table-name", "", "dynamodb table name")
	cmd.PersistentFlags().Int64Var(&rateLimit, "rate-limit", 0, "token bucket size")
	cmd.PersistentFlags().IntVar(&interval, "interval", limiter.DefaultInterval, "interval to add tokens to a bucket")
	cmd.PersistentFlags().Int64Var(&bucketSize, "bucket-size", bucketSize, "token bucket burst size")
	_ = cmd.MarkPersistentFlagRequired("table-name")
	_ = cmd.MarkPersistentFlagRequired("rate-limit")
	return cmd
}

func NewPublishMetric() *cobra.Command {
	var logGroupName, logStreamName string
	cmd := &cobra.Command{
		Use:  "publish-metric",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := newCWLClient(cmd.Context())
			if err != nil {
				return err
			}
			_, _ = c.CreateLogGroup(cmd.Context(), &cloudwatchlogs.CreateLogGroupInput{
				LogGroupName: &logGroupName,
			})
			_, _ = c.CreateLogStream(cmd.Context(), &cloudwatchlogs.CreateLogStreamInput{
				LogGroupName:  &logGroupName,
				LogStreamName: &logStreamName,
			})
			err = ReadAndPublish(cmd.Context(), cmd.InOrStdin(), c, logGroupName, logStreamName)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "published")
			return nil
		},
		SilenceUsage:       true,
		DisableSuggestions: true,
	}
	cmd.PersistentFlags().StringVar(&logGroupName, "log-group-name", "", "log group name")
	cmd.PersistentFlags().StringVar(&logStreamName, "log-stream-name", "", "log stream name")
	_ = cmd.MarkPersistentFlagRequired("log-group-name")
	_ = cmd.MarkPersistentFlagRequired("log-stream-name")
	return cmd
}

func main() {
	c, err := newDDBClient(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	rootCmd := NewRootCmd()
	rootCmd.AddCommand(
		NewCreateTableCmd(c),
		NewCreateToken(c),
		NewStartServer(c),
		NewPublishMetric(),
	)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
