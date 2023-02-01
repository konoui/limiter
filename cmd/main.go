package main

import (
	"context"
	"fmt"
	"os"
	"time"

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
	cmd.MarkPersistentFlagRequired("table-name")
	return cmd
}

func NewCreateToken(c *dynamodb.Client) *cobra.Command {
	var bucketSize int64
	var burstSize int64 = -1
	var rate int
	var tableName string
	cmd := &cobra.Command{
		Use:  "create-token",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := limiter.NewTokenBucket(
				bucketSize,
				burstSize,
				limiter.WithInterval(time.Duration(rate)*time.Second),
			)

			if burstSize != -1 {
				burstSize = bucketSize * 2
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
	cmd.PersistentFlags().Int64Var(&bucketSize, "bucket-size", 0, "token bucket size")
	cmd.PersistentFlags().IntVar(&rate, "rate", 1, "rate/second")
	cmd.PersistentFlags().Int64Var(&burstSize, "bucket-burst-size", burstSize, "token bucket burst size")
	cmd.MarkPersistentFlagRequired("table-name")
	cmd.MarkPersistentFlagRequired("bucket-size")
	return cmd
}

func NewStartServer(c *dynamodb.Client) *cobra.Command {
	var bucketSize int64
	var burstSize int64 = -1
	var rate int
	var tableName string
	cmd := &cobra.Command{
		Use:  "start-server",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := limiter.NewTokenBucket(
				bucketSize,
				burstSize,
				limiter.WithInterval(time.Duration(rate)*time.Second),
			)

			if burstSize != -1 {
				burstSize = bucketSize * 2
			}

			rl := limiter.New(tableName, bucket, c)
			return start(":8080", rl, "x-api-key")
		},
		SilenceUsage:       true,
		DisableSuggestions: true,
	}

	cmd.PersistentFlags().StringVar(&tableName, "table-name", "", "dynamodb table name")
	cmd.PersistentFlags().Int64Var(&bucketSize, "bucket-size", 0, "token bucket size")
	cmd.PersistentFlags().IntVar(&rate, "rate", 1, "rate/second")
	cmd.PersistentFlags().Int64Var(&burstSize, "bucket-burst-size", burstSize, "token bucket burst size")
	cmd.MarkPersistentFlagRequired("table-name")
	cmd.MarkPersistentFlagRequired("bucket-size")
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
	)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
