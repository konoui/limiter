package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/konoui/limiter"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slog"
)

const (
	// https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html
	envRunOnLambda    = "AWS_LAMBDA_FUNCTION_NAME"
	envConfigFilepath = "LIMITER_CONFIG_FILEPATH"
)

var (
	logger = slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
)

func NewRootCmd(rl map[string]*limiter.RateLimit) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:  "limiter",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if rl != nil {
				mux, err := testHandler(rl)
				if err != nil {
					return err
				}
				return StartServer("", mux)
			}
			return fmt.Errorf("non lambda environment")
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
	var fpath, key string
	cmd := &cobra.Command{
		Use:  "create-token",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := newLimiters(fpath, c)
			if err != nil {
				return err
			}

			rl, ok := r[key]
			if !ok {
				return fmt.Errorf("%s does not exist in %s", key, fpath)
			}

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
	cmd.PersistentFlags().StringVar(&key, "config-name", "", "config name in the config file")
	cmd.PersistentFlags().StringVar(&fpath, "config-file", "", "config file path")
	_ = cmd.MarkPersistentFlagRequired("config-file")
	_ = cmd.MarkPersistentFlagRequired("config-name")
	return cmd
}

func NewStartServer(c *dynamodb.Client) *cobra.Command {
	var filepath string
	cmd := &cobra.Command{
		Use:  "start-server",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			rl, err := newLimiters(filepath, c)
			if err != nil {
				return err
			}
			mux, err := testHandler(rl)
			if err != nil {
				return err
			}
			return StartServer(":8080", mux)
		},
		SilenceUsage:       true,
		DisableSuggestions: true,
	}

	cmd.PersistentFlags().StringVar(&filepath, "config-file", "", "config file path")
	_ = cmd.MarkPersistentFlagRequired("config-file")
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

	var r map[string]*limiter.RateLimit
	if _, ok := os.LookupEnv(envRunOnLambda); ok {
		r, err = newLimiters(os.Getenv(envConfigFilepath), c)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	rootCmd := NewRootCmd(r)
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

func newLimiters(filepath string, client *dynamodb.Client) (map[string]*limiter.RateLimit, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cfg, err := NewConfig(f)
	if err != nil {
		return nil, err
	}

	ret := map[string]*limiter.RateLimit{}
	for key, v := range cfg {
		rl, err := limiter.New(v, client, limiter.WithLogger(logger.WithGroup(key+"_limiter")))
		if err != nil {
			return nil, err
		}
		ret[key] = rl
	}

	return ret, err
}
