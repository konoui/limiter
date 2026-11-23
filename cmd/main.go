package main

import (
	"context"
	"fmt"
	"os"

	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"github.com/konoui/limiter"
	"github.com/spf13/cobra"
)

const (
	// https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html
	envRunOnLambda    = "AWS_LAMBDA_FUNCTION_NAME" //nolint
	envConfigFilepath = "LIMITER_CONFIG_FILEPATH"
)

var (
	logger = slog.New(slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{
			Level: logLevel(),
		}))
)

func NewRootCmd(rl map[string]*limiter.RateLimit, flush func(context.Context) error) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:  "limiter",
		Args: cobra.MinimumNArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			if rl == nil {
				return fmt.Errorf("not running on AWS Lambda (%s is not set). run `limiter start-server --config-file <path>` to serve locally", envRunOnLambda)
			}
			mux, err := testHandler(rl)
			if err != nil {
				return err
			}
			return StartServer("", flushAfter(flush, mux))
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
		RunE: func(cmd *cobra.Command, _ []string) error {
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
		RunE: func(cmd *cobra.Command, _ []string) error {
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

			fmt.Fprintf(cmd.OutOrStdout(), "new token: %s\n", uid.String())
			return nil
		},
		SilenceUsage:       true,
		DisableSuggestions: true,
	}
	cmd.PersistentFlags().StringVar(&key, "config-name", "", "config name in the config file")
	cmd.PersistentFlags().StringVar(&fpath, "config-file", "", "config file path")
	_ = cmd.MarkPersistentFlagRequired("config-file")
	_ = cmd.MarkPersistentFlagRequired("config-name")
	return cmd
}

func NewRevokeToken(c *dynamodb.Client) *cobra.Command {
	var fpath, key, token string
	cmd := &cobra.Command{
		Use:  "revoke-token",
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			r, err := newLimiters(fpath, c)
			if err != nil {
				return err
			}
			rl, ok := r[key]
			if !ok {
				return fmt.Errorf("%s does not exist in %s", key, fpath)
			}
			if err := rl.RevokeTokens(cmd.Context(), token); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "revoked. running instances allow it until their registration cache expires")
			return nil
		},
		SilenceUsage:       true,
		DisableSuggestions: true,
	}
	cmd.PersistentFlags().StringVar(&key, "config-name", "", "config name in the config file")
	cmd.PersistentFlags().StringVar(&fpath, "config-file", "", "config file path")
	cmd.PersistentFlags().StringVar(&token, "token", "", "token to revoke")
	_ = cmd.MarkPersistentFlagRequired("config-file")
	_ = cmd.MarkPersistentFlagRequired("config-name")
	_ = cmd.MarkPersistentFlagRequired("token")
	return cmd
}

func NewStartServer(c *dynamodb.Client) *cobra.Command {
	var filepath string
	cmd := &cobra.Command{
		Use:  "start-server",
		Args: cobra.MinimumNArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
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

func main() {
	ctx := context.Background()
	c, err := newDDBClient(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	flush, err := setupMetrics(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = flush(ctx) }()

	var r map[string]*limiter.RateLimit
	if _, ok := os.LookupEnv(envRunOnLambda); ok {
		r, err = newLimiters(os.Getenv(envConfigFilepath), c)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1) //nolint:gocritic // nothing to flush yet
		}
	}

	rootCmd := NewRootCmd(r, flush)
	rootCmd.AddCommand(
		NewCreateTableCmd(c),
		NewCreateToken(c),
		NewRevokeToken(c),
		NewStartServer(c),
	)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newLimiters(filepath string, client *dynamodb.Client) (map[string]*limiter.RateLimit, error) {
	f, err := os.Open(filepath) //nolint:gosec // the config path is given by the operator
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
			return nil, fmt.Errorf("config %q in %s: %w", key, filepath, err)
		}
		ret[key] = rl
	}

	return ret, err
}

// logLevel is info by default. LIMITER_LOG_LEVEL=debug enables debug logs.
func logLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(os.Getenv("LIMITER_LOG_LEVEL"))); err != nil {
		return slog.LevelInfo
	}
	return l
}
