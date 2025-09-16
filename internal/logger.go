package internal

import (
	"flag"
	"log/slog"
	"os"
)

func NewLogger() *slog.Logger {
	var logLevel slog.LevelVar

	levelFlag := flag.String("log-level", "info", "logging level: debug, info, warn, error")
	flag.Parse()

	switch *levelFlag {
	case "debug":
		logLevel.Set(slog.LevelDebug)
	case "info":
		logLevel.Set(slog.LevelInfo)
	case "warn":
		logLevel.Set(slog.LevelWarn)
	case "error":
		logLevel.Set(slog.LevelError)
	default:
		logLevel.Set(slog.LevelInfo)
	}

	debugFlag := flag.Bool("debug", false, "set debug log level")

	if *debugFlag {
		logLevel.Set(slog.LevelDebug)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: &logLevel,
	}))

	return logger
}

var Logger = NewLogger()
