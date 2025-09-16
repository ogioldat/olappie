package internal

import (
	"flag"
	"log/slog"
	"os"
)

var Logger *slog.Logger

func InitLogger() {
	var logLevel slog.LevelVar

	debugFlag := flag.Bool("debug", false, "debug: set debug log level (default false)")
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

	if *debugFlag {
		logLevel.Set(slog.LevelDebug)
	}

	Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: &logLevel,
	}))
}
