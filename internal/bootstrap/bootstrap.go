package bootstrap

import (
	"log/slog"
	"os"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/gateways"
	"samarth/payment-service/internal/adapters/gateways/payu"
	"samarth/payment-service/internal/adapters/gateways/razorpay"
	"samarth/payment-service/internal/adapters/gateways/stripe"
)

func GatewayRegistry(cfg *config.Config) *gateways.Registry {
	registry := gateways.NewRegistry()
	registry.Register("stripe", stripe.New(stripe.Config{
		APIKey:  os.Getenv("STRIPE_API_KEY"),
		BaseURL: os.Getenv("STRIPE_BASE_URL"),
		Timeout: cfg.Gateway.HTTPTimeout,
	}))
	registry.Register("razorpay", razorpay.New(razorpay.Config{
		KeyID:     os.Getenv("RAZORPAY_KEY_ID"),
		KeySecret: os.Getenv("RAZORPAY_KEY_SECRET"),
		BaseURL:   os.Getenv("RAZORPAY_BASE_URL"),
		Timeout:   cfg.Gateway.HTTPTimeout,
	}))
	registry.Register("payu", payu.New(payu.Config{
		MerchantKey:  os.Getenv("PAYU_MERCHANT_KEY"),
		MerchantSalt: os.Getenv("PAYU_MERCHANT_SALT"),
		BaseURL:      os.Getenv("PAYU_BASE_URL"),
		Timeout:      cfg.Gateway.HTTPTimeout,
	}))
	return registry
}

func ParseLogLevel(level string) slog.Level {
	switch level {
	case "error":
		return slog.LevelError
	case "warn":
		return slog.LevelWarn
	case "debug":
		return slog.LevelDebug
	case "trace":
		return slog.Level(-8)
	default:
		return slog.LevelInfo
	}
}
