package bootstrap

import "samarth/payment-service/internal/ports"

func LogDeprecations(log ports.Logger, deprecations []string) {
	for _, d := range deprecations {
		log.Warn(ports.LogEventConfigDeprecated, map[string]any{"warning": d})
	}
}
