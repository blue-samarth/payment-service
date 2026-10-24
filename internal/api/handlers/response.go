package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const (
	maxPaymentBodyBytes = 64 << 10
	maxRefundBodyBytes  = 4 << 10
	maxCancelBodyBytes  = 4 << 10
)

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, allowEmpty bool, dst any) bool {
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	err := json.NewDecoder(http.MaxBytesReader(w, body, limit)).Decode(dst)
	var tooLarge *http.MaxBytesError
	switch {
	case err == nil, allowEmpty && errors.Is(err, io.EOF):
		return true
	case errors.As(err, &tooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("request body must be at most %d bytes", limit))
	default:
		writeError(w, http.StatusBadRequest, "invalid_request_body", "request body is not valid JSON")
	}
	return false
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: errorBody{Code: code, Message: message}})
}
