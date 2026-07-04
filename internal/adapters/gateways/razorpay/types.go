package razorpay

type rzpOrder struct {
	ID       string            `json:"id"`
	Status   string            `json:"status"`
	Amount   int64             `json:"amount"`
	Currency string            `json:"currency"`
	Receipt  string            `json:"receipt"`
	Notes    map[string]string `json:"notes"`
}

type rzpRefund struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

type rzpError struct {
	Code        string `json:"code"`
	Description string `json:"description"`
	Reason      string `json:"reason"`
	Source      string `json:"source"`
}

type rzpErrorEnvelope struct {
	Error *rzpError `json:"error"`
}
