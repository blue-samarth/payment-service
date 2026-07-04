package stripe

import "encoding/json"

type stripePaymentIntent struct {
	ID               string            `json:"id"`
	Status           string            `json:"status"`
	Amount           int64             `json:"amount"`
	Currency         string            `json:"currency"`
	Metadata         map[string]string `json:"metadata"`
	LastPaymentError *stripeError      `json:"last_payment_error"`
	LatestCharge     json.RawMessage   `json:"latest_charge"`
	Charges          *stripeChargeList `json:"charges"`
}

type stripeChargeList struct {
	Data []stripeCharge `json:"data"`
}
type stripeBalanceTxn struct {
	Fee int64 `json:"fee"`
}
type stripePaymentMethod struct {
	Card *stripeCardDetails `json:"card"`
}
type stripeErrorEnvelope struct {
	Error *stripeError `json:"error"`
}

type stripeCharge struct {
	ID                   string               `json:"id"`
	BalanceTransaction   json.RawMessage      `json:"balance_transaction"`
	PaymentMethodDetails *stripePaymentMethod `json:"payment_method_details"`
}

func (c stripeCharge) balanceTransactionFee() int64 {
	if bt := decodeExpanded[stripeBalanceTxn](c.BalanceTransaction); bt != nil {
		return bt.Fee
	}
	return 0
}

// decodeExpanded returns the decoded object for a Stripe expandable field, or
// nil when the field is absent, null, or an unexpanded string id.
func decodeExpanded[T any](raw json.RawMessage) *T {
	if len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	return &v
}

type stripeCardDetails struct {
	Brand   string `json:"brand"`
	Last4   string `json:"last4"`
	Network string `json:"network"`
}

type stripeRefund struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

type stripeError struct {
	Type        string `json:"type"`
	Code        string `json:"code"`
	DeclineCode string `json:"decline_code"`
	Message     string `json:"message"`
}
