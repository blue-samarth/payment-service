package stripe

type stripePaymentIntent struct {
	ID               string            `json:"id"`
	Status           string            `json:"status"`
	Amount           int64             `json:"amount"`
	Currency         string            `json:"currency"`
	Metadata         map[string]string `json:"metadata"`
	LastPaymentError *stripeError      `json:"last_payment_error"`
	Charges          *stripeChargeList `json:"charges"`
}

type stripeChargeList struct {
	Data []stripeCharge `json:"data"`
}

type stripeCharge struct {
	ID                   string               `json:"id"`
	BalanceTransaction   *stripeBalanceTxn    `json:"balance_transaction"`
	PaymentMethodDetails *stripePaymentMethod `json:"payment_method_details"`
}

func (c stripeCharge) balanceTransactionFee() int64 {
	if c.BalanceTransaction == nil {
		return 0
	}
	return c.BalanceTransaction.Fee
}

type stripeBalanceTxn struct {
	Fee int64 `json:"fee"`
}

type stripePaymentMethod struct {
	Card *stripeCardDetails `json:"card"`
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

type stripeErrorEnvelope struct {
	Error *stripeError `json:"error"`
}
