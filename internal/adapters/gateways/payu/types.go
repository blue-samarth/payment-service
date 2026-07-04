package payu

import "encoding/json"

type payuVerifyResponse struct {
	Status             int                    `json:"status"`
	Msg                string                 `json:"msg"`
	TransactionDetails map[string]payuTxnInfo `json:"transaction_details"`
}

type payuTxnInfo struct {
	Txnid    string `json:"txnid"`
	Status   string `json:"status"`
	Amount   string `json:"amt"`
	MihpayID string `json:"mihpayid"`
}

type payuRefundResponse struct {
	Status    int    `json:"status"`
	Msg       string `json:"msg"`
	RequestID string `json:"request_id"`
}

func decodeJSON(data []byte, out any) error {
	return json.Unmarshal(data, out)
}
