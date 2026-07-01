package ports

import "testing"

func TestStandardTags(t *testing.T) {
	tags := StandardTags("prod", "v1.2.3", "razorpay", "card", "merchant-1")
	want := map[string]string{
		"environment":     "prod",
		"service_version": "v1.2.3",
		"gateway_id":      "razorpay",
		"payment_method":  "card",
		"merchant_id":     "merchant-1",
	}
	for k, v := range want {
		if tags[k] != v {
			t.Errorf("tag %q = %q, want %q", k, tags[k], v)
		}
	}
}

func TestMergeTags(t *testing.T) {
	base := map[string]string{"environment": "prod", "gateway_id": "stripe"}
	additional := map[string]string{"gateway_id": "razorpay", "payment_method": "upi"}
	merged := MergeTags(base, additional)

	if merged["environment"] != "prod" {
		t.Errorf("expected base tag preserved, got %q", merged["environment"])
	}
	if merged["gateway_id"] != "razorpay" {
		t.Errorf("expected additional to override base, got %q", merged["gateway_id"])
	}
	if merged["payment_method"] != "upi" {
		t.Errorf("expected additional tag added, got %q", merged["payment_method"])
	}
	if base["gateway_id"] != "stripe" {
		t.Error("MergeTags must not mutate the base map")
	}
}
