INSERT INTO gateway_config
    (gateway_id, display_name, is_active, min_amount, max_amount,
     supported_currencies, supported_methods,
     idempotency_capable, supports_cancel, supports_partial_refund, priority)
VALUES
    ('stripe', 'Stripe', true, 0, 100000000,
     ARRAY['INR'], ARRAY['card'],
     true, true, true, 100)
ON CONFLICT (gateway_id) DO NOTHING;

INSERT INTO gateway_timeouts
    (gateway_id, payment_method, gateway_timeout_sec, payment_method_buffer_sec)
VALUES ('stripe', 'card', 25, 5)
ON CONFLICT (gateway_id, payment_method) DO NOTHING;

INSERT INTO gateway_fee_models
    (gateway_id, payment_method, fixed_paise, percentage_bps)
VALUES ('stripe', 'card', 100, 200)
ON CONFLICT (gateway_id, payment_method) DO NOTHING;
