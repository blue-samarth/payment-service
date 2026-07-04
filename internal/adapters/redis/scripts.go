package redis

import (
	_ "embed"

	goredis "github.com/redis/go-redis/v9"
)

//go:embed scripts/token_bucket.lua
var tokenBucketLua string

//go:embed scripts/cb_transition.lua
var cbTransitionLua string

//go:embed scripts/cb_record_failure.lua
var cbRecordFailureLua string

//go:embed scripts/cb_record_success.lua
var cbRecordSuccessLua string

//go:embed scripts/cb_set_score.lua
var cbSetScoreLua string

var tokenBucketScript = goredis.NewScript(tokenBucketLua)

var cbTransitionScript = goredis.NewScript(cbTransitionLua)

var cbRecordFailureScript = goredis.NewScript(cbRecordFailureLua)

var cbRecordSuccessScript = goredis.NewScript(cbRecordSuccessLua)

var cbSetScoreScript = goredis.NewScript(cbSetScoreLua)
