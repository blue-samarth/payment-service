std = "lua51"

read_globals = {
	"redis",
	"cjson",
	"cmsgpack",
	"struct",
	"bit",
	"KEYS",
	"ARGV",
}

files["internal/adapters/redis/scripts"] = {
	globals = {},
}
