-- Atomic sliding window counter with cost-weighted quota (ARGV[5]).
-- Executes atomically on Redis — no other command can interleave.
--
-- Why Lua? The naive approach (INCRBY + EXPIRE as two commands) has a race:
--   1. Client A calls INCRBY key 1  -> key = 1
--   2. Client B calls INCRBY key 1  -> key = 2
--   3. Client A calls EXPIRE key 60 -> TTL set
--   4. Client B calls EXPIRE key 60 -> TTL reset (doubles the window!)
-- This script eliminates that race entirely.
--
-- KEYS[1]: current window key   e.g. "rl:user123:1716000060"
-- KEYS[2]: previous window key  e.g. "rl:user123:1716000000"
-- ARGV[1]: limit
-- ARGV[2]: window size in seconds
-- ARGV[3]: current time as Unix timestamp (float, seconds)
-- ARGV[4]: window start timestamp (floor to window boundary)
-- ARGV[5]: cost (number of quota units this request consumes; default 1)
--
-- Returns: {allowed (0|1), current_count, effective_count}

local curr_key  = KEYS[1]
local prev_key  = KEYS[2]
local limit     = tonumber(ARGV[1])
local window    = tonumber(ARGV[2])
local now       = tonumber(ARGV[3])
local win_start = tonumber(ARGV[4])
local cost      = tonumber(ARGV[5]) or 1

-- elapsed fraction of the current window (0.0 – 1.0)
local elapsed_pct = (now - win_start) / window

-- weighted contribution of the previous window
local prev_count  = tonumber(redis.call('GET', prev_key)) or 0
local prev_weight = 1.0 - elapsed_pct
local effective   = math.floor(prev_count * prev_weight)

-- current window count
local curr_count = tonumber(redis.call('GET', curr_key)) or 0
effective = effective + curr_count

-- deny if adding cost would exceed the limit
if effective + cost > limit then
    return {0, curr_count, effective}
end

-- atomically increment by cost
local new_count = redis.call('INCRBY', curr_key, cost)

-- set expiry only on first write:
-- when the key was absent, INCRBY initialises to 0 then adds cost → new_count == cost.
-- subsequent writes produce new_count > cost, so EXPIRE is never reset.
if new_count == cost then
    redis.call('EXPIRE', curr_key, window * 2)
end

return {1, new_count, effective + cost}
