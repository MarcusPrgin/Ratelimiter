-- Atomic token bucket with cost-weighted quota.
--
-- State is one hash per key: `tk` (tokens remaining, fractional) and `ts` (when
-- tokens was last recomputed, epoch ms). Refill is lazy -- computed from elapsed
-- time on each call -- so idle keys cost nothing and there is no sweeper.
--
-- Read, refill, consume and write must be one atomic step. Split across commands,
-- two nodes both read the same token count and both consume it, admitting twice
-- the burst.
--
-- The clock comes from redis.call('TIME') so all nodes share one clock; see
-- sliding_window.lua for why that is safe under replication.
--
-- KEYS[1] : hash-tagged bucket key, e.g. "tb:{user:alice}"
-- ARGV[1] : refill rate in tokens per millisecond
-- ARGV[2] : burst capacity (max tokens)
-- ARGV[3] : cost (tokens this request consumes, >= 1)
-- ARGV[4] : extra idle slack in ms added to the key TTL
--
-- Returns : { allowed(0|1), remaining, reset_after_ms, retry_after_ms }

local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])
local slack = tonumber(ARGV[4])

local t   = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local state  = redis.call('HMGET', key, 'tk', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])

-- Absent or partially-written state starts as a full bucket.
if tokens == nil or ts == nil then
    tokens = burst
    ts     = now
end

-- Refill. A negative delta means the Redis clock stepped backwards; treat it as
-- no elapsed time rather than draining the bucket.
local delta = now - ts
if delta > 0 then
    tokens = math.min(burst, tokens + delta * rate)
end

local allowed = 0
local retry   = 0
if tokens >= cost then
    tokens  = tokens - cost
    allowed = 1
else
    retry = math.ceil((cost - tokens) / rate)
end

-- Time until the bucket is full again, i.e. when the full burst allowance returns.
local reset_after = math.ceil((burst - tokens) / rate)

-- A bucket that has refilled to capacity is indistinguishable from a fresh one,
-- so expiring it then loses no information. The slack keeps near-full buckets
-- resident instead of churning them.
redis.call('HSET', key, 'tk', string.format('%.6f', tokens), 'ts', string.format('%d', now))
redis.call('PEXPIRE', key, reset_after + slack)

return { allowed, math.floor(tokens), reset_after, retry }
