-- Atomic sliding window counter with cost-weighted quota.
--
-- Runs atomically on Redis: no other command interleaves, so the read-then-write
-- sequence below cannot race across nodes. Doing this with separate commands is
-- what introduces the classic bug --
--
--   1. node A: INCRBY key 1   -> 1
--   2. node B: INCRBY key 1   -> 2
--   3. node A: PEXPIRE key W  -> TTL set
--   4. node B: PEXPIRE key W  -> TTL reset, so the window silently extends
--
-- and it is also why the deny decision has to be made in the same atomic step as
-- the increment: checking in one command and incrementing in another lets two
-- nodes both observe "one slot left" and both take it.
--
-- The clock comes from redis.call('TIME'), not from the caller. Every node then
-- shares one clock, so window boundaries agree even when app servers have
-- drifted. This makes the script non-deterministic, which is fine on Redis 5+
-- where effect replication is used (Redis 7 does this by default): the replica
-- receives the resulting writes, not the script.
--
-- KEYS[1] : hash-tagged base key, e.g. "rl:{user:alice}". Window keys are derived
--           from it. The hash tag keeps every derived key in one Cluster slot.
-- ARGV[1] : limit (quota units per window)
-- ARGV[2] : window length in milliseconds
-- ARGV[3] : cost (quota units this request consumes, >= 1)
--
-- Returns : { allowed(0|1), effective_count, remaining, reset_after_ms, retry_after_ms }

local base   = KEYS[1]
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost   = tonumber(ARGV[3])

local t       = redis.call('TIME')
local now     = t[1] * 1000 + math.floor(t[2] / 1000)
local win     = now - (now % window)
local elapsed = now - win

local curr_key = base .. ':' .. win
local prev_key = base .. ':' .. (win - window)

local prev_c = tonumber(redis.call('GET', prev_key)) or 0
local curr_c = tonumber(redis.call('GET', curr_key)) or 0

-- Weighted carry-over from the previous window. Rounding rather than truncating
-- matters: math.floor systematically under-counts the carry-over, which biases
-- the limiter toward admitting more than the configured limit.
local prev_w    = (window - elapsed) / window
local effective = math.floor(prev_c * prev_w + 0.5) + curr_c

local reset_after = window - elapsed

if effective + cost > limit then
    -- How long until enough of the previous window ages out to fit this request.
    -- The carry-over decays at prev_c/window units per ms; with an empty previous
    -- window nothing decays before the next boundary, so wait out the window.
    local retry = reset_after
    if prev_c > 0 then
        local need = effective + cost - limit
        retry = math.ceil(need / (prev_c / window))
        if retry > reset_after or retry < 0 then
            retry = reset_after
        end
    end
    return { 0, effective, 0, reset_after, retry }
end

local new_count = redis.call('INCRBY', curr_key, cost)

-- The key name is bound to `win`, so its correct lifetime is a fixed absolute
-- deadline: two windows past the window start, which keeps it readable as the
-- previous window for exactly one more window. Recomputing that deadline on
-- every write is idempotent -- the TTL shortens as `now` advances rather than
-- being pushed out. This is deliberately unconditional. Setting the TTL only on
-- the first write (when new_count == cost) saves one O(1) command but leaks the
-- key forever if that single PEXPIRE is ever lost, which happens whenever the
-- key is created by anything other than this script.
redis.call('PEXPIRE', curr_key, win + window * 2 - now)

local effective_after = effective + cost
local remaining       = limit - effective_after
if remaining < 0 then
    remaining = 0
end

return { 1, effective_after, remaining, reset_after, 0 }
