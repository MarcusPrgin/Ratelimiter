-- Escalate a key into the penalty box.
--
-- Called once a node has seen Threshold denials for the key inside its strike
-- window; the strike counting itself is local (see recordStrike), so this script's
-- job is the shared bookkeeping: bump the offence count and write the resulting
-- penalty where every node can see it.
--
-- Runs atomically. Split across commands, two nodes crossing the threshold at the
-- same moment would each increment the offence count and each write a penalty,
-- charging one offence twice.
--
-- KEYS[1] : hash-tagged base key, e.g. "pen:{user:alice}". Sub-keys derive from it,
--           so the hash tag keeps them in one Cluster slot.
-- ARGV[1] : base penalty in ms
-- ARGV[2] : max penalty in ms
--
-- Returns : { penalty_ms, offence_count }

local base     = KEYS[1]
local base_pen = tonumber(ARGV[1])
local max_pen  = tonumber(ARGV[2])

local pen_key   = base .. ':p'
local count_key = base .. ':n'

local n = redis.call('INCR', count_key)
-- The offence count outlives the penalty it produced, so a repeat offender resumes
-- escalating instead of restarting at the base penalty.
redis.call('PEXPIRE', count_key, max_pen)

-- Exponential backoff, guarded against overflow. 2^n as a double loses integer
-- precision past n=53 and reaches inf near n=1024; unguarded, base * 2^n became a
-- non-finite value that Redis then rejected as an invalid TTL — so a persistent
-- offender stopped being penalised at all. The result is clamped regardless, so
-- anything past 2^30 can short-circuit to the cap.
local pen = max_pen
if n <= 30 then
    pen = base_pen * (2 ^ (n - 1))
    if pen > max_pen then
        pen = max_pen
    end
end
pen = math.floor(pen)
if pen < 1 then
    pen = 1
end

redis.call('SET', pen_key, n, 'PX', pen)

return { pen, n }
