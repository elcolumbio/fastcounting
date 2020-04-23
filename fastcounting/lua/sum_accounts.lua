redis.setresp(3)
local result = {}
for i, value in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], ARGV[3])) do
    local account = value[2]['double']
    local atomic = value[1]
    if result[account] == nil then result[account] = 0 end
    result[account] = result[account] + redis.call('HGET', 'atomicID:' .. atomic, 'amount')
end

local xaccounts = {}
local xsums = {}
for xaccount, xsum in pairs(result) do
    xaccounts[#xaccounts+1] = xaccount
    xsums[#xsums+1] = xsum
end

for i=1, #xsums do
    xaccounts[#xaccounts + 1] = xsums[i]
end
return xaccounts