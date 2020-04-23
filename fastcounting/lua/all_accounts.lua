redis.setresp(3)
local accounts = {}
local hash = {}
for i, value in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], ARGV[3])) do
    local account = value[2]['double']
    if not hash[account] then
        accounts[#accounts+1] = account
        hash[account] = true
    end
end
if ARGV[4] == nil then return accounts end
