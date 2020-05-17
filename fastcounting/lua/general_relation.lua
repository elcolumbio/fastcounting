redis.setresp(3)

for m, generalID in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])) do
    local inwords = {''}
    local haben = {''}

    for i, atomic in pairs(redis.call(
        'ZRANGEBYSCORE', 'general:atomic', generalID, generalID)) do
        local atomic_hash = redis.call('HGETALL', 'atomicID:' .. atomic)['map']
        local part = atomic_hash['accountID'] .. ': ' .. atomic_hash['amount']/100 .. '€'
        if atomic_hash['kontenseite'] == 'Soll' then
            inwords[#inwords+1] = part
        else
            haben[#haben+1] = part
        end
    end

    haben[#haben] = '### ' .. haben[#haben]
    for ix=1, #haben do
        inwords[#inwords+1] = haben[ix]
    end

    local relation = table.concat(inwords, ', ')
    redis.call('HSET', 'generalID:' .. generalID, 'relation', relation)
end
return true