redis.setresp(3)
for i, atomicID in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])) do
    local atomic = redis.call('HGETALL', 'atomicID:' .. atomicID)['map']
    local general = redis.call('HGETALL', 'generalID:' .. atomic['generalID'])['map']
    local system = redis.call('HGETALL', 'accountsystem:' .. atomic['accountID'])['map']
    if next(system)==nil then
        system = redis.call('HGETALL', 'accountsystem:special_account')['map'] end

    
    local inwords = {}
    local haben = {}
    for i, atomicx in pairs(redis.call(
        'ZRANGEBYSCORE', 'general:atomic', atomic['generalID'], atomic['generalID'])) do
        local inatomic = redis.call('HGETALL', 'atomicID:' .. atomicx)['map']
        local part = inatomic['accountID'] .. ': ' .. inatomic['amount']/100 .. '€'
        if inatomic['kontenseite'] == 'Soll' then
            inwords[#inwords+1] = part
        else
            haben[#haben+1] = part
        end
    end

    haben[#haben] = '### ' .. haben[#haben]
    for i=1, #haben do
        inwords[#inwords+1] = haben[i]
    end
    local relation = table.concat(inwords, ', ')


    redis.call('XADD','atomicview',
        general['date'] .. '-' .. i, 
        'general', atomic['generalID'],
        'date', general['date'],
        'jourdate', general['jourdat'],
        'status', general['status'],
        'text', atomic['text'],
        'kontenseite', atomic['kontenseite'],
        'amount', atomic['amount'],
        'account', atomic['accountID'],
        'batchID', atomic['batchID'],
        'account_name', system['Kontenbezeichnung'],
        'system_kat', system['Kontenkategorie'],
        'system_type', system['Kontenunterart'],
        'system_tax', system['Steuer'],
        'relations', relation
        )
end
return true