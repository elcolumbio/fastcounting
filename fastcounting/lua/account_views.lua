redis.setresp(3)
for i, atomicID in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])) do

    local atomic = redis.call('HGETALL', 'atomicID:' .. atomicID)['map']
    local general = redis.call('HGETALL', 'generalID:' .. atomic['generalID'])['map']
    local system = redis.call('HGETALL', 'accountsystem:' .. atomic['accountID'])['map']
    if next(system)==nil then
        system = redis.call('HGETALL', 'accountsystem:special_account')['map'] end

    redis.call('XADD','account:' .. atomic['accountID'],
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
        'relations', general['relation']
        )
end
return true