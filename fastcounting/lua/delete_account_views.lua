for i, account in pairs(accounts) do
    redis.call('DEL', 'account:' .. account)
end
return true