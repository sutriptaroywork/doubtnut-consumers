local sum = 0
local val1 = redis.call('llen', KEYS[1])
local val2 = redis.call('llen', KEYS[2])
local val3 = redis.call('llen', KEYS[3])

sum = sum + val1
sum = sum + val2
sum = sum + val3

return sum