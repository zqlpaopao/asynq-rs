pub const ENQUEUE: &str = "ENQUEUE";
pub const DEQUEUE: &str = "DEQUEUE";
pub const CALLBACK: &str = "CALLBACK";

// enqueueCmd enqueues a given task message.
//
// Input:
// KEYS[1] -> ZQL:{<qname>}:t:<task_id>
// KEYS[2] -> ZQL:{<qname>}:pending
// --
// ARGV[1] -> task message data
// ARGV[2] -> task ID
// ARGV[3] -> current unix time in nsec
// ARGV[4] -> task life time
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID already exists
pub const RDB_ENQUEUE_CMD: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end
redis.call("HSET", KEYS[1],"msg", ARGV[1],"state", "pending","pending_since", ARGV[3])
redis.call("EXPIRE", KEYS[1], ARGV[4])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
"#;

// Input:
// KEYS[1] -> ZQL:{<qname>}:pending
// KEYS[2] -> ZQL:{<qname>}:t:
// --
// ARGV[1] -> get the list len
// Output:
// Returns 0 if no pending task is found in the given queue.
// Returns a list Task
pub const RDB_DEQUEUE_CMD: &str = r#"
local len = redis.call('LLEN',KEYS[1])
if len == 0 then
    return {}
end
local loop = tonumber(ARGV[1])
if len < loop then
    loop = len
end
local list = {}
for i = 1, loop do
    local key = redis.call('RPOP', KEYS[1])
    local keys = KEYS[2]..key
    redis.call('HSET', keys, 'state', 'active')
    redis.call('HDEL', keys, 'pending_since')
    local msg = redis.call('HGET', keys, 'msg')
    table.insert(list, msg)
end
return list
"#;

// KEYS[1] -> ZQL:{<qname>}:t:<task_id>
// KEYS[2] -> ZQL:{<qname>}:pending
// KEYS[3] -> ZQL:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[4] -> ZQL:{<qname>}:failed:<yyyy-mm-dd>
// KEYS[5] -> ZQL:{<qname>}:processed
// KEYS[6] -> ZQL:{<qname>}:failed
// -------
// ARGV[1] -> task ID
// ARGV[2] -> updated base.TaskMessage value
// ARGV[3] -> task state
// ARGV[4] -> completed_at UNIX timestamp
// ARGV[5] -> running times
// ARGV[6] -> 1 success 2 fail
// ARGV[7] -> stats expiration timestamp
// ARGV[8] -> max int64 value
pub const RDB_COMPLETED_CMD: &str = r#"
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state",ARGV[3],"completed_at", ARGV[4],"run_times",ARGV[5])
if tonumber(ARGV[6]) == tonumber(1) then
	local n = redis.call("GET", KEYS[3])
	if tonumber(n) == tonumber(ARGV[8]) then
	    redis.call("SET", KEYS[3], 1)
	elseif tonumber(n) == tonumber(1) then
	    redis.call("INCR", KEYS[3])
	    redis.call("EXPIRE", KEYS[3], ARGV[7])
	else
	    redis.call("INCR", KEYS[3])
	end
	local total = redis.call("GET", KEYS[5])
    if tonumber(total) == tonumber(ARGV[8]) then
    	redis.call("SET", KEYS[5], 1)
    else
    	redis.call("INCR", KEYS[5])
    end
end

if tonumber(ARGV[6]) == tonumber(2) then
	local n = redis.call("GET", KEYS[4])
	if tonumber(n) == tonumber(ARGV[8]) then
	    redis.call("SET", KEYS[4], 1)
	elseif tonumber(n) == tonumber(1) then
	    redis.call("INCR", KEYS[4])
	    redis.call("EXPIRE", KEYS[4], ARGV[7])
	else
	    redis.call("INCR", KEYS[4])
	end
	local total = redis.call("GET", KEYS[6])
    if tonumber(total) == tonumber(ARGV[8]) then
    	redis.call("SET", KEYS[6], 1)
    else
    	redis.call("INCR", KEYS[6])
    end
end
return 1
"#;
