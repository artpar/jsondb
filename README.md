JsonDb
==

Execute an sql over json data

`go build`
                                                                       
`cat data.json | ./jsondb --sql "select t.txn_id, t.amount from data t"`

or

`./jsondb --data=data.json --sqlfile=1.sql`


Todo
- Only `concat`,`max`,`min` functions are implemented, need to implement other functions
- Only `+`,`/`,`*`,`%` is implemented in math operators, need to implement other functions
- The implementation currently might be undefined in certain cases, need to check that.


we can use this now, the output of jsondb can be fed to jsondb again and queried over
`cat data.json | ./jsondb --sql "select t.txn_id, t.amount from data t"  | ./jsondb --sql="select max(t.amount) from data t"`
