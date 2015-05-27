JsonDb
==

Execute an sql over json data

`go build`
                                                                       
`cat data.json | ./jsondb --sql "select t.txn_id, t.amount from data t"`

or

`./jsondb --datafile=data.json --sqlfile=1.sql`


Todo
- Only concat and max functions are implemented, need to implement other functions
- Only + is implemented in math operators, need to implement other functions
- The implementation currently might be undefined in certain cases, need to check that.

