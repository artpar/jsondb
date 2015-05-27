SELECT
/* comment 1*/
/* comment 2 */

  (t.txn_id) txn_id,
  max(t.amount), (t.stamp_created), 3+4, 6+5+3, concat(t.txn_id, t.txn_time), t.amount + 100
FROM data t
GROUP BY stamp_created
