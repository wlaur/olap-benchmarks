select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  cast(sum(l_quantity) as double) / nullif(count(l_quantity), 0) as avg_qty,
  cast(sum(l_extendedprice) as double) / nullif(count(l_extendedprice), 0) as avg_price,
  cast(sum(l_discount) as double) / nullif(count(l_discount), 0) as avg_disc,
  count(*) as count_order
from
  lineitem
where
  l_shipdate <= date '1998-12-01' - interval '90' day
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;
