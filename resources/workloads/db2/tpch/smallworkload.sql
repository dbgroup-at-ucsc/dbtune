-- query
select
	l_extendedprice, l_discount
from
	tpch.lineitem,
	tpch.part
where
		p_partkey = l_partkey
		and p_brand = 'Brand#13'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 6 and l_quantity <= 16
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON';
-- query
select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				tpch.partsupp,
				tpch.supplier,
				tpch.nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'JAPAN';
