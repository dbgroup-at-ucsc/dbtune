--query
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2 using codeunits16) as cntrycode,
			c_acctbal
		from
			tpch.customer
		where
			substring(c_phone from 1 for 2 using codeunits16) in
				('24', '11', '14', '23', '31', '26', '10')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					tpch.customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2 using codeunits16) in
						('24', '11', '14', '23', '31', '26', '10')
			)
			and not exists (
				select
					*
				from
					tpch.orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode
