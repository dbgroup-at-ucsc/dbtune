--NOTE: the following procedure has to be defined in the DB2
--      instance in order to execute the functional tests correctly

--CREATE PROCEDURE db2perf_quiet_drop(IN statement VARCHAR(1000))
--LANGUAGE SQL
--BEGIN
   --DECLARE SQLSTATE CHAR(5);
   --DECLARE NotThere    CONDITION FOR SQLSTATE '42704';
   --DECLARE NotThereSig CONDITION FOR SQLSTATE '42883';

   --DECLARE EXIT HANDLER FOR NotThere, NotThereSig
      --SET SQLSTATE = '     ';

   --SET statement = 'DROP ' || statement;
   --EXECUTE IMMEDIATE statement;
--END

CALL db2perf_quiet_drop('TABLE tpch.nation');
CALL db2perf_quiet_drop('TABLE tpch.region');
CALL db2perf_quiet_drop('TABLE tpch.part');
CALL db2perf_quiet_drop('TABLE tpch.supplier');
CALL db2perf_quiet_drop('TABLE tpch.partsupp');
CALL db2perf_quiet_drop('TABLE tpch.customer');
CALL db2perf_quiet_drop('TABLE tpch.orders');
CALL db2perf_quiet_drop('TABLE tpch.lineitem');
CALL db2perf_quiet_drop('SCHEMA tpch RESTRICT');

CREATE SCHEMA tpch;

-- Sccsid:     @(#)dss.ddl	2.1.8.1
CREATE TABLE tpch.nation  (
    n_nationkey INTEGER NOT NULL,
    n_name      CHAR(25) NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment   VARCHAR(152)
);

CREATE TABLE tpch.region (
    r_regionkey INTEGER NOT NULL,                         
    r_name      CHAR(25) NOT NULL,                        
    r_comment   VARCHAR(152)
);

CREATE TABLE tpch.part (
    p_partkey     INTEGER NOT NULL,                       
    p_name        VARCHAR(55) NOT NULL,                   
    p_mfgr        CHAR(25) NOT NULL,                      
    p_brand       CHAR(10) NOT NULL,                      
    p_type        VARCHAR(25) NOT NULL,                   
    p_size        INTEGER NOT NULL,                       
    p_container   CHAR(10) NOT NULL,                      
    p_retailprice DECIMAL(15,2) NOT NULL,                 
    p_comment     VARCHAR(23) NOT NULL
);

CREATE TABLE tpch.supplier ( 
    s_suppkey   INTEGER NOT NULL,                          
    s_name      CHAR(25) NOT NULL,                         
    s_address   VARCHAR(40) NOT NULL,                      
    s_nationkey INTEGER NOT NULL,                          
    s_phone     CHAR(15) NOT NULL,                         
    s_acctbal   DECIMAL(15,2) NOT NULL,                    
    s_comment   VARCHAR(101) NOT NULL
);

CREATE TABLE tpch.partsupp ( 
    ps_partkey     INTEGER NOT NULL,                           
    ps_suppkey     INTEGER NOT NULL,                           
    ps_availqty    INTEGER NOT NULL,                           
    ps_supplycost  DECIMAL(15,2)  NOT NULL,                    
    ps_comment     VARCHAR(199) NOT NULL
);

CREATE TABLE tpch.customer (
    c_custkey    INTEGER NOT NULL,                            
    c_name       VARCHAR(25) NOT NULL,                        
    c_address    VARCHAR(40) NOT NULL,                        
    c_nationkey  INTEGER NOT NULL,                            
    c_phone      CHAR(15) NOT NULL,                           
    c_acctbal    DECIMAL(15,2)   NOT NULL,                    
    c_mktsegment CHAR(10) NOT NULL,                           
    c_comment    VARCHAR(117) NOT NULL
);

CREATE TABLE tpch.orders (
    o_orderkey      INTEGER NOT NULL,                         
    o_custkey       INTEGER NOT NULL,                         
    o_orderstatus   CHAR(1) NOT NULL,                         
    o_totalprice    DECIMAL(15,2) NOT NULL,                   
    o_orderdate     DATE NOT NULL,                            
    o_orderpriority CHAR(15) NOT NULL,                        
    o_clerk         CHAR(15) NOT NULL,                        
    o_shippriority  INTEGER NOT NULL,                         
    o_comment       VARCHAR(79) NOT NULL
);

CREATE TABLE tpch.lineitem (
    l_orderkey      INTEGER NOT NULL,                             
    l_partkey       INTEGER NOT NULL,                             
    l_suppkey       INTEGER NOT NULL,                             
    l_linenumber    INTEGER NOT NULL,                             
    l_quantity      DECIMAL(15,2) NOT NULL,                       
    l_extendedprice DECIMAL(15,2) NOT NULL,                    
    l_discount      DECIMAL(15,2) NOT NULL,                       
    l_tax           DECIMAL(15,2) NOT NULL,                       
    l_returnflag    CHAR(1) NOT NULL,                             
    l_linestatus    CHAR(1) NOT NULL,                             
    l_shipdate      DATE NOT NULL,                                
    l_commitdate    DATE NOT NULL,                                
    l_receiptdate   DATE NOT NULL,                                
    l_shipinstruct  CHAR(25) NOT NULL,                           
    l_shipmode      CHAR(10) NOT NULL,                           
    l_comment       VARCHAR(44) NOT NULL
);
