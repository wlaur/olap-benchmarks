-- Layouts reviewed against https://github.com/apache/doris/blob/master/tools/tpch-tools/ddl/create-tpch-tables-sf1.sql
-- Canonical column types/nullability; one replica, no colocation or precomputed views.


DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS lineitem;

CREATE TABLE region (
    r_regionkey BIGINT NOT NULL,
    r_name VARCHAR(25) NOT NULL,
    r_comment VARCHAR(152)
) ENGINE = OLAP
DUPLICATE KEY (r_regionkey)
DISTRIBUTED BY HASH (r_regionkey) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE nation (
    n_nationkey BIGINT NOT NULL,
    n_name VARCHAR(25) NOT NULL,
    n_regionkey BIGINT NOT NULL,
    n_comment VARCHAR(152)
) ENGINE = OLAP
DUPLICATE KEY (n_nationkey)
DISTRIBUTED BY HASH (n_nationkey) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE supplier (
    s_suppkey BIGINT NOT NULL,
    s_name VARCHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey BIGINT NOT NULL,
    s_phone VARCHAR(15) NOT NULL,
    s_acctbal DECIMAL(15, 2) NOT NULL,
    s_comment VARCHAR(101) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (s_suppkey)
DISTRIBUTED BY HASH (s_suppkey) BUCKETS 6
PROPERTIES ("replication_num" = "1");

CREATE TABLE customer (
    c_custkey BIGINT NOT NULL,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey BIGINT NOT NULL,
    c_phone VARCHAR(15) NOT NULL,
    c_acctbal DECIMAL(15, 2) NOT NULL,
    c_mktsegment VARCHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (c_custkey)
DISTRIBUTED BY HASH (c_custkey) BUCKETS 12
PROPERTIES ("replication_num" = "1");

CREATE TABLE part (
    p_partkey BIGINT NOT NULL,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr VARCHAR(25) NOT NULL,
    p_brand VARCHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INTEGER NOT NULL,
    p_container VARCHAR(10) NOT NULL,
    p_retailprice DECIMAL(15, 2) NOT NULL,
    p_comment VARCHAR(23) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (p_partkey)
DISTRIBUTED BY HASH (p_partkey) BUCKETS 12
PROPERTIES ("replication_num" = "1");

CREATE TABLE partsupp (
    ps_partkey BIGINT NOT NULL,
    ps_suppkey BIGINT NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment VARCHAR(199) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (ps_partkey)
DISTRIBUTED BY HASH (ps_partkey) BUCKETS 12
PROPERTIES ("replication_num" = "1");

CREATE TABLE orders (
    o_orderkey BIGINT NOT NULL,
    o_orderdate DATE NOT NULL,
    o_custkey BIGINT NOT NULL,
    o_orderstatus VARCHAR(1) NOT NULL,
    o_totalprice DECIMAL(15, 2) NOT NULL,
    o_orderpriority VARCHAR(15) NOT NULL,
    o_clerk VARCHAR(15) NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR(79) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (o_orderkey, o_orderdate)
DISTRIBUTED BY HASH (o_orderkey) BUCKETS 32
PROPERTIES ("replication_num" = "1");

CREATE TABLE lineitem (
    l_shipdate DATE NOT NULL,
    l_orderkey BIGINT NOT NULL,
    l_partkey BIGINT NOT NULL,
    l_suppkey BIGINT NOT NULL,
    l_linenumber INTEGER NOT NULL,
    l_quantity DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount DECIMAL(15, 2) NOT NULL,
    l_tax DECIMAL(15, 2) NOT NULL,
    l_returnflag VARCHAR(1) NOT NULL,
    l_linestatus VARCHAR(1) NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode VARCHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL
) ENGINE = OLAP
DUPLICATE KEY (l_shipdate, l_orderkey)
DISTRIBUTED BY HASH (l_orderkey) BUCKETS 32
PROPERTIES ("replication_num" = "1");
