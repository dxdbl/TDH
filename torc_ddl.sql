CREATE TABLE test.test3_orc (
`id` int COMMENT "NULL",
`name` string COMMENT "NULL" )
COMMENT 测试表
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS ORC
TBLPROPERTIES('transactional'='true');
CREATE TABLE test.test2_orc (
`id` int COMMENT "编号",
`name` string COMMENT "NULL" )
COMMENT 姓名
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS ORC
TBLPROPERTIES('transactional'='true');
