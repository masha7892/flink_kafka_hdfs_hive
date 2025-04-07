create database if not exists zhaohua_liu_flink_test;
use zhaohua_liu_flink_test;
show tables ;
drop table if exists zhaohua_liu_flink_test.student_info;


create external table if not exists zhaohua_liu_flink_test.student_info(
    id int,
    name string,
    school string,
    ts bigint
)
    partitioned by(ds string)
    row format delimited fields terminated by ","
    stored as orc
    location "/flink/flink/zhaohua_liu_flink_test/student_info"
    tblproperties ("orc.compress=snappy");


load data inpath "/flink/flink/kafka/20250406"  overwrite into table   zhaohua_liu_flink_test.student_info partition (ds="20250406");
load data inpath "/flink/flink/kafka/20250407"  overwrite into table   zhaohua_liu_flink_test.student_info partition (ds="20250407");

select * from zhaohua_liu_flink_test.student_info;


-- {"info":{"id":125,"name":"张三"},"school":"长安小学","ds":"20250406","ts":1743900102057}
-- {"info":{"id":126,"name":"李四"},"school":"南京小学","ds":"20250406","ts":1743900168054}
-- {"info":{"id":127,"name":"王五"},"school":"长安小学","ds":"20250407","ts":1743900192051}
-- {"info":{"id":127,"name":"赵六"},"school":"北京小学","ds":"20250407","ts":1743900213063}