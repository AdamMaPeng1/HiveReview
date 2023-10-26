/*
TODO 动态分区
    hive 中的动态分区，需要进行一些配置：
  */
-- 1) 开启动态分区（默认是开启的）
set hive.exec.dynamic.partition=true;
-- 2) 关闭严格模式
set hive.exec.dynamic.partition.mode=nonstrict;
-- 3) 修改所有MR节点的动态分区个数:默认1000
set hive.exec.max.dynamic.partitions=2000;
-- 4) 修改单个节点 动态分区个数：默认100
set hive.exec.dynamic.partitions.pernode=365;
-- 5) 通过Hive 最大可以创建的HDFS 的文件个数, 默认100000
set hive.exec.max.created.files=200000;
-- 6) 当有空分区生成时，是否报异常，默认false
set hive.error.on.empty.partition=false;

-- 需求：将dept表中的数据按照地区（loc字段），插入到目标表dept_partition的相应分区中。
-- 1） 查询 dept 表
select * from demo.dept;
-- 2) 创建dept_partition 表， 以 loc 为动态分区

-- 2.2 创建动态分区表
create table if not exists demo.dept_dynamic_partition(
    deptno int      comment "部门编号",
    dname  string   comment "部门名称"
)
comment "部门动态分区表"
partitioned by (loc string)
row format delimited fields terminated by '\t';

-- 2.1 开启参数
-- 2.1.1 开启动态分区
set hive.exec.dynamic.partition=true;
-- 2.1.2 开启非严格模式
set hive.exec.dynamic.partition.mode=nonstrict;
-- 向动态分区表中插入数据
insert into table demo.dept_dynamic_partition
    partition(loc)
select deptno,dname,loc from demo.dept;

-- 查看 分区表有多少个分区
show partitions demo.dept_dynamic_partition;

/*
TODO 分桶表
   分区：分目录
   分桶：分文件
*/
-- 创建student表
create table if not exists demo.stuBucket(
    id int,
    name string
)
clustered by (id) into 4 buckets
row format delimited fields terminated by '\t';

desc database extended demo;
desc formatted demo.stuBucket;

select * from demo.stuBucket;

show tables from demo;

-- 创建分桶表2
create table if not exists demo.stuBucket2(
    id int,
    name string
)
clustered by (id) into 5 buckets
row format delimited fields terminated by '\t';
-- 通过insert 方式向 stuBucket2 中插入数据
insert into table  demo.stuBucket2 select * from demo.stuBucket;
select * from demo.stuBucket2;

/*
TODO 抽样查询：tablesample(bucket 1 out of 3 on id)
*/
select * from demo.stuBucket tablesample (bucket 3 out of 3 on id);

/*
TODO 函数
-- 数值类
max,min,sum,avg,sqrt,
-- 字符串类
-- 日期类

-- 函数1：nvl()
    空字段赋值: nvl(字段A， 赋值X)

-- 函数2： case when then else end

*/
-- 1、查看系统自带的函数
show functions;
-- 2、查看某个函数的用法
desc function upper;
-- 3. 查看某个函数的详细用法
desc function extended upper;

-- 空字段赋值: nvl(field, newValue);
select
    id,
    name,
    nvl(id, 1111) isNull
from demo.stuBucket;

-- 查看 emp ，如果 comm 为null，则赋值为 -1
select  *,nvl(comm, -1) from demo.emp;

-- 查询：如果员工的 comm 为null ， 则用 领导 id 代替
select comm, nvl(comm, mgr) from demo.emp;

-- 验证 ：case when then else end
create table demo.emp_sex(
name string,
dept_id string,
sex string)
row format delimited fields terminated by "\t";
/*需求：求出不同部门男女各多少人。
dept_Id     男       女
A     		2       1
B     		1       2
  */
select * from demo.emp_sex;
    select
    *,
    case sex when '男' then 1 else 0 end as male,
    case sex when '女' then 1 else 0 end as female
    from demo.emp_sex;

select
    dept_id,
    sum(male) maleCount,
    sum(female) femaleCount
from (
    select
    *,
    case sex when '男' then 1 else 0 end as male,
    case sex when '女' then 1 else 0 end as female
    from demo.emp_sex
) t
group by dept_id;














































