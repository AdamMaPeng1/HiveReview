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

-- 函数2： case when then else end  -- 可将

-- 行转列：
    concat(string A/col, string B/col ……) ： 返回输入字符串连接后的结果， 支持任意个输入字符串
    concat_ws(separator, str1/arrar<array>, str2,..)  : 在每个字符串中添加分隔符进行拼接
    concat_set(col) : 分组后的聚合函数：函数只接受 基本数据类型，它的主要作用是将某字段的值进行去重汇总，产生 array 类型字段。

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

-- CASE when then else end 的用法
   select
    *,
    case sex when '男' then 1
        when '女1' then 2
        else 0 end as male,
    case sex when '女' then 1 else 0 end as female
    from demo.emp_sex

/*行转列：concat() , concat_set()
    需求：
       将如下的数据转换成：
          name	constellation	blood_type
           孙悟空  白羊座	    A
           大海   射手座 A
           宋宋   白羊座 B
           猪八戒  白羊座 A
           凤姐	射手座	A
           苍老师	白羊座	B
       转换成：
            射手座,A            大海|凤姐
            白羊座,A            孙悟空|猪八戒
            白羊座,B            宋宋|苍老师

  */
-- 1、创建 表 ： pracCollectSet
create table if not exists demo.pracCollectSet(
    name string,
    constellation string,
    blood_type string
)
row format delimited fields terminated by '\t';
-- 2、装载数据
/*需求：
       将如下的数据转换成：
          name	constellation	blood_type
           孙悟空  白羊座	    A
           大海   射手座 A
           宋宋   白羊座 B
           猪八戒  白羊座 A
           凤姐	射手座	A
           苍老师	白羊座	B
       转换成：
            射手座,A            大海|凤姐
            白羊座,A            孙悟空|猪八戒
            白羊座,B            宋宋|苍老师
  */
-- 1）先合并 星座+血型
select
    name,
    concat_ws(',', constellation,blood_type)
from demo.pracCollectSet;
-- 需求最终结果：2）根据星座+血型的组合字段分组，分组后的name字段进行聚合，使用 collect_set(name)形成去重数组
select
    concat_ws('|', collect_set(name)),
    con
from (
    select
        name,
        concat_ws(',', constellation,blood_type) con
    from demo.pracCollectSet
) t
group by con;

/*函数：
  列转行：
      炸裂：explode(col):将 hive一列中复杂的 array 或者 map 结构拆分成多行
      侧写：lateral view  udtf(expression) tableAlias as columnAlias
      split(str, regex) : 通过正则表达式将字符串切分为字符数组
        如： > SELECT split('oneAtwoBthreeC', '[ABC]') FROM src LIMIT 1;
               ["one", "two", "three"]
  需求：
将如下数据：
movie	category
《疑犯追踪》	悬疑,动作,科幻,剧情
《Lie to me》	悬疑,警匪,动作,心理,剧情
《战狼2》	战争,动作,灾难
转换成：
《疑犯追踪》	悬疑
《疑犯追踪》	动作
《疑犯追踪》	科幻
《疑犯追踪》	剧情
《Lie to me》	悬疑
《Lie to me》	警匪
《Lie to me》	动作
《Lie to me》	心理
《Lie to me》	剧情
《战狼2》	战争
《战狼2》	动作
《战狼2》	灾难
  */
-- 1、创建表：pracExplode
create table demo.pracExplode(
    movie string,
    category string)
row format delimited fields terminated by "\t";
-- 2、将数据加载到表中
load data local inpath "/home/atguigu/practiceFile/pracExplode.txt" into table demo.pracExplode;

select
    movie,
    split(category, ',')[1]
from demo.pracExplode;


select
    movie,
    categoryAlias
from demo.pracExplode
lateral view
    explode(split(category, ',')) explodeAlias as categoryAlias;


select * from demo.userinfo;

-- select
--     name father,
--     childrenName,
--     childrenAge
-- from demo.userinfo
-- lateral view
--     explode(children) userInfoAlias as childrenName,childrenAge ;






























