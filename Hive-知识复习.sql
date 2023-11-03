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
select * from demo.stuBucket tablesample (bucket 3 out of 3 on demo.stuBucket.id);

select sqrt(4);
select round(rand()*100);
/*
TODO 函数
-- 单行函数
    数值函数
        round(double x[, int y]) : 四舍五入
        sqrt() ：开方
        ceil() ：向上取整
        floor()：向下取整
        rand() : 生成 0，1之间的随机数
        abs()  : 绝对值
    字符串函数：
        substring(string a, int start, int end) : 截取字符串
        upper():
        lower():
        split():
        concat():
        concat_ws():
        replaces()   :
        regexp_replace:
        length():
        get_json_object():
    日期函数：
        unix_timestamp():
        from_unixtime():
        current_date() :
        current_timestamp():
        month():
        day():
        hour():
        datediff:
        date_add:
        date_sub():
        date_format():
    流程控制函数:
        nvl()
        if()
        case when then else end
        coalesce(col1,col2,defaultValue): 新增加一列，对col1，col2 列的空值进行查找，如果都为空，则新增列为dV，如果都不是则为col1的值。
    集合函数
        array(val1,val2,……):
        array_contains(Array<T>, value):
        sort_array():
        size(array<T>):
        map(k1,v1,k2,v2):
        map_keys(map<k,v>):返回map中的key
        map_values(map<k,v>):
        struct(v1,v2,v3):
        named_struct():声明 struct 的属性和值

-- 聚合函数:
       count(), min(), max() ,sum(), avg() ,……
       -- 1.collect_list
       -- 2.collect_set
-- 炸裂函数
       explode():
           语法一：explode(array<T> a)
           语法二：explode(map<K,V> m)
        posexplode():
           语法：posexplode(array<T> a)
        inline():
            语法：inline(array<struct<f1:T1,...,fn:Tn>> a)
                select inline(`array`(struct('Id',1,'Name','Adam','Age',18),struct('Id',2,'Name','Tom','Age',22))); 所有数据都作为值出现
                select inline(`array`(named_struct('Id',1,'Name','Adam','Age',18),named_struct('Id',2,'Name','Tom','Age',22))) ; --struct 中的第一个数据作为字段名出现
-- 窗口函数
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

/*TODO 函数-单行函数-数值函数*/
select abs(-12.5),
       sqrt(9),
       round(-12.341535,3),
       round(rand()*100),
       `floor`(12.6),
       ceil(-12.6);
/*TODO 函数-单行函数-字符串函数*/
select length('Adam'),
       repeat('Adam', 2),
       upper('ads'),
       lower('Ada'),
       substring('Adam Mapeng', -5),
       replace('Adam Mapeng', 'a', 'e'),
       trim('  Adam Mapeng   '),
       regexp_replace('100-200', '(\\d+)', 'num'),
        'dfsaaaa' regexp 'dfsa+',
         'dfsaaaa' regexp 'dfsb+',
         split('192.168.11.12','\\.'),
         concat('Adam', 'Mapeng'),
         concat_ws('.',`array`("192","168","11","12")),
         get_json_object('[{"name":"大海海","sex":"男","age":"25"},{"name":"小宋宋","sex":"男","age":"47"}]','$.[0].name')
       ;
/*TODO 函数-单行函数-日期函数
--    日期函数：
        unix_timestamp():
        from_unixtime():
        current_date() :
        current_timestamp():
        month():
        day():
        hour():
        datediff:
        date_add:
        date_sub():
        date_format():
  */
select
    year('2023-12-21'),
    month('2023-12-21'),
    weekofyear('2023-12-21'),
    dayofmonth('2023-12-21'),
    `dayofweek`('2023-12-21'),
    day('2023-12-21'),
    `current_date`(),
    `current_timestamp`(),
    date_add(`current_date`(),1),
    date_sub(`current_date`(),1),
    datediff('2023-10-15','2023-10-01'),
    date_format(`current_timestamp`(),'yyyy/MM/dd HH:mm:ss');
/*TODO 函数-单行函数-集合函数*/
select
    `array`(3,4,5,3)[1],
    array_contains(`array`(2,32,4,55,46),2),
    sort_array(`array`(2,32,4,55,46)),
    size(`array`(2,32,4,55,46)),
    `map`('name','Adam','age',12)['name'],
    map_keys(`map`('name','Adam','age',12)),
    map_values(`map`('name','Adam','age',12)),
    struct('adam',12),
    named_struct('Name','Adam','age',18);
/*TODO 函数-单行函数-流程控制函数*/
--空字段赋值: nvl(field, newValue);
select
    id,
    name,
    nvl(id, 1111) isNull
from demo.stuBucket;
-- 查看 emp ，如果 comm 为null，则赋值为 -1
select  *,nvl(comm, -1) from demo.emp;
-- 查询：如果员工的 comm 为null ， 则用 领导 id 代替
select comm, nvl(comm, mgr) from demo.emp;
--case when then else end 函数
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
    from demo.emp_sex;

/*练习： 函数-单行函数*/
-- 1.建表
create  table  demo.singleLineFunc(
    name string,  --姓名
    sex  string,  --性别
    birthday string, --出生年月
    hiredate string, --入职日期
    job string,   --岗位
    salary double, --薪资
    bonus double,  --奖金
    friends array<string>, --朋友
    children map<string,int> --孩子
);
-- 2.向表中插入数据
 insert into demo.singleLineFunc
  values('张无忌','男','1980/02/12','2022/08/09','销售',3000,12000,array('阿朱','小昭'),map('张小无',8,'张小忌',9)),
        ('赵敏','女','1982/05/18','2022/09/10','行政',9000,2000,array('阿三','阿四'),map('赵小敏',8)),
        ('宋青书','男','1981/03/15','2022/04/09','研发',18000,1000,array('王五','赵六'),map('宋小青',7,'宋小书',5)),
        ('周芷若','女','1981/03/17','2022/04/10','研发',18000,1000,array('王五','赵六'),map('宋小青',7,'宋小书',5)),
        ('郭靖','男','1985/03/11','2022/07/19','销售',2000,13000,array('南帝','北丐'),map('郭芙',5,'郭襄',4)),
        ('黄蓉','女','1982/12/13','2022/06/11','行政',12000,null,array('东邪','西毒'),map('郭芙',5,'郭襄',4)),
        ('杨过','男','1988/01/30','2022/08/13','前台',5000,null,array('郭靖','黄蓉'),map('杨小过',2)),
        ('小龙女','女','1985/02/12','2022/09/24','前台',6000,null,array('张三','李四'),map('杨小过',2));
    select * from demo.singleLineFunc;
-- 需求1 ： 统计每个月的入职人数
select
    month(replace(hiredate,'/','-')) hireMonth,
    count(name) hireCount
from
    demo.singleLineFunc
group by
    month(replace(hiredate,'/','-'));

-- 需求2：查询每个人的年龄（年 + 月）
/*
name	age
张无忌	42年8月
赵敏	    40年5月
宋青书	41年7月
*/
-- 错误解答
select
    name,
    concat(`floor`(datediff(`current_date`(),replace(birthday,'/','-'))/365),'年',
           `round`(datediff(`current_date`(),replace(birthday,'/','-'))%365/30),'月')
from demo.singleLineFunc;
-- 正确解答
--1） 将 birthday 中的 / 替换成标准日期的 - 分隔符
select
    name,
    replace(birthday,'/','-')
from
    demo.singleLineFunc;
--2）获取 当前时间与生日的年、月差值
select
    name,
    (year(`current_date`()) - year(birth)) yearDiff,
    (month(`current_date`()) - month(birth)) monthDiff
from (
    select
        name,
        replace(birthday,'/','-') birth
    from
        demo.singleLineFunc
) t1;
-- 判断 年，月是否>=0,
select
    name,
    concat( `if`(monthDiff>=0, yearDiff, yearDiff-1), '年',`if`(monthDiff>=0, monthDiff, monthDiff+12), '月') age
from (
    select
        name,
        (year(`current_date`()) - year(birth)) yearDiff,
        (month(`current_date`()) - month(birth)) monthDiff
    from (
        select
            name,
            replace(birthday,'/','-') birth
        from
            demo.singleLineFunc
    ) t1
) t2;
-- 案例3：按照薪资，奖金的和进行倒序排序，如果奖金为null，置位0
-- 1.如果奖金为null，则置为 0
select
    name,
    salary + `if`(bonus is null, 0, bonus) totalSal,
    salary,
    `if`(bonus is null, 0, bonus) bonu
from demo.singleLineFunc
order by salary + `if`(bonus is null, 0, bonus) desc;
-- 优化：使用 nvl 函数， & order by 是在字段结果后的，所以可以使用别名
select
    name,
    (salary + nvl(bonus,0)) totalSal
from
    demo.singleLineFunc
order by
    totalSal;
-- 案例4：查询每个人有多少个朋友
select
    name,
    size(friends) friendsCount
from demo.singleLineFunc;
-- 案例5：查询每个人的孩子的姓名
-- 1）先得到孩子的列表
select
    name,
    map_keys(children) Childrens
from demo.singleLineFunc;
-- 2）将孩子的列表进行炸裂
select
    name parentName,
    c1 childrenName
from (
    select
        name,
        map_keys(children) Childrens
    from demo.singleLineFunc
) t1
lateral view
    explode(Childrens) tmpAlias as c1;
-- 案例六：查询每个岗位男女各多少人
select
    job,
    nvl(sum(case sex when '男' then 1 end),0) male,
    nvl(sum(case sex when '女' then 1 end),0) famale
from demo.singleLineFunc
group by job;
-- 优化：if
select
    job,
    sum(`if`(sex='男',1,0)) male,
    sum(`if`(sex='女',1,0)) female
from demo.singleLineFunc
group by
    job;
/*
TODO 函数-聚合函数
-- 1.collect_list
-- 2.collect_set
*/
-- 1.collect_list
select
    sex,
    collect_list(job) jobList
from demo.singleLineFunc
group by
    sex;
-- 2.collect_set
select
    sex,
    collect_set(job) jobList
from demo.singleLineFunc
group by
    sex;
-- （1）每个月的入职人数以及姓名
select
    month(replace(hiredate,'/','-')) Months,
    count(name) hireCount,
    collect_list(name)
from demo.singleLineFunc
group by
    month(replace(hiredate,'/','-'));

/*TODO-函数-炸裂函数
  --
       explode():
           语法一：explode(array<T> a)
           语法二：explode(map<K,V> m)
        posexplode():
           语法：posexplode(array<T> a)
        inline():
            语法：inline(array<struct<f1:T1,...,fn:Tn>> a)
                select inline(`array`(struct('Id',1,'Name','Adam','Age',18),struct('Id',2,'Name','Tom','Age',22))); 所有数据都作为值出现
                select inline(`array`(named_struct('Id',1,'Name','Adam','Age',18),named_struct('Id',2,'Name','Tom','Age',22))) ; --struct 中的第一个数据作为字段名出现
*/
select explode(array(1,3,4,5,6)) as num;
select explode(`map`('name','Adam','age',18)) as (col1,col2);
select posexplode(array(1,3,4,5,6)) as (index,num);
-- select posexplode(`map`('name','Adam','age',18)) as (col1,col2);     posexplode()只接受 array 数组
select inline(`array`(named_struct('Id',1,'Name','Adam','Age',18),named_struct('Id',2,'Name','Tom','Age',22))) ;
select inline(`array`(struct('Id',1,'Name','Adam','Age',18),struct('Id',2,'Name','Tom','Age',22))) ;
/*
需求：
    统计各分类的电影数量
剧情	2
动作	3
心理	1
*/
-- 1) 炸裂，将电影的所属类别均展开
select
    movie,
    cateAlias
from
    demo.pracExplode
lateral view explode(split(category,',')) categoryAlias as cateAlias;
-- 按照类别统计电影数量
select
    cateAlias,
    count(movie) movieCount
from (
    select
        movie,
        cateAlias
    from
        demo.pracExplode
    lateral view explode(split(category,',')) categoryAlias as cateAlias
) t1
group by cateAlias;

/*
高级聚合函数： collect_list(), collect_set()
*/
-- 需求：求每个月的入职人员和姓名
select
    month(replace(birthday,'/','-')) month,
    count(name) hireCount,
    collect_set(name) nameList
from
    demo.singleLineFunc
group by
    month(replace(birthday,'/','-'));



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
-- 最终sql
select
    movie,
    categoryAlias
from demo.pracExplode
lateral view
    explode(split(category, ',')) explodeAlias as categoryAlias;

-- select
--     name father,
--     childrenName,
--     childrenAge
-- from demo.userinfo
-- lateral view
--     explode(children) userInfoAlias as childrenName,childrenAge ;

/*函数：开窗函数
    over()
    lag():
    lead():
    ntile()

需求：
数据样例如下：
name，orderdate，cost
jack,2017-01-01,10
tony,2017-01-02,15
jack,2017-02-03,23
tony,2017-01-04,29
jack,2017-01-05,46
（1）查询在2017年4月份购买过的顾客及总人数
（2）查询顾客的购买明细及月购买总额
（3）上述的场景, 将每个顾客的cost按照日期进行累加
（4）查询每个顾客上次的购买时间
（5）查询前20%时间的订单信息
*/
-- 1.建表
create table demo.pracOver(
name string,
orderdate string,
cost int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
-- 2.装载数据
load data local inpath "/home/atguigu/practiceFile/pracOver.txt" into table demo.pracOver;
-- 3.需求1：查询在2017年4月份购买过的顾客及总人数
show databases ;


























