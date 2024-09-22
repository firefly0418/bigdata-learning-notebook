# 拉链表概述

## 缓慢变化维

通常我们用一张维度表来维护维度信息，比如用户手机号码信息。然而随着时间的变化，某些用户信息会发生改变，这就是所谓的缓慢变化维。需要注意的是，这里的缓慢变化是相对事实表而言的，事实表的变化速度要快得多。

针对缓慢变化维问题，通常有以下几种处理方式：

**1）仅保留每个用户最新的一条维度信息**

​	这种方法比较简单粗暴，维度只考虑最新就行，保证了维度值的唯一性。但缺点是无法查看历史信息，在需要回溯查看数据的场景就不适用了，可能需要去原始数据查询，及其不方便。

**2）仅保留每个用户最初的一条维度信息**

​	这种就相当于一次填写，终身不允许修改，那么在实际关联数据时，很可能获取的是无效的维度信息。比如某个用户的手机号以及变了，但是维度表中仍然保留最初的手机号，这就导致数据关联结果是错误的。而且对于用户来说，一旦手残录入错误就无法再更改，用户的体验也是不好的。

**3）用新增行的方式在维度表中同时保留所有变化的维度信息**

​	这种方式其实跟拉链表很接近了，就是用户每改一次信息，就在维度表中新增一行，只不过这里的历史数据和新增数据如何区分，以及他们的有效时间范围如何区分，就是需要着重考虑的问题了。

**4）用新增列的方式在维度表中同时保留所有变化的维度信息**

​	这个方式的优势就是维度表的行数可以不变，只需要新增列，但是缺点也很明显，新增列意味着表结构会一直变化，而且也没有办法确定到底要新增几列。

## 拉链表定义

拉链表就是记录一个事物从开始到当前状态的变化过程的数据表，主要是用于维度发生变化的场景，也即我们常说的缓慢变化维。

比如说我们用一张维度表记录用户的手机号码，但是随着时间推进，用户可能某一天会换手机号，这时我们的维度表就需要相应的更改，这时我们就可以用拉链表来进行记录，这就实现了保留历史数据的同时，还能查询最新维度信息。可以说拉链表其实是解决缓慢变化维的最佳方案了。

一个简单的拉链表示例如下：

| userid | tel  | start_dt |  end_dt  |
| :----: | :--: | :------: | :------: |
|   01   | 111  | 20240101 | 20240601 |
|   01   | 222  | 20240602 | 99991231 |
|   02   | 333  | 20240101 | 99991231 |

每行记录都表示一个用户的属性值以及对应的日期有效范围，如果是最新的数据，则结束日期是99991231。用户01的联系方式发生过变化，因此会有两条数据记录。

# 拉链表的实现

## 常规拉链表

### 历史数据

现在有一批数据如下所示，表示用户的属性值以及传回来的日期和时间戳（单位s）：

```sql
with data1 as (
    select '01' as userid, 'ab' as addr, '20220101' as dt, 1641039513 as ts union all
    select '01' as userid, 'ab' as addr, '20220103' as dt, 1641211200 as ts union all
    select '01' as userid, 'cd' as addr, '20220108' as dt, 1641607200 as ts union all
    select '02' as userid, 'ab' as addr, '20220101' as dt, 1641039480 as ts union all
    select '02' as userid, 'bc' as addr, '20220104' as dt, 1641261600 as ts union all
    select '02' as userid, 'cd' as addr, '20220109' as dt, 1641639600 as ts union all
    select '03' as userid, 'ab' as addr, '20220101' as dt, 1641038400 as ts union all
    select '03' as userid, 'cd' as addr, '20220101' as dt, 1641002400 as ts union all
    select '03' as userid, 'ab' as addr, '20220107' as dt, 1641520800 as ts
)
```

历史数据的处理规则：

**1）同一天仅保留最新一条数据**

```sql
select userid, addr, dt, ts
from (
	select 
		userid, addr, dt, ts
		row_number() over (partition by userid, dt order by ts desc) rn
	from data1
) ta
where rn = 1;
```

**2）获取每个用户每个属性最早的一条数据**

```sql
with data2 as (
    select userid, addr, dt, ts
    from (
        select 
            userid, addr, dt, ts,
            row_number() over (partition by userid, dt order by ts desc) rn
        from data1
    ) ta
    where rn = 1
)
select userid, addr, dt, ts
from (
    select
        userid, addr, dt, ts,
        row_number() over (partition by userid, addr order by dt) rn
    from data2
) tb
where rn = 1;
```

这样处理以后数据如下所示：
<br />![image.png](/hive/images/image-20240922152535397.png)<br />

**3）获取当前行的下一行日期数据并处理截止日期**

这一步我们需要得到每个用户每个属性的下一行，用来获取当前属性的截止日期。截止日期的处理条件：如果为空则用99991231填充，否则就用next_dt减一天来填充。

上一步的处理结果我们放到data3中，部分代码会做省略处理：

```sql
with data3 as (
    select userid, addr, dt, ts
    from (
        select
            userid, addr, dt, ts,
            row_number() over (partition by userid, addr order by dt) rn
        from data2
    ) tb
    where rn = 1
)
select
	userid, addr, dt start_dt,
	if(next_dt is null, '99991231', date_format(date_add(from_unixtime(unix_timestamp(next_dt, 'yyyyMMdd'), 'yyyy-MM-dd'), -1), 'yyyyMMdd')) 
	end_dt
from (
    select
        userid, addr, dt, ts,
        lead(dt) over (partition by userid order by dt) next_dt
    from data3
) tc
```

得到的结果如下：
<br />![image.png](/hive/images/image-20240922153641147.png#pic_center=180x180)<br />


完整的代码如下：

```sql
with data1 as (
    select '01' as userid, 'ab' as addr, '20220101' as dt, 1641039513 as ts union all
    select '01' as userid, 'ab' as addr, '20220103' as dt, 1641211200 as ts union all
    select '01' as userid, 'cd' as addr, '20220108' as dt, 1641607200 as ts union all
    select '02' as userid, 'ab' as addr, '20220101' as dt, 1641039480 as ts union all
    select '02' as userid, 'bc' as addr, '20220104' as dt, 1641261600 as ts union all
    select '02' as userid, 'cd' as addr, '20220109' as dt, 1641639600 as ts union all
    select '03' as userid, 'ab' as addr, '20220101' as dt, 1641038400 as ts union all
    select '03' as userid, 'cd' as addr, '20220101' as dt, 1641002400 as ts union all
    select '03' as userid, 'ab' as addr, '20220107' as dt, 1641520800 as ts
)
, data2 as (
    select userid, addr, dt, ts
    from (
        select 
            userid, addr, dt, ts,
            row_number() over (partition by userid, dt order by ts desc) rn
        from data1
    ) ta
    where rn = 1
)
, data3 as (
    select userid, addr, dt, ts
    from (
        select
            userid, addr, dt, ts,
            row_number() over (partition by userid, addr order by dt) rn
        from data2
    ) tb
    where rn = 1
)
select
	userid, addr, dt start_dt,
	if(next_dt is null, '99991231', date_format(date_add(from_unixtime(unix_timestamp(next_dt, 'yyyyMMdd'), 'yyyy-MM-dd'), -1), 'yyyyMMdd')) 
	end_dt
from (
    select
        userid, addr, dt, ts,
        lead(dt) over (partition by userid order by dt) next_dt
    from data3
) tc
```



### 每日新增数据

新增数据如下：

```sql
with new_data1 as (
    select '01' as userid, 'ab' as addr, '20220121' as dt, 1642723200 as ts union all
    select '02' as userid, 'cd' as addr, '20220121' as dt, 1642723200 as ts union all
    select '04' as userid, 'ef' as addr, '20220121' as dt, 1642723200 as ts union all
    select '04' as userid, 'xg' as addr, '20220121' as dt, 1642723300 as ts union all
    select '05' as userid, 'xy' as addr, '20220127' as dt, 1642723200 as ts
)
```

新增数据的处理：

**1）保留最新一条数据**

新增数据的处理很简单，因为一般是增量读取某一天的数据，因此我们只要保证每个用户只保留最新一条数据即可。

```sql
select userid, addr, dt, ts
from (
    select 
        userid, addr, dt, ts,
        row_number() over (partition by userid, dt order by ts desc) rn
    from new_data1
) ta
where rn = 1
```

处理之后结果如下所示，可以看到每个用户只剩下了最新的一条数据：
<br />![image.png](/hive/images/image-20240922155434534.png#pic_center=180x180)<br />


2）结束日期均设置为99991231

```sql
with new_data2 as (
    select userid, addr, dt, ts
    from (
        select 
            userid, addr, dt, ts,
            row_number() over (partition by userid, dt order by ts desc) rn
        from new_data1
    ) ta
    where rn = 1
)
select userid, addr, dt start_dt, '99991231' end_dt
from new_data2;
```

### 历史数据与新增数据的合并

**1）历史数据与新增数据的全连接**

取历史数据的开链数据（结束日期为99991231）与新增数据进行全连接：

```sql
select 
	t1.userid old_userid, t1.addr old_addr, t1.start_dt old_start_dt, t1.end_dt old_end_dt,
	t2.userid new_userid, t2.addr new_addr, t2.start_dt new_start_dt, t2.end_dt new_end_dt
from (
    select userid, addr, start_dt, end_dt
    from history_data
    where end_dt = '99991231'
) t1
full join new_data t2
on t1.userid = t2.userid
;
```

全连接的结果如下：
<br />![image.png](/hive/images/image-20240922174651446.png#pic_center=180x180)<br />


**2）全连接以后的条件处理**

**a）新旧属性相同或新旧属性不同且旧属性开始日期较大，则仅保留old数据**

主要针对两种情况：

一是当新旧属性相同时，仅保留旧属性，这是因为大多数情况下旧属性的日期比较早。不过如果出现重刷数据时，可能新属性的日期早于旧属性，这时应当只保留旧属性。

二是当新旧属性不同，且旧属性的开始日期大于新属性的开始日期时，这也是发生了回刷数据的情况，此时仅保留旧属性。

```sql
select
	old_userid userid, old_addr addr, old_start_dt start_dt, old_end_dt end_dt
from data_join
where old_addr = new_addr or (old_addr != new_addr and old_start_dt >= new_start_dt);
```

需要处理的数据是这一条：
<br />![image.png](/hive/images/image-20240922183840937.png#pic_center=180x180)<br />


**b）新旧属性不同，new不为空时保留new，否则保留old**

此时针对的是三种情况：

一是只有old数据则保留old数据；二是只有new数据则保留new数据；三是old与new都不为空且不相同时，仅保留new数据。

```sql
select
	coalesce(new_userid, old_userid) userid,
	coalesce(new_addr, old_addr) addr,
	coalesce(new_start_dt, old_start_dt) start_dt,
	coalesce(new_end_dt, old_end_dt) end_dt
from data_join
where old_addr is null or new_addr is null or (old_addr != new_addr and old_start_dt < new_start_dt);
```

这里处理的数据是这几条：
![image-20240922184003135](/hive/images/image-20240922184003135.png)

**c）old与new同时不为空且不相同，保留old数据并对old数据的结束日期做处理**

此时这条数据的new部分已经在第二种情形中做了保留，而old数据需要做一个闭链处理，也就是用新增数据的开始日期做填充。

```sql
select
	old_userid userid,
	old_addr addr,
	old_start_dt start_dt,
	date_format(from_unixtime(unix_timestamp(new_start_dt, 'yyyyMMdd')-24*3600, 'yyyy-MM-dd'), 'yyyyMMdd') end_dt
from data_join
where old_addr != new_addr and old_start_dt < new_start_dt;
```

这里处理的是这条数据：
![image-20240922184100331](/hive/images/image-20240922184003135.png)


完整的代码如下：

```sql
with history_data as (
    select '01' as userid, 'ab' as addr, '20220101' as start_dt, '20220107' as end_dt union all
    select '01' as userid, 'cd' as addr, '20220108' as start_dt, '99991231' as end_dt union all
    select '02' as userid, 'ab' as addr, '20220101' as start_dt, '20220103' as end_dt union all
    select '02' as userid, 'bc' as addr, '20220104' as start_dt, '20220108' as end_dt union all
    select '02' as userid, 'cd' as addr, '20220109' as start_dt, '99991231' as end_dt union all
    select '03' as userid, 'ab' as addr, '20220101' as start_dt, '99991231' as end_dt
)
, new_data as (
    select '01' as userid, 'ab' as addr, '20220121' as start_dt, '99991231' as end_dt union all
    select '02' as userid, 'cd' as addr, '20220121' as start_dt, '99991231' as end_dt union all
    select '04' as userid, 'xg' as addr, '20220121' as start_dt, '99991231' as end_dt union all
    select '05' as userid, 'xy' as addr, '20220121' as start_dt, '99991231' as end_dt
)
, data_join as (
    select 
        t1.userid old_userid, t1.addr old_addr, t1.start_dt old_start_dt, t1.end_dt old_end_dt,
        t2.userid new_userid, t2.addr new_addr, t2.start_dt new_start_dt, t2.end_dt new_end_dt
    from (
        select userid, addr, start_dt, end_dt
        from history_data
        where end_dt = '99991231'
    ) t1
    full join new_data t2
    on t1.userid = t2.userid
)
select
	old_userid userid, old_addr addr, old_start_dt start_dt, old_end_dt end_dt
from data_join
where old_addr = new_addr or (old_addr != new_addr and old_start_dt >= new_start_dt)
union all
select
	coalesce(new_userid, old_userid) userid,
	coalesce(new_addr, old_addr) addr,
	coalesce(new_start_dt, old_start_dt) start_dt,
	coalesce(new_end_dt, old_end_dt) end_dt
from data_join
where old_addr is null or new_addr is null or (old_addr != new_addr and old_start_dt < new_start_dt)
union all
select
	old_userid userid,
	old_addr addr,
	old_start_dt start_dt,
	date_format(from_unixtime(unix_timestamp(new_start_dt, 'yyyyMMdd')-24*3600, 'yyyy-MM-dd'), 'yyyyMMdd') end_dt
from data_join
where old_addr != new_addr and old_start_dt < new_start_dt;
```

最终的结果如下：
![image-20240922184544890](/hive/images/image-20240922184003135.png)


## 分区拉链表

分区拉链表其实只要将end_dt当作分区日期即可，这样每次取历史数据的开链数据与新增数据计算，得到的数据中包含了一部分99991231分区数据，一部分是新增日期分区（通常是该日期前一天）数据。之后采用动态分区写入的方式，覆盖写指定分区即可。

分区拉链表的优势：

- 写入时只需要按分区写入，不需要全表覆盖写，当数据表的体量较大时，优势比较大；







