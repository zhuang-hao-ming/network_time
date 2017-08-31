
## 脚本功能

### dump_rar_to_db.py

将gps log文件导入数据库。

解压rar文件后，在脚本中指定解压后文件夹的名称，导入的日期区间，id区间，数据表名，然后运行脚本。

### match.py

对于tracks表中的轨迹进行路径匹配，得到和路网重叠的轨迹。

注意：
1. 无论是观察概率还是转移概率， 0 =< p <= 1，对于转移概率，可能出现路径长度为0的情况，此时转移概率为无穷大，应该把它修正为1，否则将发送错误。
2. 搜索半径不宜太小，可以通过实验选定一个值，使得候选点数目>=5。


相关数据表：
1. tracks: 轨迹表，记录组成轨迹的gps_id(引用gps_log_valid.id)，形似如下
```sql
id;points;
3;"{6620312,6742180,6859399,6989629,7133994,7283411,7459430,7682549,7806974,7998900,8169190,8309119,8461679,8720289,6637243,7030556,7179597,7341226,7501349,7702207,7974644,8144506,8425313,8688926,8865065,7217271,7376209,7570786,7780071,7965194,8179681,8493117 (...)"
```

2. gps_log_valid: 有效的gps记录表

```sql
geom;id;log_time;unknown_1;car_id;velocity;direction;on_service;is_valid;
"POINT(800720.215826334 2493425.31159623)";8217330;"2016-05-10 07:32:26";"H";"B4WQ73";1;0;t;t
```

3. shenzhen_line1: 路网表

```sql
gid;startnode;endnode;length;trafficflo;reverse_cost;geom;
76;50011755;50011733;13.997303;1;-1;"LINESTRING(791709.6112 2502650.8141,791703.0492 2502663.0321)"
```

4. path_cost: 距离表，直线距离在5km内的路网点间的网络距离

```sql
start_vid;end_vid;agg_dis;
50008452;50008885;5019.0866451
```

5. match_track: 匹配后轨迹表， 记录匹配完成的轨迹

```sql
id; track_id;line_id;gps_log_id;source;target;length;fraction;v;log_time;geom
5627;3;4806;6620312;50017355;50017267;84.5521927;0.979002073905374;0;"2016-05-10 07:30:11";"POINT(794440.810015321 2499945.41765456)"
```
实现思路：
1. 将所有track分成和和cpu_count()相等数目的组，每一组交由一个进程处理
2. 对于每一条track从数据库获取和它相关的最近点和距离数据，交由st_match算法进行匹配
3. 将匹配结果插入数据库





### construct_track


1. construct_track_all.py，对于构建失败的轨迹，采取打断然后继续构建的策略。
2. construct_track_true.py，对于构建失败的轨迹，直接抛弃。

在这个实验中，为了保证轨迹的完整性，采用脚本2。如果实验要求保留尽可能多的轨迹数据，而不在意轨迹的长度可以选择脚本1。

功能：

将match_track中匹配好的轨迹，还原为路网中一条完整的路径。


实现思路：

1. 选择一条匹配好的轨迹
2. 对轨迹上相邻的记录点，根据记录点的时间和速度，以及记录点在路网上的距离，判断两个记录点的连接是否合法
3. 对于2如果合法则记录连接记录点的路网线id，继续检查。如果被合法则停止检查。
4. 对于3，如果不合法，在脚本2中直接丢弃该条轨迹，在脚本1中，从不合法的位置打断，检查已有的轨迹长度，如果长度超过一个阈值则保留下来，对剩余的记录点继续检查。


### get_red_point_track.py

功能：

从construct_tracks表中提取出红灯轨迹。





### calculate_time

1. calculate_time.py: 用一个月份的数据构建新路网
2. calculate_time_two_month.py: 用两个月份的数据构建新路网
3. calculat_time_prob: 在构建的路网中加入了概率信息

功能：

从red_point_track计算时间



### validate_result


1. validate_result.py: 一般
2. validate_result_shortest.py: 验证结果包括最短路径
3. validate_result_prob.py: 验证结果包括概率信息

功能： 

计算真实轨迹时间和估计轨迹时间


### validate_8_10.py


功能：

8点和10点红灯轨迹通过时间比较

### build_new_routing_network

功能：

使用calculate_time得到的结果，构建新的导航路网。

### pick_virtual_pnt

功能：

虚拟节点选择

### road_graph.py(弃用)

功能：

导入网络数据到内存中。

将网络数据导入内存以后，使用networkx提供的最短路径算法，平均每条路径耗时 0.5s，这个效率难以接受。
改用pgrouting。

### probobilitu_path.py（未完成）

新路网概率分析

### 其它脚本

1. config.py: 数据库连接配置信息
2. connect.py: 连接数据库
3. insert.py: 数据库插入函数
4. query.py: 数据库查询函数



## 数据表

### shenzhen_line1

导入深圳路网数据：

```sql
shp2pgsql -s 32649 -W GBK shenzhen_line.shp shenzhen_line > shenzhen_line.sql
psql -f shenzhen_line.sql road_network postgres
```

进一步处理：

```sql
-- 导入的数据几何字段是multilinestring，将它转化为linestring
SELECT AddGeometryColumn ('public','shenzhen_line','geom_l',32649,'LINESTRING',2);
UPDATE shenzhen_line SET geom_l = ST_GeometryN(geom, 1)::geometry(linestring, 32649)
-- 新建shenzhen_line1表，修改原始数据类型，删除不要字段
CREATE TABLE shenzhen_line1 AS
SELECT  gid,
        startnode::integer,
        endnode::integer,
        length::float,
        trafficflo::integer,
        geom_l as geom
FROM
        shenzhen_line;
-- 添加逆行成本字段
ALTER TABLE shenzhen_line1 ADD COLUMN reverse_cost FLOAT;
-- 更新逆行成本值
UPDATE shenzhen_line1 SET reverse_cost = length WHERE trafficflo = 0;
UPDATE shenzhen_line1 SET reverse_cost = -1 WHERE trafficflo = 1;
-- 建立空间索引， 清理
CREATE INDEX shenzhen_line1_gidx ON shenzhen_line1 USING GIST(geom);
CLUSTER shenzhen_line1 USING shenzhen_line1_gidx;
VACUUM ANALYZE shenzhen_line1;
```


## shenzhen_point1

导入深圳路网节点数据：

```sql
shp2pgsql -s 32649 -W GBK shenzhen_point.shp shenzhen_point > shenzhen_point.sql
psql -f shenzhen_point.sql road_network postgres
```

进一步处理：

```sql
-- 新建shenzhen_point1表，修改数据类型，删除不需要的字段
CREATE TABLE shenzhen_point1 AS
SELECT
    id::integer,
    geom
FROM
    shenzhen_point;
-- 建立空间索引， 清理
CREATE INDEX shenzhen_point1_gidx ON shenzhen_point1 USING GIST(geom);
CLUSTER shenzhen_point1 USING shenzhen_point1_gidx;
VACUUM ANALYZE shenzhen_point1;
```

### gps_log

一个月份的原始gps数据表

```sql
-- 创建gps_log表
DROP TABLE IF EXISTS gps_log
CREATE TABLE gps_log
(
    id SERIAL PRIMARY KEY, -- 主键
    log_time TIMESTAMP NOT NULL, -- 记录时间
    unknown_1 TEXT, -- 未知
    car_id TEXT NOT NULL, -- 出租车id
    velocity SMALLINT NOT NULL, -- 速度 m/s
    direction SMALLINT NOT NULL, -- 方向角
    on_service BOOLEAN NOT NULL, -- 是否载客
    is_valid BOOLEAN NOT NULL    -- log是否有效
);
-- 往gps_log表插入几何字段
SELECT AddGeometryColumn('public', 'gps_log', 'geom', 32649, 'POINT', 2);

-- 插入数据后执行
SET maintenance_work_mem TO '2047MB'; -- 调大maintenance_work_mem，可以加快建立索引的速度

CREATE INDEX gps_log_gidx ON gps_log USING GIST(geom); -- 建立空间索引
VACUUM ANALYZE gps_log; -- 清理存储，重建统计信息
CLUSTER gps_log USING gps_log_gidx; -- 聚类
ANALYZE gps_log; -- 重建统计信息
SET maintenance_work_mem TO '16MB' -- 恢复maintenance
```

### gps_log_valid
```sql
-- 对原始的gps_log进行清理
-- 只要7：30-8：30
-- 载客的
-- 在路网的30米缓冲区内
CREATE TABLE
	gps_log_valid AS
SELECT DISTINCT ON (gp.id) gp.*
FROM
	gps_log gp, shenzhen_line1 r
WHERE
	gp.log_time::time between '07:30:00' AND '08:30:00' AND
	gp.velocity >= 0 AND
	gp.direction >= 0 AND
	gp.is_valid is true AND
	gp.on_service is true AND
	ST_DWithin(r.geom, gp.geom, 30);
-- 插入数据后, 建立索引， cluster, 清理存储， 重建统计
CREATE INDEX gps_log_valid_gidx ON gps_log_valid USING GIST(geom);
CREATE INDEX gps_log_valid_t_idx ON gps_log_valid(id);
VACUUM ANALYZE gps_log_valid;
CLUSTER gps_log_valid USING gps_log_valid_gidx;
ANALYZE gps_log_valid;
```
### tracks

轨迹表

```sql
--
CREATE TABLE tracks
(
	id SERIAL PRIMARY KEY,
	points INTEGER []
);
-- 插入数据后执行
CREATE INDEX tracks_idx ON tracks(id);
VACUUM ANALYZE tracks;
CLUSTER tracks USING tracks_idx;
ANALYZE tracks;
```

## path_cost

直线距离小于5km的路网点之间的网络距离表

```sql
CREATE TABLE path_cost
(
	start_vid BIGINT, -- 起点id
	end_vid BIGINT, -- 终点id
	agg_dis FLOAT -- 距离
)

-- 4 create index
CREATE INDEX path_cost_idx ON path_cost(start_vid, end_vid);
-- 5 cluster table
CLUSTER path_cost USING path_cost_idx;
-- 6 vaccum and analyze
VACUUM ANALYZE path_cost;
```






### match_track

在match_track表中冗余了一些gps_log_valid表的数据，是考虑到gps_log_valid表较大，连接操作的成本较高。为了保持设计的一致性，
将shenzhen_line1的数据也一并冗余了。

```sql
DROP TABLE IF EXISTS match_track;
CREATE TABLE match_track
(
	id SERIAL PRIMARY KEY, -- 主键
	track_id BIGINT, -- 轨迹id
	line_id BIGINT, -- 匹配点所在的道路id
	gps_log_id BIGINT,-- 匹配点对应的gps_log_id
	source BIGINT, -- 匹配点所在道路的source node
	target BIGINT, -- 匹配点所在道路的target node
	length FLOAT, -- 匹配点所在道路的 length
	fraction FLOAT, -- 匹配点距道路source node的距离占length的比例
	v INTEGER, -- 匹配点的速度
	log_time TIMESTAMP -- 匹配点的时间
)
SELECT AddGeometryColumn('public', 'match_track', 'geom', 32649, 'POINT', 2); -- 匹配点几何数据

-- 插入数据完成后运行
CREATE INDEX match_track_idx ON match_track(track_id); -- 建立索引
CLUSTER match_track USING match_track_idx; -- 聚类索引
VACUUM ANALYZE match_track; -- 清理存储空间，重建统计信息
```







### construct_tracks

还原轨迹表,将match_tracks还原成连通道路列表。

```sql
CREATE TABLE construct_tracks
(
	id SERIAL PRIMARY KEY,
	track_id INTEGER,
	line_ids INTEGER[]
)
CREATE INDEX construct_tracks1_idx ON construct_tracks1(track_id)
cluster construct_tracks1 using construct_tracks1_idx
vacuum construct_tracks1
ANALYZE construct_tracks1
```

### red_point_track
红灯轨迹表
记录一条起始都是红灯的轨迹，以及估计的中点

```sql
DROP TABLE IF EXISTS red_point_track;
CREATE TABLE red_point_track
(
	id SERIAL PRIMARY KEY,
	line_ids INTEGER [],
	ratio FLOAT,
	middle_id INTEGER,
	track_id INTEGER,
    track_seg_id INTEGER
);
CREATE INDEX red_point_track_idx ON red_point_track(track_id);
cluster red_point_track using red_point_track_idx;
vacuum red_point_track;
ANALYZE red_point_track;
```

### new_track_time

结果表，经过相邻节点组成的路段的时间

```sql
drop table if exists new_track_time;
create table new_track_time
(
	id serial primary key,
	line_ids INTEGER[],
	begin_middle INTEGER,
	begin_ratio FLOAT,
	end_middle INTEGER,
	end_ratio FLOAT,
	avg_time FLOAT,
    agg_len FLOAT,
	time_list FLOAT[]
);
```

### new_network

根据new_track_time构建的新路径网络表

```sql
CREATE TABLE new_network
(
	id SERIAL PRIMARY KEY,
	start_node INTEGER,
	end_node INTEGER,
	avg_time FLOAT,
	agg_len FLOAT,
	reverse_length FLOAT
);

CREATE INDEX %I ON %I(start_node, end_node);
CLUSTER %I USING %I;
ANALYZE %I;
```