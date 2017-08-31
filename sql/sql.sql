-- 创建gps_log表

CREATE TABLE gps_log
(
    id SERIAL PRIMARY KEY,
    log_time TIMESTAMP NOT NULL,
    unknown_1 TEXT,
    car_id TEXT NOT NULL,
    velocity SMALLINT NOT NULL,
    direction SMALLINT NOT NULL,
    on_service BOOLEAN NOT NULL,
    is_valid BOOLEAN NOT NULL    
);
-- 往gps_log表插入几何字段
SELECT AddGeometryColumn('public', 'gps_log', 'geom', 32649, 'POINT', 2);

SET maintenance_work_mem TO '2047MB';
-- 插入数据后，建立索引， 并且cluster
CREATE INDEX gps_log_gidx ON gps_log USING GIST(geom);
VACUUM ANALYZE gps_log;
CLUSTER gps_log USING gps_log_gidx;
ANALYZE gps_log;
SET maintenance_work_mem TO '16MB'

# 对原始的gps_log进行清理
# 只要7：30-8：30
# 载客的
# 在路网的30米缓冲区内
CREATE TABLE 
	gps_log_valid AS
SELECT DISTINCT ON (gp.id) gp.* 
FROM 
	gps_log gp, line_uffer r 
WHERE 
	gp.log_time::time between '07:30:00' AND '08:30:00' AND 
	gp.velocity >= 0 AND
	gp.direction >= 0 AND 
	gp.is_valid is true AND
	gp.on_service is true AND
	ST_Contains(r.geom, gp.geom);
# 插入数据后, 建立索引， 并且cluster
CREATE INDEX gps_log_valid_gidx ON gps_log_valid USING GIST(geom);
VACUUM ANALYZE gps_log_valid;
CLUSTER gps_log_valid USING gps_log_valid_gidx;
ANALYZE gps_log_valid;



CREATE TABLE road_buffer AS
SELECT ST_Buffer(geom, 30) AS geom FROM shenzhen_line1;
CREATE INDEX line_uffer_gidx ON line_uffer USING GIST(geom);
VACUUM ANALYZE line_uffer;
CLUSTER line_uffer USING line_uffer_gidx;
ANALYZE line_uffer;



# 轨迹表
CREATE TABLE tracks 
(
	id SERIAL PRIMARY KEY,
	points INTEGER []
);
CREATE INDEX tracks_idx ON tracks(id);
VACUUM ANALYZE tracks;
CLUSTER tracks USING tracks_idx;
ANALYZE tracks;





SELECT * FROM road_buffer LIMIT 1;
-- 验证轨迹
SELECT gp.geom, gp.direction AS v FROM gps_log_valid gp, tracks t WHERE t.id=6584 AND gp.id = ANY(t.points) ORDER BY log_time;
-- 导出验证轨迹
-- pgsql2shp -f track.shp -h localhost -u postgres -P 123456 road_gps "SELECT gp.geom, gp.direction AS v FROM gps_log_valid gp, tracks t WHERE t.id=6584 AND gp.id = ANY(t.points) ORDER BY log_time;"


SELECT * FROM tracks WHERE array_length(points, 1) > 10 LIMIT 1;



-- select closest point

WITH closest_points AS
(
	SELECT 
		gps.geom as geom_log,
		r.geom_l as geom_line,
		r.source as source,
		r.target as target,
		r.gid as line_id,
		gps.id as log_id,
		gps.direction as v,
		ST_ClosestPoint(r.geom_l, gps.geom) as geom_closest,
		r.cost as length
	FROM
		shenzhen_network r, 
		gps_log_valid gps
	WHERE  
		gps.id in (1494167,1502698,1511616,1520537,1529545,1538291,1547068,1565190,1574253,1583447,1592171,1601214) AND
		ST_DWithin(gps.geom, r.geom_l,  30)
)	
SELECT 
	ST_X(geom_log) AS log_x,
	ST_Y(geom_log) AS log_y,
	ST_X(geom_closest) AS p_x,
	ST_Y(geom_closest) AS p_y,
	line_id,
	log_id,
	v,
	source,
	target,	
	length,
	ST_LineLocatePoint(geom_line, geom_log) as fraction
FROM
	closest_points;

SELECT 
	ST_X(gps.geom) AS log_x,
	ST_Y(gps.geom) AS log_y,
	ST_X(ST_ClosestPoint(r.geom_l, gps.geom)) AS p_x,
	ST_Y(ST_ClosestPoint(r.geom_l, gps.geom)) AS p_y,
	r.gid AS line_id,
	gps.id AS log_id,
	gps.direction AS v,
	r.source AS source,
	r.target AS target,
	ST_LineLocatePoint(r.geom_l, ST_ClosestPoint(r.geom_l, gps.geom))
	
FROM 
	shenzhen_network r, 
	gps_log_valid gps 
WHERE 
	gps.id in (1494167,1502698,1511616,1520537,1529545,1538291,1547068,1565190,1574253,1583447,1592171,1601214) AND
	ST_DWithin(gps.geom, r.geom_l,  30);



-- 导入深圳路网数据
-- shp2pgsql -s 32649 -W GBK shenzhen_network.shp shenzhen_network > shenzhen_network.sql
-- psql -f shenzhen_network.sql road_gps postgres
VACUUM ANALYZE shenzhen_network;


-- 使用pgrouting
CREATE EXTENSION pgrouting;
-- 建立拓扑信息
ALTER TABLE shenzhen_network ADD COLUMN "source" integer;
ALTER TABLE shenzhen_network ADD COLUMN "target" integer;
SELECT pgr_createTopology('shenzhen_network', 1, 'geom', 'gid');


-- 最短路径查询例子
SELECT * FROM pgr_dijkstra('
    SELECT gid AS id,
         source,
         target,
         shape_leng::float AS cost
        FROM shenzhen_network',
    12036, 10544, directed := false);
-- 验证
SELECT routing.seq, network.geom FROM shenzhen_network network, (SELECT * FROM pgr_dijkstra('SELECT gid AS id,source,target,shape_leng::float AS cost FROM shenzhen_network', 12036, 10544, directed := false)) routing WHERE network.gid = routing.edge ORDER BY seq;

--- 导出验证结果
pgsql2shp -f routing.shp -h localhost -u postgres -P 123456 road_gps "SELECT routing.seq, network.geom FROM shenzhen_network network, (SELECT * FROM pgr_dijkstra('SELECT gid AS id,source,target,shape_leng::float AS cost FROM shenzhen_network', 10519, 549, directed := false)) routing WHERE network.gid = routing.edge ORDER BY seq;"







-- 得到线上两个点之间的距离

WITH
data AS (
	SELECT ST_GeomFromText('POINT(796449.433546537 2502766.24045783)', 32649) pta,
		ST_GeomFromText('POINT(795563.273692348 2503973.71146982)', 32649) ptb,
		ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid = 22205
)
SELECT 
	ST_Length(
		ST_LineSubstring(
			line,
			least(ST_LineLocatePoint(line, pta), ST_LineLocatePoint(line, ptb)),
			greatest(ST_LineLocatePoint(line, pta), ST_LineLocatePoint(line, ptb))
			)
		)
FROM 
	data;







-- distance from a point on the line to the line's start vertext

WITH
data AS (
SELECT 
	ST_GeomFromText('POINT(796449.433546537 2502766.24045783)', 32649) pta,
	ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid=22205
)
SELECT  ST_LineLocatePoint(line, pta), ST_Length(line) FROM data;


-- distance between two vertex

SELECT *
FROM pgr_dijkstraCost('
    SELECT 
		gid  AS id,
		source,
		target,
		cost as cost,
		cost as reverse_cost
	FROM
		shenzhen_network ORDER BY gid',
    Array [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27],
    Array [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27],
    directed := false);

SELECT * FROM pgr_dijkstraCostMatrix(
    '
	SELECT gid as id, source, target, cost, cost as reverse_cost FROM shenzhen_network	
    ',
    (SELECT array_agg(id) FROM shenzhen_network_vertices_pgr WHERE id < 1000)
);



INSERT INTO path_cost
SELECT *
FROM pgr_dijkstraCost('
    SELECT 
		gid  AS id,
		source,
		target,
		cost as cost,
		cost as reverse_cost
	FROM
		shenzhen_network ORDER BY gid',
    32,
    (SELECT array_agg(b.id) FROM shenzhen_network_vertices_pgr a, shenzhen_network_vertices_pgr b WHERE ST_DWithin(a.the_geom, b.the_geom, 5000) AND a.id = 32 GROUP BY a.id),
    directed := false);

drop table cost;
CREATE TABLE path_cost(
	start_vid bigint,
	end_vid bigint,
	agg_dis float
)

delete from path_cost; 

SELECT * FROM path_cost WHERE start_vid = 1 AND end_vid =1;
CREATE INDEX path_cost_idx ON path_cost(start_vid, end_vid)
CLUSTER path_cost USING path_cost_idx;
VACUUM ANALYZE path_cost;



select * FROM shenzhen_network_vertices_pgr;
DO $$
DECLARE
	i INTEGER := 13001;
	n INTEGER := 16697;
BEGIN
	WHILE i <= n LOOP
		i := i+1;
		RAISE NOTICE 'Counter: %', i;
		
		
		
		INSERT INTO path_cost SELECT * FROM pgr_dijkstraCost('
    SELECT gid AS id,
         startnode as source,
         endnode as target,
         length AS cost,
         reverse_cost
        FROM shenzhen_line1',
		    i,
		    (SELECT array_agg(b.id) FROM shenzhen_network_vertices_pgr a, shenzhen_network_vertices_pgr b WHERE ST_DWithin(a.the_geom, b.the_geom, 5000) AND a.id = i GROUP BY a.id),
		    directed := false);
		
		
	END LOOP;
END;
$$

select * from shenzhen_point1 limit 1;
 

SELECT array_agg(id) FROM shenzhen_network_vertices_pgr WHERE id < 5

SELECT array_agg(b.id) FROM shenzhen_network_vertices_pgr a, shenzhen_network_vertices_pgr b WHERE ST_DWithin(a.the_geom, b.the_geom, 5000) AND a.id = 1 GROUP BY a.id;












SELECT * FROM shenzhen_network_vertices_pgr;
SELECT ST_LineLocatePoint(line, pta)
SELECT * FROM tracks WHERE id=19;






------- create poi table


DROP TABLE points_of_interest3;
CREATE TABLE points_of_interest4(
    pid BIGSERIAL,

    edge_id BIGINT,
    side CHAR,
    fraction FLOAT
  
);


-- insert one point
WITH
data AS (
	SELECT 
		798663.892742802 x,
		2500760.31480463 y,
		22203 edge_id,
		'b' side,
		ST_GeomFromText('POINT(798663.89274280 2500760.31480463)', 32649) pta,	
		ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid = 22203
		
)
INSERT INTO points_of_interest4(

	edge_id,
	side,
	fraction) 
SELECT edge_id, side, ST_LineLocatePoint(line, pta)FROM data;

--- insert one point
WITH
data AS (
	SELECT 
		797554.654871642 x,
		2501842.40943049 y,
		22204 edge_id,
		'b' side,
		ST_GeomFromText('POINT(797554.654871642 2501842.40943049)', 32649) pta,		
		ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid = 22204
)
INSERT INTO points_of_interest4(

	edge_id,
	side,
	fraction
)
SELECT  edge_id, side, ST_LineLocatePoint(line, pta)FROM data;

--- path between two point on the middle of a line


WITH
data AS (
	SELECT 
		797554.654871642 x,
		2501842.40943049 y,
		22204 edge_id,
		'b' side,
		ST_GeomFromText('POINT(797554.654871642 2501842.40943049)', 32649) pta,		
		ST_GeometryN(geom, 1)::geometry(linestring, 32649) line FROM shenzhen_network WHERE gid = 22204
)


WITH 
poi AS (
	SELECT * FROM points_of_interest WHERE track_id = 19
)
SELECT * FROM pgr_withPointsCost (
        'SELECT 
		gid  AS id,
		source,
		target,
		shape_leng::float as cost

	FROM
		shenzhen_network ORDER BY gid',
        'SELECT pid, edge_id, fraction, side from poi',
        Array [-448], Array[-575],directed:=false, driving_side:='b');

DROP TABLE points_of_interest5;
CREATE TABLE points_of_interest5 AS SELECT * from points_of_interest WHERE pid in (1405, 5887, 792);

SELECT * FROM points_of_interest5;
SELECT * from points_of_interest WHERE fraction = 1;
SELECT * from points_of_interest WHERE edge_id = 22204;
SELECT pid, edge_id, fraction, side from points_of_interest WHERE pid = 575;


SELECT * FROM shenzhen_network WHERE gid = 21120;

SELECT * from points_of_interest WHERE pid < 0;


SELECT ST_AsText(geom), * FROM points_of_interest LIMIT 1;
DELETE FROM points_of_interest;





-- extract linestring form multilinestring

SELECT AddGeometryColumn ('public','shenzhen_network','geom_l',32649,'LINESTRING',2);
UPDATE shenzhen_network SET geom_l = ST_GeometryN(geom, 1)::geometry(linestring, 32649)
ALTER TABLE shenzhen_network ADD COLUMN cost FLOAT;
UPDATE shenzhen_network SET cost = ST_Length(geom_l)
CREATE INDEX shenzhen_network_gidx ON shenzhen_network USING GIST(geom_l);
CLUSTER shenzhen_network USING shenzhen_network_gidx;
VACUUM ANALYZE shenzhen_network;

--- create table poi

CREATE TABLE points_of_interest(
    pid BIGSERIAL,    
    log_x FLOAT,
    log_y FLOAT,
    p_x FLOAT,
    p_y FLOAT,
    v FLOAT,
    gps_log_id BIGINT, 
    edge_id BIGINT,
    side CHAR,
    fraction FLOAT,
    geom geometry,
    source BIGINT,
    target BIGINT,
    track_id BIGINT
);

INSERT INTO points_of_interest(
	log_x,
	log_y,
	p_x,
	p_y,
	v,
	gps_log_id, 
	edge_id,
	side,
	fraction,
	geom,
	source,
	target,
	track_id)

SELECT 
	ST_X(gps.geom) AS log_x,
	ST_Y(gps.geom) AS log_y,
	ST_X(ST_ClosestPoint(r.geom_l, gps.geom)) AS p_x,
	ST_Y(ST_ClosestPoint(r.geom_l, gps.geom)) AS p_y,
	gps.direction AS v,
	gps.id AS gps_log_id,
	r.gid AS edge_id,
	'b' AS side,
	ST_LineLocatePoint(r.geom_l, gps.geom) AS fraction,
	ST_ClosestPoint(r.geom_l, gps.geom) AS geom,
	r.source AS source,
	r.target AS target,
	t.id AS track_id
	
FROM 
	shenzhen_network r, 
	gps_log_valid gps,
	tracks t
WHERE 
	array_length(t.points, 1) > 15 AND
	gps.id = ANY(t.points)  AND
	ST_DWithin(gps.geom, r.geom,  30)
;

UPDATE points_of_interest SET pid = -source WHERE fraction = 0; 
UPDATE points_of_interest SET pid = -target WHERE fraction = 1;
UPDATE points_of_interest SET edge_id = 0 WHERE pid < 0;
UPDATE points_of_interest SET fraction = 0 WHERE pid < 0; 


CREATE INDEX points_of_interest_idx ON points_of_interest(track_id);
CLUSTER points_of_interest USING points_of_interest_idx
VACUUM ANALYZE points_of_interest



SELECT * FROM tracks WHERE array_length(points, 1) > 15 LIMIT 1;


SELECT 
	ST_X(gps.geom) AS log_x,
	ST_Y(gps.geom) AS log_y,
	ST_X(ST_ClosestPoint(r.geom_l, gps.geom)) AS p_x,
	ST_Y(ST_ClosestPoint(r.geom_l, gps.geom)) AS p_y,
	gps.direction AS v,
	gps.id AS gps_log_id,
	r.gid AS edge_id,
	'b' AS side,
	ST_LineLocatePoint(r.geom_l, gps.geom) AS fraction,
	ST_ClosestPoint(r.geom_l, gps.geom) AS geom,
	r.source AS source,
	r.target AS target
	
	
FROM 
	shenzhen_network r, 
	gps_log_valid gps,
	
WHERE 
	
	gps.id = ANY ("{2829550,2834054,2838927,2843599,2848349,2853258,2857972,2862800,2867704,2873457,2873463,2877453,2882288,2887327,2892246,2897096,2902997}")  AND
	ST_DWithin(gps.geom_l, r.geom,  30)
;









SELECT source, target, cost FROM shenzhen_network WHERE source = 917;


SELECT id FROM shenzhen_network_vertices_pgr;

DROP TABLE match_track1;
CREATE TABLE match_track
(
	id SERIAL PRIMARY KEY,
	track_id BIGINT,
	line_id BIGINT,
	gps_log_id BIGINT,
	source BIGINT,
	target BIGINT,
	length FLOAT,
	fraction FLOAT,
	v INTEGER,
	log_time TIMESTAMP		
)
SELECT AddGeometryColumn('public', 'match_track', 'geom', 32649, 'POINT', 2);

	
SELECT * FROM match_track where track_id = 14  order by id ;
select count(distinct track_id) from match_track;
select * from match_track limit 1;

CREATE INDEX match_track_idx ON match_track(track_id, id);
CLUSTER match_track USING match_track_idx;
VACUUM ANALYZE match_track;



INSERT INTO match_track(track_id, geom, line_id)
    VALUES(1,  ST_GeomFromText('POINT(796449.433546537 2502766.24045783)', 32649), 3)

delete from match_track;

