
CREATE OR REPLACE FUNCTION make_match_track_table(table_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('DROP TABLE IF EXISTS %I;', table_name);
EXECUTE format('
CREATE TABLE %I
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
);
', table_name);

PERFORM AddGeometryColumn('public', table_name, 'geom', 32649, 'POINT', 2); -- 匹配点几何数据
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION make_match_track_table_idx(table_name VARCHAR, idx_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('CREATE INDEX %I ON %I(track_id);', idx_name, table_name);
EXECUTE format('CLUSTER %I USING %I;', table_name, idx_name);
EXECUTE format('ANALYZE %I;', table_name);
 
END;
$$
LANGUAGE plpgsql;






select make_match_track_table('match_track_5_8');
select make_match_track_table('match_track_5_10');
select make_match_track_table('match_track_6_8');
select make_match_track_table('match_track_6_10');
select make_match_track_table('match_track_7_8');
select make_match_track_table('match_track_7_10');
select make_match_track_table('match_track_8_8');
select make_match_track_table('match_track_8_10');


select make_match_track_table_idx('match_track_5_8', 'match_track_5_8_idx');
select make_match_track_table_idx('match_track_5_10', 'match_track_5_10_idx');
select make_match_track_table_idx('match_track_6_8', 'match_track_6_8_idx');
select make_match_track_table_idx('match_track_6_10', 'match_track_6_10_idx');
select make_match_track_table_idx('match_track_7_8', 'match_track_7_8_idx');
select make_match_track_table_idx('match_track_7_10', 'match_track_7_10_idx');
select make_match_track_table_idx('match_track_8_8', 'match_track_8_8_idx');
select make_match_track_table_idx('match_track_8_10', 'match_track_8_10_idx');
