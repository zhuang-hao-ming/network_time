


CREATE OR REPLACE FUNCTION make_red_point_track_table(table_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('DROP TABLE IF EXISTS %I;', table_name);
EXECUTE format('
CREATE TABLE %I
(
	id SERIAL PRIMARY KEY,
	line_ids INTEGER [],
	ratio FLOAT,
	middle_id INTEGER,
	track_id INTEGER,
	track_seg_id INTEGER
);
', table_name);
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION make_red_point_track_idx(table_name VARCHAR, idx_name VARCHAR)
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




select make_red_point_track_table('red_point_track_5_10');
select make_red_point_track_idx('red_point_track_5_10', 'red_point_track_5_10_idx');


select make_red_point_track_table('red_point_track_6_10_1');
select make_red_point_track_idx('red_point_track_6_10_1', 'red_point_track_6_10_1_idx');


select make_red_point_track_table('red_point_track_7_10');
select make_red_point_track_idx('red_point_track_7_10', 'red_point_track_7_10_idx');


select make_red_point_track_table('red_point_track_8_10');

select make_red_point_track_idx('red_point_track_8_10', 'red_point_track_8_10_idx');

select make_red_point_track_table('red_point_track_5_8');
select make_red_point_track_idx('red_point_track_5_8', 'red_point_track_5_8_idx');

select make_red_point_track_table('red_point_track_6_8');
select make_red_point_track_idx('red_point_track_6_8', 'red_point_track_6_8_idx');

select make_red_point_track_table('red_point_track_7_8');
select make_red_point_track_idx('red_point_track_7_8', 'red_point_track_7_8_idx');

select make_red_point_track_table('red_point_track_8_8');
select make_red_point_track_idx('red_point_track_8_8', 'red_point_track_8_8_idx');

