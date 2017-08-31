

CREATE OR REPLACE FUNCTION make_new_track_time_table(table_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('DROP TABLE IF EXISTS %I;', table_name);
EXECUTE format('
create table %I
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
', table_name);
END;
$$
LANGUAGE plpgsql;



select make_new_track_time_table('new_track_time_8_10_1');
select make_new_track_time_table('new_track_time_8_10_2');

select make_new_track_time_table('new_track_time_5_10_1');

select make_new_track_time_table('new_track_time_5_10_2');

select make_new_track_time_table('new_track_time_67_8_1');
select make_new_track_time_table('new_track_time_67_10_1');
select make_new_track_time_table('new_track_time_8_8_2');

select make_new_track_time_table('new_track_time_7_8_1');
select make_new_track_time_table('new_track_time_7_8_2');

select make_new_track_time_table('new_track_time_7_10_1');
select make_new_track_time_table('new_track_time_7_10_2');

select make_new_track_time_table('new_track_time_7_10_1');
select make_new_track_time_table('new_track_time_56_10_3');

select * from new_track_time_56_10_3;




