CREATE OR REPLACE FUNCTION make_construct_tracks_table(table_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('DROP TABLE IF EXISTS %I;', table_name);
EXECUTE format('
CREATE TABLE %I
(
	id SERIAL PRIMARY KEY,
	track_id INTEGER,
	line_ids INTEGER[]
);
', table_name);


END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION make_construct_tracks_table_idx(table_name VARCHAR, idx_name VARCHAR)
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



select make_construct_tracks_table('construct_tracks_5_8');
select make_construct_tracks_table_idx('construct_tracks_5_8', 'construct_tracks_5_8_idx');

select make_construct_tracks_table('construct_tracks_5_10');
select make_construct_tracks_table_idx('construct_tracks_5_10', 'construct_tracks_5_10_idx');

select make_construct_tracks_table('construct_tracks_6_8');
select make_construct_tracks_table_idx('construct_tracks_6_8', 'construct_tracks_6_8_idx');
select make_construct_tracks_table('construct_tracks_6_10');
select make_construct_tracks_table_idx('construct_tracks_6_10', 'construct_tracks_6_10_idx');

select make_construct_tracks_table('construct_tracks_7_8');
select make_construct_tracks_table_idx('construct_tracks_7_8', 'construct_tracks_7_8_idx');

select make_construct_tracks_table('construct_tracks_7_10');
select make_construct_tracks_table_idx('construct_tracks_7_10', 'construct_tracks_7_10_idx');

select make_construct_tracks_table('construct_tracks_8_8');
select make_construct_tracks_table_idx('construct_tracks_8_8', 'construct_tracks_8_8_idx');


select make_construct_tracks_table('construct_tracks_8_10');
select make_construct_tracks_table_idx('construct_tracks_8_10', 'construct_tracks_8_10_idx');


