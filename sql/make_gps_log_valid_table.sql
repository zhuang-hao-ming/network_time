-- function for create gps_log_valid_xx table
CREATE OR REPLACE FUNCTION make_gps_log_valid_table(valid_table_name VARCHAR, log_table_name VARCHAR, line_table_name VARCHAR, begin_time VARCHAR, end_time VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('DROP TABLE IF EXISTS %I;', table_name);
EXECUTE format('
CREATE TABLE
	%I AS
SELECT DISTINCT ON (gp.id) gp.*
FROM
	%I gp, %I r
WHERE
	gp.log_time::time between %I AND %I AND
	gp.velocity >= 0 AND
	gp.direction >= 0 AND
	gp.is_valid is true AND
	gp.on_service is true AND
	ST_DWithin(r.geom, gp.geom, 30);
', valid_table_name, log_table_name, line_table_name, begin_time, end_time);


END;
$$
LANGUAGE plpgsql;

-- function for create gps_log_valid_idx
CREATE OR REPLACE FUNCTION make_gps_log_valid_idx(table_name VARCHAR, gidx_name VARCHAR, idx_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
SET maintenance_work_mem TO '2047MB'; -- 调大maintenance_work_mem，可以加快建立索引的速度
EXECUTE format('CREATE INDEX %I ON %I USING GIST(geom);', gidx_name, table_name);
EXECUTE format('CREATE INDEX %I ON %I(id);', idx_name, table_name);
EXECUTE format('CLUSTER %I USING %I;', table_name, gidx_name);
EXECUTE format('ANALYZE %I;', table_name);
SET maintenance_work_mem TO '16MB' -- 恢复maintenance
 
END;
$$
LANGUAGE plpgsql;


-------------------- test
select make_gps_log_valid_table('gps_log_58_valid', 'gps_log_58', 'shenzhen_line1', '07:30:00', '08:30:00');
-- 由于后续的查询主要是基于id的，所以在id上建立索引，比空间索引的效果好。但是为了提高closest point查询等空间查询的速度，建立空间索引也是必要的
select make_gps_log_valid_idx('gps_log_valid_58', 'gps_log_valid_58_gidx', 'gps_log_valid_58_idx'); 