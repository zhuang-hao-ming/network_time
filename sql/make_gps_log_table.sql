-- function for create gps_log__xx table
CREATE OR REPLACE FUNCTION make_gps_log_table(table_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('DROP TABLE IF EXISTS %I;', table_name);
EXECUTE format('
CREATE TABLE %I
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
', table_name);

PERFORM AddGeometryColumn('public', table_name, 'geom', 32649, 'POINT', 2); -- 插入几何字段
END;
$$
LANGUAGE plpgsql;

-- function for create gps_log_idx
CREATE OR REPLACE FUNCTION make_gps_log_idx(table_name VARCHAR, idx_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
SET maintenance_work_mem TO '2047MB'; -- 调大maintenance_work_mem，可以加快建立索引的速度
EXECUTE format('CREATE INDEX %I ON %I USING GIST(geom);', idx_name, table_name);
EXECUTE format('CLUSTER %I USING %I;', table_name, idx_name);
EXECUTE format('ANALYZE %I;', table_name);
SET maintenance_work_mem TO '16MB' -- 恢复maintenance
 
END;
$$
LANGUAGE plpgsql;


-------------------- test
select make_gps_log_table('gps_log_58');
-- 建立空间索引耗时较长（比索引对于生成gps_log_valid表的改善时间长），所以无需建立索引，直接生成gps_log_valid表即可。
select make_gps_log_idx('gps_log_58', 'gps_log_58_gidx'); 