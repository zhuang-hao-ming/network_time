


-- 检验相邻的两个gps log直线距离是否小于30m
CREATE OR REPLACE FUNCTION test_the_same_point(pre_x FLOAT, pre_y FLOAT, now_x FLOAT, now_y FLOAT)
RETURNS BOOLEAN
AS
$$
DECLARE
	dis FLOAT := 0;
BEGIN
	dis = (pre_x - now_x) ^ 2 + (pre_y - now_y) ^ 2;
	IF dis < 900 THEN
		return true;
	END IF;
	return false;	
END;
$$
LANGUAGE plpgsql;







-- 检验两个相邻的gps log是否属于同一个轨迹
CREATE OR REPLACE FUNCTION test_time_dis_constrain(pre_x FLOAT, pre_y FLOAT, now_x FLOAT, now_y FLOAT, pre_time TIMESTAMP, now_time TIMESTAMP, time_threshold INTEGER DEFAULT 120, speed_threshold INTEGER DEFAULT 33)
RETURNS BOOLEAN 
AS
$$
DECLARE
	delta INTEGER := 0;
	dis FLOAT := 0;
BEGIN
	-- the interval of two logs
	SELECT EXTRACT (EPOCH FROM (pre_time - now_time)) INTO delta;
	delta := abs(delta);

	-- debug message
	-- RAISE INFO 'pre_time %, now_time %, pre_x % , pre_y %, now_x %, now_y % ,delta %', pre_time, now_time, pre_x, pre_y, now_x, now_y, delta;

	
	IF delta > time_threshold THEN
		return false;
	END IF;
	
	dis = (pre_x - now_x) ^ 2 + (pre_y - now_y) ^ 2;
	
	IF dis > (delta * speed_threshold) ^ 2 THEN
		return false;
	END IF;
	return true;	
END;

$$
LANGUAGE plpgsql;




--- insert data to track table
--- this function iterate the gps_log_valid table in order and make track according to criterias given by test_time_dis_constrain and test_the_same_point function
CREATE OR REPLACE FUNCTION make_track(table_name VARCHAR, log_table_name VARCHAR)
RETURNS VOID 
AS $$
DECLARE 
	log_ids INT[];
	pre_log RECORD;
	cur_log RECORD;
	cur_logs REFCURSOR;
BEGIN
	-- Open the gps log cursor
	OPEN cur_logs FOR EXECUTE format('SELECT id,log_time, car_id, direction AS v, ST_X(geom) AS x, ST_Y(geom) AS y  FROM %I ORDER BY car_id, log_time', log_table_name);
	
	
	-- fetch the first log
	FETCH cur_logs INTO pre_log;
	log_ids := ARRAY[pre_log.id];
	LOOP
		-- fetch row into the rec_log
		FETCH cur_logs INTO cur_log;
		EXIT WHEN NOT FOUND;

		IF test_the_same_point(pre_log.x, pre_log.y, cur_log.x, cur_log.y) THEN
			
					
		ELSIF test_time_dis_constrain(pre_log.x, pre_log.y, cur_log.x, cur_log.y, pre_log.log_time, cur_log.log_time) THEN
			log_ids := array_append(log_ids, cur_log.id);
			pre_log := cur_log;
			--RAISE INFO 'append %', cur_log.id;
			
		ELSE	
			--RAISE INFO 'insert %', log_ids;
			EXECUTE format('INSERT INTO %I(points) VALUES($1)', table_name) USING log_ids;
			--INSERT INTO tracks_1(points) VALUES(log_ids);
			log_ids := ARRAY[cur_log.id];
			pre_log := cur_log;
		END IF;
				
	END LOOP;

	IF array_length(log_ids, 1) > 0 THEN	
		EXECUTE format('INSERT INTO %I(points) VALUES($1)', table_name) USING log_ids;
		--INSERT INTO tracks_1(points) VALUES(log_ids);
		--RAISE INFO 'end insert %', log_ids;
	END IF;
  
   -- Close the cursor
	CLOSE cur_logs;

	RETURN;
END; $$
LANGUAGE plpgsql;



--- make track_table
CREATE OR REPLACE FUNCTION make_tracks_table(table_name VARCHAR, log_table_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
	EXECUTE format('drop table IF EXISTS %I;', table_name);
	EXECUTE format('
	CREATE TABLE %I
	(
	id SERIAL PRIMARY KEY,
	points INTEGER[]
	);
	', table_name);
	PERFORM make_track(table_name, log_table_name); 

END;
$$
LANGUAGE plpgsql;


--- make track_table_idx
CREATE OR REPLACE FUNCTION make_tracks_table_idx(table_name VARCHAR, idx_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('CREATE INDEX %I ON %I(id);', idx_name, table_name);
EXECUTE format('CLUSTER %I USING %I;', table_name, idx_name);
EXECUTE format('ANALYZE %I;', table_name); -- 清理存储空间，重建统计信息
END;
$$
LANGUAGE plpgsql;


------------------------------------------- test 

-- 生成tracks表

SELECT make_tracks_table('tracks_test', 'gps_log_valid_test'); 

-- 建立索引
select make_tracks_table_idx('tracks_6_10', 'tracks_6_10_idx')
select make_tracks_table_idx('tracks_7_8', 'tracks_7_8_idx');
select make_tracks_table_idx('tracks_7_10', 'tracks_7_10_idx');
select make_tracks_table_idx('tracks_8_8', 'tracks_8_8_idx');
select make_tracks_table_idx('tracks_8_10', 'tracks_8_10_idx');




















