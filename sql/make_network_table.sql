





CREATE OR REPLACE FUNCTION make_new_network_table(table_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('DROP TABLE IF EXISTS %I;', table_name);
EXECUTE format('
CREATE TABLE %I
(
	id SERIAL PRIMARY KEY,
	start_node INTEGER,
	end_node INTEGER,
	avg_time FLOAT,	
	agg_len FLOAT,
	reverse_length FLOAT
);
', table_name);


END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION make_new_network_idx(table_name VARCHAR, idx_name VARCHAR)
RETURNS VOID
AS 
$$
BEGIN
EXECUTE format('CREATE INDEX %I ON %I(start_node, end_node);', idx_name, table_name);
EXECUTE format('CLUSTER %I USING %I;', table_name, idx_name);
EXECUTE format('ANALYZE %I;', table_name);
 
END;
$$
LANGUAGE plpgsql;







select make_new_network_table('new_network_7_8_1');
select make_new_network_idx('new_network_7_8_1','new_network_7_8_1_idx');


select make_new_network_table('new_network_7_8_2');
select make_new_network_idx('new_network_7_8_2','new_network_7_8_2_idx');


select make_new_network_table('new_network_7_10_1');
select make_new_network_idx('new_network_7_10_1','new_network_7_10_1_idx');

select make_new_network_table('new_network_7_10_2');
select make_new_network_idx('new_network_7_10_2','new_network_7_10_2_idx');


select make_new_network_table('new_network_6_8_1');
select make_new_network_idx('new_network_6_8_1','new_network_6_8_1_idx');


select make_new_network_table('new_network_6_8_2');
select make_new_network_idx('new_network_6_8_2','new_network_6_8_2_idx');


select make_new_network_table('new_network_6_10_1');
select make_new_network_idx('new_network_6_10_1','new_network_6_10_1_idx');

select make_new_network_table('new_network_6_10_2');
select make_new_network_idx('new_network_6_10_2','new_network_6_10_2_idx');



select id, agg_len from new_network_6_8_1;



        SELECT * FROM pgr_dijkstra('
		    SELECT
				 id,
				 start_node as source,
				 end_node as target,
				 avg_time AS cost,
				 reverse_length AS reverse_cost
			FROM
				new_network_6_8_1',
		    14895000,
		    76855000,
		    directed := true) order by seq;


	




