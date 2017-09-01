# -*- encoding: utf-8
import psycopg2
from config import config

def get_nodes():
    sql = '''
        SELECT id FROM shenzhen_network_vertices_pgr;
    '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_edges():
    sql = '''
        SELECT source, target, cost FROM shenzhen_network;
        '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()  


def get_logs(limit=10, offset=0):
    sql = '''
            SELECT id,log_time, car_id, direction AS v, ST_X(geom) AS x, ST_Y(geom) AS y  FROM gps_log_valid ORDER BY car_id, log_time LIMIT %s OFFSET %s;
          '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (limit, offset))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#
# 查询距离
#
def get_distance_rows_1(start_vids, end_vids):
    sql = '''
        SELECT * FROM path_cost WHERE start_vid in %s AND end_vid in %s order by agg_dis;
        '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (start_vids, end_vids))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#
# 查询距离
#
def get_distance_rows(vids):
    sql = '''
        SELECT * FROM path_cost WHERE start_vid in %s AND end_vid in %s;
        '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (vids, vids))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#
# 获得gps轨迹
#
def get_tracks(table_name='tracks_6'):

    sql = '''
            select * from {} where array_length(points, 1) > 30 ORDER BY id;
          '''.format(table_name)
    # sql = '''
    #     select * from tracks where id=186758 ORDER BY id;
    # '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#
# 得到一条轨迹的gps_log的在道路上的最近点
# 1. 只考虑距离gps_log距离小于50米的道路
# 2. 返回的最近点先按照gps_log_id排序然后按照距离从小到大排序（在数据库中不好限制只返回5条最近点，所以全部返回，在客户端中来限制）
# @param {{tuple}} log_ids a list of valid gps_log_id
# @return {{list of record}}
# @list.record {{tuple}} (gps_log_x, gps_log_y, closest_x, closest_y, line_id, log_id, line_source_id, line_target_id, length_of_line, fracton_of_the_distance_of_closest_point_to_source_of_length_of_line)
def get_closest_points(log_ids, line_table_name='shenzhen_line1', log_table_name='gps_log_valid_6'):

    sql ='''
        with closest_points as
        (
        select
            gps.geom as geom_log,
            r.geom as geom_line,
            gps.id as log_id,
            r.gid as line_id,
            r.startnode as source,
            r.endnode as target,
            r.length as length,
            r.trafficflo as flo,
            ST_ClosestPoint(r.geom, gps.geom) as geom_closest,
            gps.velocity as v,
            gps.log_time as log_time,
            ST_X(ST_EndPoint(r.geom))-ST_X(ST_StartPoint(r.geom)) as line_x,
	        ST_Y(ST_EndPoint(r.geom))-ST_Y(ST_StartPoint(r.geom)) as line_y
        from
            {} r, {} gps
        where
            gps.id in %s and
            ST_DWithin(gps.geom, r.geom,  50)
        )
        select
            ST_X(geom_log) AS log_x,
            ST_Y(geom_log) AS log_y,
            ST_X(geom_closest) AS p_x,
            ST_Y(geom_closest) AS p_y,
            line_id,
            log_id,
            source,
            target,
            length,
            flo,
            ST_LineLocatePoint(geom_line, geom_log) as fraction,
            v,
            log_time,
            line_x,
            line_y
        from
            closest_points
        order by
            log_time, st_distance(geom_closest, geom_log)
    '''.format(line_table_name, log_table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (log_ids,))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#
# 获得一条匹配好的轨迹的点
# @param track_id 轨迹id
# @param table_name 表名
#
def get_match_track(track_id=14, table_name='match_track_6'):
    sql = '''
            SELECT id, line_id, source, target, length, fraction, v, log_time, track_id FROM {} where track_id = %s  order by id ;
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (track_id, ))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_dijkstra_path(begin_vid, end_vid):
    sql = '''
            SELECT edge, cost FROM pgr_dijkstra('
    SELECT gid AS id,
         startnode as source,
         endnode as target,
         length AS cost,
         reverse_cost
        FROM shenzhen_line1',
    %s, %s, directed := true)  order by seq;
          '''
    conn = None
    try:
        parmas = config()
        conn = psycopg2.connect(**parmas)
        cur = conn.cursor()
        cur.execute(sql, (begin_vid, end_vid))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



#
# 获得新轨迹数据
#
def get_all_new_track_time(table_name='new_track_time'):
    sql = '''
        select id, line_ids, begin_middle, begin_ratio, end_middle, end_ratio, avg_time,agg_len, time_list from {} order by begin_middle;
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#
# 获取匹配轨迹表中所有的轨迹id
# @param table_name 表名
#
def get_track_ids(table_name='match_track_6'):
    sql = '''
        select distinct track_id from {} order by track_id;
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()




#
# 得到红灯轨迹
#
def get_red_point_tracks(track_id, table_name='red_point_track'):
    sql = '''
        select * from {} where track_id = %s order by id;
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (track_id,))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#
# 得到所有红灯轨迹
#
def get_all_red_tracks(table_name='red_point_track'):
    sql = '''
        select * from {0} order by track_id,id;
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_all_match_pnts(table_name='match_track1'):
    sql = '''
            SELECT id, line_id, source, target, length, fraction, v, log_time,track_id FROM {} order by track_id, id;
              '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#
# 获得线的方向
#
def get_line_flo(table_name='shenzhen_line1'):
    sql = '''
        select gid, trafficflo from {};
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#
# 得到线id, 线的起点和终点长度
#
def get_line_nodes(table_name='shenzhen_line1'):
    sql = '''
        select gid, startnode, endnode, length from {};
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#
# 返回构建好的轨迹段id, track_id, line_ids
#
def get_construct_tracks(table_name='construct_tracks1'):

    sql = '''
        select id,track_id,line_ids from {};
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_new_path_dis(begin_id, end_id, table_name):

    sql = '''
        SELECT * FROM pgr_dijkstra('
		    SELECT
				 id,
				 start_node as source,
				 end_node as target,
				 avg_time AS cost,
				 reverse_length AS reverse_cost
			FROM
				{}',
		    %s,
		    %s,
		    directed := true) order by seq;
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (begin_id, end_id))
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_new_network(table_name):
    sql = '''
        select id, agg_len from {};
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_all_new_track_time_old(table_name='new_track_time'):
    sql = '''
        select id, line_ids, begin_middle, begin_ratio, end_middle, end_ratio, avg_time, time_list from {} order by begin_middle;
          '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()