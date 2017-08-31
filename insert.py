# -*- encoding: utf-8
import psycopg2
import config
import datetime



def insert_track(tracks):

    sql = '''
    INSERT INTO tracks(points) VALUES (%s);
    '''
    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, tracks)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_match(match_list, closest_points, track_id, table_name='match_track_6'):

    sql = '''
    INSERT INTO {}(track_id, geom, line_id,gps_log_id, source, target, length, fraction, v, log_time)
    VALUES(%s,  ST_GeomFromText('POINT(%s %s)', 32649), %s, %s, %s, %s, %s, %s, %s, %s)
    '''.format(table_name)

    records = []
    for idx in match_list:
        now_log_x, now_log_y, now_p_x, now_p_y, now_line_id, now_gps_log_id, now_source, now_target, now_length, now_flo, now_fraction, now_v, now_log_time, now_line_x, now_line_y = closest_points[idx]
        point_data = '{0} {1}'.format(now_p_x, now_p_y)
        records.append((track_id, now_p_x, now_p_y,now_line_id, now_gps_log_id, now_source, now_target, now_length, now_fraction, now_v, now_log_time))

    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, records)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_red_point_track(records, table_name='red_point_track'):
    sql = '''
    insert into {}(line_ids, ratio, middle_id, track_id, track_seg_id)
    values(%s, %s, %s, %s, %s)
    '''.format(table_name)



    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, records)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#
# 将构建好的轨迹插入数据库中
# @param records 轨迹数组
# @param table_name 数据表名称
#
def insert_construct_track(records, table_name='construct_tracks_6'):
    items = []
    for track_id, track_line_ids_list in records:
        for track_line_ids in track_line_ids_list:
            items.append((track_id, track_line_ids))

    sql = '''
    INSERT INTO {0}(track_id, line_ids) VALUES(%s, %s)
    '''.format(table_name)

    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, items)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()




def insert_new_track_time(records, table_name='new_track_time'):

    sql = '''
insert into {}(line_ids,begin_middle,begin_ratio,end_middle,end_ratio, avg_time, agg_len, time_list)
values (%s,%s,%s,%s,%s,%s, %s, %s)
    '''.format(table_name)

    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, records)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()






def insert_logs(gps_logs, table_name):
    # sql = '''
    # INSERT INTO gps_log(log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, geom)
    # VALUES (%s, %s, %s, %s, %s, %s, %s, ST_GeomFromText('POINT(%s %s)', 4326));

    # '''
    sql = '''
    INSERT INTO {}(log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, geom)
    VALUES (%s, %s, %s, %s, %s, %s, %s, ST_Transform(ST_GeomFromText('POINT(%s %s)', 4326), 32649));
    '''.format(table_name)
    

    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, gps_logs)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_lines(lines, table_name='new_network_6_1'):
    sql = '''
    INSERT INTO {}(start_node, end_node, avg_time, agg_len, reverse_length)
    VALUES(%s, %s, %s, %s, %s)
    '''.format(table_name)



    conn = None
    try:
        params = config.config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, lines)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()




if __name__ == '__main__':
    log_time = datetime.datetime.strptime('20090501' + '005754', '%Y%m%d%H%M%S')    
    unknown_1 = 'H'
    car_id = '13013814358'
    log = 114.076150
    lat = 22.543683
    velocity = 42
    direction = 8
    on_service = False
    is_valid = True

    a_gps_log = (log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, log, lat)

    #insert_logs([a_gps_log])
    a_track = ([21312,32114],)
    insert_track([a_track])





