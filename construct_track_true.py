# -*- encoding: utf-8

import time
from query import get_dijkstra_path, get_track_ids, get_match_track, get_line_flo
from insert import insert_construct_track
from multiprocessing import Pool, cpu_count
from match import get_distance_rows

config = {
    'match_track_table_name': 'match_track_8_10',
    'construct_tracks_table_name': 'construct_tracks_8_10',
    'line_table_name': 'shenzhen_line1',
}


def get_line_id_flo_dict():
    line_id_flo_dict = {}
    for row in get_line_flo(table_name=config['line_table_name']):
        id, flo = row
        line_id_flo_dict[id] = flo
    return line_id_flo_dict

#
# 将数据库返回的tuple转化为字典
#
def point_tuple_to_dict(point_tuple):
    id, line_id, source, target, length, fraction, velocity, log_time, track_id = point_tuple
    return {
        'id': id,
        'line_id': int(line_id),
        'source': int(source),
        'target': int(target),
        'length': length,
        'fraction': fraction,
        'velocity': velocity,
        'log_time': log_time,
        'track_id': track_id
    }


#
# 判断一条直接连通路径是否合法
#
def valid_connect_path(pnt1, pnt2, line_id_flo_dict):

    pnt1_v = pnt1['velocity']
    pnt2_v = pnt2['velocity']
    pnt1_t = pnt1['log_time']
    pnt2_t = pnt2['log_time']
    line_id_1 = pnt1['line_id']
    line_id_2 = pnt2['line_id']
    max_v = max(pnt1_v, pnt2_v, 33)
    ratio_1 = pnt1['fraction']
    ratio_2 = pnt2['fraction']
    len_1 = pnt1['length']
    len_2 = pnt2['length']
    target_1 = pnt1['target']
    target_2 = pnt2['target']
    source_1 = pnt1['source']
    source_2 = pnt2['source']
    flo_1 = line_id_flo_dict[line_id_1]

    agg_dis = 0

    # 同一条路径
    if line_id_1 == line_id_2:
        agg_dis = (ratio_2 - ratio_1) * len_1
        if flo_1 == 0:
            agg_dis = abs(agg_dis)
    else:  # 相邻路径
        if source_1 == source_2:
            agg_dis = ratio_1 * len_1 + ratio_2 * len_2

        elif source_1 == target_2:
            agg_dis = ratio_1 * len_1 + (1-ratio_2)* len_2

        elif target_1 == source_2:
            agg_dis = (1 - ratio_1) * len_1 + ratio_2 * len_2

        elif target_1 == target_2:
            agg_dis = (1 - ratio_1) * len_1 + (1-ratio_2) * len_2

    real_dis = (pnt2_t - pnt1_t).total_seconds() * max_v

    # 时间不合法
    if pnt2_t < pnt1_t:
        return False
    # 单向路径上逆行不合法
    if agg_dis < 0:
        return False
    # 规划路径比最大真实路径长不合法
    if agg_dis > real_dis:
        return False

    return True


#
# 判断一条dijkstra路径是否合法
#
def valid_dijkstra_path(agg_cost, pnt1, pnt2):

    pnt1_v = pnt1['velocity']
    pnt2_v = pnt2['velocity']
    pnt1_t = pnt1['log_time']
    pnt2_t = pnt2['log_time']

    max_v = max(pnt1_v, pnt2_v, 33)
    elapse = (pnt2_t - pnt1_t).total_seconds()

    real_cost = max_v * elapse

    if pnt2_t < pnt1_t:
        return False

    if agg_cost > real_cost:
        print 'false', 'agg_cost:{} real {} {}'.format(agg_cost, real_cost, real_cost / agg_cost)
        return False

    return True


def is_direct_connect(pnt1, pnt2, line_id_flo_dict):
    line_id_1 = pnt1['line_id']
    line_id_2 = pnt2['line_id']
    target_1 = pnt1['target']
    target_2 = pnt2['target']
    source_1 = pnt1['source']
    source_2 = pnt2['source']
    flo_1 = line_id_flo_dict[line_id_1]
    flo_2 = line_id_flo_dict[line_id_2]

    if line_id_1 == line_id_2:
        return True

    if flo_1 == 0:
        if flo_2 == 0:
            if source_1 == source_2 or source_1 == target_2 or target_1 == source_2 or target_1 == target_2:
                return True
        else:
            if source_1 == source_2 or target_1 == source_2:
                return True
    else:
        if flo_2 == 0:
            if target_1 == source_2 or target_1 == target_2:
                return True
        else:
            if target_1 == source_2:
                return True
    return False


def get_dijkstra_path_from_pnt(pnt1, pnt2, line_id_flo_dict):
    line_id_1 = pnt1['line_id']
    line_id_2 = pnt2['line_id']
    target_1 = pnt1['target']
    target_2 = pnt2['target']
    source_1 = pnt1['source']
    source_2 = pnt2['source']
    flo_1 = line_id_flo_dict[line_id_1]
    flo_2 = line_id_flo_dict[line_id_2]
    ratio_1 = pnt1['fraction']
    ratio_2 = pnt2['fraction']
    len_1 = pnt1['length']
    len_2 = pnt2['length']

    id_arr = [source_1, source_2, target_1, target_2]

    dis_rows = get_distance_rows(tuple(id_arr))  # 从数据库中得到所有可能要用到的网络距离
    dis_dict = {}  # 字典， dict[start_vid][end_vid]
    for row in dis_rows:
        if dis_dict.get(row[0]) is None:
            dis_dict[row[0]] = {}
        dis_dict[row[0]][row[1]] = row[2]

    MAX_PATH_DIS = 9999999
    min_path_dis = MAX_PATH_DIS
    max_source = -1
    max_target = -1

    if flo_1 == 0:  # pre双向
        if flo_2 == 0:  # now双向

            source_ids = [source_1, target_1]
            target_ids = [source_2, target_2]


            for id_x, key_x in enumerate(source_ids):
                for id_y, key_y in enumerate(target_ids):
                    try:
                        routing_dis = dis_dict[key_x][key_y]
                    except Exception as err:
                        routing_dis = MAX_PATH_DIS
                    if id_x == 0:  # 从p1的source出发
                        routing_dis += len_1 * ratio_1
                    elif id_x == 1:  # 从p1的target出发
                        routing_dis += len_1 * (1.0 - ratio_1)

                    if id_y == 0:  # 到达p2的source
                        routing_dis += len_2 * ratio_2
                    elif id_y == 1:  # 到达p2的target
                        routing_dis += len_2 * (1.0 - ratio_2)

                    if routing_dis < min_path_dis:
                        min_path_dis = routing_dis
                        max_source = key_x
                        max_target = key_y

        else:  # now单
            source_ids = [source_1, target_1]
            target_ids = [source_2]

            for id_x, key_x in enumerate(source_ids):
                for key_y in target_ids:
                    try:
                        routing_dis = dis_dict[key_x][key_y]
                    except Exception as err:
                        routing_dis = MAX_PATH_DIS

                    if id_x == 0:  # 从p1的source出发
                        routing_dis += len_1 * ratio_1
                    elif id_x == 1:  # 从p1的target出发
                        routing_dis += len_1 * (1.0 - ratio_1)
                    routing_dis += len_2 * ratio_2
                    if routing_dis < min_path_dis:
                        min_path_dis = routing_dis
                        max_source = key_x
                        max_target = key_y

    else:  # pre单向
        if int(flo_2) == 0:  # now双向
            source_ids = [target_1]
            target_ids = [source_2, target_2]

            for key_x in source_ids:
                for id_y, key_y in enumerate(target_ids):
                    try:
                        routing_dis = dis_dict[key_x][key_y]
                    except Exception as err:
                        routing_dis = MAX_PATH_DIS

                    routing_dis += len_1 * (1.0 - ratio_1)

                    if id_y == 0:  # 到达p2的source
                        routing_dis += len_2 * ratio_2
                    elif id_y == 1:  # 到达p2的target
                        routing_dis += len_2 * (1.0 - ratio_2)

                    if routing_dis < min_path_dis:
                        min_path_dis = routing_dis
                        max_source = key_x
                        max_target = key_y

        else:  # now单向

            try:
                routing_dis = dis_dict[target_1][source_2]
            except Exception as err:
                routing_dis = MAX_PATH_DIS
            routing_dis += len_1 * (1.0 - ratio_1)
            routing_dis += len_2 * ratio_2
            min_path_dis = routing_dis
            max_source = target_1
            max_target = source_2

    if min_path_dis < MAX_PATH_DIS:
        return min_path_dis, get_dijkstra_path(max_source, max_target)
    else:
        return False


#
# 还原轨迹，得到组成轨迹的道路id集合
#
def get_track_line_ids(match_points, line_id_flo_dict):
    line_ids_list = []

    line_ids = [match_points[0]['line_id']]
    pre_line_id = match_points[0]['line_id']

    for i in range(1, len(match_points)):
        pnt1 = match_points[i-1]
        pnt2 = match_points[i]

        # 不是直接相连
        if not is_direct_connect(pnt1, pnt2, line_id_flo_dict):

            res = get_dijkstra_path_from_pnt(pnt1, pnt2, line_id_flo_dict)
            if not res:
                return []

            agg_cost, rows = res

            if not valid_dijkstra_path(agg_cost, pnt1, pnt2): # 不是一条有效的dijkstra路径
                return []

            for row in rows:
                edge_id = int(row[0])

                if edge_id != -1:
                    if edge_id != pre_line_id:
                        line_ids.append(edge_id)
                        pre_line_id = edge_id
            line_id = pnt2['line_id']
            if line_id != pre_line_id:
                line_ids.append(line_id)
                pre_line_id = line_id

        else:  # 直接相连
            if valid_connect_path(pnt1, pnt2, line_id_flo_dict):
                line_id = pnt2['line_id']
                if line_id != pre_line_id:
                    line_ids.append(line_id)
                    pre_line_id = line_id
            else:
                return []

    if len(line_ids) > 5:
        line_ids_list.append(line_ids)

    return line_ids_list


#
# 处理一条轨迹
#
def calculate_a_track(track_id, line_id_flo_dict):

    begin = time.time()  # 计时

    match_points = [point_tuple_to_dict(row) for row in get_match_track(track_id=track_id, table_name=config['match_track_table_name'])]  # 组成匹配轨迹的点
    track_line_ids_list = get_track_line_ids(match_points, line_id_flo_dict)  # 将轨迹还原
    if len(track_line_ids_list) <= 0:
        print 'cannot '
        return None
    print('elapse {}'.format(time.time() - begin))
    return track_id, track_line_ids_list


#
# 处理一组轨迹
#
def calculate_tracks(track_ids, line_id_flo_dict):
    records = []
    for track_id in track_ids:
        item = calculate_a_track(track_id, line_id_flo_dict)
        if item is not None:
            records.append(item)
    if len(records) > 0:
        insert_construct_track(records, table_name=config['construct_tracks_table_name'])


def main():
    line_id_flo_dict = get_line_id_flo_dict()
    track_ids = [int(row[0]) for row in get_track_ids(table_name=config['match_track_table_name'])]  # 读取所有匹配轨迹的id
    #
    # calculate_tracks(track_ids, line_id_flo_dict)
    # return
    lists_of_small_track_set = []  # 将轨迹等分为cpu_count组
    size_of_small_track_set = len(track_ids) / cpu_count()  # 组的大小
    for i in range(0, len(track_ids), size_of_small_track_set):  # 相邻的轨迹分在同一组，有利于数据库缓存命中
        lists_of_small_track_set.append(track_ids[i:i+size_of_small_track_set])

    pool = Pool(processes=cpu_count())
    list_of_result = []
    for idx, small_track in enumerate(lists_of_small_track_set):
        # parallel match
        x = pool.apply_async(calculate_tracks, (small_track,line_id_flo_dict))
        list_of_result.append(x)
    for x in list_of_result:
        x.get()  # 这个写法，可以捕获进程池中的异常
    pool.close()
    pool.join()

if __name__ == '__main__':
    begin = time.time()
    main()
    print('all time: {}'.format(time.time()-begin))