# -*- encoding: utf-8

import time
from query import get_dijkstra_path, get_track_ids, get_match_track
from insert import insert_construct_track
from multiprocessing import Pool, cpu_count


config = {
    'match_track_table_name': 'match_track_6',
    'construct_tracks_table_name': 'construct_tracks_6_1',

}


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
def valid_connect_path(pnt1, pnt2):

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

    if line_id_1 == line_id_2:
        real_dis = abs(ratio_1 - ratio_2) * len_1
    else:
        real_dis = (1 - ratio_1) * len_1 + ratio_2 * len_2
    max_dis = (pnt2_t - pnt1_t).total_seconds() * max_v

    if real_dis > max_dis:
        return False
    else:
        return True


#
# 判断一条dijkstra路径是否合法
#
def valid_dijkstra_path(agg_cost, pnt1, pnt2):
    agg_cost += (1 - pnt1['fraction']) * pnt1['length']
    agg_cost += pnt2['fraction'] * pnt2['length']
    pnt1_v = pnt1['velocity']
    pnt2_v = pnt2['velocity']
    pnt1_t = pnt1['log_time']
    pnt2_t = pnt2['log_time']
    max_v = max(pnt1_v, pnt2_v, 33)
    elapse = (pnt2_t - pnt1_t).total_seconds()

    real_cost = max_v * elapse

    if agg_cost > real_cost:
        print 'false', 'agg_cost:{} real {} {}'.format(agg_cost, real_cost, real_cost / agg_cost)
        return False
    else:
        return True


#
# 还原轨迹，得到组成轨迹的道路id集合
#
def get_track_line_ids(match_points):
    line_ids_list = []

    line_ids = [match_points[0]['line_id']]
    pre_line_id = match_points[0]['line_id']

    for i in range(1, len(match_points)):
        pnt1 = match_points[i-1]
        pnt2 = match_points[i]

        # 不是直接相连
        if pnt1['target'] != pnt2['source'] and (pnt1['line_id'] != pnt2['line_id']):

            rows = get_dijkstra_path(pnt1['target'], pnt2['source'])
            if len(rows) == 0:  # dijkstra查询没有结果？
                if len(line_ids) > 5:
                    line_ids_list.append(line_ids)
                line_ids = [pnt2['line_id']]
                pre_line_id = pnt2['line_id']
                continue

            agg_cost = sum([row[1] for row in rows])
            if not valid_dijkstra_path(agg_cost, pnt1, pnt2): # 不是一条有效的dijkstra路径
                if len(line_ids) > 5:
                    line_ids_list.append(line_ids)
                line_ids = [pnt2['line_id']]
                pre_line_id = pnt2['line_id']
                continue

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
            if valid_connect_path(pnt1, pnt2):
                line_id = pnt2['line_id']
                if line_id != pre_line_id:
                    line_ids.append(line_id)
                    pre_line_id = line_id
            else:
                if len(line_ids) > 5:
                    line_ids_list.append(line_ids)
                line_ids = [pnt2['line_id']]
                pre_line_id = pnt2['line_id']
                continue

    if len(line_ids) > 5:
        line_ids_list.append(line_ids)

    return line_ids_list


#
# 处理一条轨迹
#
def calculate_a_track(track_id):

    begin = time.time()  # 计时

    match_points = [point_tuple_to_dict(row) for row in get_match_track(track_id=track_id, table_name=config['match_track_table_name'])]  # 组成匹配轨迹的点
    track_line_ids_list = get_track_line_ids(match_points)  # 将轨迹还原
    if len(track_line_ids_list) <= 0:
        return None
    print('elapse {}'.format(time.time() - begin))
    return track_id, track_line_ids_list


#
# 处理一组轨迹
#
def calculate_tracks(track_ids):
    records = []
    for track_id in track_ids:
        item = calculate_a_track(track_id)
        if item is not None:
            records.append(item)
    if len(records) > 0:
        insert_construct_track(records, table_name=config['construct_tracks_table_name'])


def main():

    track_ids = [int(row[0]) for row in get_track_ids(table_name=config['match_track_table_name'])]  # 读取所有匹配轨迹的id

    lists_of_small_track_set = []  # 将轨迹等分为cpu_count组
    size_of_small_track_set = len(track_ids) / cpu_count()  # 组的大小
    for i in range(0, len(track_ids), size_of_small_track_set):  # 相邻的轨迹分在同一组，有利于数据库缓存命中
        lists_of_small_track_set.append(track_ids[i:i+size_of_small_track_set])

    pool = Pool(processes=cpu_count())
    list_of_result = []
    for idx, small_track in enumerate(lists_of_small_track_set):
        # parallel match
        x = pool.apply_async(calculate_tracks, (small_track,))
        list_of_result.append(x)
    for x in list_of_result:
        x.get()  # 这个写法，可以捕获进程池中的异常
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()