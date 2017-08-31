# -*- encoding: utf-8

from query import get_tracks, get_closest_points, get_distance_rows
from insert import insert_match
from get_path_distance import get_path_distance, MAX_PATH_DIS
import math
import networkx as nx
import time
from multiprocessing import Pool, cpu_count

config = {
    'tracks_table_name': 'tracks_8_10',
    'line_table_name': 'shenzhen_line1',
    'log_table_name': 'gps_log_valid_8_10',
    'match_track_table_name': 'match_track_8_10'
}



# 标准正态分布的概率密度函数
def normal_distribution(x, u = 0.0, sigma = 20.0):    
    return (1.0 / ( ((2 * math.pi)  ** 0.5) * sigma)) * math.exp( -( (x-u)**2 / (2 * sigma**2) ) )




# observation probability
def get_observation_prob(closest_point):

    log_x, log_y, p_x, p_y, line_id, gps_log_id, source, target, length, flo, fraction, v, log_time, line_x, line_y = closest_point

    dis = euclidan_dis(log_x, log_y, p_x, p_y)

    return normal_distribution(dis)

# 欧氏距离
def euclidan_dis(x1, y1, x2, y2):
    return ( (x1 - x2) ** 2 + (y1 - y2) ** 2 ) ** 0.5










# 转移概率
def get_transmission_probability(pre_closest_point, now_closest_point, dis_dict):
    p_path_dis = get_path_distance(pre_closest_point, now_closest_point, dis_dict)  # 两个匹配点的路径距离

    pre_log_x, pre_log_y, pre_p_x, pre_p_y, pre_line_id, pre_gps_log_id, pre_source, pre_target, pre_length, pre_flo, pre_fraction, pre_v, pre_log_time, pre_line_x, pre_line_y = pre_closest_point
    now_log_x, now_log_y, now_p_x, now_p_y, now_line_id, now_gps_log_id, now_source, now_target, now_length, now_flo, now_fraction, now_v, now_log_time, now_line_x, now_line_y = now_closest_point

    log_dis = euclidan_dis(pre_log_x, pre_log_y, now_log_x, now_log_y)  # 两个gps点的直线距离
    # print log_dis

    prob = log_dis / (p_path_dis+0.00001)  # 转移概率
    if prob > 1:
        prob = 1
    if prob < 0:
        prob = 0
    return prob


#
# 构造一个st_match图
# @param log_ids {{list}} log_id list
# @param closest_points {{list}} closest point list
# @param log_closest_dict {{dict}} log_id: [closest_pnt_idx1, closest_pnt_idx2 ...]
#
def construct_graph(log_ids, closest_points, log_closest_dict, dis_dict):

    g = nx.Graph()
    pre_layer_idx = []
    for log_idx, log_id in enumerate(log_ids):
        closest_idxs = log_closest_dict[log_id]
        now_layer_idx = []
        for closest_idx in closest_idxs:
            now_layer_idx.append(closest_idx)
            observation_prob = get_observation_prob(closest_points[closest_idx])  # 计算节点的观察概率
            g.add_node(closest_idx, observation_prob=observation_prob)  # 向匹配图中添加一个节点
            if log_idx == 0:
                continue  # 第一层不需要计算转移概率
            else:
                for idx in pre_layer_idx:  # 计算前一层到当前层连接概率

                    transmission_prob = get_transmission_probability(closest_points[idx],
                                                                     closest_points[closest_idx],
                                                                     dis_dict)  # 计算连接概率
                    g.add_edge(idx, closest_idx, transmission_prob=transmission_prob)  # 往图中添加一条连接边

        pre_layer_idx = now_layer_idx
    
    return g

#
# 从匹配图中获得匹配结果
# @param {{networkx}} g 匹配图
# @param {{list}} log_ids gps log 列表
# @param {{dict}} log_closest_dict gps log 和 closest point字典
#


def find_match_sequence(g, log_ids, log_closest_dict):

    f = {}
    pre = {}

    for idx in log_closest_dict[log_ids[0]]:
        f[idx] = g.node[idx]['observation_prob']  # 第一层所有节点的最大概率
    for layer_idx, log_id in enumerate(log_ids[1:]):  # 从第二层开始遍历
        for p_idx in log_closest_dict[log_id]:  # 对于第二层的每一个节点
            max_f = -99999999
            for p_p_idx in log_closest_dict[log_ids[layer_idx]]:  # 它的概率是，前一层的累积概率加上到当前的转移概率
                alt = g.edge[p_p_idx][p_idx]['transmission_prob'] * g.node[p_idx]['observation_prob'] + f[p_p_idx]
                if alt > max_f:
                    max_f = alt
                    pre[p_idx] = p_p_idx # 记录当前取到最大累积概率的路径
            f[p_idx] = max_f  # 记录当前的累积概率
    max_c = -99999999
    max_key = None
    for key, val in f.items():
        if val >= max_c:
            max_key = key
            max_c = val
        else:
            continue
    r_list = []
    
    for i in range(1, len(log_ids)):

        r_list.append(max_key)
        max_key = pre[max_key]

    r_list.append(max_key)
    r_list.reverse()
    return r_list


def match_a_track(track, process_idx, idx, len_of_tracks):

    begin_track = time.time()

    log_closest_dict = {}

    for log_id in track[1]:  # 遍历轨迹的gps log id
        log_closest_dict[int(log_id)] = []

    closest_points = get_closest_points(tuple(track[1]), config['line_table_name'], config['log_table_name'])  # get gps log id's closest point in raod network 慢

    source_arr = []  # 记录所有最近点所在的线的source和target的id

    for idx, point in enumerate(closest_points):
        log_x, log_y, p_x, p_y, line_id, gps_log_id, source, target, length, flo, fraction, v, log_time, line_x, line_y = point
        if len(log_closest_dict[int(gps_log_id)]) >= 5:  # 只记录最近的5个最近点
            continue
        source_arr.append(source)
        source_arr.append(target)
        log_closest_dict[int(gps_log_id)].append(idx)

    dis_rows = get_distance_rows(tuple(source_arr))  # 从数据库中得到所有可能要用到的网络距离
    dis_dict = {}  # 字典， dict[start_vid][end_vid]
    for row in dis_rows:
        if dis_dict.get(row[0]) is None:
            dis_dict[row[0]] = {}
        dis_dict[row[0]][row[1]] = row[2]

    g = construct_graph(track[1], closest_points, log_closest_dict, dis_dict)

    match_list = find_match_sequence(g, track[1], log_closest_dict)

    insert_match(match_list, closest_points, track[0], table_name=config['match_track_table_name'])

    print('track id ({0}): length {1} time: {2} elapse: {3}/{4}-{5}'.
          format(track[0], len(track[1]), time.time() - begin_track, idx, len_of_tracks, process_idx))


def match_tracks(tracks, process_idx):
    len_of_tracks = len(tracks)
    for idx, track in enumerate(tracks):
        match_a_track(track, process_idx, idx+1, len_of_tracks)
    return 1

def main():

    tracks = get_tracks(table_name=config['tracks_table_name'])  # 获得轨迹

    lists_of_small_track = []
    size_of_small_track = len(tracks)/ cpu_count()
    for i in range(0, len(tracks), size_of_small_track):
        lists_of_small_track.append(tracks[i:i+size_of_small_track])

    pool = Pool(processes=cpu_count())
    result_list = []
    for idx, small_track in enumerate(lists_of_small_track):
        # parallel match
        res = pool.apply_async(match_tracks, (small_track, idx))
        result_list.append(res)
    for x in result_list:
        x.get()  # 这个写法，可以捕获进程池中的异常
    pool.close()
    pool.join()

if __name__ == '__main__':
    begin = time.time()
    main()
    print('all time:{}'.format(time.time() - begin))
