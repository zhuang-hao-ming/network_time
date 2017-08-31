# -*- encoding: utf-8

import csv
import time
from query import get_construct_tracks, get_track_ids, get_line_nodes
from insert import insert_red_point_track
from collections import defaultdict


config = {
    'match_track_table_name': 'match_track_8_10',
    'construct_tracks_table_name': 'construct_tracks_8_10',
    'line_table_name': 'shenzhen_line1',
    'red_point_track_table_name': 'red_point_track_8_10_1',
     'red_point_file_name': r'D:\app\data\csv\red_point_class_11.csv',
    #'red_point_file_name': r'D:\app\data\csv\readpoint_class.csv',
}

#
# 道路的起始点和长度信息
#
def get_line_nodes_dict():
    line_nodes_dict = {}
    for row in get_line_nodes(table_name=config['line_table_name']):
        id, source, target, length = row
        line_nodes_dict[id] = {
            'source': source,
            'target': target,
            'length': length
        }

    return line_nodes_dict


#
# 读取红灯数据
#
def get_redpoint_id_idx_dict():
    redpoint_id_idx_dict = {}
    read_class_csv = config['red_point_file_name']
    with open(read_class_csv, 'rb') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        for row in reader:
            for pnt_id in row[1:]:
                redpoint_id_idx_dict[int(pnt_id)] = int(row[0])
    return redpoint_id_idx_dict


#
# 还原好的轨迹数据
#
def get_track_id_line_ids():

    track_id_line_ids = defaultdict(list)
    for row in get_construct_tracks(table_name=config['construct_tracks_table_name']):
        id, track_id, line_ids = row
        track_id_line_ids[track_id].append( (id,line_ids))
    return track_id_line_ids


#
# 得到组成一条红灯轨迹的道路id以及道路的中点
#
def get_connect_line_ids(pre_idx, now_idx, track_line_ids, line_nodes_dict):
    line_ids = []
    line_length = []
    pre_line_id = -1

    for j in range(pre_idx, now_idx):
        line_id = track_line_ids[j + 1]

        if line_id != pre_line_id:
            line_ids.append(line_id)
            line_length.append(line_nodes_dict[line_id]['length'])
            pre_line_id = line_id

    half_of_sum = sum(line_length) / 2
    agg_sum = 0
    ratio = 0
    middle_idx = -1
    for idx, length in enumerate(line_length):
        agg_sum += length
        if agg_sum >= half_of_sum:
            middle_idx = idx
            ratio = (agg_sum - half_of_sum) / length
            break

    if middle_idx == -1:
        raise Exception('middle_idx = -1')
    return line_ids, 1-ratio, line_ids[middle_idx]


#
# 处理一条轨迹
#
def calculate_a_track(track_id, redpoint_id_idx_dict, line_nodes_dict, track_id_line_ids):

    begin = time.time()  # 计时

    track_line_ids_list = track_id_line_ids[track_id]  # 获取还原轨迹
    if len(track_line_ids_list) <= 0:
        return None


    records = [] # 红灯轨迹

    for track_seg_id, track_line_ids in track_line_ids_list:
        red_points = []  #
        pre_red_pnt_id = -1  #
        # 遍历轨迹上的路网，选取红灯，如果两个相邻红灯是相同红灯，只保留id小的
        for idx, line_id in enumerate(track_line_ids[:-1]):
            target = line_nodes_dict[line_id]['target']
            if target in redpoint_id_idx_dict:
                if redpoint_id_idx_dict.get(pre_red_pnt_id) != redpoint_id_idx_dict.get(target):
                    red_points.append((idx,line_id))
                    pre_red_pnt_id = target



        # 遍历两个相邻的红灯
        for i in range(1, len(red_points)):
            pre_idx, pre_line_id = red_points[i-1]
            now_idx, now_line_id = red_points[i]

            line_ids, ratio, middle_id = get_connect_line_ids(pre_idx, now_idx, track_line_ids, line_nodes_dict)

            records.append((line_ids, ratio, middle_id, track_id, track_seg_id))




    print('elapse {}'.format(time.time() - begin))
    return records

#
# 处理一组轨迹
#
def calculate_tracks(track_ids, redpoint_id_idx_dict, line_nodes_dict, track_id_line_ids):
    records = []
    for track_id in track_ids:
        items = calculate_a_track(track_id, redpoint_id_idx_dict, line_nodes_dict, track_id_line_ids)
        if items is not None:
            records.extend(items)
        if len(records) > 10000:
            insert_red_point_track(records, table_name=config['red_point_track_table_name'])
            records = []
    if len(records) > 0:
        insert_red_point_track(records, table_name=config['red_point_track_table_name'])


def main():
    track_ids = [int(row[0]) for row in get_track_ids(table_name=config['match_track_table_name'])]  # 读取所有轨迹id
    redpoint_id_idx_dict = get_redpoint_id_idx_dict()  # 读取红灯数据， 构建一个以红灯id为键， 红灯类为值的字典
    line_nodes_dict =  get_line_nodes_dict()  # 读取道路和它的节点以及长度信息
    track_id_line_ids = get_track_id_line_ids() # 读取还原好的轨迹

    calculate_tracks(track_ids, redpoint_id_idx_dict, line_nodes_dict, track_id_line_ids)





















if __name__ == '__main__':
    begin  = time.time()
    main()
    print('all time {}'.format(time.time()-begin))