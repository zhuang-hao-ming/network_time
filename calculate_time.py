# -*- encoding: utf-8
from query import get_red_point_tracks, get_match_track, get_track_ids, get_line_nodes, get_construct_tracks, get_all_red_tracks, get_all_match_pnts
from insert import insert_new_track_time
import time
from datetime import timedelta
from collections import defaultdict

config = {
    'match_track_table_name': 'match_track_8_10',
    'construct_tracks_table_name': 'construct_tracks_8_10',
    'line_table_name': 'shenzhen_line1',
    'red_point_track_table_name': 'red_point_track_8_10_1',
    'new_track_time_table_name': 'new_track_time_8_10_1',
}


#
# 获得轨迹-轨迹段-红灯轨迹字典
#
def get_track_id_red_tracks():
    track_id_red_tracks = defaultdict(lambda : defaultdict(list))
    for row in get_all_red_tracks(table_name=config['red_point_track_table_name']):
        track = track_tuple_to_dict(row)
        track_id_red_tracks[track['track_id']][track['track_seg_id']].append(track)
    return track_id_red_tracks

#
# 获得轨迹-轨迹点
#
def get_track_id_match_pnts():
    track_id_match_pnts = defaultdict(list)
    for row in get_all_match_pnts(table_name=config['match_track_table_name']):
        pnt = point_tuple_to_dict(row)
        track_id_match_pnts[pnt['track_id']].append(pnt)
    return track_id_match_pnts


#
# 获得轨迹段id， 和组成轨迹段的道路
#
def get_track_seg_id_lines_dict():
    track_seg_id_lines_dict = {}
    for row in get_construct_tracks(table_name=config['construct_tracks_table_name']):
        id, track_id, line_ids = row
        track_seg_id_lines_dict[id] = line_ids
    return track_seg_id_lines_dict


#
# 获得线段长度
#
def get_line_id_length_dict():
    line_id_length_dict = {}
    for id, _1, _2, length in get_line_nodes(table_name=config['line_table_name']):
        line_id_length_dict[int(id)] = length
    return line_id_length_dict






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

def track_tuple_to_dict(row):
    id, line_ids, ratio, middle_id, track_id,track_seg_id = row
    return {
        'id': id,
        'line_ids': line_ids,
        'ratio': ratio,
        'middle_id': middle_id,
        'track_id': track_id,
        'track_seg_id': track_seg_id
    }
#
# 得到出现频数 >= 10的红灯路段字典，以line_ids做键，频数做值
#
def get_hight_freq_dict(tracks):
    track_freq_dict = {}
    for track in tracks:
        key = tuple(track['line_ids'])
        if track_freq_dict.get(key):
            track_freq_dict[key] += 1
        else:
            track_freq_dict[key] = 1

    for key, val in track_freq_dict.items():
        if val < 10:
            track_freq_dict.pop(key)
    return track_freq_dict



# 获得新轨迹的长度
def get_new_track_length(line_id_length_dict, track_1, track_2):
    line_ids = track_1['line_ids'] + track_2['line_ids']

    begin_middle = track_1['middle_id']
    begin_ratio = track_1['ratio']
    end_middle = track_2['middle_id']
    end_ratio = track_2['ratio']

    begin_idx = -1
    end_idx = -1
    for idx, line_id in enumerate(line_ids):
        if line_id == begin_middle:
            begin_idx = idx
            continue
        if line_id == end_middle:
            end_idx = idx
            continue

    agg_len = 0
    agg_len += line_id_length_dict[begin_middle] * (1-begin_ratio)
    agg_len += line_id_length_dict[end_middle] * end_ratio
    for idx in range(begin_idx+1, end_idx):
        agg_len += line_id_length_dict[line_ids[idx]]
    return agg_len







#
# 获得轨迹上和节点不在同一条道路上的点到节点的距离
#
def get_path_distance(idx_of_middle_id, middle_ratio, pnt, all_line_ids, line_id_length_dict):
    pnt_line_id = pnt['line_id']
    idx_of_pnt_line_id = all_line_ids.index(pnt_line_id)

    sum_of_len = 0

    if idx_of_pnt_line_id < idx_of_middle_id:
        sum_of_len += (1 - pnt['fraction']) * pnt['length']
        sum_of_len += line_id_length_dict[all_line_ids[idx_of_middle_id]] * middle_ratio
        for line_id in all_line_ids[idx_of_pnt_line_id+1:idx_of_middle_id-1]:
            sum_of_len += line_id_length_dict[line_id]
    elif idx_of_pnt_line_id > idx_of_middle_id:
        sum_of_len += line_id_length_dict[all_line_ids[idx_of_middle_id]] * (1-middle_ratio)
        sum_of_len +=  pnt['fraction'] * pnt['length']
        for line_id in all_line_ids[idx_of_middle_id+1:idx_of_pnt_line_id-1]:
            sum_of_len += line_id_length_dict[line_id]
    else:
        pnt_ratio = pnt['fraction']
        length = line_id_length_dict[all_line_ids[idx_of_middle_id]]
        sum_of_len = abs(pnt_ratio-middle_ratio) * length
    return sum_of_len

#
# 获得经过红灯轨迹中点的时间
#
def get_pnt_time(track_seg_id, track, match_pnts, line_id_length_dict, track_seg_id_lines_dict):

    all_line_ids = track_seg_id_lines_dict[track_seg_id]
    red_line_ids = track['line_ids']

    middle_id = track['middle_id']
    idx_of_middle_id = all_line_ids.index(middle_id)
    ratio = track['ratio']

    left_pnts = []
    right_pnts = []
    in_left_pnts = []
    in_right_pnts = []
    for pnt in match_pnts:
        pnt_line_id = pnt['line_id']
        try:
            idx_of_pnt_line_id = all_line_ids.index(pnt_line_id)
        except Exception as err:
            continue
        if pnt_line_id in red_line_ids:
            if idx_of_pnt_line_id < idx_of_middle_id:
                in_left_pnts.append(pnt)
            elif idx_of_pnt_line_id > idx_of_middle_id:
                in_right_pnts.append(pnt)
            else:
                pnt_ratio = pnt['fraction']
                if pnt_ratio <= ratio:
                    in_left_pnts.append(pnt)
                else:
                    in_right_pnts.append(pnt)
        else:
            if idx_of_pnt_line_id < idx_of_middle_id:
                left_pnts.append(pnt)
            elif idx_of_pnt_line_id > idx_of_middle_id:
                right_pnts.append(pnt)

    pnt_t = None

    if len(in_left_pnts) >= 1 and len(in_right_pnts) >= 1:  # 前后均有打点
        left_nearest = in_left_pnts[-1]
        right_nearest = in_right_pnts[0]

        left_t = left_nearest['log_time']
        right_t = right_nearest['log_time']
        left_d = get_path_distance(idx_of_middle_id, ratio, left_nearest, all_line_ids, line_id_length_dict)
        right_d = get_path_distance(idx_of_middle_id, ratio, right_nearest, all_line_ids, line_id_length_dict)
        delta_seconds = (right_t - left_t).total_seconds() * left_d / (left_d + right_d)
        pnt_t = left_t + timedelta(seconds=delta_seconds)

    elif len(in_left_pnts) >= 1:
        for i in range(len(in_left_pnts) - 1, -1, -1):
            left_nearest = in_left_pnts[i]
            left_velocity = left_nearest['velocity']
            if left_velocity < 5:
                continue
            left_t = left_nearest['log_time']
            left_d = get_path_distance(idx_of_middle_id,ratio, left_nearest, all_line_ids, line_id_length_dict)
            delta_seconds = left_d / left_velocity
            pnt_t = left_t + timedelta(seconds=delta_seconds)
            break
    elif len(in_right_pnts) >= 1:
        for i in range(0, len(in_right_pnts), 1):
            right_nearest = in_right_pnts[i]
            right_velocity = right_nearest['velocity']
            if right_velocity < 5:
                continue
            right_t = right_nearest['log_time']
            right_d = get_path_distance(idx_of_middle_id, ratio, right_nearest, all_line_ids, line_id_length_dict)
            delta_seconds = right_d / right_velocity
            pnt_t = right_t - timedelta(seconds=delta_seconds)
            break
    if pnt_t is None and len(left_pnts) >= 1 and len(right_pnts) >= 1:
        left_nearest = left_pnts[-1]
        right_nearest = right_pnts[0]

        left_t = left_nearest['log_time']
        right_t = right_nearest['log_time']

        left_d = get_path_distance(idx_of_middle_id, ratio, left_nearest, all_line_ids, line_id_length_dict)
        right_d = get_path_distance(idx_of_middle_id, ratio, right_nearest, all_line_ids, line_id_length_dict)

        delta_seconds = (right_t - left_t).total_seconds() * left_d / (left_d + right_d)
        pnt_t = left_t + timedelta(seconds=delta_seconds)


    return pnt_t


def main():
    begin = time.time()
    track_seg_id_lines_dict = get_track_seg_id_lines_dict()  # 轨迹和组成轨迹的线段的字典
    line_id_length_dict = get_line_id_length_dict()  # 线段和线段长度的字典
    track_ids = [int(row[0]) for row in get_track_ids(table_name=config['match_track_table_name'])]  # 读取所有轨迹id
    track_id_red_tracks = get_track_id_red_tracks()  # 轨迹-轨迹段-红灯轨迹
    track_id_match_pnts = get_track_id_match_pnts()  # 轨迹-轨迹点

    new_track_time_dict = {}  # 新轨迹和时间字典

    elapse_small_0_cnt = 0
    for cnt_idx, track_id in enumerate(track_ids):

        print('{} {}'.format(cnt_idx, track_id))
        match_pnts = track_id_match_pnts[track_id]  # 当前轨迹的点

        # red_tracks = [track_tuple_to_dict(row) for row in get_red_point_tracks(track_id)]  # 当前轨迹上的红灯段
        # match_pnts = [point_tuple_to_dict(row) for row in get_match_track(track_id)]  # 当前轨迹的点
        red_tracks_dict = track_id_red_tracks[track_id]  # 当前轨迹上的轨迹段-红灯段字典
        for track_seg_id, red_tracks in red_tracks_dict.items():  # 遍历每个轨迹段-红灯段列表
            for i in range(1, len(red_tracks)):  # 遍历红灯段
                track_1 = red_tracks[i-1]
                track_2 = red_tracks[i]
                pnt_t_1 = get_pnt_time(track_seg_id, track_1, match_pnts, line_id_length_dict, track_seg_id_lines_dict)
                pnt_t_2 = get_pnt_time(track_seg_id, track_2, match_pnts, line_id_length_dict, track_seg_id_lines_dict)
                if pnt_t_1 is None or pnt_t_2 is None:
                    #print 'none'
                    continue
                elapse = (pnt_t_2 - pnt_t_1).total_seconds()
                if elapse < 0:
                    print track_seg_id
                    print track_1
                    print track_2
                    elapse_small_0_cnt += 1
                    continue

                key = tuple(track_1['line_ids'] + track_2['line_ids'])

                agg_len = get_new_track_length(line_id_length_dict, track_1, track_2)

                if new_track_time_dict.get(key) is None:
                    new_track_time_dict[key] = {
                        'line_ids': key,
                        'begin_middle': (track_1['middle_id'], track_1['ratio']),
                        'end_middle': (track_2['middle_id'], track_2['ratio']),
                        'time': [elapse],
                        'agg_len': agg_len
                    }
                else:
                    new_track_time_dict[key]['time'].append(elapse)

    cnt = 0

    records = []
    for key, val in new_track_time_dict.items():
        time_list = val['time']
        if len(time_list) >= 10:
            cnt += 1
            avg_time = sum(time_list) / len(time_list)
            records.append((list(key), val['begin_middle'][0],val['begin_middle'][1],val['end_middle'][0],val['end_middle'][1], avg_time, val['agg_len'], time_list))

    print cnt
    insert_new_track_time(records, table_name=config['new_track_time_table_name'])
    print('time < 0 cnt : {}'.format(elapse_small_0_cnt))
    print('elapse {}'.format(time.time() - begin))


if __name__ == '__main__':
    main()
