# -*- encoding: utf-8

from calculate_time import get_pnt_time, track_tuple_to_dict, point_tuple_to_dict
from query import get_construct_tracks, get_all_new_track_time, get_track_ids, get_line_nodes, get_all_red_tracks, get_all_match_pnts, get_new_path_dis
import csv
from collections import defaultdict
config = {
    'match_track_table_name': 'match_track_5_10',
    'construct_tracks_table_name': 'construct_tracks_5_10',
    'line_table_name': 'shenzhen_line1',
    'red_point_track_table_name': 'red_point_track_5_10_1',
    'new_track_time_table_name': 'new_track_time_56_10_3',
    'output_csv_name': 'validate_result_5_10_3.csv'
}

#
# 获得线段长度
#
def get_line_id_length_dict():
    line_id_length_dict = {}
    for id, _1, _2, length in get_line_nodes(table_name=config['line_table_name']):
        line_id_length_dict[int(id)] = length
    return line_id_length_dict

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
# 获得轨阶段-线段字典
#
def get_track_seg_id_lines_dict():
    track_seg_id_lines_dict = {}
    for row in get_construct_tracks(config['construct_tracks_table_name']):
        track_seg_id, track_id, line_ids = row
        track_seg_id_lines_dict[track_seg_id] = line_ids
    return track_seg_id_lines_dict

#
# 获得新轨迹-时间
#
def get_new_tracks():
    new_tracks = {}
    for row in get_all_new_track_time(table_name=config['new_track_time_table_name']):
        id, line_ids, begin_middle, begin_ratio, end_middle, end_ratio, avg_time, std_time, _2 = row
        key = tuple(line_ids)
        new_tracks[key] = {
            'avg_time': avg_time,
            'std_time': std_time
        }

    return new_tracks


def main():
    csv_file = open(config['output_csv_name'], 'wb')
    csv_writer = csv.writer(csv_file, delimiter=',')

    track_id_red_tracks = get_track_id_red_tracks() # 轨迹 - 还原轨迹 - 红灯轨迹
    track_id_match_pnts = get_track_id_match_pnts() # 轨迹 - 匹配点
    track_seg_id_lines_dict = get_track_seg_id_lines_dict()  # 轨迹和组成轨迹的线段的字典
    line_id_length_dict = get_line_id_length_dict()  # 线段和线段长度的字典
    track_ids = [int(row[0]) for row in get_track_ids(table_name=config['match_track_table_name'])]  # 读取所有轨迹id
    new_tracks = get_new_tracks()  # 新的轨迹

    cnt = 0





    for cnt_idx, track_id in enumerate(track_ids):

        #print('{} {}'.format(cnt_idx, track_id))

        begin_node = -1
        end_node = -1

        match_pnts = track_id_match_pnts[track_id]  # 当前轨迹的点
        red_tracks_dict = track_id_red_tracks[track_id]  # 还原轨迹-红灯轨迹列表
        if len(red_tracks_dict.items()) > 1:
            continue  # 只看没有分段的
        for track_seg_id, red_tracks in red_tracks_dict.items(): # 遍历 还原轨迹 - 红灯轨迹

            if len(red_tracks) < 2:  # 一条轨迹上至少要有两个红灯轨迹
                continue

            track_begin = red_tracks[0]
            track_end = red_tracks[-1]

            begin_node = int(str(track_begin['middle_id']) + str(int(track_begin['ratio'] * 10000)))
            end_node = int(str(track_end['middle_id']) + str(int(track_end['ratio'] * 10000)))

            try:
                begin_time = get_pnt_time(track_seg_id, track_begin, match_pnts, line_id_length_dict, track_seg_id_lines_dict)
                end_time = get_pnt_time(track_seg_id, track_end, match_pnts, line_id_length_dict, track_seg_id_lines_dict)
            except Exception as err:
                print err
                continue
            if begin_time is None or end_time is None:  # 计算起止时间失败
                # print 'none'
                continue
            real_elapse = (end_time - begin_time).total_seconds()



            expect_elapse = 0

            expect_elapse_60_b = 0
            expect_elapse_60_t = 0
            expect_elapse_70_b = 0
            expect_elapse_70_t = 0
            expect_elapse_80_b = 0
            expect_elapse_80_t = 0
            expect_elapse_90_b = 0
            expect_elapse_90_t = 0

            expect_success = True
            for i in range(1, len(red_tracks)):
                track_1 = red_tracks[i - 1]
                track_2 = red_tracks[i]
                key = tuple(track_1['line_ids'] + track_2['line_ids'])
                avg_time = new_tracks.get(key)

                if avg_time is None:
                    expect_success = False
                    break

                mean = new_tracks.get(key)['avg_time']
                std = new_tracks.get(key)['std_time']

                expect_elapse += mean
                expect_elapse_60_b += (mean - 0.8416 * std)
                expect_elapse_60_t += (mean + 0.8416 * std)
                expect_elapse_70_b += (mean - 1.0364 * std)
                expect_elapse_70_t += (mean + 1.0364 * std)
                expect_elapse_80_b += (mean - 1.2816 * std)
                expect_elapse_80_t += (mean + 1.2816 * std)
                expect_elapse_90_b += (mean - 1.6449 * std)
                expect_elapse_90_t += (mean + 1.6449 * std)

            if real_elapse < 100 or expect_elapse < 100:
                 continue
            if expect_success:
                # id_file.write('{},{}\n'.format(cnt, track_id))
                shortest_dis = 0#get_new_path_dis(begin_node, end_node, config['new_network_table_name'])[0][0]
                print('{} expect_elapse: {} , real_elapse: {} shortest dis {}'.format(cnt, expect_elapse, real_elapse, shortest_dis))
                cnt+=1
                csv_writer.writerow([expect_elapse, real_elapse, expect_elapse_60_b, expect_elapse_60_t,expect_elapse_70_b, expect_elapse_70_t,expect_elapse_80_b, expect_elapse_80_t,expect_elapse_90_b, expect_elapse_90_t])
    csv_file.close()
    # id_file.close()
if __name__ == '__main__':
    main()
