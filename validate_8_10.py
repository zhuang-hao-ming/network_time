# -*- encoding: utf-8 -*-
from query import get_all_new_track_time
import csv
import numpy as np
config = {
    'match_track_table_name': 'match_track_5_10',
    'construct_tracks_table_name': 'construct_tracks_5_10',
    'line_table_name': 'shenzhen_line1',
    'red_point_track_table_name': 'red_point_track_6_10_1',
    'new_track_time_table_name': 'new_track_time_6_10',
    'output_csv_name': 'aug_8_vs_10_1.csv'
}

#
# 获得新轨迹-时间
#
def get_new_tracks(table_name):
    new_tracks = {}
    for row in get_all_new_track_time(table_name=table_name):
        id, line_ids, begin_middle, begin_ratio, end_middle, end_ratio, avg_time, _1, time_list = row
        key = tuple(line_ids)
        new_tracks[key] = {
            'avg_time': avg_time,
            'time_list': time_list
        }

    return new_tracks



def main():
    csv_file = open(config['output_csv_name'], 'wb')
    csv_writer = csv.writer(csv_file, delimiter=',')

    new_track_time_10 = get_new_tracks('new_track_time_8_10_1')
    new_track_time_8 = get_new_tracks('new_track_time_8_8_1')
    cnt = 0
    for key in new_track_time_8.keys():
        if key in new_track_time_10:
            track_8 = new_track_time_8[key]
            track_10 = new_track_time_10[key]

            avg_time_8 = track_8['avg_time']
            avg_time_10 = track_10['avg_time']

            time_list_8 = track_8['time_list']
            time_list_10 = track_10['time_list']

            std_8 = np.std(time_list_8)
            std_10 = np.std(time_list_10)

            csv_writer.writerow([avg_time_8, std_8, avg_time_10, std_10])
            cnt += 1
            print('{} 8点： {} 10点 {}'.format(cnt, avg_time_8, avg_time_10))


    csv_file.close()



if __name__ == '__main__':
    main()