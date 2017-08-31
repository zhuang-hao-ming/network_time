# -*- encoding: utf-8 -*-
from query import get_all_new_track_time_old
import csv
import numpy as np
config = {
    'match_track_table_name': 'match_track_6_10',
    'construct_tracks_table_name': 'construct_tracks_6_10',
    'line_table_name': 'shenzhen_line1',
    'red_point_track_table_name': 'red_point_track_6_10_1',
    'new_track_time_table_name': 'new_track_time_6_10',
    'output_csv_name': 'analyze_56_10_3.csv'
}

#
# 获得新轨迹-时间
#
def get_new_tracks(table_name):
    new_tracks = {}
    for row in get_all_new_track_time_old(table_name=table_name):
        id, line_ids, begin_middle, begin_ratio, end_middle, end_ratio, avg_time, time_list = row
        key = tuple(line_ids)
        new_tracks[key] = {
            'avg_time': avg_time,
            'time_list': time_list
        }

    return new_tracks



def main():

    csv_file = open(config['output_csv_name'], 'wb')
    csv_writer = csv.writer(csv_file, delimiter=',')


    new_track_time = get_new_tracks('new_track_time_56_10_3')
    cnt = 0
    for key in new_track_time.keys():

        track = new_track_time[key]

        time_list = track['time_list']



        time_list = np.array(time_list)
        one_fourth = np.percentile(time_list, 25)
        two_fourth = np.percentile(time_list, 50)
        three_forth =np.percentile(time_list, 75)

        dis1 = (three_forth - one_fourth) * 2
        dis2 = (three_forth - one_fourth) * 1.5

        top_limit = two_fourth + dis1
        bottom_limit = two_fourth - dis2


        median_mask = np.logical_and(time_list<top_limit, time_list>bottom_limit)
        valid_time_list = time_list[median_mask]

        mean = np.mean(valid_time_list)
        std_dev = np.std(valid_time_list,ddof=1)

        csv_writer.writerow([mean, std_dev])



        cnt += 1
        print('{} {} {}'.format(cnt, mean, std_dev))


    csv_file.close()



if __name__ == '__main__':
    main()