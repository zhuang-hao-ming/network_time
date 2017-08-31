# -*- encoding: utf-8 -*-
from query import get_all_new_track_time
from insert import insert_lines

#
# 获得新轨迹-时间
#
def get_new_tracks(table_name):
    new_tracks = {}
    for row in get_all_new_track_time(table_name=table_name):
        id, line_ids, begin_middle, begin_ratio, end_middle, end_ratio, avg_time, agg_len, time_list = row
        key = tuple(line_ids)
        new_tracks[key] = {
            'avg_time': avg_time,
            'begin_middle': begin_middle,
            'begin_ratio': begin_ratio,
            'end_middle': end_middle,
            'end_ratio': end_ratio,
            'agg_len': agg_len
        }

    return new_tracks

def main():
    new_tracks = get_new_tracks(table_name='new_track_time_7_10_2')
    lines = []
    for key, val in new_tracks.items():
        end_ratio = val['end_ratio']
        end_ratio_id = int(end_ratio * 10000)
        begin_ratio = val['begin_ratio']
        begin_ratio_id = int(begin_ratio * 10000)
        begin_node = int(str(val['begin_middle']) + str(begin_ratio_id))
        end_node = int(str(val['end_middle']) + str(end_ratio_id))
        avg_time = val['avg_time']
        agg_len = val['agg_len']
        reverse_length = -1

        lines.append((begin_node, end_node, avg_time, agg_len, reverse_length))
        print begin_node, end_node, avg_time, agg_len, reverse_length
    insert_lines(lines, table_name='new_network_7_10_2')
if __name__ == '__main__':
    main()