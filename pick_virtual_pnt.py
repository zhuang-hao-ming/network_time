# -*- encoding: utf-8 -*-
# 选择虚拟节点
# author: haoming

import arcpy
from collections import defaultdict
from query import get_distance_rows_1

MIN_DIS = 99999999
MAX_DIS = 99999999
shenzhen_line_fc = r'D:\app\data\shp\shenzhen_line.shp'
redpoint_fc = r'D:\app\data\shp\redpoint1.shp'
point_fc = r'D:\app\data\shp\shenzhen_point.shp'
general_dis = 80
association_dis = 25
del_dis = 250
output_shp = True
output_csv_file_name = 'red_point_class_13.csv'
new_pnt_shp_name =  r'D:\app\data\shp\new_pnt_dict14.shp'
red_pnt_shp_name = r'D:\app\data\shp\red_pnt_dict14.shp'


def get_del_keep_idx(key_list, node_line_dict):
    max_len = -1
    max_idx = -1
    for idx in range(0, len(key_list)):
        key = key_list[idx]
        lines = node_line_dict[key]
        length = 0
        for line in lines:
            length += line['length']
        if length > max_len:
            max_len = length
            max_idx = idx
    return max_idx

def get_pnt_dict():

    pnt_dict = {}
    with arcpy.da.SearchCursor(point_fc, ['ID', 'SHAPE@']) as cursor:
        for row in cursor:
            pnt_dict[int(row[0])] = row[1]

    return pnt_dict


#
# 读取原始红灯shp
#
def get_red_pnt_dict():
    main_node_id = 0
    red_pnt_dict = defaultdict(list)
    with arcpy.da.SearchCursor(redpoint_fc, ['ID', 'MainNodeID']) as cursor:
        for row in cursor:
            if int(row[1]) == 0:
                main_node_id += 1
                red_pnt_dict[main_node_id].append(int(row[0]))
                continue
            red_pnt_dict[int(row[1])].append(int(row[0]))
    return red_pnt_dict


def get_line_dict():

    line_dict = {}
    with arcpy.da.SearchCursor(shenzhen_line_fc, ['OBJECTID_1', 'StartNode', 'EndNode', 'TrafficFlo', 'Length']) as cursor:
        for row in cursor:
            line_dict[int(row[0])] = {
                'start_node': int(row[1]),
                'end_node': int(row[2]),
                'flo': int(row[3]),
                'length': float(row[4])
            }

    return line_dict


#
# 获得主要红灯列表和所有红灯列表
#
def get_old_pnt_list(red_pnt_dict):
    id_list_of_main_old_pnt = []  # 主要红灯id
    id_list_of_all_old_pnt = []  # 所有红灯id

    for key, val in red_pnt_dict.items():
        id_list_of_main_old_pnt.append(key)
        id_list_of_all_old_pnt.append(key)
        for sub_id in val:
            if sub_id not in id_list_of_all_old_pnt:
                id_list_of_all_old_pnt.append(sub_id)

    return id_list_of_main_old_pnt, id_list_of_all_old_pnt


#
# 获得候选虚拟点和候选其它点
#
def get_candidate_pnt_list(line_dict, id_list_of_all_old_pnt):

    end_node_line_dict = defaultdict(list)  # node_id: [line1, line2 ...] 汇流点
    start_node_line_dict = defaultdict(list)  # node_id: [line1, line2 ...] 分流点
    node_line_dict = defaultdict(list)

    virtual_node_list = []  # 候选虚拟
    other_node_list = []  # 候选其它节点

    for key, val in line_dict.items():
        # 不考虑双向道路， 在这个路网中，双向路没有设置红绿灯的意义
        # flo = val['flo']
        # length = val['length']
        # if flo == 0 and length < 20:
        #     continue

        end_node = int(val['end_node'])
        start_node = int(val['start_node'])
        val['id'] = key

        node_line_dict[end_node].append(val)
        node_line_dict[start_node].append(val)

        end_node_line_dict[end_node].append(val)
        start_node_line_dict[start_node].append(val)

    # 候选虚拟点
    for key, val in end_node_line_dict.items():
        if len(val) >= 2 and key not in id_list_of_all_old_pnt:
            virtual_node_list.append(key)

    # 候选交点
    for key, val in start_node_line_dict.items():
        if len(val) >= 2 and key not in virtual_node_list and key not in id_list_of_all_old_pnt:
            other_node_list.append(key)

    return virtual_node_list, other_node_list, node_line_dict


def main():
    pnt_dict = get_pnt_dict()  ## 可视化输出
    red_pnt_dict = get_red_pnt_dict()  ## main_key: [main_key, sub_key_1, sub_key_2 ...]
    line_dict = get_line_dict()  ## line_id: {start_node: , end_node: }

    # 主要红灯id列表, 所有红灯id列表
    id_list_of_main_old_pnt, id_list_of_all_old_pnt = get_old_pnt_list(red_pnt_dict)
    virtual_node_list, other_node_list, node_line_dict = get_candidate_pnt_list(line_dict, id_list_of_all_old_pnt)

    print('1. len of virtual: {}'.format(len(virtual_node_list)))
    print('1. len of other: {}'.format(len(other_node_list)))

    # 与真实交通信号灯的泛化

    id_list_of_virtual_node = virtual_node_list[:]
    id_list_of_other_node = other_node_list[:]

    id_list_of_old_pnt = id_list_of_main_old_pnt
    id_list_of_new_pnt = virtual_node_list + other_node_list

    dis_rows_old_to_new = get_distance_rows_1(tuple(id_list_of_old_pnt), tuple(id_list_of_new_pnt))
    dis_dict_old_to_new = {}  # 字典， dict[start_vid][end_vid]
    for row in dis_rows_old_to_new:
        if dis_dict_old_to_new.get(row[0]) is None:
            dis_dict_old_to_new[row[0]] = {}
            dis_dict_old_to_new[row[0]][row[1]] = row[2]

    dis_rows_new_to_old = get_distance_rows_1(tuple(id_list_of_new_pnt), tuple(id_list_of_old_pnt))
    dis_dict_new_to_old = {}  # 字典， dict[start_vid][end_vid]
    for row in dis_rows_new_to_old:
        if dis_dict_new_to_old.get(row[0]) is None:
            dis_dict_new_to_old[row[0]] = {}
            dis_dict_new_to_old[row[0]][row[1]] = row[2]

    cnt_of_general = 0

    for virtual_key in virtual_node_list:
        min_dis = MIN_DIS
        min_old_key = -1
        for old_key in red_pnt_dict.keys():
            try:
                dis1 = dis_dict_old_to_new[old_key][virtual_key]
            except Exception as err:
                dis1 = MAX_DIS
            try:
                dis2 = dis_dict_new_to_old[virtual_key][old_key]
            except Exception as err:
                dis2 = MAX_DIS

            dis = min(dis1, dis2)

            if dis < min_dis:
                min_dis = dis
                min_old_key = old_key
        if min_dis < general_dis:
            # 泛化
            if virtual_key not in id_list_of_all_old_pnt:
                red_pnt_dict[min_old_key].append(virtual_key)
                cnt_of_general += 1

            id_list_of_virtual_node.remove(virtual_key)

    for other_key in other_node_list:
        min_dis = MIN_DIS
        min_old_key = -1

        for old_key in red_pnt_dict.keys():
            try:
                dis1 = dis_dict_old_to_new[old_key][other_key]
            except Exception as err:
                dis1 = MAX_DIS
            try:
                dis2 = dis_dict_new_to_old[other_key][old_key]
            except Exception as err:
                dis2 = MAX_DIS

            dis = min(dis1, dis2)
            if dis < min_dis:
                min_dis = dis
                min_old_key = old_key
        if min_dis < general_dis:
            # 泛化
            if other_key not in id_list_of_all_old_pnt:
                red_pnt_dict[min_old_key].append(other_key)
                cnt_of_general += 1
            id_list_of_other_node.remove(other_key)

    virtual_node_list = id_list_of_virtual_node[:]
    other_node_list = id_list_of_other_node[:]

    print('general cnt: {}'.format(cnt_of_general))
    print('2. len of virtual: {}'.format(len(virtual_node_list)))
    print('2. len of other: {}'.format(len(other_node_list)))

    # 2. 与真实交通信号灯的删除操作

    del_cnt = 0
    for virtual_key in id_list_of_virtual_node:
        for old_key in id_list_of_main_old_pnt:
            try:
                dis1 = dis_dict_old_to_new[old_key][virtual_key]
            except Exception as err:
                dis1 = MAX_DIS
            try:
                dis2 = dis_dict_new_to_old[virtual_key][old_key]
            except Exception as err:
                dis2 = MAX_DIS
            dis = min(dis1, dis2)
            if dis < del_dis:
                del_cnt+=1
                virtual_node_list.remove(virtual_key)
                break

    print('del cnt: {}'.format(del_cnt))
    print('3. len of virtual: {}'.format(len(virtual_node_list)))
    print('3. len of other: {}'.format(len(other_node_list)))


    # 3. 虚拟交通信号灯的关联
    new_pnt_groups = [] # 距离50米内分组
    dis_rows = get_distance_rows_1(tuple(virtual_node_list), tuple(virtual_node_list))
    dis_dict = {}  # 字典， dict[start_vid][end_vid]

    for row in dis_rows:

        if dis_dict.get(row[0]) is None:
            dis_dict[row[0]] = {}
        dis_dict[row[0]][row[1]] = row[2]

        if row[2] < association_dis:

            in_group = False
            for group in new_pnt_groups:
                if row[0] in group or row[1] in group:
                    group.append(row[0])
                    group.append(row[1])
                    in_group = True
                    break
            if not in_group:
                new_pnt_groups.append([row[0], row[1]])

    new_pnt_dict = {}  # 虚拟红灯字典: 以main_id为键,[main_id,sub_id]为值

    for group in new_pnt_groups:  # 组与组之间可能会重复， 组内可能会重复
        group = list(set(group))  # 组内去重

        main_key_idx = -1
        main_key = -1
        for idx in range(0, len(group)):  # 找到组内第一个还未被使用的key作为main_key
            key = group[idx]
            if key in virtual_node_list:
                main_key_idx = idx
                main_key = key
                virtual_node_list.remove(key)
                break

        if main_key == -1:  # 当前组的id在别的组已经重复过
            continue

        new_pnt_dict[main_key] = [main_key]  # 新建组
        print main_key
        for idx in range(main_key_idx+1, len(group)):  # 遍历组内剩余的id
            key = group[idx]
            if key in virtual_node_list:  # 如果id未被使用,加入组
                    new_pnt_dict[main_key].append(key)
                    virtual_node_list.remove(key)

    print('len of new_pnt_dict: {}'.format(len(new_pnt_dict.items())))
    print('4. len of virtual: {}'.format(len(virtual_node_list)))
    print('4. len of other: {}'.format(len(other_node_list)))




    pre_del_keys = virtual_node_list + new_pnt_dict.keys()

    # 4. 虚拟交通信号灯的删除
    del_cnt = 0
    del_key_pair = defaultdict(list)
    for key_1 in pre_del_keys:
        for key_2 in pre_del_keys:
            try:
                dis1 = dis_dict[key_1][key_2]
            except Exception as err:
                dis1 = MAX_DIS
            try:
                dis2 = dis_dict[key_2][key_1]
            except Exception as err:
                dis2 = MAX_DIS
            dis = min(dis1, dis2)
            if dis < 100:
                del_key_pair[key_1].append(key_2)

    has_del_key = []
    for key, val in del_key_pair.items():
        if key in has_del_key:
            continue
        key_list = [key] + val

        main_key_idx_list = []
        for idx in range(0, len(key_list)):
            key_1 = key_list[idx]
            if key_1 in new_pnt_dict:
                main_key_idx_list.append(idx)

        # keep_idx = -1
        # if len(main_key_idx_list) > 0:
        #     keep_idx = main_key_idx_list[0]
        #     main_key_idx_list.remove(keep_idx)
        # else:
        #     keep_idx = 0

        keep_idx = get_del_keep_idx(key_list, node_line_dict)
        print keep_idx


        for idx in range(0, len(key_list)):
            if idx == keep_idx:
                continue
            key_1 = key_list[idx]
            if key_1 in main_key_idx_list:
                val = new_pnt_dict.pop(key_1, None)
                has_del_key.append(key_1)
                if val is not None:
                    del_cnt += len(val)
            else:
                if key_1 in virtual_node_list:
                    virtual_node_list.remove(key_1)
                    has_del_key.append(key_1)
                    del_cnt+=1

    print('del cnt: {}'.format(del_cnt))
    print('5. len of virtual: {}'.format(len(virtual_node_list)))
    print('5. len of other: {}'.format(len(other_node_list)))



    # 虚拟交通信号灯集建立
    for key in virtual_node_list:
        if key in new_pnt_dict:
            print 'error!'
        new_pnt_dict[key] = [key]
    print('len of virtual node {}'.format(len(new_pnt_dict.keys())))

    for key, val in new_pnt_dict.items():
        id_list_of_all_old_pnt.append(key)
        for key_1 in val:
            if key_1 not in id_list_of_all_old_pnt:
                id_list_of_all_old_pnt.append(key_1)






    # 5. 虚拟交通信号灯的泛化
    cnt_of_general = 0

    dis_rows_old_to_new = get_distance_rows_1(tuple(new_pnt_dict.keys()), tuple(other_node_list))
    dis_dict_old_to_new = {}  # 字典， dict[start_vid][end_vid]
    for row in dis_rows_old_to_new:

        if dis_dict_old_to_new.get(row[0]) is None:
            dis_dict_old_to_new[row[0]] = {}
            dis_dict_old_to_new[row[0]][row[1]] = row[2]

    dis_rows_new_to_old = get_distance_rows_1(tuple(other_node_list), tuple(new_pnt_dict.keys()))
    dis_dict_new_to_old = {}  # 字典， dict[start_vid][end_vid]
    for row in dis_rows_new_to_old:

        if dis_dict_new_to_old.get(row[0]) is None:
            dis_dict_new_to_old[row[0]] = {}
            dis_dict_new_to_old[row[0]][row[1]] = row[2]

    for other_key in other_node_list:
        min_dis = MIN_DIS
        min_old_key = -1
        for old_key in new_pnt_dict.keys():
            try:
                dis1 = dis_dict_old_to_new[old_key][other_key]
            except Exception as err:
                dis1 = MAX_DIS
            try:
                dis2 = dis_dict_new_to_old[other_key][old_key]
            except Exception as err:
                dis2 = MAX_DIS
            dis = min(dis1, dis2)
            if dis < min_dis:
                min_dis = dis
                min_old_key = old_key
        if min_dis < general_dis:
            # 泛化
            if other_key not in id_list_of_all_old_pnt:
                new_pnt_dict[min_old_key].append(other_key)
                cnt_of_general += 1

    print('general cnt: {}'.format(cnt_of_general))
    print('6. len of virtual: {}'.format(len(virtual_node_list)))
    print('6. len of other: {}'.format(len(other_node_list)))

    import csv
    csv_file = open(output_csv_file_name, 'wb')
    csv_writer = csv.writer(csv_file, delimiter=',')


    idx = 0
    for key, val in red_pnt_dict.items():
        idx += 1
        csv_writer.writerow([idx] + val)
    for key, val in new_pnt_dict.items():
        idx += 1
        csv_writer.writerow([idx] + val)


    csv_file.close()

    if output_shp:
        import os
        new_pnt_fc = new_pnt_shp_name
        arcpy.CreateFeatureclass_management(out_path=os.path.dirname(new_pnt_fc),
                                            out_name=os.path.basename(new_pnt_fc),
                                            geometry_type='POINT',
                                            has_m='DISABLED',
                                            has_z='DISABLED',
                                            spatial_reference=point_fc)

        arcpy.AddField_management(in_table=new_pnt_fc, field_name='main_id', field_type='INTEGER')
        arcpy.AddField_management(in_table=new_pnt_fc, field_name='sub_id', field_type='INTEGER')
        new_pnt_insert_cursor = arcpy.da.InsertCursor(new_pnt_fc, ['SHAPE@', 'main_id', 'sub_id'])

        red_pnt_fc = red_pnt_shp_name
        arcpy.CreateFeatureclass_management(out_path=os.path.dirname(red_pnt_fc),
                                            out_name=os.path.basename(red_pnt_fc),
                                            geometry_type='POINT',
                                            has_m='DISABLED',
                                            has_z='DISABLED',
                                            spatial_reference=point_fc)

        arcpy.AddField_management(in_table=red_pnt_fc, field_name='main_id', field_type='INTEGER')
        arcpy.AddField_management(in_table=red_pnt_fc, field_name='sub_id', field_type='INTEGER')
        red_pnt_insert_cursor = arcpy.da.InsertCursor(red_pnt_fc, ['SHAPE@', 'main_id', 'sub_id'])

        for key, val in red_pnt_dict.items():
            for sub_key in val:
                red_pnt_insert_cursor.insertRow([pnt_dict[sub_key], key, sub_key])

        for key, val in new_pnt_dict.items():
            for sub_key in val:
                new_pnt_insert_cursor.insertRow([pnt_dict[sub_key], key, sub_key])
                break

        del red_pnt_insert_cursor
        del new_pnt_insert_cursor



if __name__ == '__main__':
    main()