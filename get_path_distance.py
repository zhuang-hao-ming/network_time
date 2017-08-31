# -*- encoding: utf-8

MAX_PATH_DIS = 99999999


# 判断向量(x1,y1)和（x2,y2）是否共向
def is_same_direction(x1, y1, x2, y2):
    if x1*x2 + y1*y2 > 0:
        return True
    else:
        return False


#
# 获得两个closest point间的网络距离
# @param {{tuple}} pre_closest_point 前一个最近点
# @param {{tuple}} now_closest_point 当前的最近点
# @param {{dict}} dis_dict 网络距离字典
# @return {{float}} 网络距离
#
def get_path_distance(pre_closest_point, now_closest_point, dis_dict):
    pre_log_x, pre_log_y, pre_p_x, pre_p_y, pre_line_id, pre_gps_log_id, pre_source, pre_target, pre_length, pre_flo, pre_fraction, pre_v, pre_time, pre_line_x, pre_line_y = pre_closest_point
    now_log_x, now_log_y, now_p_x, now_p_y, now_line_id, now_gps_log_id, now_source, now_target, now_length, now_flo, now_fraction, now_v, now_time, now_line_x, now_line_y = now_closest_point

    if pre_line_id == now_line_id:  # 共线
        x1 = now_log_x - pre_log_x
        y1 = now_log_y - pre_log_y
        x2 = now_p_x - pre_p_x
        y2 = now_p_y - pre_p_y

        if is_same_direction(x1, y1, x2, y2) and (int(now_flo) == 0 or is_same_direction(x2, y2, pre_line_x, pre_line_y)):  # 共向
                path_dis = abs(pre_fraction - now_fraction) * pre_length
                return path_dis  # 返回道路上的距离

        else:  # 反向
            if int(now_flo) == 0:  # 道路双向
                return MAX_PATH_DIS  # 认为这是一种错误的情况， 返回一个大值
            else:  # 道路单向
                return MAX_PATH_DIS  # 认为这是一种错误的情况， 返回一个大值

    else:  # 不共线
        if int(pre_flo) == 0:  # pre双向
            if int(now_flo) == 0:  # now双向

                source_ids = [pre_source, pre_target]
                target_ids = [now_source, now_target]

                min_path_dis = MAX_PATH_DIS
                for id_x, key_x in enumerate(source_ids):
                    for id_y, key_y in enumerate(target_ids):
                        try:
                            routing_dis = dis_dict[key_x][key_y]
                        except Exception as err:
                            if key_x == key_y:
                                routing_dis = 0
                            else:
                                routing_dis = MAX_PATH_DIS

                        if id_x == 0:  # 从p1的source出发
                            routing_dis += pre_length * pre_fraction
                        elif id_x == 1:  # 从p1的target出发
                            routing_dis += pre_length * (1.0 - pre_fraction)

                        if id_y == 0:  # 到达p2的source
                            routing_dis += now_length * now_fraction
                        elif id_y == 1:  # 到达p2的target
                            routing_dis += now_length * (1.0 - now_fraction)

                        if routing_dis < min_path_dis:
                            min_path_dis = routing_dis

                return min_path_dis
            else:  # now单
                source_ids = [pre_source, pre_target]
                target_ids = [now_source]
                min_path_dis = MAX_PATH_DIS
                for id_x, key_x in enumerate(source_ids):
                    for key_y in target_ids:
                        try:
                            routing_dis = dis_dict[key_x][key_y]
                        except Exception as err:
                            if key_x == key_y:
                                routing_dis = 0
                            else:
                                routing_dis = MAX_PATH_DIS

                        if id_x == 0:  # 从p1的source出发
                            routing_dis += pre_length * pre_fraction
                        elif id_x == 1:  # 从p1的target出发
                            routing_dis += pre_length * (1.0 - pre_fraction)
                        routing_dis += now_length * now_fraction
                        if routing_dis < min_path_dis:
                            min_path_dis = routing_dis
                return min_path_dis
        else:  # pre单向
            if int(now_flo) == 0:  # now双向
                source_ids = [pre_target]
                target_ids = [now_source, now_target]
                min_path_dis = MAX_PATH_DIS
                for key_x in source_ids:
                    for id_y, key_y in enumerate(target_ids):
                        try:
                            routing_dis = dis_dict[key_x][key_y]
                        except Exception as err:
                            if key_x == key_y:
                                routing_dis = 0
                            else:
                                routing_dis = MAX_PATH_DIS

                        routing_dis += pre_length * (1.0 - pre_fraction)

                        if id_y == 0:  # 到达p2的source
                            routing_dis += now_length * now_fraction
                        elif id_y == 1:  # 到达p2的target
                            routing_dis += now_length * (1.0 - now_fraction)

                        if routing_dis < min_path_dis:
                            min_path_dis = routing_dis
                return min_path_dis
            else:  # now单向

                try:
                    routing_dis = dis_dict[pre_target][now_source]
                except Exception as err:
                    if pre_target == now_source:
                        routing_dis = 0
                    else:
                        routing_dis = MAX_PATH_DIS

                routing_dis += pre_length * (1.0 - pre_fraction)
                routing_dis += now_length * now_fraction
                return routing_dis
