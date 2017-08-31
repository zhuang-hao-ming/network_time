# -*- encoding: utf-8 -*-
# author: haoming
# 将出租车记录导入数据库
#

import datetime
import time
import os.path
from insert import insert_logs
from multiprocessing import Pool, cpu_count

config = {
    'gps_log_table_name': 'gps_log_5',
    'month': 5,
    'id_list': range(91,103+1) + range(121,133+1),
    'file_name_pattern': r'D:\app\data\{month:02d}\{day:02d}\2016{month:02d}{day:02d}_{id:03d}.TXT'
}

# 2016年5月的工作日
work_day_in_may = range(3, 6+1) + range(9, 13+1) + range(16, 20+1) + range(23, 27+1) + range(30, 31+1)
# 2016年6月的工作日
work_day_in_june = range(1, 3+1) + range(6, 8+1) + range(12, 17+1) + range(20, 24+1) + range(27, 30+1)
# 2016年7月的工作日
work_day_in_july = [1] + range(4, 8+1) + range(11, 15+1) + range(18, 22+1) + range(25, 29+1)
# 2016年8月的工作日
work_day_in_aug = range(1, 5+1) + range(8, 12+1) + range(15, 19+1) + range(22, 26+1) + range(29, 31+1)
work_day_dict = {
    5: work_day_in_may,
    6: work_day_in_june,
    7: work_day_in_july,
    8: work_day_in_aug
}


def insert_a_log_file_to_db(f):
    gps_logs_of_a_hour = [] 
    for line in f:
        try:
            items = line.strip().split(',')
            lon = float(items[4])
            lat = float(items[5])
            if lon < (108-0.5) or lon > (114 + 0.5) or lat < (0+0.5) or (lat > 84+0.5):
                # utm49n 的bound是 log: 108-114 lat: 0-84 ,考虑了误差，将范围扩大0.5
                continue
            log_time = datetime.datetime.strptime(items[0] + items[1].zfill(6), '%Y%m%d%H%M%S')
            unknown_1 = items[2]
            car_id = items[3][2:]

            velocity = int(float(items[6]))
            direction = int(float(items[7]))
            on_service = bool(int(items[8]))
            is_valid = bool(int(items[9]))
            if not is_valid or not on_service:
                continue
            a_gps_log = (log_time, unknown_1, car_id, velocity, direction, on_service, is_valid, lon, lat)
            gps_logs_of_a_hour.append(a_gps_log)
        except Exception as error:
            print('error: {0} is omit'.format(error))
    print('{0}'.format(len(gps_logs_of_a_hour)))
    insert_logs(gps_logs_of_a_hour, table_name=config['gps_log_table_name'])


def insert_worker(log_files):
    begin_tick = time.time()
    for log_file in log_files:
        with open(log_file) as f:
            insert_a_log_file_to_db(f)
        print('{0} s for insert {1}'.format(time.time() - begin_tick, log_file))


#
# @month {{int}} 月份
# @begin_day {{int}} 起始日期(包含)
# @end_day {{int}} 终止日期(包含)
# @begin_id {{int}} 起始id(包含)
# @end_id {{int}} 终止id(包含)
#
def main(month, id_list):
    # 收集所有要导入数据库的文件的文件名
    log_file_name_base = config['file_name_pattern']
    log_files = []
    for day in range(1, 32):
        if day not in work_day_dict[month]:
            continue
        for id in id_list:
            log_file_name = log_file_name_base.format(month=month, day=day, id=id)
            if os.path.isfile(log_file_name):
                log_files.append(log_file_name)
    # 并行导入
    lists_of_small_set = []  # 将轨迹等分为cpu_count组
    size_of_small_set = len(log_files) / cpu_count()  # 组的大小
    for i in range(0, len(log_files), size_of_small_set):  # 相邻的轨迹分在同一组，有利于数据库缓存命中
        lists_of_small_set.append(log_files[i:i+size_of_small_set])

    pool = Pool(processes=cpu_count())
    list_of_result = []
    for small_set in lists_of_small_set:
        x = pool.apply_async(insert_worker, (small_set,))
        list_of_result.append(x)
    for x in list_of_result:
        x.get()  # 这个写法，可以捕获进程池中的异常
    pool.close()
    pool.join()




if __name__ == '__main__':
    begin = time.time()
    main(config['month'], config['id_list'])
    print('{0} elapse'.format(time.time() - begin))