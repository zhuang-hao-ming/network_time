#
# 从原始gps log中构建轨迹的脚本。
# 但是直接用sql写会更快一些所以改用sql写。
#
#
#






import datetime
from query import get_logs
from insert import insert_track

def test_time_dis_constrain(pre_log, now_log,time_threshold = 120, speed_threshold = 33):
    
    
    datetime_pre = pre_log[0] # pre timestamp
    datetime_now = now_log[0] # now timestamp

    pre_x = pre_log[1] # pre x
    pre_y = pre_log[2] # pre y

    now_x = now_log[1] # now x
    now_y = now_log[2] # now y


    delta = abs((datetime_now - datetime_pre).total_seconds())
    if delta > time_threshold: # the delta of two log must within time_threshold(/s)
     #   print('time constrain')
        return False

    dis = (now_x - pre_x) ** 2 + (now_y - pre_y) ** 2 # the distance between two log must be compliant to speed_threshold
    #print('delata: {0}, dis: {1}'.format(delta, dis))
    if dis > (delta * speed_threshold) ** 2:
        #print('speed constrain')
        return False

    return True



def main():
    offset = 0
    limit = 100000
    if True:
        print('offset: {0}, limit: {1}'.format(offset, limit))
        logs = get_logs(limit=limit, offset=offset)    
        if len(logs) <= 0:
            pass

        tracks = []
        a_track = [logs[0][0]]
        pre_log = logs[0]

        for log in logs[1:]:
            

            if test_time_dis_constrain((pre_log[1], pre_log[4], pre_log[5]), (log[1], log[4], log[5])):
                a_track.append(log[0])
                pre_log = log
                continue
            else:
                print(a_track)
                tracks.append((a_track,))
                a_track = [log[0]]
                pre_log = log
        if len(a_track) > 0:
            print(a_track)
            tracks.append((a_track,))
        
        
        insert_track(tracks)
        offset += limit


if __name__ == '__main__':
    main()