

import arcpy
import os
from query import get_all_new_track_time

shenzhen_line_fc = r'D:\app\data\shp\shenzhen_line.shp'
out_fc = r'D:\app\data\shp\new_network.shp'


def tuple_to_dict(row):
    id, line_ids, begin_middle, begin_ratio, end_middle, end_ratio, avg_time = row
    return {
        'id': id,
        'line_ids': line_ids,
        'begin_middle': begin_middle,
        'begin_ratio': begin_ratio,
        'end_middle': end_middle,
        'end_ratio': end_ratio,
        'avg_time': avg_time,
    }

def get_new_tracks():
    return [tuple_to_dict(row) for row in get_all_new_track_time()]

def get_line_pnt_dict():


    line_pnt_dict = {}
    with arcpy.da.SearchCursor(shenzhen_line_fc, ['OBJECTID_1', 'SHAPE@']) as cursor:
        for row in cursor:

            points = []
            for part in row[1]:
                for pnt in part:
                    if pnt:
                        points.append((pnt.X,pnt.Y))
                    else:
                        print('interior ring')
            line_pnt_dict[row[0]] = points
    return line_pnt_dict


def main():
    line_pnt_dict = get_line_pnt_dict()


    arcpy.CreateFeatureclass_management(out_path=os.path.dirname(out_fc),
                                        out_name=os.path.basename(out_fc),
                                        geometry_type='POLYLINE',
                                        has_m='DISABLED',
                                        has_z='DISABLED',
                                        spatial_reference=shenzhen_line_fc)

    arcpy.AddField_management(in_table=out_fc, field_name='avg_time', field_type='DOUBLE')

    insert_cursor = arcpy.da.InsertCursor(out_fc, ['SHAPE@', 'avg_time'])
    new_tracks = get_new_tracks()
    for track in new_tracks:
        pnts = []
        for line_id in track['line_ids']:
            pnts.extend(line_pnt_dict[line_id])
        arcpy_pnts = [arcpy.Point(*pnt) for pnt in pnts]
        line = arcpy.Polyline(arcpy.Array(arcpy_pnts))
        insert_cursor.insertRow([line, track['avg_time']])
    del  insert_cursor
if __name__ == '__main__':
    main()