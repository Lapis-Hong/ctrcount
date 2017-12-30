#!/usr/bin/env python
#coding=utf8
from __future__ import division
import sys
from operator import itemgetter
from itertools import groupby
from collections import defaultdict, Counter


def read_mapper_output(file):
    for line in file:
        yield line.rstrip().split('\t', 1)
    

def field_count():
    """统计某字段取值个数，其分布"""
    cnt_dic = {}
    data = read_mapper_output(sys.stdin)
    for line in data:
        field = line[0]
        cnt_dic[field] = cnt_dic.get(field, 0) + 1
    for field, cnt in cnt_dic.items():
	    print('%s\t%s' % (fields, cnt))


def costType():
    """统计某字段某取值占比，其分布"""
    cpm_count = 0
    total_count = 0
    data = read_mapper_output(sys.stdin)
    for line in data:
        line = line[0]
        total_count += 1
        try:
            cost_type = int(line)
        except ValueError:  
            continue
        if cost_type == 0:
            cpm_count += 1
    if total_count == 0:
        cpm_ratio = 0
    else:
        cpm_ratio = cpm_count / total_count 
    print('%s\t%s' % (cpm_count, cpm_ratio))


def location_ratio():
    """统计location的比例，以及location总数的分布"""
    data = read_mapper_output(sys.stdin)
    id_count = 0
    loc_count_dic = defaultdict(int)
    id_loc_count_dic = defaultdict(int)
    for cur_id, group in groupby(data, itemgetter(0)):
        id_count += 1
        id_loc_count = 0
        #cur_loc = ''
        #for _, loc in group:
            #if loc != cur_loc:  # distinct by (rid, loc) pair
               #id_loc_count += 1  # for each rid, uniq location number +1 
               #loc_count_dic[loc] += 1  # each location type +1 
            #cur_loc = loc
        #id_loc_count_dic[id_loc_count] += 1  
        loc_set = set(loc for _, loc in group) # distinct rid, loc
        for loc in loc_set:
            loc_count_dic[loc] += 1
        id_loc_count_dic[len(loc_set)] += 1

    print('Each location percentage:')
    result_1 = sorted(loc_count_dic.items(), key=lambda d: d[0])
    for loc, cnt in result_1:
        print('%s\t%.2f%%' % (loc, cnt/id_count*100))

    print('\nEach requestId number of locations distribution:') 
    result_2 = sorted(id_loc_count_dic.items(), key=lambda d: d[0])
    for loc_cnt, cnt in result_2:
        print('%s\t%.2f%%' %(loc_cnt, cnt/id_count*100))


def rids_per_uid_dist():
    """统计每个uid请求数的分布情况"""
    rids_per_u_dic = defaultdict(int)  
    data = read_mapper_output(sys.stdin)
    cur_u = ''
    u_cnt = 0
    rid_cnt = 0
    for line in data:
        u, rid, _ = line[0].split(',')
        if u == cur_u: # second line of a new uid
            if rid != cur_rid:
                rid_cnt += 1 
                cur_rid = rid 
        else:  # start of a new uid
            u_cnt += 1
            cur_u, cur_rid = u, rid
            if rid_cnt: # first else no rid_cnt
                rids_per_u_dic[rid_cnt] += 1
            rid_cnt = 1
    rids_per_u_dic[rid_cnt] += 1  # last uid rid_cnt
        
    print('\nEach uid number of requestId distribution:')
    result = sorted(rids_per_u_dic.items(), key=lambda d: d[0])
    for rid_cnt, cnt in result:
        print('%d\t%d\t%.4f%%' %(rid_cnt, cnt, cnt/u_cnt*100 ))


def loc_dist_under_rids():
    """在每个uid rid总次数条件下，统计location总数的分布"""
    data = read_mapper_output(sys.stdin)
    cur_u, cur_rid, cur_loc = data.next()[0].split(',')
    rid_cnt = 1
    loc_cnt = 1
    rid_loc_dic = defaultdict(Counter)  # key是每个uid的rid次数，value是字典，location总数的计数
    loc_dic = defaultdict(int)  # each rid total location count dict
    for line in data:
        u, rid, loc = line[0].split(',')
        if u == cur_u:
            if rid == cur_rid:
                if cur_loc != loc:
                    loc_cnt += 1  # 按(rid, loc)去重后location计数
                    cur_loc = loc
            else:  
                rid_cnt += 1
                loc_dic[loc_cnt] += 1  # each rid location总数计数
                cur_rid, cur_loc = rid, loc
                loc_cnt = 1
        else:
            loc_dic[loc_cnt] += 1
            cur_u, cur_rid, cur_loc = u, rid, loc
            rid_loc_dic[rid_cnt].update(Counter(loc_dic))  # add dict value 
            loc_dic.clear()
            rid_cnt = 1
            loc_cnt = 1 
            

    rid_loc_dic[rid_cnt].update(Counter(loc_dic))
    print('rids_per_u\t 1 2 3 4 5 6 7 8 9')
    for rid_cnt, counter in rid_loc_dic.items():
        counter = sorted(counter.items(), key=lambda d: int(d[0]))
        total = sum([w[1] for w in counter])
        loc_dist = [str(round(w[1]/total, 4)) for w in counter]
        print('%d\t%s' %(rid_cnt,  ' '.join(loc_dist)))

            
def rid_loc_stat_per_uid():
    """统计每一个uid下 requestId请求数，每个请求的location数量列表，平均location数量"""
    print('uid\ttotal request number\teach request location number list\taverage location number')
    data = read_mapper_output(sys.stdin)
    first_line_flag = True
    total_loc_list = []
    total_rid = 1
    total_loc = 1
    for line in data:
        u, rid, loc = line[0].split(',')
        if first_line_flag:
            cur_u, cur_rid, cur_loc = u, rid, loc
            first_line_flag = False
        if u == cur_u:
            if rid == cur_rid:
                if loc != cur_loc:  # distinct loc
                    total_loc += 1
            else:
                total_rid += 1
                cur_rid, cur_loc = rid, loc
                total_loc_list.append(total_loc)
                total_loc = 1
                
        else:  # start of a new uid
            total_loc_list.append(total_loc)
            assert total_rid == len(total_loc_list)
            if total_rid < 100: # filter by each uid total request count
                print('%s\t%s\t%s\t%s' % (cur_u, total_rid, total_loc_list, sum(total_loc_list)/total_rid))
            cur_u, cur_rid, cur_loc = u, rid, loc
            total_loc_list = []
            total_rid = 1
            total_loc = 1
    # print last line info
    total_loc_list.append(total_loc)
    print('%s\t%s\t%s\t%s' % (cur_u, total_rid, total_loc_list, sum(total_loc_list)/total_rid))


def deep_user(min_rids=5, min_avg_locs=3.5):
    """找出上拉习惯的用户"""
    data = read_mapper_output(sys.stdin)
    cur_uid, cur_rid, cur_loc = data.next()[0].split(',')
    rid_cnt = 1
    loc_cnt = 1
    loc_cnt_tot = 0
    for line in data:
        uid, rid, loc = line[0].split(',')
        if uid == cur_uid:
            if rid == cur_rid:
                if loc != cur_loc:  # total location count per rid
                    loc_cnt += 1
                    cur_loc = loc
            else:
                rid_cnt += 1
                loc_cnt_tot += loc_cnt
                cur_rid, cur_loc = rid, loc
                loc_cnt = 1
        else:
            loc_cnt_tot += loc_cnt # add last request location count
            loc_cnt_avg = loc_cnt_tot / rid_cnt
            if rid_cnt >= min_rids and loc_cnt_avg >= min_avg_locs:
                print('%s\t%s' %(uid, loc_cnt_avg)) 
            cur_uid, cur_rid, cur_loc = uid, rid, loc
            rid_cnt = 1
            loc_cnt = 1
            loc_cnt_tot = 0
    if rid_cnt > min_rids and loc_cnt_avg > min_avg_locs:
        print('%s\t%s' %(uid, loc_cnt_avg))

if __name__ == "__main__":
    # field_count()
	# costType()
    # location_ratio()
    #rid_loc_stat_per_uid()
    # loc_dist_under_rids()
    deep_user()
