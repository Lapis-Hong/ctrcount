import sys
import numpy as nip
from numpy.linalg import norm
from scipy.stats import pearsonr, spearmanr


def cosine(x, y):
    x_arr, y_arr = np.array(x), np.array(y)
    return np.dot(x_arr, y_arr) / float(norm(x)*norm(y))


def pull_down_up_analysis(rid_min_cnt=1, rid_max_cnt=50):
    first_line_flag = True
    rids_vec = []
    locs_vec = []
    # for line in open('stat.sorted'):
    for line in sys.stdin:
        line = line.strip().split('\t')
        rids, locs = int(line[1]), float(line[3])
        if rids > rid_max_cnt or rids < rid_min_cnt:
            continue
        rids_vec.append(rids)
        locs_vec.append(locs)
        if first_line_flag:
            cur_rids = rids
            cnt = 0
            sum_locs = 0
            first_line_flag = False
            print('requestId_cnt \t avg_location_cnt')
        if rids == cur_rids:
            cnt += 1
            sum_locs += locs
        else:
            print('%s\t%s' % (cur_rids, sum_locs / cnt))
            cur_rids = rids
            cnt = 1
            sum_locs = locs
    print('%s\t%s' % (cur_rids, sum_locs / cnt))

    # print('Cosine:\n %s' % cosine(rids_vec, locs_vec))
    # print('Pearson corrcoef:\n %s' % np.corrcoef(rids_vec, locs_vec)[0][1])
    print('Pearson corrcoef:\n %s' % pearsonr(rids_vec, locs_vec)[0])
    print('Spearman corrcoef:\n %s' % spearmanr(rids_vec, locs_vec)[0])
      
if __name__ == '__main__':
    pull_down_up_analysis()
