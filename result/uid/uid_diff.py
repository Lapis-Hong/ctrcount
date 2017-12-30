import sys
file1 = sys.argv[1]
file2 = sys.argv[2]

set1 = set()
set2 = set()

for line in open('uid.'+file1):
    uid = line.strip().split('\t')[0]
    set1.add(uid)

for line in open('uid.'+file2):
    uid = line.strip().split('\t')[0]
    set2.add(uid)

common_cnt = len(set1 & set2)
min_cnt = min(len(set1), len(set2))
print("Deep uid similarity: %s" %(float(common_cnt) / min_cnt)) 
