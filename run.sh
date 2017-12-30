#!/bin/bash
source /srv/nbs/0/algo/dinghongquan/ctrcount/common.path
source /srv/nbs/0/algo/dinghongquan/ctrcount/hdfs.path

result_dir=result/uid
if [ ! -d "$result_dir" ]; then  
  mkdir -p "$result_dir" 
  echo "Already make dir: $result_dir"
fi

yesterday=`date -d "yesterday" +%Y-%m-%d`
date=2017-12-25
hours=(00 01)

###########统计上一个月的数据
for i in `seq 5 -1 1`
###########统计每小时的数据
#for var in ${hours[*]}
do
    date=`date -d "-${i} day $yesterday" +%Y-%m-%d`
    input_path=${input_pre_dir}/dt=${date}/*/*.lzo
    #input_path=${input_pre_dir}/dt=${yesterday}/*/*.lzo
    echo input data path: ${input_path}
    # hadoop job
    #$HADOOP fs -test -e ${output_path}/_SUCCESS
    if $HADOOP fs -test -d ${output_path}; then
        $HADOOP fs -rm -r ${output_path}
    fi
        $HADOOP jar ${STREAMING} \
            -D stream.map.output.field.separator='\t' \
            -D stream.num.map.output.key.fields=1 \
            -D map.output.key.field.separator=, \
            -jobconf mapred.reduce.tasks=1 \
            -jobconf mapred.job.priority=VERY_HIGH \
            -file /srv/nbs/0/algo/dinghongquan/ctrcount/reducer.py \
            -file /srv/nbs/0/algo/dinghongquan/ctrcount/mapper.py \
            -mapper mapper.py \
            -reducer reducer.py \
            -input ${input_path} \
            -output ${output_path}/
    #数据取回本地
   #$HADOOP fs -cat ${output_path}/${date}/part-00000 > result/${var}
   $HADOOP fs -cat ${output_path}/part-00000 > $result_dir/uid.${date}
   echo "See result in $result_dir/${date}"
done

#########合并数据
#>costType
#cat $result_dir/* > costType

