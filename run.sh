bsub -b -I -q $queue_name -N 1 -np 1 -cgsp 64 -J $job_name -share_size 14000 ./SWQC -i $in1 -I $in2 -w 1 -o $out1 -O $out2
