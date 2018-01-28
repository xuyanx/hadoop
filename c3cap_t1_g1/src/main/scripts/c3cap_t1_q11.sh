#!/bin/bash

##cmd  <input data path> <output path>

sudo hadoop fs -rm -r $2
sudo hadoop jar c3cap_t1-0.0.1.jar com.xyz.hadoop.c3cap_t1.q11.RankPopularAirport $1 $2
sudo hadoop fs -cat rank_output/part-* |sort -k2 -nr|head -n10
