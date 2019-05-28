#!/usr/bin/env bash
mvn clean package -Dmaven.test.skip=true -Ptest
create 'fruit_mr2','info'
yarn jar hbase-1.0-SNAPSHOT.jar mr.hdfs2hbase.HdfsDriver /input_fruit/fruit.tsv