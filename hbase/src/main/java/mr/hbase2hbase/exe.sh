#!/usr/bin/env bash
# 打包
mvn clean package -Dmaven.test.skip=true -Ptest
# 如果没有表需要先创建表
create 'fruit_mr','info'
# 执行mr
yarn jar hbase-1.0-SNAPSHOT.jar mr.hbase2hbase.FruitDriver