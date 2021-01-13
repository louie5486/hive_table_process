#!/bin/bash
# ------------------------------------------------------------------
# [Author] LouieChen 
# [version] v1.1 
# [release date] 2020/12/15
# use this script to transfer hive text table into hive parquet table
# 1. get original table schema and self parser to get raw columns、partition column.
# 2. create temp table like with original table.
# 3. parser temp table to get 'date' column type, replace date column type to string column type.
# 4. create parquet format table like with temp table.
# 5. insert data to parquet table from original table. 
#
# [History]
# v1.0 for partition table
# v1.1 for all situation
# ------------------------------------------------------------------

#---- source 、original table
t="sales_bank"

beeline -u "jdbc:hive2://hapm1.com.tw:10000/$d;principal=hive/_HOST@TAISHINMIT.COM.TW;" --showHeader=false --silent=true --outputformat=tsv2 -e "show create table $t;" > src_table.sql
raw_cols=$(cat src_table.sql | tr '\n' ' ' | grep -io "CREATE TABLE .*" | cut -d"(" -f2- | cut -f1 -d")" | sed 's/`//g'|sed 's/, */,/g' | sed 's/^ *//g')
ptn_cols=$(cat src_table.sql | tr '\n' ' ' | grep -io "PARTITIONED BY .*" | cut -f1 -d")" | cut -d"(" -f2- | sed 's/`//g' |sed 's/^ *//g')
str=$(cat src_table.sql |grep "date," | sed 's/`//g' | sed 's/ date,//g' |sed 's/^ *//g')
echo $str
read -r -a array <<< $str
#echo "${array[0]}"
echo "raw_cols ----" $raw_cols
echo "ptn_cols ----" $ptn_cols

#----create temp_table
beeline -u "jdbc:hive2://hapm1.com.tw:10000/$d;principal=hive/_HOST@TAISHINMIT.COM.TW;" --showHeader=false --silent=true --outputformat=tsv2 -e "create table if not exists tmp_$t like $t";
#----alter temp table column type
for element in "${array[@]}"
do
echo "alter column: $element"
beeline -u "jdbc:hive2://hapm1.com.tw:10000/$d;principal=hive/_HOST@TAISHINMIT.COM.TW;" --showHeader=false --silent=true --outputformat=tsv2 -e "alter table tmp_$t change $element $element string";
done
#----create parquet table like temp table
beeline -u "jdbc:hive2://hapm1.com.tw:10000/$d;principal=hive/_HOST@TAISHINMIT.COM.TW;" --showHeader=false --silent=true --outputformat=tsv2 -e "create table if not exists ext_$t like tmp_$t stored as parquet";

#----insert original data to parquet table
if [ -z "$ptn_cols" ]
then
final_cols=$(echo "(" $raw_cols ")")
echo "----" $final_cols
beeline -u "jdbc:hive2://hapm1.com.tw:10000/$d;principal=hive/_HOST@TAISHINMIT.COM.TW;" --verbose=true --showHeader=false --silent=true --outputformat=tsv2 -e "set hive.exec.dynamic.partition = true;set hive.exec.dynamic.partition.mode = nonstrict;insert into ext_$t select * from $t";
else
ptn_cols_value=$(echo $ptn_cols | sed 's/ .*$//g')
final_cols=$(echo "(" $raw_cols "," $ptn_cols ")")
echo "ptn_cols_value ----" $ptn_cols_value
echo "----" $final_cols
beeline -u "jdbc:hive2://hapm1.com.tw:10000/$d;principal=hive/_HOST@TAISHINMIT.COM.TW;" --verbose=true --showHeader=false --silent=true --outputformat=tsv2 -e "set hive.exec.dynamic.partition = true;set hive.exec.dynamic.partition.mode = nonstrict;insert into ext_$t partition($ptn_cols_value) select * from $t";
fi

#---- show parquet table schema
staging_ddl=`beeline -u "jdbc:hive2://hapm1.com.tw:10000/$d;principal=hive/_HOST@TAISHINMIT.COM.TW;" --showHeader=false --silent=true --outputformat=tsv2 -e "show create table ext_$t;"`;
echo "staging_ddl----" $staging_ddl
dir=$(echo $staging_ddl |tr '\n' ' ' | grep -io " LOCATION .*" | grep -m1 -o "'.*" | sed "s/' .*//g"|cut -c2-);
echo "dir -----" $dir

