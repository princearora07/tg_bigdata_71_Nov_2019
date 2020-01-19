export runtime=`date -u +"%Y%m%d%H%M%S"`
export log_file=/home/cloudera/Desktop/delta_cdm_$runtime.log
exec &> ${log_file}
export SPARK_HOME=/usr/lib/spark/lib
echo "execution started..."
max_val=`hive -e "select max(load_date) from dl_ds_s1.customers ;"`
echo $max_val
echo "max value fetched..."
hive -e "set hive.exec.dynamic.partition.mode=nonstrict;
insert into cdm.customers partition (load_date)
select custno, concat(firstname,lastname) as name ,
case when gender = 'M' then 'Male'
when gender = 'F' then 'Female'
else 'N/A'
end as gender,isActive, load_date from dl_ds_s1.customers
where load_date ='$max_val'";
echo "partition added in cdm table successfully..";