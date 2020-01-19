export runtime=`date -u +"%Y%m%d%H%M%S"`
export log_file=/home/cloudera/Desktop/delta_vdm_$runtime.log
exec &> ${log_file}
export SPARK_HOME=/usr/lib/spark/lib
echo "execution started..."
max_val=`hive -e "select max(load_date) from cdm.customers ;"`
echo $max_val
echo "max value fetched..."
hive -e "drop view if exists vdm.view_customers;"
hive -e "create view if not exists vdm.view_customers as
select * from cdm.customers where load_date = '$max_val'";
echo "partition added in view successfully.."
