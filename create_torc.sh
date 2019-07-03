#!/bin/bash

# inceptor连接参数
inceptor_server=192.168.1.23
inceptor_port=10000

# 将进度写入日志文件,每次运行脚本,日志文件都会重新生成
echo "# THIS IS LOG FILE" >create_torc.log

# 顺序读取table.list文件的每一行(每行都是一个表名,格式为 <库名.表名> )
while read line 
do
    # 将库名和表名分离
    db_name=`echo $line | awk -F'.' '{print $1}'`
    tbl_name=`echo $line | awk -F'.' '{print $2}'`
    
    # 输出日志
    echo -e "\033[41;33m >>>>>>>>>> START CREATE DDL OF ${db_name}.${tbl_name} <<<<<<<<<< \033[0m"
    echo ">>>>>>>>>> START CREATE DDL OF ${db_name}.${tbl_name} <<<<<<<<<<" >>create_torc.log
    
    # 获取表的 COMMENT 
    table_comment=`beeline -u "jdbc:hive2://${inceptor_server}:${inceptor_port}/default;principal=hive/node23@TDH" --maxwidth=999999 --showHeader=false -e "select commentstring  from system.tables_v where database_name = '${db_name}' and table_name = '${tbl_name}';" | grep \| |awk '{print $2}'`
    
    # 输出日志
    echo -e "\033[45;37m ##### THE TABLE COMMENT OF ${db_name}.${tbl_name} IS ${table_comment} ##### \033[0m"
    echo "##### THE TABLE COMMENT OF ${db_name}.${tbl_name} IS ${table_comment} #####" >>create_torc.log
    
    # 通过数据字典表查找表的第一个列名(当作分桶字段)
    first_column=`beeline -u "jdbc:hive2://${inceptor_server}:${inceptor_port}/default;principal=hive/node23@TDH" --maxwidth=999999 --showHeader=false -e "select column_name  from system.columns_v where database_name = '${db_name}' and table_name = '${tbl_name}' and  column_id = '0';" | grep \| |awk '{print $2}'`
    
    # 输出日志
    echo -e "\033[45;37m ##### THE FIRST COLUMN OF ${db_name}.${tbl_name} IS ${first_column} ##### \033[0m"
    echo "##### THE FIRST COLUMN OF ${db_name}.${tbl_name} IS ${first_column} #####" >>create_torc.log
    
    # 通过数据字典求列的COUNT值(即表有多少列)
    column_num=`beeline -u "jdbc:hive2://${inceptor_server}:${inceptor_port}/default;principal=hive/node23@TDH" --maxwidth=999999 --showHeader=false -e "select count(column_name)  from system.columns_v where database_name = '${db_name}' and table_name = '${tbl_name}';" | grep \| |awk '{print $2}'`
    
    # 输出日志
    echo -e "\033[45;37m ##### THE COLUMN NUMBER OF ${db_name}.${tbl_name} IS ${column_num} ##### \033[0m"
    echo "##### THE COLUMN NUMBER OF ${db_name}.${tbl_name} IS ${column_num} #####" >>create_torc.log
    
    # 拼凑建表语句(可以选择是否加入 <IF NOT EXISTS> (视具体情况而定))
    echo -e "CREATE TABLE ${db_name}.${tbl_name}_orc (" >>torc_ddl.sql
    
    # 处理列名和列注释,如果是最后一列,则 COMMENT 后面不加逗号<,>
    beeline -u "jdbc:hive2://${inceptor_server}:${inceptor_port}/default;principal=hive/node23@TDH" --maxwidth=999999 --showHeader=false -e "select column_name,column_type,commentstring from system.columns_v where database_name = '${db_name}' and table_name = '${tbl_name}';" | grep \| |awk '{if (FNR=='${column_num}') {print "`"$2"`",$4,"COMMENT","\""$6"\"",")"} else {print "`"$2"`",$4,"COMMENT","\""$6"\""","}}' >> torc_ddl.sql
	
	# 建表语句中添加表的 COMMENT 
    echo -e "COMMENT \"${table_comment}\"" >> torc_ddl.sql
	
    # 拼凑分桶字段
    echo -e "CLUSTERED BY (${first_column}) INTO 1 BUCKETS\nSTORED AS ORC\nTBLPROPERTIES('transactional'='true');" >> torc_ddl.sql
    
    # 输出日志
    echo -e "\033[42;30m >>>>>>>>>> CREATE DDL OF ${db_name}.${tbl_name} SUCCESS <<<<<<<<<< \033[0m"
    echo ">>>>>>>>>> CREATE DDL OF ${db_name}.${tbl_name} SUCCESS <<<<<<<<<<" >>create_torc.log
    
	# table.list 为读取的文件名
done < table.list

# table.list 示例如下 <test数据库下面有 test1,test2,test3 三张表>
# test.test1
# test.test2
# test.test1
