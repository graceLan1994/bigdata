#!/bin/bash
source /etc/profile
DATAX_HOME="/opt/software/datax"
SHELL_PATH="/root/datax_job"
SCRIPT_PATH=${SHELL_PATH}/job
DATAX_LOG=${SHELL_PATH}/logs/datax.log.`date "+%Y-%m-%d"`
HDFS_PATH="hdfs://mycluster"
#START_TIME=$(date -d "-1 day" +%Y-%m-%d)
#END_TIME=$(date "+%Y-%m-%d")
#DT_TIME=$(date -d "-1 day" +%Y%m%d)
START_TIME=""
END_TIME=""
DT_TIME=""
#失效日期
INVALID_DATE=""
#失效天数
INVALID_DAYS=2
#是否清除失效数据：默认清除
IS_CLEAR_INVALID_DATA=1
#参数
COMPRESS="snappy"
WRITEMODE="nonConflict"
FIELDDELIMITER=","
FILETYPE="orc"
PATH_HIVE="/warehouse/tablespace/managed/hive/"
# "/user/hive/warehouse/"
MYSQL_URL=""
#数据库用户名
MYSQL_USERNAME="root"
#数据库密码
MYSQL_PASSWD="Ndsc@lan2023"
#默认同步目标库名
TARGET_DB_NAME="persona"
#同步源库名
SOURCE_DB_NAME=""
#同步源表名
SOURCE_TABLE_NAME=""
#业务名称
BUSINESS_NAME=""
#datax json文件
DATAX_JSON_FILE=/temp
# 数据库实例信息
declare -A db_instance_conf
# 数据库用户名
declare -A db_instance_user_conf
# 数据库密码
declare -A db_instance_pwd_conf
# 数据库实例与库映射关系
declare -A db_instance_maps
# 初始化数据库实例配置
function initInstanceConf()
{   
        # 主业务线 ywx1
        db_instance_conf["db_main_data"]="jdbc:mysql://ndsc101:3306/"
        db_instance_user_conf["db_main_data"]="root"
        db_instance_pwd_conf["db_main_data"]="Ndsc@lan2023"
         # 业务线2 ywx2
        # db_instance_conf["db_data"]="jdbc:mysql://192.168.1.2:3306/"
        # db_instance_user_conf["db_data"]="admin"
        # db_instance_pwd_conf["db_data"]="123456"
        # ...
        
}
# 初始化库和数据库实例映射关系
function initDbAndInstanceMaps()
{
        #主业务线
        db_instance_maps["ywx1_db_main_persona"]="db_main_data"
        
        # #业务线2
        # db_instance_maps["ywx2_db_data"]="db_data"
        # #业务线3
        # db_instance_maps["ywx3_db_insurance"]="db_ywx3"
        
        # ...
        
        # ...
        
        # db_instance_maps["dss_db_dss"]="db_dss"
		
}

#时间处理  传入参数 yyyy-mm-dd
function DateProcess()
{
echo "日期时间为"$1
if echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y%m%d > /dev/null 2>&1
  then :
        START_TIME=$(date -d $1 "+%Y-%m-%d")
        END_TIME=$(date -d "$1 +1 day" +%Y-%m-%d)
        DT_TIME=$(date -d $1 +"%Y%m%d")
        INVALID_DATE=$(date -d "$1 -$INVALID_DAYS day" +%Y%m%d)
        echo 时间正确: $START_TIME / $END_TIME / $DT_TIME / $INVALID_DATE;
else
  echo "输入的日期格式不正确，应为yyyy-mm-dd";
  exit 1;
fi;

}

function DataConnect()
{
        db_business_key="$BUSINESS_NAME""_""$SOURCE_DB_NAME"
        db_instance_key=${db_instance_maps["$db_business_key"]}
        echo $db_business_key $db_instance_key
        if [ ! -n "$db_instance_key" ]; then
                echo "当前数据库连接信息不存在，请确认业务和数据库连接是否正确或联系管理员添加"
                exit 1;
        fi
        db_instance_value=${db_instance_conf["$db_instance_key"]}
	    MYSQL_USERNAME=${db_instance_user_conf["$db_instance_key"]}
        MYSQL_PASSWD=${db_instance_pwd_conf["$db_instance_key"]}
	echo $db_instance_value
        if [ ! -n "$db_instance_value" ]; then
                echo "当前数据库连接信息不存在，请确认业务和数据库连接是否正确或联系管理员添加"
                exit 1;
        fi
        MYSQL_URL="$db_instance_value$SOURCE_DB_NAME"
}

#每天运行 执行dataX
function BaseDataxMysql2Hive()
{
        #清除重复同步数据分区&新增分区
        su hdfs 
        hive -e "ALTER TABLE $TARGET_DB_NAME.$TARGET_TABLE_NAME DROP IF EXISTS PARTITION(day='$DT_TIME');ALTER TABLE $TARGET_DB_NAME.$TARGET_TABLE_NAME ADD IF NOT EXISTS PARTITION (day='$DT_TIME')";
        #执行同步
        echo "开始执行同步"
    if ! python ${DATAX_HOME}/bin/datax.py -p"-DtargetDBName=$TARGET_DB_NAME -DtargetTableName=$TARGET_TABLE_NAME  -DjdbcUrl=$MYSQL_URL -Dusername=$MYSQL_USERNAME -Dpassword=$MYSQL_PASSWD -DsourceTableName=$SOURCE_TABLE_NAME -DhdfsPath=$HDFS_PATH -DstartTime=$START_TIME -DendTime=$END_TIME -DisCompress=$COMPRESS -DwriteMode=$WRITEMODE -DfileType=$FILETYPE -DfieldDelimiter='$FIELDDELIMITER' -DfileName=$TARGET_TABLE_NAME  -Dpath=${PATH_HIVE}$TARGET_DB_NAME.db/$TARGET_TABLE_NAME/day=$DT_TIME" $DATAX_JSON_FILE;then
        echo "command failed"
        exit 1;
        fi

    echo "同步结束"
    #删除定义的失效日期数据
    if(($IS_CLEAR_INVALID_DATA==1));then
        echo "清除失效$INVALID_DATE天数的历史数据"
        hive -e "ALTER TABLE $TARGET_DB_NAME.$TARGET_TABLE_NAME DROP IF EXISTS PARTITION (day<=${INVALID_DATE});"
        fi
    #同步分区元数据
        #hive -e "ANALYZE TABLE $TARGET_DB_NAME.$TARGET_TABLE_NAME PARTITION (day=${DT_TIME}) COMPUTE STATISTICS;"
        #删除分区数据
}

function parseArgs()
{
        while getopts ":d:ab:s:m:f:t:n:u:p:" opt
        do
            case $opt in
                d)
            echo "参数d的值$OPTARG"
                DateProcess $OPTARG
                ;;
                a)
                IS_CLEAR_INVALID_DATA=0
                echo "参数a的值$OPTARG"
                ;;
                b)
                echo "参数b的值$OPTARG"
                BUSINESS_NAME=$OPTARG
                ;;
                m)
                echo "参数m的值$OPTARG"
                SOURCE_DB_NAME=$OPTARG
                ;;
                s)
                echo "参数s的值$OPTARG"
                SOURCE_TABLE_NAME=$OPTARG
                ;;
                f)
                echo "参数f的值$OPTARG"
                DATAX_JSON_FILE=$OPTARG
                ;;
                n)
                echo "参数n的值$OPTARG"
                TARGET_DB_NAME=$OPTARG
                ;;
                t)
                echo "参数t的值$OPTARG"
                TARGET_TABLE_NAME=$OPTARG
                ;;
                u)
                echo "参数u的值$OPTARG"
                MYSQL_USERNAME=$OPTARG
                ;;
                p)
                echo "参数t的值$OPTARG"
                MYSQL_PASSWD=$OPTARG
                ;;
                ?)
                echo "未知参数"
                exit 1;;
                :)
            echo "没有输入任何选项 $OPTARG"
            ;;
        esac done
}

function judgeParams()
{
        if  [ ! -n "$DT_TIME" ] ;then
            echo "you have not input a etlDate! format {-d yyyy-mm-dd} "
            exit 1;
        fi

        if  [ ! -n "$BUSINESS_NAME" ] ;then
            echo "you have not input a businessName! incloud(xxx,xxxx,x,xx) example {-b xxx}"
            exit 1;
        fi

        if  [ ! -n "$SOURCE_DB_NAME" ] ;then
            echo "you have not input a sourceDB!"
            exit 1;
        fi

        if  [ ! -n "$SOURCE_TABLE_NAME" ] ;then
            echo "you have not input a sourceTable example {-s user_info}!"
            exit 1;
        fi

        if  [ ! -n "$DATAX_JSON_FILE" ] ;then
            echo "you have not input a dataxJson! example {-f ods_ywx1_user_info_di.json}"
            exit 1;
        fi
        if  [ ! -n "$TARGET_TABLE_NAME" ] ;then
            echo "you have not input a targetTable! example {-t ods_ywx1_user_info_di}"
            exit 1;
        fi
}


function startsync()
{

        #初始化数据库实例
        initInstanceConf
        #初始化库和数据库实例映射关系
        initDbAndInstanceMaps
        #解析参数
        parseArgs "$@"
        #初始化数据链接
        DataConnect
        #判断参数
        judgeParams
        #同步数据
        BaseDataxMysql2Hive
}

# -d: 处理时间
# -b：业务线 (ywx,ywx1,ywx1,...,ywxn)
# -m：源数据库
# -a：增量数据不清除分区数据：默认清除
# -s：源数据表
# -n：目标数据库
# -t：目标数据表
# -f：datax同步json文件
# -p：密码
# -u：用户名

startsync "$@"

