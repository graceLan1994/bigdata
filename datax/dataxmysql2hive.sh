#源表 -s
SOURCE_TABLE_NAME="hb_data_new"
#目标表 -t
TARGET_TABLE_NAME="ods_social_media_post_hb"
#datax 文件 -f
DATAX_FILE="/root/datax_job/job/ods_social_media_post_hb.json"
# "${BASE_DIR_PATH}/ods_social_media_post_hb.json"
ETL_DATE="2023-04-02"
# ${ETL_DATE}
#每天同步的时间yyyy-mm-dd
BUSINESS_NAME="ywx1_db_main"
#${BUSINESS_NAME}
#根据主题域
SOURCE_DB_NAME="persona"
# ${SOURCE_DB_NAME}
#
#!/bin/bash
source /etc/profile
sh dataxsync.sh -d $ETL_DATE -b $BUSINESS_NAME -m $SOURCE_DB_NAME -s $SOURCE_TABLE_NAME -t $TARGET_TABLE_NAME -f $DATAX_FILE

sh dataxsync.sh -d "2023-04-02" -b "ywx1_db_main" -m "persona" -s "hb_data_new" -t "ods_social_media_post_hb" -f "/root/datax_job/job/ods_social_media_post_hb.json"
