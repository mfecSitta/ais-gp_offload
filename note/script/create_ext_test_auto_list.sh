#! /bin/env bash

# sh /data/AIS-EDW/gpoffload/temp_raw_log/note/script/create_ext_test.sh <database_name> <table_name> <column_name>
# sh /data/AIS-EDW/gpoffload/temp_raw_log/note/script/create_ext_test.sh prod_db_summary customer_profile_ma_monthly promotion_name


now_date=$(date +%Y%m%d_%H%M%S)


inp_db_name=$1
inp_tb_name=$2

start_ts_txt=$(date "+%Y%m%d_%H%M%S")
pid=$$
sum_log="/data/AIS-EDW/gpoffload/temp_raw_log/note/script/log/log_summary_${inp_db_name}_${inp_tb_name}_check_th_cols_${start_ts_txt}_${pid}.log"
dtl_log="/data/AIS-EDW/gpoffload/temp_raw_log/note/script/log/log_detail_${inp_db_name}_${inp_tb_name}_check_th_cols_${start_ts_txt}_${pid}.log"

secho()
{
    echo -e "$(date +"%Y-%m-%d %H:%M:%S") $*" | tee -a ${sum_log} ${dtl_log}
}

decho()
{
    echo -e "$(date +"%Y-%m-%d %H:%M:%S") $*" | tee -a ${dtl_log}
}

run_psql_command() {
    local sql_query="$1"
    #local err_file="psql_error.tmp"
    local output
    local exit_code

    # ??? psql: stdout ????????, stderr ??????
    output=$(psql -v ON_ERROR_STOP=1 -d "$DB_NAME" -c "$sql_query" 2>"$err_file")
    exit_code=$?
    
    #echo "debuggggggggg err_file: ${err_file}"

    # ???? error message ?????????????????
    local error_msg=$(cat "$err_file")
    rm -f "$err_file"

    if [ $exit_code -eq 0 ]; then
        decho "--- [SUCCESS] ---"
        secho "Result: $output"
        return 0
    else
        decho "--- [FAILED] ---"
        decho "Exit Code: $exit_code"
        secho "Error: $error_msg"
        return 1
    fi
}


inp_column_name=$3

# --- ??????? Error ??? User Input ---

# 1. ????????????????? 3 ??????
# if [ -z "$inp_db_name" ] || [ -z "$inp_tb_name" ] || [ -z "$inp_column_name" ]; then
if [ -z "$inp_db_name" ] || [ -z "$inp_tb_name" ]; then
    echo "ERROR: Missing parameters!"
    echo "Usage: $0 <db_name> <table_name>"
    exit 1
fi

# 2. ??????????????? (Security Check - ??????? SQL Injection ?????????)
# ?????????????????? ????????, ?????? ??? Underscore ????????
if [[ ! "$inp_db_name" =~ ^[a-zA-Z0-9_]+$ ]]; then
    echo "ERROR: Invalid DB name. Use only alphanumeric and underscores."
    exit 1
fi


DB_NAME="prodgp"

cmd_get_cols="select th_column_name from gpoffload.gp_list_th_col where original_table_name = '${inp_db_name}.${inp_tb_name}' and trim(upper(active_flag)) = 'Y';"
all_active_th_cols=$(psql -tA -d prodgp -c "${cmd_get_cols}")
exit_code=$?

if [ $exit_code -ne 0 ]; then
	decho "ERROR: Fail to get list of columns."
	exit 1
fi

if [ ! -z "${all_active_th_cols}" ]; then
	secho "================ SUMMARY ================"
	secho "Table: ${inp_db_name}.${inp_tb_name}"
	secho "========================================="
	secho "List all configured TH columns:\n${all_active_th_cols}"
else
	decho "ERROR: Not found any active columns."
	exit 1
fi

list_fail=""
while read -r col; do 
	decho "START: ${col}"
	inp_column_name="${col}"
	err_file="/data/AIS-EDW/gpoffload/temp_raw_log/note/script/tmp/${inp_db_name}_${inp_tb_name}_${inp_column_name}_${start_ts_txt}_${pid}.tmp"

	sql_create_ext_conv="
	CREATE WRITABLE EXTERNAL TABLE gpoffload.test_${inp_tb_name}__${inp_column_name}_conv
			( "${inp_column_name}_conv" text )
			LOCATION ('pxf://mig/test/test_${inp_tb_name}__${inp_column_name}_conv?PROFILE=file:parquet&SERVER=nfsobjgpdws')
			FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
			ENCODING 'UTF8';
	"

	sql_create_ext="
	CREATE WRITABLE EXTERNAL TABLE gpoffload.test_${inp_tb_name}__${inp_column_name}
			( "${inp_column_name}" text )
			LOCATION ('pxf://mig/test/test_${inp_tb_name}__${inp_column_name}?PROFILE=file:parquet&SERVER=nfsobjgpdws')
			FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
			ENCODING 'UTF8';
	"

	sql_insert_conv="
	INSERT INTO gpoffload.test_${inp_tb_name}__${inp_column_name}_conv
	SELECT 
	convert_from(convert_to("${inp_column_name}",'utf8'),'win874')::text
	FROM ${inp_db_name}.${inp_tb_name};
	"

	sql_insert="
	INSERT INTO gpoffload.test_${inp_tb_name}__${inp_column_name}
	SELECT 
	"${inp_column_name}"
	FROM ${inp_db_name}.${inp_tb_name};
	"



	decho ""
	secho "========================================="
	secho "Column: ${inp_column_name}"
	secho "========================================="
	fail_flg=0

	decho ""
	decho "------------------------------------------------"
	decho "sql_create_ext_conv"
	decho "${sql_create_ext_conv}"

	run_psql_command "${sql_create_ext_conv}"
	result_sql_create_ext_conv=$?
	# 1. sql_create_ext_conv
	if [ $result_sql_create_ext_conv -eq 0 ]; then
		secho "sql_create_ext_conv   : PASS"
	else
		secho "sql_create_ext_conv   : ERROR"
		fail_flg=0
	fi


	decho "------------------------------------------------"
	decho ""

	decho ""
	decho "------------------------------------------------"
	decho "sql_create_ext"
	decho "${sql_create_ext}"

	run_psql_command "${sql_create_ext}"
	result_sql_create_ext=$?
	# 2. sql_create_ext
	if [ $result_sql_create_ext -eq 0 ]; then
		secho "sql_create_ext        : PASS"
	else
		secho "sql_create_ext        : ERROR"
		fail_flg=0
	fi
	secho ""

	decho "------------------------------------------------"
	decho ""


	decho ""
	decho "------------------------------------------------"
	decho "sql_insert_conv"
	decho "${sql_insert_conv}"

	run_psql_command "${sql_insert_conv}"
	result_sql_insert_conv=$?
	# 3. sql_insert_conv
	if [ $result_sql_insert_conv -eq 0 ]; then
		secho "sql_insert_conv: PASS"
	else
		secho "sql_insert_conv: ERROR"
		fail_flg=1
	fi

	decho "------------------------------------------------"
	decho ""


	decho ""
	decho "------------------------------------------------"
	decho "sql_insert"
	decho "${sql_insert}"

	run_psql_command "${sql_insert}"
	result_sql_insert=$?

	decho "------------------------------------------------"
	decho ""
	# 4. sql_insert
	if [ $result_sql_insert -eq 0 ]; then
		secho "sql_insert     : PASS"
	else
		secho "sql_insert     : ERROR"
		fail_flg=1
	fi
	secho ""
	
	if [ ${fail_flg} -eq 1 ]; then
		list_fail="$(echo -e "${list_fail}\n${inp_column_name}")"
	fi

done <<< "${all_active_th_cols}"
secho "List all configured TH columns:\n${all_active_th_cols}"
secho "---"
secho "List all error columns:${list_fail}"
decho ""
decho "End of script."
decho "Summary log: ${sum_log}"
