#! /bin/env bash

now_date=$(date +%Y%m%d_%H%M%S)

inp_date=$1

# 1. ??????????????????????
if [ -z "$inp_date" ]; then
    echo "ERROR: Please provide a date in YYYYMMDD format."
    echo "Usage: $0 20260101"
    exit 1
fi

# 2. ???????????????????? 8 ??????????? (Regex)
if [[ ! "$inp_date" =~ ^[0-9]{8}$ ]]; then
    echo "ERROR: Invalid format. Date must be 8 digits (YYYYMMDD)."
    exit 1
fi

# 3. ?????????????????????????????????? (???? ?????? 20260230 ???? 20261301)
# ??? -d ????????????????? ????????????? date ????? exit code != 0
if ! date -d "$inp_date" >/dev/null 2>&1; then
    echo "ERROR: Date '$inp_date' is not a valid date (e.g., month 1-12, days 1-31)."
    exit 1
fi

echo "SUCCESS: Date $inp_date is valid."


path_log_run="/data/AIS-EDW/gpoffload/log/${inp_date}/*/*.csv"
path_log_output="/data/AIS-EDW/gpoffload/temp_raw_log/note/log/"
file_name_tmp="summary_log_${inp_date}_${now_date}.txt.tmp"
file_name="summary_log_${inp_date}_${now_date}.txt"

echo "path_log_run: ${path_log_run}"
echo "path_log_output: ${path_log_output}"
#echo "file_name_tmp: ${file_name_tmp}"

echo "sed -s '1d' ${path_log_run} > ${path_log_output}${file_name}"
sed -s '1d' ${path_log_run} > ${path_log_output}${file_name}

total_lines=$(cat ${path_log_run} | wc -l)
total_files=$(ls ${path_log_run} | wc -l)
total_line_without_header=$((total_lines - total_files))

total_log_output=$(cat ${path_log_output}${file_name} | wc -l)

echo "total_line_without_header: ${total_line_without_header}"
echo "total_log_output: ${total_log_output}"

if [ "$total_line_without_header" -eq "$total_log_output" ]; then
    status_export="compare pass"
else
    status_export="compare fail"
fi


#cp ${path_log_output}${file_name_tmp} ${path_log_output}${file_name}

echo ""
echo ""
echo "=============================================================="
echo "Summary"
echo ""
echo "Output file: ${path_log_output}${file_name}"
echo "compare source vs output file: ${status_export}"
echo ""
