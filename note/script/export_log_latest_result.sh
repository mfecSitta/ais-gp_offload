#!/bin/bash

# --- 1. Setup Variables ---
now_date=$(date +%Y%m%d_%H%M%S)
RUN_DATE=$(date +%Y%m%d)
SCHEMA_ARG=$1
SCRIPT_NAME=$(basename "$0")
DB_NAME="prodgp"

# --- ANSI Color Codes ---
RED='\033[0;31m'
GREEN='\033[38;5;28m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# --- 2. Validation ---
usage() {
    echo "Usage: $0 <SCHEMA_NAME>"
    echo "Example: $0 xxx_analytics"
    exit 1
}

if [ -z "$SCHEMA_ARG" ]; then
    echo -e "${RED}[ERROR]${NC}: Please provide a schema name."
    usage
fi

# --- 3. Define Path & File ---
path_export="/data/AIS-EDW/gpoffload/temp_raw_log/note/log/schema_reports/${RUN_DATE}/"
FILE_NAME="report_latest_${SCHEMA_ARG}_${now_date}.csv"
FULL_PATH="${path_export}${FILE_NAME}"

# สร้าง Folder ถ้ายังไม่มี
check_and_create_dir() {
    local target_dir="$1"

    if [ ! -d "$target_dir" ]; then
        echo "[$SCRIPT_NAME] 📂 Folder not found. Creating path: $target_dir"
        mkdir -p "$target_dir"
        
        # ตรวจสอบ Exit Status ($?) ของคำสั่ง mkdir ล่าสุด
        if [ $? -ne 0 ]; then
            echo -e "${RED}[ERROR]${NC} [$SCRIPT_NAME]: Cannot create directory $target_dir. Please check permissions."
            exit 1
        fi
    fi
}

check_and_create_dir "$path_export"

# --- 4. Prepare SQL Query ---
# ใช้ Pipe (|) เป็นตัวคั่นเพื่อความปลอดภัยของ Error Message
SQL_QUERY="SELECT 
    db_name             
    , schema_name
    , table_name            
    , external_tbl             
    , export_sts               
    , export_err_msg           
    , reconcile_gp_sts         
    , reconcile_gp_err_msg     
    , reconcile_pq_sts         
    , reconcile_pq_err_msg     
    , reconcile_compare_sts    
    , reconcile_compare_err_msg
FROM
    gpoffload.log_latest_result 
WHERE 
    schema_name = '${SCHEMA_ARG}'"

# --- 5. Execution (Export) ---
echo "=============================================================="
echo -e "  ${BLUE}EXTRACTING LATEST STATUS${NC}"
echo "=============================================================="
echo -e "Schema : ${YELLOW}${SCHEMA_ARG}${NC}"
echo -e "Target : ${BLUE}${FULL_PATH}${NC}"
echo "--------------------------------------------------------------"

# รัน psql \copy
psql -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "\\copy ($SQL_QUERY) TO '$FULL_PATH' WITH CSV HEADER DELIMITER '|' QUOTE '\"';"

if [ $? -eq 0 ]; then
    ROW_COUNT=$(wc -l < "$FULL_PATH")
    # ลบ Header ออก 1 บรรทัด
    ROW_COUNT=$((ROW_COUNT - 1))
    
    if [ "$ROW_COUNT" -le 0 ]; then
        echo -e "${YELLOW}![WARNING]! -> Exported successfully, but NO DATA found for schema '${SCHEMA_ARG}'${NC}"
    else
        echo -e "${GREEN}✔ [SUCCESS]${NC}: Exported ${YELLOW}${ROW_COUNT}${NC} tables."
    fi
else
    echo -e "${RED}✘ [ERROR]${NC}: Failed to export data. Please check database connection or permissions."
    exit 1
fi

echo "=============================================================="