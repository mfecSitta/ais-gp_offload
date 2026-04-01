#!/bin/bash

# --- 1. Setup Variables & Validation ---
now_date=$(date +%Y%m%d_%H%M%S)
START_DATE=$1
END_DATE=$2

# --- ANSI Color Codes ---
RED='\033[0;31m'
GREEN='\033[38;5;28m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color (ล้างค่าสีกลับเป็นปกติ)


# --- [NEW] Control Flags (กำหนดเป็น Y เพื่อเปิด หรือ N เพื่อปิด) ---
RUN_OFFLOAD="Y"
RUN_REC_PQ="Y"
RUN_REC_GP="N"

usage() {
    echo "Usage: $0 <START_DATE> <END_DATE>"
    echo "Format: YYYYMMDD"
    exit 1
}

if [ "$#" -ne 2 ]; then usage; fi

# Function ตรวจสอบวันที่
check_date() {
    local input_date=$1
    if ! [[ "$input_date" =~ ^[0-9]{8}$ ]]; then
        echo -e "${RED}[ERROR]${NC}: Invalid format '$input_date'. Please use YYYYMMDD."
        exit 1
    fi
    date -d "$input_date" >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC}: Date '$input_date' is invalid (does not exist)."
        exit 1
    fi
    echo -e "${GREEN}[OK]${NC} Date '$input_date' is valid."
}

check_date "$START_DATE"
check_date "$END_DATE"

if [ "$START_DATE" -gt "$END_DATE" ]; then
    echo -e "${RED}[ERROR]${NC}: Start date ($START_DATE) cannot be after End date ($END_DATE)."
    exit 1
fi


# --- 2. Define Output Paths & Files ---
path_log_output="/data/AIS-EDW/gpoffload/temp_raw_log/note/log/"

# ตรวจสอบว่ามี Folder ปลายทางไหม ถ้าไม่มีให้สร้าง (รวมถึงสร้าง Parent Directory ด้วย)
if [ ! -d "$path_log_output" ]; then
    echo "📂 Folder not found. Creating path: $path_log_output"
    mkdir -p "$path_log_output"
    
    # ตรวจสอบอีกครั้งว่าสร้างสำเร็จไหม (กรณีไม่มี Permission)
    if [ $? -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC}: Cannot create directory $path_log_output. Please check permissions."
        exit 1
    fi
fi

# กำหนดชื่อไฟล์ Output แยกตามประเภท
FILE_OFFLOAD="summary_offloadgp_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"
FILE_REC_PQ="summary_rec_pq_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"
FILE_REC_GP="summary_rec_gp_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"

FULL_PATH_OFFLOAD="${path_log_output}${FILE_OFFLOAD}"
FULL_PATH_REC_PQ="${path_log_output}${FILE_REC_PQ}"
FULL_PATH_REC_GP="${path_log_output}${FILE_REC_GP}"

TOTAL_EXP_OFFLOAD=0
TOTAL_EXP_REC_PQ=0
TOTAL_EXP_REC_GP=0

# ตรวจสอบ Path เบื้องต้นเฉพาะส่วนที่เปิดใช้งาน เคลียร์ไฟล์เก่าและตัวแปรสะสม
if [ "$RUN_OFFLOAD" == "Y" ]; then > "$FULL_PATH_OFFLOAD"; fi
if [ "$RUN_REC_PQ" == "Y" ]; then > "$FULL_PATH_REC_PQ"; fi
if [ "$RUN_REC_GP" == "Y" ]; then > "$FULL_PATH_REC_GP"; fi

echo -e "${GREEN}[OK]${NC} Processing started for period: $START_DATE to $END_DATE"
echo "Status: Offload=$RUN_OFFLOAD, RecPQ=$RUN_REC_PQ, RecGP=$RUN_REC_GP"
echo "--------------------------------------------------------------"


# --- 3. Main Loop (Date) ---
CURRENT_DATE="$START_DATE"
while [ "$CURRENT_DATE" -le "$END_DATE" ]; do
    echo "📅 Date: $CURRENT_DATE"

    # --- SOURCE 1: Offload GP (ใช้ sed -s '1d') ---
    if [ "$RUN_OFFLOAD" == "Y" ]; then
        echo "-------"
        SRC_OFFLOAD="/MNT/GP_DWS/mig_stat_log/*/${CURRENT_DATE}/offloadgp*.csv"
        echo "SRC_OFFLOAD: ${SRC_OFFLOAD}"
        if ls ${SRC_OFFLOAD} 1> /dev/null 2>&1; then

            #echo "   -> Processing Offload GP (Removing Headers)..." # old process for local
            echo "   -> Processing Offload GP (Full Append)..."

            #sed -s '1d' ${SRC_OFFLOAD} >> "$FULL_PATH_OFFLOAD" # old process for local
            cat ${SRC_OFFLOAD} >> "$FULL_PATH_OFFLOAD"
        
            #raw_ln=$(cat ${SRC_OFFLOAD} | wc -l) # old process for local
            #f_cnt=$(ls -1 ${SRC_OFFLOAD} | wc -l) # old process for local
            #data_ln=$((raw_ln - f_cnt)) # old process for local
            data_ln=$(cat ${SRC_OFFLOAD} | wc -l)
            TOTAL_EXP_OFFLOAD=$((TOTAL_EXP_OFFLOAD + data_ln))
        else
            echo -e "${YELLOW}![MISSING]! -> No Offload GP files found for $CURRENT_DATE${NC}"
        fi
    fi

    # --- SOURCE 2: Reconcile Parquet ---
    if [ "$RUN_REC_PQ" == "Y" ]; then
        echo "-------"
        SRC_REC_PQ="/MNT/GP_DWS/mig_reconcile_query_parquet_output/${CURRENT_DATE}/*/*/stat_csv/log_stat*.csv"
        echo "SRC_REC_PQ: ${SRC_REC_PQ}"
        if ls ${SRC_REC_PQ} 1> /dev/null 2>&1; then

            echo "   -> Processing Reconcile PQ (Full Append)..."
            cat ${SRC_REC_PQ} >> "$FULL_PATH_REC_PQ"
        
            data_ln=$(cat ${SRC_REC_PQ} | wc -l)
            TOTAL_EXP_REC_PQ=$((TOTAL_EXP_REC_PQ + data_ln))
        else
            echo -e "${YELLOW}![MISSING]! -> No Reconcile PQ files found for $CURRENT_DATE${NC}"
        fi
    fi

    # --- SOURCE 3: Reconcile GP ---
    if [ "$RUN_REC_GP" == "Y" ]; then
        echo "-------"
        SRC_REC_GP="/MNT/GP_DWS/mig_reconcile_query_gp_output/${CURRENT_DATE}/*/*/stat_csv/log_stat*.csv"
        echo "SRC_REC_GP: ${SRC_REC_GP}"
        if ls ${SRC_REC_GP} 1> /dev/null 2>&1; then

            echo "   -> Processing Reconcile GP (Full Append)..."
            cat ${SRC_REC_GP} >> "$FULL_PATH_REC_GP"

            data_ln=$(cat ${SRC_REC_GP} | wc -l)
            TOTAL_EXP_REC_GP=$((TOTAL_EXP_REC_GP + data_ln))
        else
            echo -e "${YELLOW}![MISSING]! -> No Reconcile GP files found for $CURRENT_DATE${NC}"
        fi
    fi

    CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +%Y%m%d)
    echo ""
done


# --- 4. Final Verification Function ---
verify_result() {
    local label=$1
    local expected=$2
    local file_path=$3
    local run_flag=$4

    if [ "$run_flag" != "Y" ]; then
        echo "[$label] - SKIPPED"
        echo ""
        return
    fi

    local actual=$(wc -l < "$file_path")
    
    echo "[$label]"
    echo "   Expected: $expected | Actual: $actual"
    if [ "$expected" -eq "$actual" ]; then
        echo -e "   Status: ${GREEN}PASS${NC}"
    else
        echo -e "   Status: ${RED}FAIL${NC}"
    fi
    echo "   File: $file_path"
    echo ""
}

# เพิ่ม [ -n "$VAR" ] เพื่อเช็คว่าตัวแปรต้องไม่ว่าง
#[ -n "$FULL_PATH_OFFLOAD" ] && [ -f "$FULL_PATH_OFFLOAD" ] && [ ! -s "$FULL_PATH_OFFLOAD" ] && rm "$FULL_PATH_OFFLOAD"

echo "=============================================================="
echo "VERIFICATION SUMMARY"
echo "=============================================================="
verify_result "OFFLOAD GP" "$TOTAL_EXP_OFFLOAD" "$FULL_PATH_OFFLOAD" "$RUN_OFFLOAD"
verify_result "REC PARQUET" "$TOTAL_EXP_REC_PQ" "$FULL_PATH_REC_PQ" "$RUN_REC_PQ"
verify_result "REC GP" "$TOTAL_EXP_REC_GP" "$FULL_PATH_REC_GP" "$RUN_REC_GP"

echo "[Summary Files]"
echo "files exported to: $path_log_output"
echo "File offload: $FULL_PATH_OFFLOAD"
echo "File rec parquet: $FULL_PATH_REC_PQ"
echo "File rec GP: $FULL_PATH_REC_GP"
echo "=============================================================="