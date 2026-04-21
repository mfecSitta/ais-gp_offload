#!/bin/bash

# --- 1. กำหนดค่าสี ---
RED='\033[0;31m'
GREEN='\033[38;5;28m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# --- 2. ฟังก์ชันช่วยงาน ---
#  ฟังก์ชันตรวจสอบ Folder
ensure_dir() {
    local target_dir="$1"
    if [ ! -d "$target_dir" ]; then
        mkdir -p "$target_dir" || {
            echo -e "${RED}[ERR!]${NC}: Cannot create directory $target_dir" >&2
            exit 1
        }
        echo -e "[INFO]: Created $target_dir"
    fi
}

# ฟังก์ชันตรวจสอบความถูกต้องของวันที่
check_date() {
    local input_date=$1
    if ! [[ "$input_date" =~ ^[0-9]{8}$ ]]; then
        echo -e "${RED}[ERR!]${NC}: Invalid format '$input_date'. Please use YYYYMMDD."
        exit 1
    fi
    # ตรวจสอบว่าเป็นวันที่ที่มีอยู่จริงหรือไม่
    date -d "$input_date" >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo -e "${RED}[ERR!]${NC}: Date '$input_date' is invalid (does not exist)."
        exit 1
    fi
    echo -e "${GREEN}[PASS]${NC}: Valid date: $input_date"
}

# --- 3. เริ่มต้นกระบวนการ ---

if ! command -v uuidgen > /dev/null 2>&1; then
    echo -e "${RED}[ERR!]${NC}: command 'uuidgen' not found."
    exit 1
fi
MY_UUID=$(uuidgen)
RUN_TIME=$(date +"%Y%m%d_%H%M%S")

# รับพารามิเตอร์
DATE_START=$1
DATE_END=$2
DB_INPUT=$3
SCHEMA_INPUT=$4

if [[ -z "$DATE_START" || -z "$DATE_END" ]]; then
    echo -e "[INFO]: Usage: $0 <YYYYMMDD_START> <YYYYMMDD_END> [database] [schema]"
    exit 1
fi

echo -e "[INFO]: Validating input dates..."
check_date "$DATE_START"
check_date "$DATE_END"

if [ "$DATE_START" -gt "$DATE_END" ]; then
    echo -e "${RED}[ERR!]${NC}: Start date ($DATE_START) cannot be after End date ($DATE_END)."
    exit 1
fi
echo ""

echo -e "[INFO]: Validating database and schema parameters..."
if [[ -n "$DB_INPUT" ]]; then
    DATABASES=("$DB_INPUT")
    echo -e "[INFO]: Database specified: $DB_INPUT"
else
    DATABASES=("prodgp" "misgp" "csmgp")
    echo -e "[INFO]: No database specified. Defaulting to: ${DATABASES[*]}"
fi

if [[ -n "$SCHEMA_INPUT" ]]; then
    SCHEMA_PATTERN="$SCHEMA_INPUT"
    echo -e "[INFO]: Schema specified: $SCHEMA_INPUT"
else
    SCHEMA_PATTERN="*"
    echo -e "[INFO]: No schema specified. Using wildcard (*)"
fi
echo ""

# กำหนด Path และเตรียม Folder ---
OUTPUT_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/output"
SOURCE_BASE="/MNT/GP_DWS/encryption/stat_log"
TEMP_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/tmp/stat_log_process_$MY_UUID"
LIST_OUTPUT=()

ensure_dir "$OUTPUT_DIR"
ensure_dir "$TEMP_DIR"
echo ""

echo -e "[INFO]: Starting log collection and processing..."
echo -e "[INFO]: Date Range: $DATE_START to $DATE_END"
echo "---------------------------------------"
echo -e "[INFO]: UUID: $MY_UUID"
echo "---------------------------------------"

# --- 4. เริ่มประมวลผลราย Database ---
for DB in "${DATABASES[@]}"; do
    echo -e "[INFO]: Processing Database: $DB"
    
    RAW_COLLECT_FILE="$TEMP_DIR/raw_$DB.txt"
    > "$RAW_COLLECT_FILE"

    PREV_TOTAL=0
    CURRENT_DATE="$DATE_START"

    while [ "$CURRENT_DATE" -le "$DATE_END" ]; do
        FILES_PATTERN="$SOURCE_BASE/$CURRENT_DATE/$DB/$SCHEMA_PATTERN/*.sum"
        
        RECORDS_TODAY=0
        # ตรวจสอบว่ามีไฟล์ .sum สำหรับวันปัจจุบันหรือไม่
        echo -e "[INFO]: Checking for files: $FILES_PATTERN"
        if ls $FILES_PATTERN >/dev/null 2>&1; then
            # นับจำนวนบรรทัดในไฟล์ .sum ทั้งหมดที่ตรงกับ pattern
            RECORDS_TODAY=$(cat $FILES_PATTERN 2>/dev/null | wc -l)

            # รวมข้อมูลจากไฟล์ .sum ทั้งหมดเข้าไปในไฟล์รวบรวม
            cat $FILES_PATTERN >> "$RAW_COLLECT_FILE"

            # ตรวจสอบความสมบูรณ์ของข้อมูลหลังจากรวมไฟล์
            EXPECTED_TOTAL=$((PREV_TOTAL + RECORDS_TODAY))
            ACTUAL_TOTAL=$(wc -l < "$RAW_COLLECT_FILE")

            if [ "$ACTUAL_TOTAL" -ne "$EXPECTED_TOTAL" ]; then
                echo -e "${RED}[ERR!]${NC}: Data Integrity Mismatch on $CURRENT_DATE!"
                echo -e "         Expected: $EXPECTED_TOTAL | Actual: $ACTUAL_TOTAL"
                echo -e "         Process terminated."
                exit 1
            fi

            echo -e "${GREEN}[PASS]${NC}: $CURRENT_DATE: +$RECORDS_TODAY records (Accumulated: $ACTUAL_TOTAL) \n"
            PREV_TOTAL=$ACTUAL_TOTAL
        else
            echo -e "${YELLOW}[WARN]${NC}: $CURRENT_DATE: No files found for DB [$DB] and Schema [$SCHEMA_PATTERN] \n"
        fi
        
        CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +"%Y%m%d")
    done

    # --- 5. สรุปผลหา Latest Record ---
    TS_OUTPUT="$OUTPUT_DIR/summary_sum_${DB}_${RUN_TIME}_${MY_UUID}.txt"

    if [ -s "$RAW_COLLECT_FILE" ]; then
        awk -F'|' '{
            table_key = $1 "|" $2
            end_date = $4
            if (max_date[table_key] == "" || end_date >= max_date[table_key]) {
                max_date[table_key] = end_date
                full_record[table_key] = $0
            }
        } END {
            for (key in full_record) print full_record[key]
        }' "$RAW_COLLECT_FILE" | sort -t'|' -k4r > "$TS_OUTPUT"

        FINAL_COUNT=$(wc -l < "$TS_OUTPUT")
        LIST_OUTPUT+=("$TS_OUTPUT")
  
        echo -e "${GREEN}[DONE]${NC}: $DB processing complete."
        echo -e "[INFO]: Saved $FINAL_COUNT unique records to: $TS_OUTPUT"
    fi
    echo "---------------------------------------"
done

echo -e "[INFO]: All databases processed."
echo -e "[INFO]: Output files are located in: $OUTPUT_DIR"
if [ ${#LIST_OUTPUT[@]} -gt 0 ]; then
    echo -e "[INFO]: Generated files:"
    printf "  %s\n" "${LIST_OUTPUT[@]}"
fi
echo -e "[INFO]: All tasks completed successfully!"