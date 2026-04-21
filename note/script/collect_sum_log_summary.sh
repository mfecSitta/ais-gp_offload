#!/bin/bash


# function to ensure directory exists, if not create it
ensure_dir() {
    local target_dir="$1"
    if [ ! -d "$target_dir" ]; then
        mkdir -p "$target_dir" || {
            echo "Error: Cannot create directory $target_dir" >&2
            exit 1
        }
        echo "Success: Created $target_dir"
    fi
}

# 1. ตรวจสอบ uuidgen
if ! command -v uuidgen > /dev/null 2>&1; then
    echo "Error: command 'uuidgen' not found."
    exit 1
fi
MY_UUID=$(uuidgen)

# 2. รับค่า Parameter
DATE_START=$1
DATE_END=$2
DB_INPUT=$3

if [[ -z "$DATE_START" || -z "$DATE_END" ]]; then
    echo "Usage: $0 <YYYYMMDD_START> <YYYYMMDD_END> [database]"
    exit 1
fi

# 3. กำหนด Databases
if [[ -n "$DB_INPUT" ]]; then
    DATABASES=("$DB_INPUT")
    echo "Database specified: $DB_INPUT"
else
    DATABASES=("prodgp" "misgp" "csmgp")
    echo "No database specified. Defaulting to: ${DATABASES[*]}"
fi

# 4. กำหนด Path
OUTPUT_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/output"
SOURCE_BASE="/MNT/GP_DWS/encryption/stat_log"
TEMP_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/tmp/stat_log_process_$MY_UUID"

ensure_dir "$OUTPUT_DIR"
ensure_dir "$TEMP_DIR"

echo "Starting log collection and processing..."
echo "Date Range: $DATE_START to $DATE_END"
echo "output directory: $OUTPUT_DIR"
echo "Temp directory: $TEMP_DIR"
echo "---------------------------------------"
echo "UUID: $MY_UUID"
echo "---------------------------------------"

# 5. เริ่มประมวลผลราย Database
for DB in "${DATABASES[@]}"; do
    echo "Processing Database: $DB"
    
    RAW_COLLECT_FILE="$TEMP_DIR/raw_$DB.txt"
    > "$RAW_COLLECT_FILE"

    # ตัวแปรสำหรับเก็บจำนวนบรรทัดสะสม (เพื่อใช้ทำ Auto Verify)
    PREV_TOTAL=0

    CURRENT_DATE="$DATE_START"
    while [ "$CURRENT_DATE" -le "$DATE_END" ]; do
        FILES_PATTERN="$SOURCE_BASE/$CURRENT_DATE/$DB/*/*.sum"
        
        RECORDS_TODAY=0
        if ls $FILES_PATTERN >/dev/null 2>&1; then
            # 1. นับจำนวน Record ของวันนี้
            RECORDS_TODAY=$(cat $FILES_PATTERN 2>/dev/null | wc -l)
            
            # 2. รวมไฟล์
            cat $FILES_PATTERN >> "$RAW_COLLECT_FILE"
            
            # 3. คำนวณค่าที่ควรจะเป็น (Expected)
            EXPECTED_TOTAL=$((PREV_TOTAL + RECORDS_TODAY))
            
            # 4. นับค่าจากไฟล์จริง (Actual)
            ACTUAL_TOTAL=$(wc -l < "$RAW_COLLECT_FILE")

            # --- [ AUTO VERIFY LOGIC ] ---
            if [ "$ACTUAL_TOTAL" -ne "$EXPECTED_TOTAL" ]; then
                echo "  [ERROR] Data Integrity Mismatch on $CURRENT_DATE!"
                echo "          Expected: $EXPECTED_TOTAL records"
                echo "          Actual  : $ACTUAL_TOTAL records"
                echo "          Process terminated to prevent data corruption."
                exit 1
            fi
            # -----------------------------

            echo "  - $CURRENT_DATE: +$RECORDS_TODAY records (Total: $ACTUAL_TOTAL) [Verified OK]"
            PREV_TOTAL=$ACTUAL_TOTAL
        else
            echo "  - $CURRENT_DATE: No files found. (Total: $PREV_TOTAL) [Skip]"
        fi
        
        CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +"%Y%m%d")
    done

    # 6. ประมวลผลหา Latest Record (Group by $1|$2) และจัดเรียง
    FINAL_OUTPUT="$OUTPUT_DIR/summary_sum_$DB.txt"
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
        }' "$RAW_COLLECT_FILE" | sort -t'|' -k4 > "$FINAL_OUTPUT"
        
        FINAL_COUNT=$(wc -l < "$FINAL_OUTPUT")
        echo "  => DONE: $DB saved with $FINAL_COUNT unique records."
    fi
    echo "---------------------------------------"
done

# rm -rf "$TEMP_DIR"
echo "All tasks completed successfully!"