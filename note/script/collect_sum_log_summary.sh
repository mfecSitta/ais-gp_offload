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
SCHEMA_INPUT=$4  # Parameter ตัวที่ 4: Schema

if [[ -z "$DATE_START" || -z "$DATE_END" ]]; then
    echo "Usage: $0 <YYYYMMDD_START> <YYYYMMDD_END> [database] [schema]"
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

# 4. กำหนด Schema Pattern
if [[ -n "$SCHEMA_INPUT" ]]; then
    SCHEMA_PATTERN="$SCHEMA_INPUT"
    echo "Schema specified: $SCHEMA_INPUT"
else
    SCHEMA_PATTERN="*"
    echo "No schema specified. Using wildcard (*)"
fi

# 5. กำหนด Path
OUTPUT_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/output"
SOURCE_BASE="/MNT/GP_DWS/encryption/stat_log"
TEMP_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/tmp/stat_log_process_$MY_UUID"

ensure_dir "$OUTPUT_DIR"
ensure_dir "$TEMP_DIR"

echo "Starting log collection and processing..."
echo "Date Range: $DATE_START to $DATE_END"
echo "---------------------------------------"
echo "UUID: $MY_UUID"
echo "---------------------------------------"

# 6. เริ่มประมวลผลราย Database
for DB in "${DATABASES[@]}"; do
    echo "Processing Database: $DB"
    
    RAW_COLLECT_FILE="$TEMP_DIR/raw_$DB.txt"
    > "$RAW_COLLECT_FILE"

    PREV_TOTAL=0
    CURRENT_DATE="$DATE_START"

    while [ "$CURRENT_DATE" -le "$DATE_END" ]; do
        # ใช้ SCHEMA_PATTERN ใน Path (ถ้าไม่ระบุจะเป็น * ถ้าระบุจะเป็นชื่อ schema)
        FILES_PATTERN="$SOURCE_BASE/$CURRENT_DATE/$DB/$SCHEMA_PATTERN/*.sum"
        
        RECORDS_TODAY=0
        # ใช้พจนานุกรม ls เพื่อเช็คไฟล์
        if ls $FILES_PATTERN >/dev/null 2>&1; then
            # 1. นับจำนวน Record ของวันนี้
            RECORDS_TODAY=$(cat $FILES_PATTERN 2>/dev/null | wc -l)
            
            # 2. รวมไฟล์
            cat $FILES_PATTERN >> "$RAW_COLLECT_FILE"
            
            # 3. คำนวณค่าที่ควรจะเป็น และนับค่าจริง
            EXPECTED_TOTAL=$((PREV_TOTAL + RECORDS_TODAY))
            ACTUAL_TOTAL=$(wc -l < "$RAW_COLLECT_FILE")

            # --- [ AUTO VERIFY LOGIC ] ---
            if [ "$ACTUAL_TOTAL" -ne "$EXPECTED_TOTAL" ]; then
                echo "  [ERROR] Data Integrity Mismatch on $CURRENT_DATE!"
                echo "          Expected: $EXPECTED_TOTAL | Actual: $ACTUAL_TOTAL"
                exit 1
            fi
            # -----------------------------

            echo "  - $CURRENT_DATE: +$RECORDS_TODAY records (Total: $ACTUAL_TOTAL) [OK]"
            PREV_TOTAL=$ACTUAL_TOTAL
        else
            echo "  - $CURRENT_DATE: No files found for database [$DB] and schema [$SCHEMA_PATTERN] [Skip]"
        fi
        
        CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +"%Y%m%d")
    done

    # 7. ประมวลผลหา Latest Record และจัดเรียง
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
        }' "$RAW_COLLECT_FILE" | sort -t'|' -k4r > "$FINAL_OUTPUT"
        
        FINAL_COUNT=$(wc -l < "$FINAL_OUTPUT")
        echo "  => DONE: $DB saved with $FINAL_COUNT unique records."
    fi
    echo "---------------------------------------"
done

# rm -rf "$TEMP_DIR"
echo "All tasks completed successfully!"