#!/bin/bash

# 1. ตรวจสอบ uuidgen ถ้าไม่มีให้ Error และ Exit ทันที
if ! command -v uuidgen > /dev/null 2>&1; then
    echo "Error: command 'uuidgen' not found. Please install it or contact admin."
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
else
    DATABASES=("prodgp" "misgp")
fi

# 4. กำหนด Path
OUTPUT_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/output"
SOURCE_BASE="/MNT/GP_DWS/encryption/stat_log"
TEMP_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/tmp/stat_log_process_$MY_UUID"

# เช็คและสร้าง Path (รวมถึง Temp Dir)
mkdir -p "$OUTPUT_DIR"
mkdir -p "$TEMP_DIR"

echo "Using UUID: $MY_UUID"
echo "Temp directory: $TEMP_DIR"

# 5. เริ่มประมวลผลราย Database
for DB in "${DATABASES[@]}"; do
    echo "Processing Database: $DB"
    
    RAW_COLLECT_FILE="$TEMP_DIR/raw_$DB.txt"
    > "$RAW_COLLECT_FILE"

    # วนลูปตามวันที่
    CURRENT_DATE="$DATE_START"
    while [ "$CURRENT_DATE" -le "$DATE_END" ]; do
        FILES_PATTERN="$SOURCE_BASE/$CURRENT_DATE/$DB/*/*.sum"
        
        if ls $FILES_PATTERN >/dev/null 2>&1; then
            cat $FILES_PATTERN >> "$RAW_COLLECT_FILE"
        fi
        
        # เพิ่มวันที่ขึ้น 1 วัน
        CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +"%Y%m%d")
    done

    # 6. ประมวลผลหา Latest Record (Group by $1|$2) และจัดเรียง
    FINAL_OUTPUT="$OUTPUT_DIR/summary_sum_$DB.txt"
    
    if [ -s "$RAW_COLLECT_FILE" ]; then
        awk -F'|' '
        {
            # key คือ Database + Schema.Table
            table_key = $1 "|" $2
            end_date = $4

            if (max_date[table_key] == "" || end_date >= max_date[table_key]) {
                max_date[table_key] = end_date
                full_record[table_key] = $0
            }
        }
        END {
            for (key in full_record) {
                print full_record[key]
            }
        }' "$RAW_COLLECT_FILE" | sort -t'|' -k4 > "$FINAL_OUTPUT"
        
        echo "  - Done: $FINAL_OUTPUT"
    else
        echo "  - No data found for $DB in the specified date range."
    fi
done

# 7. ลบไฟล์ชั่วคราว (Uncomment เพื่อใช้งาน)
# rm -rf "$TEMP_DIR"

echo "---------------------------------------"
echo "All tasks completed!"