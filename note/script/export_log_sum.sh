#!/bin/bash

# 1. รับค่า Parameter
DATE_START=$1
DATE_END=$2
DB_INPUT=$3

if [[ -z "$DATE_START" || -z "$DATE_END" ]]; then
    echo "Usage: $0 <YYYYMMDD_START> <YYYYMMDD_END> [database]"
    exit 1
fi

# 2. กำหนด Databases
if [[ -n "$DB_INPUT" ]]; then
    DATABASES=("$DB_INPUT")
else
    DATABASES=("prodgp" "misgp")
fi

# 3. กำหนด Path
OUTPUT_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/output"
SOURCE_BASE="/MNT/GP_DWS/encryption/stat_log"
TEMP_DIR="/MNT/GP_DWS/encryption/script_encryption/temp/collect_result/tmp/stat_log_process_$$"

mkdir -p "$OUTPUT_DIR"
mkdir -p "$TEMP_DIR"

# 4. เริ่มประมวลผลราย Database
for DB in "${DATABASES[@]}"; do
    echo "Processing Database: $DB"
    
    RAW_COLLECT_FILE="$TEMP_DIR/raw_$DB.txt"
    > "$RAW_COLLECT_FILE"

    # วนลูปตามวันที่เพื่อรวบรวมข้อมูลดิบ (Raw Data) ทั้งหมดก่อน
    CURRENT_DATE="$DATE_START"
    while [ "$CURRENT_DATE" -le "$DATE_END" ]; do
        FILES_PATTERN="$SOURCE_BASE/$CURRENT_DATE/$DB/*/*.sum"
        
        # ตรวจสอบและรวมไฟล์
        if ls $FILES_PATTERN >/dev/null 2>&1; then
            cat $FILES_PATTERN >> "$RAW_COLLECT_FILE"
        fi
        
        CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +"%Y%m%d")
    done

    # 5. ประมวลผลหา Latest Record และจัดเรียง
    # logic: 
    # - ใช้ awk โดยมี delimiter คือ |
    # - สร้าง associative array 'latest' เก็บสายอักขระทั้งบรรทัด โดยใช้ Column 2 (Schema.Table) เป็น key
    # - ถ้า Column 4 (End Date) ของบรรทัดปัจจุบัน ใหม่กว่าค่าที่เก็บไว้ ให้ update
    # - สุดท้าย sort ตาม Column 4
    
    FINAL_OUTPUT="$OUTPUT_DIR/summary_sum_$DB.txt"
    
    if [ -s "$RAW_COLLECT_FILE" ]; then
        awk -F'|' '
        {
            # กำหนด key โดยการรวม Database ($1) และ Schema.Table ($2)
            # ใช้ | เป็นตัวคั่นเพื่อความแม่นยำ
            table_key = $1 "|" $2
            end_date = $4

            # ตรวจสอบหาค่าวันที่ล่าสุด
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
        
        echo "  - Done: $FINAL_OUTPUT (created with latest records sorted by End Date)"
    else
        echo "  - No data found for $DB in the specified date range."
    fi
done


#rm -rf "$TEMP_DIR"
echo "---------------------------------------"
echo "All tasks completed!"