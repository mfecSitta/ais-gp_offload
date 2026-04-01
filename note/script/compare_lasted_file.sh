#!/bin/bash

# ==========================================
# 1. การจัดการ Parameter (Argument Handling)
# ==========================================
if [ "$#" -lt 2 ]; then
    echo "Error: Missing required arguments."
    echo "Usage: $0 <INPUT_PATH_1_RAW_LOG> <INPUT_PATH_2_FROM_VIEW> [MODE]"
    exit 1
fi

INPUT1=$1
INPUT2=$2
RUN_MODE=$3

# ดึงเฉพาะชื่อไฟล์ออกมาจาก Path เต็ม (เช่น summary.txt)
INPUT1_FILENAME=$(basename "$INPUT1")

# กำหนด Path
TMP_DIR="/data/AIS-EDW/gpoffload/temp_raw_log/note/log/tmp"
OUTPUT_DIR="/data/AIS-EDW/gpoffload/temp_raw_log/note/log"

# ตรวจสอบโหมดการรันเพื่อกำหนดชื่อและที่อยู่ไฟล์ Output
if [ "$RUN_MODE" == "gen" ]; then
    # ถ้าเป็นโหมด gen ให้ไปที่ OUTPUT_DIR และเติม distinct_ นำหน้าชื่อไฟล์เดิม
    FINAL_OUTPUT="$OUTPUT_DIR/distinct_$INPUT1_FILENAME"
else
    # ถ้าโหมดปกติ ให้ไปที่ TMP_DIR ตามชื่อเดิมที่ตั้งไว้
    FINAL_OUTPUT="$TMP_DIR/last_test1_from_bash.txt"
fi

echo "==========================================="
echo "   LOG PROCESSING & VALIDATION START"
echo "   Mode: ${RUN_MODE:-default}"
echo "   Target Output: $FINAL_OUTPUT"
echo "==========================================="

# ==========================================
# 2. ตรวจสอบ Path และไฟล์ต้นทาง (Validation)
# ==========================================
# ตรวจสอบ TMP_DIR (ใช้สำหรับโหมดปกติ)
if [ ! -d "$TMP_DIR" ]; then
    echo "❌ Error: TMP path not found: $TMP_DIR"
    exit 1
fi

# ตรวจสอบ OUTPUT_DIR (ใช้สำหรับโหมด gen)
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "❌ Error: Output path not found: $OUTPUT_DIR"
    exit 1
fi

if [ ! -f "$INPUT1" ]; then
    echo "❌ Error: Raw log file not found: $INPUT1"
    exit 1
fi

if [ "$RUN_MODE" != "gen" ]; then
    if [ ! -f "$INPUT2" ]; then
        echo "❌ Error: View export file not found: $INPUT2"
        exit 1
    fi
fi

echo "✅ All path and file validations passed."

# ==========================================
# 3. ส่วนการประมวลผลข้อมูล (Processing Section)
# ==========================================
echo -e "\n[STEP 1/2] Generating Output from Bash Logic..."

# แสดงคำสั่งที่ใช้สร้างไฟล์ (ใช้ตัวแปร FINAL_OUTPUT)
echo "CMD: cat \"$INPUT1\" | sort -t',' -k2,2 -k5,5r | awk -F',' '!seen[\$2]++' | sort -t',' -k5,5r > \"$FINAL_OUTPUT\""

# รันคำสั่งสร้างไฟล์
cat "$INPUT1" | sort -t',' -k2,2 -k5,5r | awk -F',' '!seen[$2]++' | sort -t',' -k5,5r > "$FINAL_OUTPUT"

if [ $? -eq 0 ]; then
    echo "  - Result: Output created successfully at $FINAL_OUTPUT"
else
    echo "  - Result: Failed to create output."
    exit 1
fi

# ==========================================
# 4. ส่วนการตรวจสอบความถูกต้อง (Validation Section)
# ==========================================
echo -e "\n[STEP 2/2] Validating Bash Logic..."

# ตรวจสอบจำนวนตารางที่ไม่ซ้ำ (เทียบกับไฟล์ FINAL_OUTPUT ที่เพิ่งสร้าง)
CLEAN_IN1_UNIQ=$(cat "$INPUT1" | cut -d',' -f2 | sort | uniq | wc -l)
OUT1_ROWS=$(wc -l < "$FINAL_OUTPUT")

echo "--- Output Integrity Check ---"
echo "  - Unique Tables in Raw Log: $CLEAN_IN1_UNIQ"
echo "  - Records in Bash Output:   $OUT1_ROWS"

if [ "$CLEAN_IN1_UNIQ" -eq "$OUT1_ROWS" ]; then
    echo "  - Result: [ PASS ]"
else
    echo "  - Result: [ FAIL ] (Unique tables count mismatch)"
fi

# ส่วน Cross-Check เทียบกับ View (ทำเฉพาะเมื่อไม่ใช่โหมด gen)
if [ "$RUN_MODE" != "gen" ]; then
    echo -e "\n--- Cross-Check: Bash Output vs Trustworthy View ---"
    echo "CMD: sort -t',' -k5,5r \"$INPUT2\" | diff - \"$FINAL_OUTPUT\""
    
    sort -t',' -k5,5r "$INPUT2" | diff - "$FINAL_OUTPUT" > /dev/null
    
    if [ $? -eq 0 ]; then
        echo "  - Result: [ PASS ] (Bash logic matches View exactly!)"
    else
        echo "  - Result: [ FAIL ] (Discrepancy found between Bash logic and View)"
        echo "    Tip: To see differences, manually run the CMD above without '> /dev/null'"
    fi
else
    echo -e "\n--- View Comparison Skipped (Gen Mode active) ---"
fi

echo -e "\n==========================================="
echo "             PROCESS COMPLETE"
echo "==========================================="