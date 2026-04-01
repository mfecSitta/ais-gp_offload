#!/bin/bash

# --- 1. Setup Variables & Validation ---

now_date=$(date +%Y%m%d_%H%M%S)

# รับค่า Input
START_DATE=$1
END_DATE=$2

# Function แสดงวิธีใช้
usage() {
    echo "Usage: $0 <START_DATE> <END_DATE>"
    echo "Format: YYYYMMDD (e.g., 20260214)"
    exit 1
}

# ตรวจสอบจำนวน Argument
if [ "$#" -ne 2 ]; then
    echo "❌ Error: Missing parameters."
    usage
fi

# Function ตรวจสอบวันที่
check_date() {
    local input_date=$1
    if ! [[ "$input_date" =~ ^[0-9]{8}$ ]]; then
        echo "❌ Error: Invalid format '$input_date'. Please use YYYYMMDD."
        exit 1
    fi
    date -d "$input_date" >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "❌ Error: Date '$input_date' is invalid (does not exist)."
        exit 1
    fi
}

check_date "$START_DATE"
check_date "$END_DATE"

if [ "$START_DATE" -gt "$END_DATE" ]; then
    echo "❌ Error: Start date ($START_DATE) cannot be after End date ($END_DATE)."
    exit 1
fi

# --- 2. Prepare Output Paths ---

path_log_output="/data/AIS-EDW/gpoffload/temp_raw_log/note/log/"
# ตั้งชื่อไฟล์โดยใช้ Start-End date เพื่อความชัดเจน หรือใช้ logic เดิมก็ได้
file_name="summary_log_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"
FULL_OUTPUT_PATH="${path_log_output}${file_name}"


# --- เพิ่มส่วนเช็คความปลอดภัย (Safety Check) ---
if [ -z "${file_name}" ] || [ -z "${path_log_output}" ]; then
    echo "❌ CRITICAL ERROR: Output path or filename is empty!"
    exit 1
fi

if [ -z "${FULL_OUTPUT_PATH}" ]; then
    echo "❌ CRITICAL ERROR: Full output path is empty!"
    exit 1
fi

# ==============================================================================


# [NEW] 🔎 PART 1: Find Previous Log & Count Lines (ทำก่อนเคลียร์ไฟล์ใหม่)
# ==============================================================================
echo "Checking for previous logs..."

# Pattern เพื่อหาไฟล์ของช่วงวันที่เดียวกัน (ไม่สนเวลา run)
SEARCH_PATTERN="${path_log_output}summary_log_${START_DATE}_to_${END_DATE}_run_*.txt"

# ค้นหาไฟล์ล่าสุด (ls -t เรียงตามเวลาใหม่สุด, head -n 1 เอาไฟล์บนสุด)
# 2>/dev/null คือถ้าไม่เจอไฟล์เลย ไม่ต้องฟ้อง error
PREV_LOG_FILE=$(ls -t $SEARCH_PATTERN 2>/dev/null | head -n 1)

PREV_LOG_LINES=0
HAS_PREV_LOG=false

if [ -n "$PREV_LOG_FILE" ]; then
    echo "✅ Found previous log: $PREV_LOG_FILE"
    # นับบรรทัดไฟล์เก่าเก็บไว้ในตัวแปร
    PREV_LOG_LINES=$(wc -l < "$PREV_LOG_FILE")
    HAS_PREV_LOG=true
    echo "   Previous lines: $PREV_LOG_LINES"
else
    echo "⚠️ No previous log found (This might be the first run)."
fi
# ==============================================================================

# เคลียร์ไฟล์เก่าทิ้งก่อน (ถ้ามี) เพื่อเริ่มเขียนใหม่
> "${FULL_OUTPUT_PATH}"

echo "path_log_output: ${path_log_output}"
echo "file_name: ${file_name}"
echo "✅ Input verified. Processing..."
echo "--------------------------------"

# ตัวแปรสำหรับเก็บยอดสะสมบรรทัดที่ควรจะเป็น (Expected Total)
GRAND_TOTAL_EXPECTED_LINES=0

CURRENT_DATE="$START_DATE"

# --- 3. Main Loop ---

while [ "$CURRENT_DATE" -le "$END_DATE" ]; do
    
    echo "Processing date: $CURRENT_DATE"
    
    # Path ของวันที่ปัจจุบัน
    path_log_run="/data/AIS-EDW/gpoffload/log/${CURRENT_DATE}/*/*.csv"
    
    # ตรวจสอบก่อนว่ามีไฟล์หรือไม่ เพื่อกัน Error
    # ใช้ ls เพื่อเช็คว่ามีไฟล์ไหม (Redirect error to null)
    if ls ${path_log_run} 1> /dev/null 2>&1; then
        
        # --- A. ส่วนประมวลผล (Processing) ---
        # ใช้ sed -s '1d' เพื่อลบบรรทัดแรกของ 'ทุกไฟล์' แล้ว append ใส่ไฟล์รวม
        # (Uncomment บรรทัดนี้เพื่อให้ทำงานจริง)
        echo "sed -s '1d' ${path_log_run} >> \"${FULL_OUTPUT_PATH}\""
        sed -s '1d' ${path_log_run} >> "${FULL_OUTPUT_PATH}"
        
        # --- B. ส่วนคำนวณยอด (Calculation Logic) ---
        # 1. นับจำนวนบรรทัดทั้งหมดของ Source (รวม Header)
        # ใช้ cat | wc -l เพื่อป้องกัน Argument list too long กรณีไฟล์เยอะมาก
        current_day_raw_lines=$(cat ${path_log_run} | wc -l)
        
        # 2. นับจำนวนไฟล์ (เพื่อรู้ว่าต้องลบ Header ออกกี่บรรทัด)
        current_day_file_count=$(ls -1 ${path_log_run} | wc -l)
        
        # 3. คำนวณยอดเนื้อหา (Raw - Headers)
        current_day_data_lines=$((current_day_raw_lines - current_day_file_count))
        
        echo "  - Files found: $current_day_file_count"
        echo "  - Raw lines: $current_day_raw_lines"
        echo "  - Expected data lines (Raw - Headers): $current_day_data_lines"
        
        # 4. บวกสะสมเข้ายอดรวมใหญ่
        GRAND_TOTAL_EXPECTED_LINES=$((GRAND_TOTAL_EXPECTED_LINES + current_day_data_lines))
        
    else
        echo "  - No CSV files found for this date."
    fi

    # เลื่อนวันถัดไป
    CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +%Y%m%d)
    echo ""
done

# --- 4. Final Verification ---

echo "=============================================================="
echo "Summary Verification"
echo ""

# นับจำนวนบรรทัดในไฟล์ผลลัพธ์สุดท้าย
ACTUAL_OUTPUT_LINES=$(wc -l < "${FULL_OUTPUT_PATH}")

echo "1. Grand Total Expected (Source - Headers): ${GRAND_TOTAL_EXPECTED_LINES}"
echo "2. Actual Lines in Output File            : ${ACTUAL_OUTPUT_LINES}"

# เปรียบเทียบ Source vs Output
if [ "$GRAND_TOTAL_EXPECTED_LINES" -eq "$ACTUAL_OUTPUT_LINES" ]; then
    status_export="COMPARE PASS ✅"
else
    status_export="COMPARE FAIL ❌"
fi

echo ""
echo "Output file: ${FULL_OUTPUT_PATH}"
echo "Status: ${status_export}"

# ==============================================================================
# [NEW] 🔎 PART 2: Compare with Previous Run
# ==============================================================================
echo "--------------------------------------------------------------"
echo "Historical Comparison (Previous vs Current)"

if [ "$HAS_PREV_LOG" = true ]; then
    echo "   - Previous Run Lines       : ${PREV_LOG_LINES}"
    echo "   - Current Run Lines        : ${ACTUAL_OUTPUT_LINES}"
    
    # คำนวณส่วนต่าง
    DIFF=$((ACTUAL_OUTPUT_LINES - PREV_LOG_LINES))
    
    if [ "$ACTUAL_OUTPUT_LINES" -eq "$PREV_LOG_LINES" ]; then
        echo "   - Status: [MATCH] ✅ (Data is identical to previous run)"
    else
        echo "   - Status: [DIFF] ⚠️ (Difference: $DIFF lines)"
    fi
else
    echo "   - Status: [SKIP] (No previous file to compare)"
fi
echo "=============================================================="

# ถ้า Fail ให้ exit code เป็น error (เผื่อเอาไปใช้ต่อใน Job Scheduler)
if [ "$GRAND_TOTAL_EXPECTED_LINES" -ne "$ACTUAL_OUTPUT_LINES" ]; then
    exit 1
fi