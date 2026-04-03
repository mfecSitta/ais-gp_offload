#!/bin/bash

# --- 1. Setup Variables & Validation ---
now_date=$(date +%Y%m%d_%H%M%S)
RUN_DATE=$(date +%Y%m%d)
START_DATE=$1
END_DATE=$2
SCRIPT_NAME=$(basename "$0")
UUID=$(uuidgen)

# --- ANSI Color Codes ---
RED='\033[0;31m'
GREEN='\033[38;5;28m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color (ล้างค่าสีกลับเป็นปกติ)

# -- greenplum -- #
DB_NAME="prodgp"


# --- Control Flags (กำหนดเป็น Y เพื่อเปิด หรือ N เพื่อปิด) ---
RUN_OFFLOAD="Y"
RUN_REC_PQ="Y"
RUN_REC_GP="Y"
RUN_COMP_CONTENT="Y"
RUN_COMP_COUNT="Y"


run_psql_command() {
    local sql_query="$1"
    local output
    local exit_code

    output=$(psql -v ON_ERROR_STOP=1 -d "$DB_NAME" -c "$sql_query" 2>"$ERR_FILE")
    exit_code=$?

    local error_msg=$(cat "$ERR_FILE")
    #rm -f "$ERR_FILE"

    #if [ $exit_code -eq 0 ]; then
    #    #echo -e "    -> ${GREEN}[SUCCESS]${NC}: Successfully ran query: ${sql_query}"
    #    #printf "    -> ${GREEN}[SUCCESS]${NC}: Successfully ran query: %s\n" "$sql_query"
    #    printf "    ${GREEN}✔${NC} ${BLUE}[SQL]${NC}: %s\n" "$sql_query"
    #    printf "      ${BLUE}[Result]${NC}: %s\n" "${output:-Success}"
    #    return 0
    #else
    #    #echo -e "    -> ${RED}[ERROR]${NC}: Failed to run query: ${sql_query}"
    #    printf "    ${RED}✘${NC} ${BLUE}[SQL]${NC}: %s\n" "$sql_query"
    #    printf "       ${RED}>${NC} Exit Code: %s\n" "$exit_code"
    #    printf "       ${RED}>${NC} Error details have been logged to: %s\n" "$ERR_FILE"
    #    printf "       ${RED}> Error:${NC} %s\n" "$error_msg"
    #    return 1
    #fi

    if [ $exit_code -eq 0 ]; then
        printf "   ${BLUE}│  └─${NC} ${GREEN}✔${NC} [SQL] : %s\n" "$sql_query"
        printf "   ${BLUE}│     └─${NC} Result: ${YELLOW}%s${NC}\n" "${output:-Success}"
        return 0
    else
        printf "   ${BLUE}│  └─${NC} ${RED}✘${NC} [SQL] : %s\n" "$sql_query"
        printf "   ${BLUE}│     └─${NC} ${RED}>${NC} Error details have been logged to: %s\n" "$ERR_FILE"
        printf "   ${BLUE}│     └─${NC} ${RED}ERROR Message:${NC}: %s\n" "$error_msg"
        return 1
    fi


}

# --- Function สำหรับตรวจสอบและสร้าง Folder ---
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
path_file_tmp="/data/AIS-EDW/gpoffload/temp_raw_log/note/script/tmp/"


check_and_create_dir "$path_log_output"
check_and_create_dir "$path_file_tmp"


# กำหนดชื่อไฟล์ Output แยกตามประเภท
FILE_OFFLOAD="summary_offloadgp_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"
FILE_REC_PQ="summary_rec_pq_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"
FILE_REC_GP="summary_rec_gp_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"
FILE_COMP_CONTENT="summary_comp_content_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"
FILE_COMP_COUNT="summary_comp_count_${START_DATE}_to_${END_DATE}_run_${now_date}.txt"

FULL_PATH_OFFLOAD="${path_log_output}${FILE_OFFLOAD}"
FULL_PATH_REC_PQ="${path_log_output}${FILE_REC_PQ}"
FULL_PATH_REC_GP="${path_log_output}${FILE_REC_GP}"
FULL_PATH_COMP_CONTENT="${path_log_output}${FILE_COMP_CONTENT}"
FULL_PATH_COMP_COUNT="${path_log_output}${FILE_COMP_COUNT}"

TOTAL_EXP_OFFLOAD=0
TOTAL_EXP_REC_PQ=0
TOTAL_EXP_REC_GP=0
TOTAL_EXP_COMP_CONTENT=0
TOTAL_EXP_COMP_COUNT=0

ERR_FILE="${path_file_tmp}${SCRIPT_NAME}_${START_DATE}_${END_DATE}_${UUID}.tmp"

# ตรวจสอบ Path เบื้องต้นเฉพาะส่วนที่เปิดใช้งาน เคลียร์ไฟล์เก่าและตัวแปรสะสม
if [ "$RUN_OFFLOAD" == "Y" ]; then > "$FULL_PATH_OFFLOAD"; fi
if [ "$RUN_REC_PQ" == "Y" ]; then > "$FULL_PATH_REC_PQ"; fi
if [ "$RUN_REC_GP" == "Y" ]; then > "$FULL_PATH_REC_GP"; fi
if [ "$RUN_COMP_CONTENT" == "Y" ]; then > "$FULL_PATH_COMP_CONTENT"; fi
if [ "$RUN_COMP_COUNT" == "Y" ]; then > "$FULL_PATH_COMP_COUNT"; fi

echo -e "${GREEN}[OK]${NC} Processing started for period: $START_DATE to $END_DATE"
echo "Status: Offload=$RUN_OFFLOAD, RecPQ=$RUN_REC_PQ, RecGP=$RUN_REC_GP, CompContent=$RUN_COMP_CONTENT, CompCount=$RUN_COMP_COUNT"
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
            echo "   -> Processing Offload GP (Full Append & Adding DB_NAME at start, FULL_PATH at end)..."

            #sed -s '1d' ${SRC_OFFLOAD} >> "$FULL_PATH_OFFLOAD" # old process for local
            #cat ${SRC_OFFLOAD} >> "$FULL_PATH_OFFLOAD"

            # a[5]     = database_name (ดึงมาจากลำดับที่ 5 ใน path)
            # $0       = ข้อมูลทั้งหมดในบรรทัดนั้น (Original Data)
            # FILENAME = Full Path ของไฟล์นั้นๆ เช่น /MNT/GP_DWS/mig_stat_log/DB_A/20260403/offloadgp_01.csv
            awk -F, -v OFS=',' '{ 
                split(FILENAME, a, "/"); 
                print a[5], $0, FILENAME 
            }' ${SRC_OFFLOAD} >> "$FULL_PATH_OFFLOAD"
        
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

            echo "   -> Processing Reconcile PQ (Full & Adding DB_NAME at start, FULL_PATH at end)..."
            #cat ${SRC_REC_PQ} >> "$FULL_PATH_REC_PQ"

            # ใช้ awk เพื่อพิมพ์ข้อมูลทั้งบรรทัด ($0) ตามด้วย Full Path (FILENAME)
            # ปรับ -F และ OFS เป็น ',' ( ',' ตามไฟล์ต้นฉบับ)
            #awk -F',' -v OFS=',' '{ print $0, FILENAME }' ${SRC_REC_PQ} >> "$FULL_PATH_REC_PQ"

            # คำอธิบาย Index จาก Path ตัวอย่างของคุณ:
            # / [1] MNT [2] GP_DWS [3] mig_reconcile_query_parquet_output [4] 20260325 [5] prodgp [6] ...
            # ดังนั้น database_name คือ a[6]
            #awk -F',' -v OFS=',' '{ 
            #    split(FILENAME, a, "/"); 
            #    print a[6], $0, FILENAME 
            #}' ${SRC_REC_PQ} >> "$FULL_PATH_REC_PQ"

            # 1. ลบตัว \r (Carriage Return) ที่อาจติดมาท้ายบรรทัด
            # 2. แยก Path เพื่อหา Database Name (ลำดับที่ 6)
            # 3. พิมพ์: [DB_Name],[ข้อมูลเดิมทุกคอลัมน์],[Full_Path]
            # ใช้ OFS="," เพื่อคุมตัวคั่นตอนออกให้ชัดเจน
            awk -F',' -v OFS=',' '{ 
                sub(/\r$/, "", $0); 
                split(FILENAME, a, "/"); 
                print a[6], $0, FILENAME 
            }' ${SRC_REC_PQ} >> "$FULL_PATH_REC_PQ"

        
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

            echo "   -> Processing Reconcile GP (Full & Adding FULL_PATH at end)..."
            #cat ${SRC_REC_GP} >> "$FULL_PATH_REC_GP"

            # ใช้ awk เพื่อพิมพ์ข้อมูลทั้งบรรทัด ($0) ตามด้วย Full Path (FILENAME)
            #awk -F',' -v OFS=',' '{ print $0, FILENAME }' ${SRC_REC_GP} >> "$FULL_PATH_REC_GP"

            # คำอธิบาย Index จาก Path ตัวอย่างของคุณ:
            # / [1] MNT [2] GP_DWS [3] mig_reconcile_query_parquet_output [4] 20260325 [5] prodgp [6] ...
            # ดังนั้น database_name คือ a[6]
            # 1. ลบตัว \r (Carriage Return) ที่อาจติดมาท้ายบรรทัด
            # 2. แยก Path เพื่อหา Database Name (ลำดับที่ 6)
            # 3. พิมพ์: [DB_Name],[ข้อมูลเดิมทุกคอลัมน์],[Full_Path]
            # ใช้ OFS="," เพื่อคุมตัวคั่นตอนออกให้ชัดเจน
            awk -F',' -v OFS=',' '{
                sub(/\r$/, "", $0);
                split(FILENAME, a, "/"); 
                print a[6], $0, FILENAME 
            }' ${SRC_REC_GP} >> "$FULL_PATH_REC_GP"

            data_ln=$(cat ${SRC_REC_GP} | wc -l)
            TOTAL_EXP_REC_GP=$((TOTAL_EXP_REC_GP + data_ln))
        else
            echo -e "${YELLOW}![MISSING]! -> No Reconcile GP files found for $CURRENT_DATE${NC}"
        fi
    fi

    # --- SOURCE 4: Compare Content ---
    if [ "$RUN_COMP_CONTENT" == "Y" ]; then
        echo "-------"
        #SRC_COMP_CONTENT="/MNT/GP_DWS/mig_compare_output/${CURRENT_DATE}/*/*/stat_csv/log_stat*.csv"
        SRC_COMP_CONTENT="/MNT/GP_DWS/mig_reconcile_stat_log/*/${CURRENT_DATE}/Compare_Detail*.csv"
        echo "SRC_COMP_CONTENT: ${SRC_COMP_CONTENT}"
        if ls ${SRC_COMP_CONTENT} 1> /dev/null 2>&1; then

            echo "   -> Processing Compare Content (Removing Headers & Adding FULL_PATH)..."

            #sed -s '1d' ${SRC_COMP_CONTENT} >> "$FULL_PATH_COMP_CONTENT"

            # ใช้ awk แทน sed เพื่อลบ Header และเพิ่ม FILENAME (Full Path) ไว้หลังสุด
            # -F'|' และ OFS='|' กรณีใช้ Pipe เป็นตัวคั่น
            # FNR > 1 คือการข้ามบรรทัดแรกของ 'ทุกไฟล์' ที่เจอใน wildcard
            # 1. ลบตัว \r ที่ท้ายบรรทัดออก เพื่อไม่ให้ Cursor ดีดกลับไปทับบรรทัดเดิม
            awk -F'|' -v OFS='|' 'FNR > 1 {
                sub(/\r$/, "", $0);
                print $0, FILENAME;
            }' ${SRC_COMP_CONTENT} >> "$FULL_PATH_COMP_CONTENT"

            #raw_ln=$(cat ${SRC_COMP_CONTENT} | wc -l)
            #f_cnt=$(ls -1 ${SRC_COMP_CONTENT} | wc -l)
            #data_ln=$((raw_ln - f_cnt))

            # นับจำนวนบรรทัดที่ Export จริง (เฉพาะบรรทัดข้อมูล)
            # ใช้ awk ตัวเดิมนับจำนวนบรรทัดที่ผ่านเงื่อนไข FNR > 1
            data_ln=$(awk 'FNR > 1 {count++} END {print count+0}' ${SRC_COMP_CONTENT})

            TOTAL_EXP_COMP_CONTENT=$((TOTAL_EXP_COMP_CONTENT + data_ln))
        else
            echo -e "${YELLOW}![MISSING]! -> No Compare Content files found for $CURRENT_DATE${NC}"
        fi
    fi

    # --- SOURCE 5: Compare Count ---
    if [ "$RUN_COMP_COUNT" == "Y" ]; then
        echo "-------"
        #SRC_COMP_COUNT="/MNT/GP_DWS/mig_compare_output/${CURRENT_DATE}/*/*/stat_csv/log_stat*.csv"
        SRC_COMP_COUNT="/MNT/GP_DWS/mig_reconcile_stat_log/*/${CURRENT_DATE}/Compare_Header*.csv"
        echo "SRC_COMP_COUNT: ${SRC_COMP_COUNT}"
        if ls ${SRC_COMP_COUNT} 1> /dev/null 2>&1; then

            echo "   -> Processing Compare Count (Removing Headers & Adding FULL_PATH)..."

            #sed -s '1d' ${SRC_COMP_COUNT} >> "$FULL_PATH_COMP_COUNT"

            # ใช้ awk แทน sed เพื่อลบ Header และเพิ่ม FILENAME (Full Path) ไว้หลังสุด
            # -F'|' และ OFS='|' กรณีใช้ Pipe เป็นตัวคั่น
            # FNR > 1 คือการข้ามบรรทัดแรกของ 'ทุกไฟล์' ที่เจอใน wildcard
            # 1. ลบตัว \r ที่ท้ายบรรทัดออก เพื่อไม่ให้ Cursor ดีดกลับไปทับบรรทัดเดิม
            awk -F'|' -v OFS='|' 'FNR > 1 {
                sub(/\r$/, "", $0);
                print $0, FILENAME;
            }' ${SRC_COMP_COUNT} >> "$FULL_PATH_COMP_COUNT"

            #raw_ln=$(cat ${SRC_COMP_COUNT} | wc -l) 
            #f_cnt=$(ls -1 ${SRC_COMP_COUNT} | wc -l) 
            #data_ln=$((raw_ln - f_cnt)) 

            # นับจำนวนบรรทัดที่ Export จริง (เฉพาะบรรทัดข้อมูล)
            # ใช้ awk ตัวเดิมนับจำนวนบรรทัดที่ผ่านเงื่อนไข FNR > 1
            data_ln=$(awk 'FNR > 1 {count++} END {print count+0}' ${SRC_COMP_COUNT})

            TOTAL_EXP_COMP_COUNT=$((TOTAL_EXP_COMP_COUNT + data_ln))
        else
            echo -e "${YELLOW}![MISSING]! -> No Compare Count files found for $CURRENT_DATE${NC}"
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


#[ -n "$FULL_PATH_OFFLOAD" ] && [ -f "$FULL_PATH_OFFLOAD" ] && [ ! -s "$FULL_PATH_OFFLOAD" ] && rm "$FULL_PATH_OFFLOAD"

echo "=============================================================="
echo "VERIFICATION SUMMARY"
echo "=============================================================="
verify_result "OFFLOAD GP" "$TOTAL_EXP_OFFLOAD" "$FULL_PATH_OFFLOAD" "$RUN_OFFLOAD"
verify_result "REC PARQUET" "$TOTAL_EXP_REC_PQ" "$FULL_PATH_REC_PQ" "$RUN_REC_PQ"
verify_result "REC GP" "$TOTAL_EXP_REC_GP" "$FULL_PATH_REC_GP" "$RUN_REC_GP"
verify_result "COMPARE CONTENT" "$TOTAL_EXP_COMP_CONTENT" "$FULL_PATH_COMP_CONTENT" "$RUN_COMP_CONTENT"
verify_result "COMPARE COUNT" "$TOTAL_EXP_COMP_COUNT" "$FULL_PATH_COMP_COUNT" "$RUN_COMP_COUNT"

echo "[Summary Files]"
echo "files exported to: $path_log_output"
echo "File offload: $FULL_PATH_OFFLOAD"
echo "File rec parquet: $FULL_PATH_REC_PQ"
echo "File rec GP: $FULL_PATH_REC_GP"
echo "File compare content: $FULL_PATH_COMP_CONTENT"
echo "File compare count: $FULL_PATH_COMP_COUNT"
echo "=============================================================="


# --- 5. Truncate and upload file to GP (Mapped to specific tables) ---
echo ""
echo "=============================================================="
echo "🗄️ DATABASE UPLOAD PROCESS (Mapping Mode)"
echo "=============================================================="
echo ""

# Function สำหรับ Truncate และ \copy ราย Table
upload_to_specific_table() {
    local file_path="$1"
    local table_name="$2"
    local label="$3"
    local flag="$4"

    if [ "$flag" == "Y" ]; then
        echo -e "\n${BLUE}│${NC}"
        printf "${BLUE}│  ${YELLOW}%s${NC}\n" "$label"
        echo -e "${BLUE}├───────────────────────────────────${NC}"

        #echo -e "      Table : $table_name"
        #echo "Processing [$label] -> Table: $table_name"
        
        # 1. ตรวจสอบว่าไฟล์มีข้อมูลไหม
        #if [ ! -s "$file_path" ]; then
        #    echo -e "      -> ${YELLOW}[SKIPPED]${NC} File is empty or not found: $file_path"
        #    return
        #fi

        # 1. ตรวจสอบว่าไฟล์มีข้อมูลไหม
        if [ ! -s "$file_path" ]; then
            echo -e "${BLUE}│${NC}  ${YELLOW}[SKIPPED]${NC} File is empty or not found: $file_path"
            echo -e "${BLUE}└───────────────────────────────────${NC}"
            return
        fi

        # 2. TRUNCATE เฉพาะตารางที่เกี่ยวข้อง
        #echo -n "      Truncating... "
        printf "${BLUE}│${NC}  Target : ${BLUE}%s${NC}\n" "$table_name"

        #run_psql_command "TRUNCATE TABLE $table_name;"
        #if [ $? -ne 0 ]; then
        #    echo -e "      -> ${RED}[ERROR]${NC} Truncate failed. Skipping upload for this table."
        #    return 1
        #fi
        echo -e "${BLUE}├─${NC} Action : Truncating..."
        run_psql_command "TRUNCATE TABLE $table_name;" || return 1

        # 3. \copy ข้อมูลเข้าตาราง
        #echo "   -> Uploading data..."
        #echo -n "      Uploading data..."
        #psql -v ON_ERROR_STOP=1 -d "$DB_NAME" -c "\copy $table_name FROM '$file_path' CSV QUOTE '\"';" 2>"$ERR_FILE"
        # สร้างตัวแปรเก็บคำสั่ง SQL (ต้อง Double Backslash ที่หน้า copy เพื่อกัน Bash แย่งใช้)

        # 3. \copy ข้อมูลเข้าตาราง
        echo -e "${BLUE}├─${NC} Action : Uploading data..."
        #local copy_cmd="\\copy $table_name FROM '$file_path' CSV QUOTE '\"';"

        if [[ "$label" == "COMPARE CONTENT" ]] || [[ "$label" == "COMPARE COUNT" ]]; then
            # สำหรับงาน Compare ใช้ Delimiter เป็น Pipe |
            local copy_cmd="\\copy $table_name FROM '$file_path' CSV DELIMITER '|' QUOTE '\"';"
        else
            # สำหรับงานอื่นๆ ใช้ Default (Comma)
            local copy_cmd="\\copy $table_name FROM '$file_path' CSV QUOTE '\"';"
        fi

        run_psql_command "$copy_cmd" || return 1

        #if [ $? -eq 0 ]; then
        #    echo -e "      ${GREEN}✔${NC} Uploaded to $table_name completed."
        #else
        #    echo -e "      ${RED}✘${NC} Upload to $table_name failed."
        #    return 1
        #fi

        echo -e "${BLUE}└───────────────────────────────────${NC}"

    else
        echo "[$label] -> Flag is 'N', skipping."
    fi
}

# --- Execution Section ---

# 1. OFFLOAD GP -> gpoffload.log_detail
upload_to_specific_table "$FULL_PATH_OFFLOAD" "gpoffload.log_detail" "OFFLOAD GP" "$RUN_OFFLOAD"
if [ $? -eq 0 ]; then
    echo -e "   -> ${GREEN}[INFO]${NC} OFFLOAD GP upload process completed."
    echo ""
else
    echo -e "   -> ${RED}[ERROR]${NC} OFFLOAD GP upload process encountered errors."
    exit 1
fi

# 2. REC PARQUET -> gpoffload.log_query_pq
upload_to_specific_table "$FULL_PATH_REC_PQ" "gpoffload.log_query_pq" "REC PARQUET" "$RUN_REC_PQ"
if [ $? -eq 0 ]; then
    echo -e "   -> ${GREEN}[INFO]${NC} REC PARQUET upload process completed."
    echo ""
else
    echo -e "   -> ${RED}[ERROR]${NC} REC PARQUET upload process encountered errors."
    exit 1
fi

# 3. REC GP -> gpoffload.log_query_gp
upload_to_specific_table "$FULL_PATH_REC_GP" "gpoffload.log_query_gp" "REC GP" "$RUN_REC_GP"
if [ $? -eq 0 ]; then
    echo -e "   -> ${GREEN}[INFO]${NC} REC GP upload process completed."
    echo ""
else
    echo -e "   -> ${RED}[ERROR]${NC} REC GP upload process encountered errors."
    exit 1
fi

# 4. COMPARE CONTENT -> gpoffload.log_reconcile_result_content
upload_to_specific_table "$FULL_PATH_COMP_CONTENT" "gpoffload.log_reconcile_result_content" "COMPARE CONTENT" "$RUN_COMP_CONTENT"
if [ $? -eq 0 ]; then
    echo -e "   -> ${GREEN}[INFO]${NC} COMPARE CONTENT upload process completed."
    echo ""
else
    echo -e "   -> ${RED}[ERROR]${NC} COMPARE CONTENT upload process encountered errors."
    exit 1
fi

# 5. COMPARE COUNT -> gpoffload.log_reconcile_status_count 
upload_to_specific_table "$FULL_PATH_COMP_COUNT" "gpoffload.log_reconcile_status_count" "COMPARE COUNT" "$RUN_COMP_COUNT"
if [ $? -eq 0 ]; then
    echo -e "   -> ${GREEN}[INFO]${NC} COMPARE COUNT upload process completed."
    echo ""
else
    echo -e "   -> ${RED}[ERROR]${NC} COMPARE COUNT upload process encountered errors."
    exit 1
fi

echo "=============================================================="
echo " ALL DATABASE TASKS COMPLETED"
echo "=============================================================="


# --- 6. Export Views to CSV ---
echo ""
echo "=============================================================="
echo "📤 DATABASE VIEW EXPORT PROCESS"
echo "=============================================================="

# กำหนด Path สำหรับเก็บไฟล์ Export (สร้าง Folder 'export_views' ภายใต้ path_log_output)
path_export_views="${path_log_output}export_views/${RUN_DATE}/"
check_and_create_dir "$path_export_views"

# รายชื่อ View ที่ต้องการดึงข้อมูล
# Format: "ชื่อ View|ชื่อไฟล์"
VIEWS_TO_EXPORT=(
    "gpoffload.vw_log_latest|export_log_latest"
    "gpoffload.vw_log_query_gp_latest|export_log_query_gp_latest"
    "gpoffload.vw_log_query_pq_latest|export_log_query_pq_latest"
    "gpoffload.vw_log_reconcile_status_count_latest|export_log_reconcile_status_count_latest"
    "gpoffload.vw_log_reconcile_result_content_latest|export_log_reconcile_result_content_latest"
)

export_view_to_csv() {
    local view_name="$1"
    local file_name="$2"
    local full_export_path="${path_export_views}${file_name}_${now_date}.csv"

    echo -e "\n${BLUE}│${NC}"
    printf "${BLUE}│  ${YELLOW}Exporting View: %s${NC}\n" "$view_name"
    echo -e "${BLUE}├───────────────────────────────────${NC}"
    printf "${BLUE}│${NC}  Target File : ${BLUE}%s${NC}\n" "$(basename "$full_export_path")"

    # ใช้ \copy ในการ Export ออกมาเป็น CSV พร้อม Header
    local export_cmd="\\copy (SELECT * FROM $view_name) TO '$full_export_path' WITH CSV HEADER DELIMITER '|' QUOTE '\"';"

    echo -e "${BLUE}├─${NC} Action : Fetching data..."
    run_psql_command "$export_cmd"
    local status=$?

    if [ $status -eq 0 ]; then
        local row_count=$(wc -l < "$full_export_path")
        # ลบ 1 ออกเพราะมี Header
        row_count=$((row_count - 1))
        printf "${BLUE}│  ${GREEN}✔${NC} Export Completed! (Rows: ${YELLOW}%s${NC})\n" "$row_count"
    else
        printf "${BLUE}│  ${RED}✘${NC} Export Failed!\n"
    fi
    echo -e "${BLUE}└───────────────────────────────────${NC}"
    
    return $status
}

# เริ่มต้นการ Export ตามรายชื่อใน Array
ANY_EXPORT_FAIL=0

for entry in "${VIEWS_TO_EXPORT[@]}"; do
    IFS="|" read -r view file <<< "$entry"
    export_view_to_csv "$view" "$file"
    if [ $? -ne 0 ]; then ANY_EXPORT_FAIL=1; fi
done

echo ""
if [ $ANY_EXPORT_FAIL -eq 0 ]; then
    echo -e "   -> ${GREEN}[INFO]${NC} All views exported successfully to: $path_export_views"
else
    echo -e "   -> ${RED}[WARNING]${NC} Some view exports encountered errors. Please check the logs."
fi

echo "=============================================================="
echo " ALL TASKS VIEW EXPORT COMPLETED"
echo "=============================================================="


# --- 7. Update Full Latest Result Table ---
echo ""
echo "=============================================================="
echo " UPDATE SUMMARY RESULT TABLE"
echo "=============================================================="

TARGET_SUMMARY_TABLE="gpoffload.log_latest_result"

echo -e "\n${BLUE}│${NC}"
printf "${BLUE}│  ${YELLOW}Target Table: %s${NC}\n" "$TARGET_SUMMARY_TABLE"
echo -e "${BLUE}├───────────────────────────────────${NC}"

# 1. Truncate ตารางเก่าออกก่อน
echo -e "${BLUE}├─${NC} Action 1: Truncating old data..."
run_psql_command "TRUNCATE TABLE $TARGET_SUMMARY_TABLE;"
if [ $? -ne 0 ]; then
    echo -e "${BLUE}│  ${RED}✘${NC} Truncate failed. Stopping process."
    exit 1
fi

# 2. Insert ข้อมูลใหม่จากการ Join Views
echo -e "${BLUE}├─${NC} Action 2: Inserting latest consolidated data..."

INSERT_SQL="INSERT INTO $TARGET_SUMMARY_TABLE
SELECT
    t.dbname AS db_name,
    t.schemaname AS schema_name,
    t.tablename AS table_name,
    exp.external_tbl AS external_tbl,
    exp.run_status AS export_sts,
    exp.err_message AS export_err_msg,
    rc_gp.run_status AS reconcile_gp_sts,
    rc_gp.error_message AS reconcile_gp_err_msg,
    rc_pq.run_status AS reconcile_pq_sts,
    rc_pq.error_message AS reconcile_pq_err_msg,
    rc_compare.run_status AS reconcile_compare_sts,
    rc_compare.error_message AS reconcile_compare_err_msg
FROM 
    gpoffload.vw_full_bk_table_info t
LEFT JOIN 
    gpoffload.vw_log_latest exp
ON t.dbname = exp.greenplum_db
AND t.schemaname || '.' || t.tablename = exp.greenplum_tbl
LEFT JOIN
    gpoffload.vw_log_query_gp_latest rc_gp
ON t.dbname = rc_gp.greenplum_db
AND t.schemaname || '.' || t.tablename = rc_gp.greenplum_tbl
LEFT JOIN 
    gpoffload.vw_log_query_pq_latest rc_pq
ON t.dbname = rc_pq.greenplum_db
AND t.schemaname || '.' || t.tablename = rc_pq.greenplum_tbl
LEFT JOIN
    gpoffload.vw_log_reconcile_status_count_latest rc_compare
ON t.dbname = rc_compare.greenplum_db
AND t.schemaname || '.' || t.tablename = rc_compare.greenplum_tbl;"

run_psql_command "$INSERT_SQL"

if [ $? -eq 0 ]; then
    printf "${BLUE}│  ${GREEN}✔${NC} Data updated successfully to ${YELLOW}%s${NC}\n" "$TARGET_SUMMARY_TABLE"
else
    printf "${BLUE}│  ${RED}✘${NC} Insert failed!\n"
    exit 1
fi

echo -e "${BLUE}└───────────────────────────────────${NC}"

echo ""
echo "=============================================================="
echo " ALL SCRIPT PROCESSES COMPLETED"
echo "=============================================================="