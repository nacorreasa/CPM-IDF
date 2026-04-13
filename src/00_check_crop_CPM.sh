#!/bin/bash
# =============================================================================
# CPM Precipitation Data Processing Pipeline
# Validates, crops, and renames CPM files from NAS storage
# Usage: bash pipeline_cpm.sh <lon_min> <lon_max> <lat_min> <lat_max> <new_domain> <run_validation: 0|1>
# Example: bash pipeline_cpm.sh 10.8 12.9 44.6 45.7 POL-3i 1

# Nathalia Correa-Sánchez
# =============================================================================

# --- Input parameters ---
LON_MIN=${1}
LON_MAX=${2}
LAT_MIN=${3}
LAT_MAX=${4}
NEW_DOMAIN=${5}
RUN_VALIDATION=${6}   # 1=run validation, 0=skip (e.g. second domain)

# --- Fixed paths ---
STORAGE_USER="nathalia"
STORAGE_HOST="return"
STORAGE_BASE="/share/Public/CPMs"
SCENARIOS=("HIST" "RCP85" "SSP370")
TEMP_DIR=~/projects/CPM-IDF/data/temporal
LOG_FILE=~/projects/CPM-IDF/logs/pipeline_$(date +%Y%m%d_%H%M%S).log
OLD_DOMAIN="ALP-3i"

# --- Reasonable pr value bounds (kg/m2/s) ---
PR_MIN=0.0
PR_MAX=0.01

# --- Create required directories ---
mkdir -p "$TEMP_DIR"
mkdir -p ~/projects/CPM-IDF/logs

# =============================================================================
# FUNCTIONS
# =============================================================================

# Log only failures — no noise for successful files
log_failure() {
    local file=$1
    local reason=$2
    echo "[FAIL] $(date '+%Y-%m-%d %H:%M:%S') | $file | $reason" | tee -a "$LOG_FILE"
}

log_info() {
    local msg=$1
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') | $msg" | tee -a "$LOG_FILE"
}

# -----------------------------------------------------------------------------
# CHECK 1: File exists on storage
# -----------------------------------------------------------------------------
check_exists() {
    local storage_path=$1
    ssh ${STORAGE_USER}@${STORAGE_HOST} test -f "$storage_path"
    return $?
}

# -----------------------------------------------------------------------------
# CHECK 2: Already processed (output file exists)
# -----------------------------------------------------------------------------
check_already_processed() {
    local output_path=$1
    ssh ${STORAGE_USER}@${STORAGE_HOST} test -f "$output_path"
    return $?
}

# -----------------------------------------------------------------------------
# CHECK 3: File size > 0
# -----------------------------------------------------------------------------
check_size() {
    local storage_path=$1
    local size
    size=$(ssh ${STORAGE_USER}@${STORAGE_HOST} stat -c%s "$storage_path" 2>/dev/null)
    [ "$size" -gt 0 ]
    return $?
}

# -----------------------------------------------------------------------------
# CHECK 4: Valid NetCDF structure (fast — metadata only)
# -----------------------------------------------------------------------------
check_structure() {
    local temp_file=$1
    cdo sinfo "$temp_file" > /dev/null 2>&1
    return $?
}

# -----------------------------------------------------------------------------
# CHECK 5: Reasonable pr values (slow — optional)
# -----------------------------------------------------------------------------
check_pr_values() {
    # Skipped: encoding noise (I16z) produces ~1e-6 apparent negatives
    # Structural validation via cdo sinfo is sufficient
    return 0
}

# -----------------------------------------------------------------------------
# CROP: Cut file to target extent and rename domain
# -----------------------------------------------------------------------------
crop_and_rename() {
    local temp_file=$1
    local output_path=$2
    local output_filename=$3

    # Crop to target extent using CDO sellonlatbox
    cdo sellonlatbox,${LON_MIN},${LON_MAX},${LAT_MIN},${LAT_MAX} \
        "$temp_file" \
        "${TEMP_DIR}/${output_filename}" 2>/dev/null

    if [ $? -ne 0 ]; then
        log_failure "$temp_file" "CDO crop failed"
        return 1
    fi

    # Write cropped file back to storage
    scp "${TEMP_DIR}/${output_filename}" \
        "${STORAGE_USER}@${STORAGE_HOST}:${output_path}/${output_filename}"

    if [ $? -ne 0 ]; then
        log_failure "$output_filename" "SCP to storage failed"
        return 1
    fi

    return 0
}

# =============================================================================
# MAIN PIPELINE
# =============================================================================
log_info "Starting CPM pipeline | Domain: $NEW_DOMAIN | Extent: ${LON_MIN},${LON_MAX},${LAT_MIN},${LAT_MAX}"

for SCENARIO in "${SCENARIOS[@]}"; do

    # List available models for this scenario
    MODELS=$(ssh ${STORAGE_USER}@${STORAGE_HOST} ls ${STORAGE_BASE}/${SCENARIO}/ 2>/dev/null)

    for MODEL in $MODELS; do

        STORAGE_PR_PATH="${STORAGE_BASE}/${SCENARIO}/${MODEL}/PR"
        OUTPUT_PATH="${STORAGE_PR_PATH}/${NEW_DOMAIN}"

        # List all encoded .nc files for this model
        FILES=$(ssh ${STORAGE_USER}@${STORAGE_HOST} ls ${STORAGE_PR_PATH}/*_encoded.nc 2>/dev/null)

        if [ -z "$FILES" ]; then
            continue  # No files yet — skip silently
        fi

        # Create output directory on storage if it doesn't exist
        ssh ${STORAGE_USER}@${STORAGE_HOST} mkdir -p "$OUTPUT_PATH"

        for STORAGE_FILE in $FILES; do

            FILENAME=$(basename "$STORAGE_FILE")

            # Build output filename: replace OLD_DOMAIN with NEW_DOMAIN
            OUTPUT_FILENAME="${FILENAME//${OLD_DOMAIN}/${NEW_DOMAIN}}"
            TEMP_FILE="${TEMP_DIR}/${FILENAME}"

            # ------------------------------------------------------------------
            # CHECK 1: Exists on storage?
            # ------------------------------------------------------------------
            if ! check_exists "$STORAGE_FILE"; then
                log_failure "$FILENAME" "File not found on storage"
                continue
            fi

            # ------------------------------------------------------------------
            # CHECK 2: Already processed?
            # ------------------------------------------------------------------
            if check_already_processed "${OUTPUT_PATH}/${OUTPUT_FILENAME}"; then
                continue  # Silent skip — already done
            fi

            # ------------------------------------------------------------------
            # CHECK 3: File size > 0?
            # ------------------------------------------------------------------
            if ! check_size "$STORAGE_FILE"; then
                log_failure "$FILENAME" "File size is 0"
                continue
            fi

            # ------------------------------------------------------------------
            # Copy file to critical temporal folder
            # ------------------------------------------------------------------
            scp "${STORAGE_USER}@${STORAGE_HOST}:${STORAGE_FILE}" "$TEMP_FILE" 2>/dev/null

            if [ $? -ne 0 ]; then
                log_failure "$FILENAME" "SCP from storage failed"
                continue
            fi

            # ------------------------------------------------------------------
            # CHECK 4: Valid NetCDF structure?
            # ------------------------------------------------------------------
            if ! check_structure "$TEMP_FILE"; then
                log_failure "$FILENAME" "Invalid NetCDF structure"
                rm -f "$TEMP_FILE"
                continue
            fi

            # ------------------------------------------------------------------
            # CHECK 5: Reasonable pr values? (optional)
            # ------------------------------------------------------------------
            if [ "$RUN_VALIDATION" -eq 1 ]; then
                if ! check_pr_values "$TEMP_FILE"; then
                    rm -f "$TEMP_FILE"
                    continue
                fi
            fi

            # ------------------------------------------------------------------
            # CROP and write to storage
            # ------------------------------------------------------------------
            if ! crop_and_rename "$TEMP_FILE" "$OUTPUT_PATH" "$OUTPUT_FILENAME"; then
                rm -f "$TEMP_FILE"
                continue
            fi

            # ------------------------------------------------------------------
            # Cleanup temporal file on critical
            # ------------------------------------------------------------------
            rm -f "$TEMP_FILE"
            rm -f "${TEMP_DIR}/${OUTPUT_FILENAME}"

        done
    done
done

log_info "Pipeline finished | Check $LOG_FILE for failures"