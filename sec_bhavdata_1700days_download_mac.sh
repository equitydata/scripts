#! /usr/bin/env bash

echo "##################################"
echo "sec_bhavdata download 1700 days (Mac)"
echo "##################################"

# Base URL
sec_bhavdata_full="https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_"

# ✅ Your download path
downloadPath="$HOME/Downloads/equitydata/sec_bhavdata_download/"
fileList="/tmp/sec_bhavdata_full_1700Days.txt"

# Create directory
mkdir -p "$downloadPath"

# Clear old file list
> "$fileList"

echo "Download path: $downloadPath"

echo "Generating URL list..."

# ---------------------------
# MAC-COMPATIBLE DATE LOOP
# ---------------------------
for ((n=0; n<1700; n++)); do
    date_var=$(date -v-"${n}"d +"%d%m%Y")
    url="${sec_bhavdata_full}${date_var}.csv"
    echo "$url" >> "$fileList"
done

echo "Sample URLs:"
head "$fileList"

echo "Starting download..."

# ---------------------------
# DOWNLOAD LOOP
# ---------------------------
while read -r line; do

    dFile=$(basename "$line")

    # Safety check
    if [[ "$line" == *"_.csv" ]]; then
        echo "Skipping invalid URL: $line"
        continue
    fi

    echo "Downloading: $line"
    echo "Saving as: $downloadPath$dFile"
    echo "----------------------------------"

    curl -L \
      -H "User-Agent: Mozilla/5.0" \
      -H "Referer: https://www.nseindia.com/" \
      --fail \
      --silent \
      --show-error \
      --output "$downloadPath$dFile" \
      "$line"

    # Avoid NSE blocking
    sleep 1

done < "$fileList"

echo "----------------------------------"
echo "Download complete!"
echo "Files saved in: $downloadPath"
