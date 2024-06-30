#!usr/bin/env bash
echo ##################################
echo "sec_bhavdata download 1700 days"
echo Author : Sangram Keshari Dash
# Example: https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_01062024.csv
# Tested on Oracle Linux 8
echo #################################

echo "file download from :" i
date --date="-0 days" +"%d%m%Y"
echo "file download till :"
date --date="-1700 days" +"%d%m%Y"

sec_bhavdata_full=https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_ && fileExtn=.csv && n=-1 &&
for i in {1..1700}; do ((n++)); date_var=`date --date="-$n days" +"%d%m%Y"`;file2download=$sec_bhavdata_full$date_var.csv; echo $file2download  >> /tmp/sec_bhavdata_full_1700Days.txt  ; done;

fileList="/tmp/sec_bhavdata_full_1700Days.txt"

cat $fileList|head

downloadPath=/tmp/sec_bhavdata_full_1700Days/
mkdir -p /tmp/sec_bhavdata_full_1700Days

while read -r line; do
    echo  " Downloading...  File -->  $line\n"
    sleep 1;

    dFile=$(echo $line|cut -d "/" -f 6)
    echo dFile is $dFile
    echo DownloadDest is --> $downloadPath$dFile
    echo "-----"

curl $line \
  -H 'Upgrade-Insecure-Requests: 1' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' \
  -H 'sec-ch-ua: "Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  --compressed --output $downloadPath$dFile


done <$fileList