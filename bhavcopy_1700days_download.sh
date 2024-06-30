#!usr/bin/env bash
echo ##################################
echo "bhavcopy  download 1700 days"
echo "Author : Sangram Keshari Dash"
# Example: https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR010624.zip
# Tested on Oracle Linux 8
echo #################################

echo "file download from :" i
date --date="-0 days" +"%d%m%Y"
echo "file download till :"
date --date="-1700 days" +"%d%m%Y"


bhavcopy=https://nsearchives.nseindia.com/archives/equities/bhavcopy/pr/PR && fileExtn=.zip && n=-1 &&
for i in {1..1700}; do ((n++)); date_var=`date --date="-$n days" +%d%m%y`;file2download=$bhavcopy$date_var$fileExtn; echo $file2download >> /tmp/bhavcopy_1700Days.txt   ; done;


fileList="/tmp/bhavcopy_1700Days.txt"

cat $fileList|head

downloadPath=/tmp/bhavcopy_1700Days/
mkdir -p /tmp/bhavcopy_1700Days

while read -r line; do
    echo  " Downloading...  File -->  $line\n"
    sleep 1;

    dFile=$(echo $line|cut -d "/" -f 8)
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