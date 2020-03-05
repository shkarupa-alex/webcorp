#!/usr/bin/env bash
set -x

if [ $# -eq 0 ]; then
    echo "No source file supplied"
    exit 1
fi


SOURCE="$1"
BASE=$(basename -- "${SOURCE}")
NAME="${BASE%.*}"
EXT="${BASE##*.}"

if [ "gz" != "${EXT}" ]; then
    echo "Wrong source file type"
    exit 1
fi

rm -rf .repairtmp
mkdir .repairtmp

gunzip -c "${SOURCE}" > .repairtmp/content.csv || true
./repaircsv.py .repairtmp/content.csv .repairtmp/"${NAME}"
gzip -c .repairtmp/"${NAME}" > .repairtmp/"${BASE}"

rm -f "${SOURCE}"
mv -f .repairtmp/"${BASE}" "${SOURCE}"

rm -rf .repairtmp
