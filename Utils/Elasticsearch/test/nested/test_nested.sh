#!/bin/sh

echo "`date --rfc-3339=seconds`: Start test." >&2

echo '---,---,---,---' >> test_results_cached.csv
echo 'query,type,N,sec' >> test_results_cached.csv

{
./test_deriv.sh
./test_keywords.sh
./test_stat_deriv.sh
./test_steps.sh
} | tee -a test_results_cached.csv

echo "`date --rfc-3339=seconds`: Test complete" >&2
