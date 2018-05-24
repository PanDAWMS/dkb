#!/bin/bash
./generate-mapping -o $2.ttl -d oracle.jdbc.OracleDriver -u [USER] -p [PASSWORD] --tables $1 jdbc:oracle:thin:@//ADCR2-ADG-S.cern.ch:10121/ADCR.cern.ch