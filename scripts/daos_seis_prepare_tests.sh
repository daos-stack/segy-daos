#!/bin/bash

mkdir data
cd data

## Get 2004 BP velocity model from https://wiki.seg.org/wiki/2004_BP_velocity_estimation_benchmark_model to test on the BP model.
wget http://s3.amazonaws.com/open.source.geoscience/open_data/bpvelanal2004/shots0601_0800.segy.gz
wget http://s3.amazonaws.com/open.source.geoscience/open_data/bpvelanal2004/shots0801_1000.segy.gz
gunzip shots0601_0800.segy.gz
gunzip shots0801_1000.segy.gz

## Read Segy files to calculate cdp and offset headers.
segyread tape=shots0601_0800.segy | suchw key1=cdp,offset key2=gx,gx key3=sx,sx a=0,0 b=1,1 c=1,-1 d=2,1 >shot601_800_headers.su
segyread tape=shots0801_1000.segy | suchw key1=cdp,offset key2=gx,gx key3=sx,sx a=0,0 b=1,1 c=1,-1 d=2,1 >shot801_1000_headers.su
## Prepare su file headers and write traces back to segy file
segyhdrs <shot601_800_headers.su
segyhdrs <shot801_1000_headers.su
segywrite tape=shots0601_0800_cdp_offset.segy <shot601_800_headers.su
segywrite tape=shots0801_1000_cdp_offset.segy <shot801_1000_headers.su