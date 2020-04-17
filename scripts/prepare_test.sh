#!/bin/bash

mkdir data
cd data
## Get 2004 BP velocity model from https://wiki.seg.org/wiki/2004_BP_velocity_estimation_benchmark_model to test on the BP model.
wget http://s3.amazonaws.com/open.source.geoscience/open_data/bpvelanal2004/vel_z6.25m_x12.5m_exact.segy.gz
gunzip vel_z6.25m_x12.5m_exact.segy.gz
## Segyread 
segyread tape=vel_z6.25m_x12.5m_exact.segy >original_segyread.su
## Sutrcount
sutrcount <original_segyread.su >original_sutrcount.su
## Sugethw
sugethw <original_segyread.su key=tracl,offset,dt output=ascii  >original_sugethw_ascii.su
sugethw <original_segyread.su key=tracl,offset,dt output=geom   >original_sugethw_geom.su 
sugethw <original_segyread.su key=tracl,offset,dt output=binary >original_sugethw_binary.su
## Suaddhead
suaddhead <original_segyread.su ns=1024 >original_suaddhead.su
## Sushw
sushw <original_segyread.su key=tracl,offset,dt    infile=original_sugethw_binary.su >original_sushw_infile.su  	
sushw <original_segyread.su key=dt    a=4000 		>original_sushw_a.su
## Susort
susort <original_segyread.su >original_sort_filetofile.su cdp
susort <original_segyread.su | sutrcount >original_sutrcount_pipefrom_susort.su
segyread tape=vel_z6.25m_x12.5m_exact.segy | susort >original_sort_pipetofile.su cdp
## Surange
surange <original_segyread.su key=tracl >original_surange.su
## Suchw
suchw <original_segyread.su key1=tracr key2=tracr a=1000  >original_suchw.su 
## Suwind
suwind <original_segyread.su key=sx min=669000 max=670000  >original_suwind.su
## Rename original header/binary generated.
mv header original_header
mv binary original_binary