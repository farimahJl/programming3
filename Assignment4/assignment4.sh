#!/bin/bash

NGS_data_R1=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R1_001_BC24EVACXX.filt.fastq
NGS_data_R2=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R2_001_BC24EVACXX.filt.fastq

# make an output directory 
output=/students/2021-2022/master/Maryam_DSLS/output/
mkdir -p ${output}
output_folder=${output}Maryam_

# parallelise the task for seveal k-mer sizes
seq 29 2 31 | parallel -j16 "velveth ${output_folder}{} {} -longPaired -fastq -separate ${NGS_data_R1} ${NGS_data_R2} &&  velvetg ${output_folder}{} && cat ${output_folder}{}/contigs.fa | python3 assignment4.py -kmers {} >> /students/2021-2022/master/Maryam_DSLS/output/kmers.csv"

mkdir -p /homes/mjalali/Documents/programming3/Assignment4/output
python3 best_kmer.py

# rm -r $output_folder*
