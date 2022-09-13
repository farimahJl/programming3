#!/bin/bash
#SBATCH --time 7:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --job-name=blastquery_farimah
#SBATCH --output=outputfile
#SBATCH --partition=assemblix
export BLASTDB=/local-fs/datasets/

mkdir -p output

for n in {1..16}
do 
    /usr/bin/time -a -o output/timings.txt -f "${n}\t%e"  blastp -query MCRA.faa -db $BLASTDB/refseq_protein/refseq_protein -num_threads $n -outfmt 6 >> output/blastoutput.txt
done

python3 assignment3.py