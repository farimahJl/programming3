#!/bin/bash
export BLASTDB=/local-fs/datasets/

mkdir -p output

for n in {1..16}
do 
    /usr/bin/time -a -o output/timings.txt -f "${n}\t%e"  blastp -query MCRA.faa -db $BLASTDB/refseq_protein/refseq_protein -num_threads $n -outfmt 6 >> output/blastoutput.txt
done

