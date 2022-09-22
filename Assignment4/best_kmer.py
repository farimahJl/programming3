import os
import pandas as pd

if __name__ == '__main__':
    output='/students/2021-2022/master/Maryam_DSLS/output/Maryam_'
    # output1='/homes/mjalali/Documents/programming3/Assignment4/output'
    path = '/students/2021-2022/master/Maryam_DSLS/output/kmers.csv'
    # csv_path='/homes/mjalali/Documents/programming3/Assignment4/output/kmers.csv'
    output_dir = "output"
    try:
        os.makedirs(output_dir)
        kmers = pd.read_csv(path, header=None)
    except FileExistsError:
        pass

    csv_path = os.path.join(output_dir, "kmers.csv")

    best_n50 = kmers.iloc[:, 1].max()
    best_kmer = kmers.iloc[kmers.iloc[:, 1].argmax(), 0]
    # print(kmers.iloc[best_kmer, 0])
    print(f"Best kmer: {best_kmer}")

    # os.system(f"mv {output}{best_kmer}/contigs.fa {output1}/contigs.fa")
    os.system(f"mv {output}{best_kmer}/contigs.fa {output_dir}/contigs.fa")
    os.system(f"mv {path} {csv_path}")
    os.system(f"rm -r /students/2021-2022/master/Maryam_DSLS/output/")
    # os.system(f"find /students/2021-2022/master/Maryam_DSLS/output/ -mindepth 1 -type d -exec rm -rf {} +")
