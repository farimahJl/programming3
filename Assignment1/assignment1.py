import multiprocessing as mp
from pathlib import Path
import argparse as ap
#import Bio.Entrez
from Bio import Entrez
#Entrez.efetch()
# "b9a1374fd8c234506e141a74db4b7eb28e08 "
 #"marry.jlal@gmail.com"
Entrez.email = "marry.jlal@gmail.com"


def get_references(pmid):
    results = Entrez.read(Entrez.elink(dbfrom="pubmed",
                                   db="pmc",
                                   LinkName="pubmed_pmc_refs",
                                   id=pmid,
                                   api_key='b9a1374fd8c234506e141a74db4b7eb28e08'))
    references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
    return references


def store_articles(pmid):
    handle = Entrez.efetch(db="pmc", id=pmid, rettype="XML", retmode="text",
                           api_key='b9a1374fd8c234506e141a74db4b7eb28e08')
    directory = Path(__file__).parent.absolute()
    output = directory / 'output'
    if not(output.exists()):
        output.mkdir(parents=True, exist_ok=False)
    #else:
    #    print('directory already exists doing nothing')
    with open(f'{output}/{pmid}.xml', 'wb') as file:
        file.write(handle.read())



if __name__ == "__main__":
    #catch input
    argparser = ap.ArgumentParser(description="Script that downloads (default) 10 articles referenced by the given PubMed ID concurrently.")
    argparser.add_argument("-n", action="store",
                           dest="n", required=False, type=int,
                           help="Number of references to download concurrently.")
    argparser.add_argument("pubmed_id", action="store", type=str, nargs=1, help="Pubmed ID of the article to harvest for references to download.")
    args = argparser.parse_args()
    print("Getting: ", args.pubmed_id)

    pmid = str(args.pubmed_id)
    
    refs = get_references(pmid)
    print(refs)


    #multiprocessing
    cpus = mp.cpu_count()
    with mp.Pool(cpus) as pool:
        results = pool.map(store_articles, refs[0:10])