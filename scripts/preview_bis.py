from itertools import islice
path = r"c:\Users\frank\Documents\FRM project\data_repository\raw\structural\bis_lbs_d_pub.csv"
with open(path, newline='', encoding='utf-8', errors='ignore') as fh:
    for line in islice(fh, 20):
        print(line.strip())