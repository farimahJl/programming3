import matplotlib.pyplot as plt
import pandas as pd
df = pd.read_csv('output/timings.txt', sep= '\t',header= None)
plt.scatter(df[0], df[1])
plt.savefig('output/timing.png')