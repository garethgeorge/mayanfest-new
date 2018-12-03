import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv('stats.csv')

plt.plot(df['x'], df['y1'], label='y1')
plt.plot(df['x'], df['y2'], label='y2')
plt.plot(df['x'], df['y3'], label='y3')

plt.xlabel('bytes')
plt.ylabel('seconds')
plt.legend()

plt.savefig('fig.pdf',bbox_inches='tight')
