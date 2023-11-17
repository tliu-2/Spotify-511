import pandas as pd
import re

if __name__ == '__main__':
    artist_src = pd.read_csv('./data/artists_mpd.slice.0-999.csv')
    artist_src['followers'] = artist_src['followers'].str.findall(r'\d+')
    print()