import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import glob
import time

# Insert your client id and client secret from the Spotify Dashboard View.
Client_ID = None
Client_Secret = None


if __name__ == '__main__':

    path = "./data/*.parquet"
    for fname in glob.glob(path):
        print(f'Processing {fname}\n')
        df = pd.read_parquet(fname)
        df = df.drop(columns=['duration_ms'])
        df['raw_track_uri'] = df['track_uri'].str.split(':')
        df['raw_track_uri'] = df['raw_track_uri'].str[2]

        unique_songs = df[['track_name', 'artist_name', 'artist_uri', 'track_uri', 'raw_track_uri']]
        unique_songs = unique_songs['raw_track_uri'].unique()

        sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=Client_ID, client_secret=Client_Secret))
        test_list = unique_songs[0:100]
        len_song = len(unique_songs)

        # There's definitely a better way to do this, but I'm too lazy to think about it.
        splice_list = []
        start = 0
        stop = 100
        while len_song > 0:
            curr_splice = unique_songs[start:stop]
            splice_list.append(curr_splice)
            len_song = len_song - 100
            start += 101
            stop += 101
            if start > len_song:
                break
            if stop > len_song:
                stop = len_song

        new_fname = fname.split('\\')
        new_fname = new_fname[1]
        df_list = []
        i = 0
        for x in splice_list:

            # Sleep to prevent going over the API rate-limit.
            time.sleep(35)
            # Pull audio features from Spotify API.
            results = sp.audio_features(tracks=test_list)

            # Convert the features to a Dataframe.
            results_df = pd.DataFrame(results)
            results_df = results_df.rename(columns={'id': 'raw_track_uri'})

            # Output the audio features for the current splice to a csv.
            results_df.to_csv(f'./data/spotify_api_pulls/{new_fname}_{i}.csv')

            # Merge together the playlist data and the audio features on the raw_track_uri.
            combined_df = pd.merge(df, results_df, on=['raw_track_uri'], how='inner')
            combined_df = combined_df.sort_values(by=['pid', 'pos'])
            # Append the combined splice to a list.
            df_list.append(combined_df)
            i += 1

        # Concat the list of dataframes to one big dataframe.
        final_df = pd.concat(df_list)

        new_fname = new_fname.replace('.parquet', '')
        final_df.to_csv(f'./data/combined_{new_fname}.csv', index=False)
