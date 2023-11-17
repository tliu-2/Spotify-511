import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import glob
import time

# Insert your client id and client secret from the Spotify Dashboard View.
Client_ID = None
Client_Secret = None


def pull_song_features(splice_list):
    df_list = []
    i = 0
    for x in splice_list:
        # Sleep to prevent going over the API rate-limit.
        # time.sleep(35)
        # Pull audio features from Spotify API.
        track_features = sp.tracks(tracks=x)

        # Convert the features to a Dataframe.
        results_df = pd.DataFrame(track_features['tracks'])
        results_df = results_df.rename(columns={'id': 'raw_track_uri'})

        # Output the audio features for the current splice to a csv.
        # Append the combined splice to a list.
        df_list.append(results_df)
        i += 1

    # Concat the list of dataframes to one big dataframe.
    final_df = pd.concat(df_list)
    return final_df


def pull_artists(splice_list):
    df_list = []
    i = 0
    for x in splice_list:
        # Sleep to prevent going over the API rate-limit.
        # time.sleep(35)
        # Pull audio features from Spotify API.
        artists = sp.artists(artists=x)

        # Convert the features to a Dataframe.
        results_df = pd.DataFrame(artists['artists'])
        results_df = results_df.rename(columns={'id': 'raw_artist_uri'})

        # Output the audio features for the current splice to a csv.
        # results_df.to_csv(f'./data/spotify_api_pulls/artists_{new_fname}_{i}.csv')

        # Merge together the playlist data and the audio features on the raw_track_uri.
        combined_df = pd.merge(df, results_df, on=['raw_artist_uri'], how='inner')
        combined_df = combined_df.sort_values(by=['pid', 'pos'])
        # Append the combined splice to a list.
        df_list.append(results_df)
        i += 1

    # Concat the list of dataframes to one big dataframe.
    final_df = pd.concat(df_list)
    return final_df


def pull_audio_features(splice_list):
    df_list = []
    i = 0
    for x in splice_list:
        # Sleep to prevent going over the API rate-limit.
        # time.sleep(35)
        # Pull audio features from Spotify API.
        song_audio_features = sp.audio_features(tracks=x)

        # Convert the features to a Dataframe.
        results_df = pd.DataFrame(song_audio_features)
        results_df = results_df.rename(columns={'id': 'raw_track_uri'})

        # Output the audio features for the current splice to a csv.
        # results_df.to_csv(f'./data/spotify_api_pulls/song_features_{new_fname}_{i}.csv')

        # Merge together the playlist data and the audio features on the raw_track_uri.
        combined_df = pd.merge(df, results_df, on=['raw_track_uri'], how='inner')
        combined_df = combined_df.sort_values(by=['pid', 'pos'])
        # Append the combined splice to a list.
        df_list.append(results_df)
        i += 1

    # Concat the list of dataframes to one big dataframe.
    final_df = pd.concat(df_list)
    return final_df


if __name__ == '__main__':

    path = "./data/*.parquet"
    for fname in glob.glob(path):
        print(f'Processing {fname}\n')
        df = pd.read_parquet(fname)
        df['raw_track_uri'] = df['track_uri'].str.split(':')
        df['raw_track_uri'] = df['raw_track_uri'].str[2]
        df['raw_artist_uri'] = df['artist_uri'].str.split(':')
        df['raw_artist_uri'] = df['raw_artist_uri'].str[2]

        unique_songs = df[['track_name', 'artist_name', 'artist_uri', 'track_uri', 'raw_track_uri', 'raw_artist_uri']]
        unique_songs = unique_songs['raw_track_uri'].unique()

        unique_artists = df[['artist_name', 'artist_uri', 'raw_artist_uri', 'track_name', 'track_uri', 'raw_track_uri']]
        unique_artists = unique_artists['raw_artist_uri'].unique()

        sp = spotipy.Spotify(
            client_credentials_manager=SpotifyClientCredentials(client_id=Client_ID, client_secret=Client_Secret))
        test_list = unique_songs[0:100]
        len_song = len(unique_songs)
        len_artists = len(unique_artists)

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

        # Awful part 2:
        splice_artist_list = []
        start_artist = 0
        stop_artist = 50
        while len_artists > 0:
            curr_splice_artist = unique_artists[start_artist:stop_artist]
            splice_artist_list.append(curr_splice_artist)
            len_artists = len_artists - 50
            start_artist += 51
            stop_artist += 51
            if start_artist > len_artists:
                break
            if stop_artist > len_artists:
                stop_artist = len_artists

        len_song = len(unique_songs)
        track_feature_list = []
        start = 0
        stop = 50
        while len_song > 0:
            curr_splice_track = unique_songs[start:stop]
            track_feature_list.append(curr_splice_track)
            len_song = len_song - 50
            start_artist += 51
            stop_artist += 51
            if start > len_song:
                break
            if stop > len_song:
                stop = len_song

        new_fname = fname.split('\\')
        new_fname = new_fname[1]

        test_splice = splice_list[0:2]
        test_artist_splice = splice_artist_list[0:2]
        test_track_splice = track_feature_list[0:2]
        audio_features = pull_audio_features(splice_list=test_splice)
        artists = pull_artists(splice_list=test_artist_splice)
        track_features = pull_song_features(splice_list=test_track_splice)

        artists['followers'] = artists['followers'].str.findall(r'\d+')
        artists['followers'] = artists['followers'].apply(lambda x: ' '.join(dict.fromkeys(x).keys()))

        new_fname = new_fname.replace('.parquet', '')
        audio_features.to_csv(f'./data/songs_{new_fname}.csv', index=False)
        artists.to_csv(f'./data/artists_{new_fname}.csv', index=False)
        track_features.to_csv(f'./data/track_feature_{new_fname}.csv', index=False)
        # final_df.to_csv(f'./data/combined_{new_fname}.csv', index=False)
