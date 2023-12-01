import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import glob
import time
import requests
import ast


# Insert your client id and client secret from the Spotify Dashboard View.
Client_ID = ''
Client_Secret = ''


def expand_artist_info(row, max_artists=3):
    artist_info = {}
    # Do a bunch of formatting to get it into a series and then return it.
    for i in range(max_artists):
        artist_info['artist_name_{}'.format(i+1)] = ''
        artist_info['artist_id_{}'.format(i+1)] = ''
        artist_info['artist_uri_{}'.format(i+1)] = ''

    for i, artist in enumerate(row['artists']):
        if i < max_artists:
            artist_info['artist_name_{}'.format(i+1)] = artist.get('name', '')
            artist_info['artist_id_{}'.format(i+1)] = artist.get('id', '')
            artist_info['artist_uri_{}'.format(i+1)] = artist.get('uri', '')
        else:
            break

    return pd.Series(artist_info)

def extract_album_details(row):
    album = row['album']
    # Do a bunch of formatting to get it into a series and then return it.
    return pd.Series([album.get('name', ''), album.get('id', ''), album.get('uri', ''), album.get('album_type', '')],
                     index=['album_name', 'album_id', 'album_uri', 'album_type'])


def pull_song_features(splice_list, market='US'):
    df_list = []
    request_count = 0
    start_time = time.time()
    length_splice = len(splice_list)
    pos = 0
    for x in splice_list:
        print(f'Song Feature: @ {pos} out of {length_splice}')
        if len(x) == 0:
            continue

        success = False
        while not success:
            try:
                current_time = time.time()
                elapsed_time = current_time - start_time

                # Reset every 30 seconds
                if elapsed_time >= 30:
                    start_time = current_time
                    request_count = 0

                # Throttle requests after 180 in a 30-second window
                if request_count >= 180:
                    time.sleep(30)  # Sleep for 30 seconds to throttle
                    start_time = time.time()
                    request_count = 0

                track_features = sp.tracks(tracks=x, market=market)
                request_count += 1

                if track_features and 'tracks' in track_features:
                    results_df = pd.DataFrame(track_features['tracks'])
                    results_df = results_df.rename(columns={'id': 'raw_track_uri'})
                    df_list.append(results_df)
                    success = True
            except SpotifyException as e:
                if e.http_status == 429:
                    retry_after = e.headers.get('Retry-After', 1)  # Use the Retry-After header if available
                    print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                    time.sleep(int(retry_after))
                else:
                    print(f"Spotify API error: {e}")
                    break  # or handle the error as needed
            except Exception as general_exception:
                print(f"An error occurred: {general_exception}")
                break  # or handle the error as needed
        pos += 1
    if df_list:
        final_df = pd.concat(df_list)
        return final_df
    else:
        return pd.DataFrame()


def pull_artists(splice_list):
    df_list = []
    request_count = 0
    start_time = time.time()
    length_splice = len(splice_list)
    pos = 0
    for x in splice_list:
        print(f'Artist Feature: @ {pos} out of {length_splice}')
        if len(x) == 0:
            continue

        success = False
        while not success:
            try:
                current_time = time.time()
                elapsed_time = current_time - start_time

                # Reset every 30 seconds
                if elapsed_time >= 30:
                    start_time = current_time
                    request_count = 0

                # Throttle requests after 180 in a 30-second window
                if request_count >= 180:
                    time.sleep(30)  # Sleep for 30 seconds to throttle
                    start_time = time.time()
                    request_count = 0

                response = sp.artists(artists=x)
                request_count += 1

                if response:
                    results_df = pd.DataFrame(response['artists'])
                    results_df = results_df.rename(columns={'id': 'raw_artist_uri'})
                    df_list.append(results_df)
                    success = True
            except SpotifyException as e:
                if e.http_status == 429:
                    retry_after = e.headers.get('Retry-After', 1)  # Use the Retry-After header if available
                    print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                    time.sleep(int(retry_after))
                else:
                    print(f"Spotify API error: {e}")
                    break  # or handle the error as needed
            except Exception as general_exception:
                print(f"An error occurred: {general_exception}")
                break  # or handle the error as needed
        pos += 1
    if df_list:
        final_df = pd.concat(df_list)
        return final_df
    else:
        return pd.DataFrame()


def pull_audio_features(splice_list):
    df_list = []
    request_count = 0
    start_time = time.time()
    length_splice = len(splice_list)
    pos = 0
    for x in splice_list:
        print(f'Audio Feature: @ {pos} out of {length_splice}')
        if len(x) == 0:
            continue

        success = False
        while not success:
            try:
                current_time = time.time()
                elapsed_time = current_time - start_time

                # Reset every 30 seconds
                if elapsed_time >= 30:
                    start_time = current_time
                    request_count = 0

                # Throttle requests after 180 in a 30-second window
                if request_count >= 180:
                    time.sleep(30)  # Sleep for 30 seconds to throttle
                    start_time = time.time()
                    request_count = 0

                response = sp.audio_features(tracks=x)
                request_count += 1

                if response:
                    results_df = pd.DataFrame(response)
                    results_df = results_df.rename(columns={'id': 'raw_track_uri'})
                    df_list.append(results_df)
                    success = True
            except SpotifyException as e:
                if e.http_status == 429:
                    retry_after = e.headers.get('Retry-After', 1)  # Use the Retry-After header if available
                    print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                    time.sleep(int(retry_after))
                else:
                    print(f"Spotify API error: {e}")
                    break  # or handle the error as needed
            except Exception as general_exception:
                print(f"An error occurred: {general_exception}")
                break  # or handle the error as needed
        pos += 1
    if df_list:
        final_df = pd.concat(df_list)
        return final_df
    else:
        return pd.DataFrame()



def generate_splices(data_list, batch_size):
    for i in range(0, len(data_list), batch_size):
        yield data_list[i:i + batch_size]


def extract_spotify_id(uri):
    parts = uri.split(':')
    if len(parts) == 3 and parts[0] == 'spotify':
        return parts[2]
    else:
        # Handle the case where the URI is not in the expected format
        # This could be logging an error, returning None, or some other action
        print(f"Unexpected URI format: {uri}")
        return None


if __name__ == '__main__':

    path = "./data/*.parquet"
    for fname in glob.glob(path):
        print(f'Processing {fname}\n')
        df = pd.read_parquet(fname)
        df['raw_track_uri'] = df['track_uri'].apply(extract_spotify_id)
        df['raw_artist_uri'] = df['artist_uri'].apply(extract_spotify_id)

        # unique_songs = df[['track_name', 'artist_name', 'artist_uri', 'track_uri', 'raw_track_uri', 'raw_artist_uri']]
        # unique_songs = unique_songs['raw_track_uri'].unique()
        #
        # unique_artists = df[['artist_name', 'artist_uri', 'raw_artist_uri', 'track_name', 'track_uri', 'raw_track_uri']]
        # unique_artists = unique_artists['raw_artist_uri'].unique()

        unique_songs = df['raw_track_uri'].unique()
        unique_artists = df['raw_artist_uri'].unique()

        sp = spotipy.Spotify(
            client_credentials_manager=SpotifyClientCredentials(client_id=Client_ID, client_secret=Client_Secret))

        song_splices = list(generate_splices(unique_songs, 100))
        song_splices_50 = list(generate_splices(unique_songs, 50))
        artist_splices = list(generate_splices(unique_artists, 50))

        # Pulling song features, artist data, and audio features
        print('Pulling Song Features')
        track_features = pull_song_features(song_splices_50)
        print('Pulling Artist Features')
        artists = pull_artists(artist_splices)
        print('Pulling Audio Features')
        audio_features = pull_audio_features(song_splices)

        new_fname = fname.split('\\')
        new_fname = new_fname[1]

        # audio_features = pull_audio_features(splice_list=splice_list)
        # artists = pull_artists(splice_list=splice_artist_list)
        # track_features = pull_song_features(splice_list=track_feature_list)

        track_features = track_features.rename(columns={'name': 'track_name', 'popularity': 'track_popularity'})

        # Extract the followers value.
        artists['followers'] = artists['followers'].apply(lambda x: x.get('total', 0) if isinstance(x, dict) else 0)

        # Ensure the 'artists' and 'album' columns are in the correct format
        track_features['artists'] = track_features['artists'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
        track_features['album'] = track_features['album'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

        # Apply the function to expand artist information
        artist_info = track_features.apply(expand_artist_info, axis=1)
        track_features = pd.concat([track_features, artist_info], axis=1)

        # Apply the function to extract album details
        album_details = track_features.apply(extract_album_details, axis=1)
        track_features = pd.concat([track_features, album_details], axis=1)


        new_fname = new_fname.replace('.parquet', '')

        audio_features.to_csv(f'./data/songs_{new_fname}.csv', index=False, encoding='utf-8-sig')
        artists.to_csv(f'./data/artists_{new_fname}.csv', index=False, encoding='utf-8-sig')
        track_features.to_csv(f'./data/track_feature_{new_fname}.csv', index=False, encoding='utf-8-sig')
        # final_df.to_csv(f'./data/combined_{new_fname}.csv', index=False)
