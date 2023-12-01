import pandas as pd
from sklearn.preprocessing import MinMaxScaler


def add_album_occurrences(df):
    """
    Adds a calculated column "album_occurrence" which is the number of times an artist appears across playlists.
    :param df: Dataframe of spotify playlist data.
    :return: Dataframe of spotify playlist data with "album_occurrence"
    """
    album_occurrence = df.groupby('artist_name')['album_name'].value_counts()
    album_occurrence = album_occurrence.reset_index()
    album_occurrence = album_occurrence.rename(columns={'count': 'album_occurrence'})
    overall_df = pd.merge(df, album_occurrence, on=['artist_name', 'album_name'], how='outer')

    return overall_df


def add_artist_occurrences(df):
    """
    Adds a calculated column "artist_occurrence" which is the number of times an artist appears across playlists.
    :param df: Dataframe of spotify playlist data.
    :return: Dataframe of spotify playlist data with "artist_occurrence"
    """
    artist_occurrence = df['artist_name'].value_counts()
    artist_occurrence = artist_occurrence.reset_index()
    artist_occurrence = artist_occurrence.rename(columns={'count': 'artist_occurrence'})
    overall_df = pd.merge(df, artist_occurrence, on=['artist_name'], how='outer')
    return overall_df


def add_song_occurrences(df):
    """
    Adds a calculated column "song_occurrence" which is the number of times an artist appears across playlists.
    :param df: Dataframe of spotify playlist data.
    :return: Dataframe of spotify playlist data with "song_occurrence"
    """
    song_occurrence = df.groupby('artist_name')['track_name'].value_counts()
    song_occurrence = song_occurrence.reset_index()
    overall_df = pd.merge(df, song_occurrence, on=['track_name', 'artist_name'], how='outer')
    overall_df = overall_df.rename(columns={'count': 'song_occurrence'})
    return overall_df


def categorize_feature(value, feature_name):
    if 'speechiness' in feature_name:
        return 'Spoken Word' if value > 0.66 else ('Music and Speech' if value > 0.33 else 'Music')
    if 'valence' in feature_name:
        return 'Positive' if value > 0.5 else 'Negative'
    if 'loudness' in feature_name:
        return 'Loud' if value > -60 else 'Quiet'
    if 'liveness' in feature_name:
        return 'Live' if value > 0.8 else 'Not Live'
    if 'instrumentalness' in feature_name:
        return 'Instrumental' if value > 0.5 else 'Vocal'
    if 'energy' in feature_name:
        return 'High Energy' if value > 0.5 else 'Low Energy'
    if 'danceability' in feature_name:
        return 'Danceable' if value > 0.5 else 'Not Danceable'
    if 'acousticness' in feature_name:
        return 'Acoustic' if value > 0.5 else 'Not Acoustic'


def normalize_audio_features(merged_df):
    # audio features to be normalized
    audio_features = [
        'danceability', 'energy', 'loudness', 'speechiness',
        'acousticness', 'instrumentalness', 'liveness', 'valence',
        'danceability_avg_artist', 'energy_avg_artist', 'loudness_avg_artist', 'speechiness_avg_artist',
        'acousticness_avg_artist', 'instrumentalness_avg_artist', 'liveness_avg_artist', 'valence_avg_artist',
        'danceability_avg_playlist', 'energy_avg_playlist', 'loudness_avg_playlist',
        'speechiness_avg_playlist', 'acousticness_avg_playlist', 'instrumentalness_avg_playlist',
        'liveness_avg_playlist', 'valence_avg_playlist'
    ]

    scaler = MinMaxScaler()

    # Fit the scaler to the audio features and transform
    merged_df[audio_features] = scaler.fit_transform(merged_df[audio_features])

    return merged_df


def join_data():
    file_list = ['0-999', '1000-1999']
    audio_features_songs = [
        'danceability', 'energy', 'loudness', 'speechiness',
        'acousticness', 'instrumentalness', 'liveness', 'valence'
    ]

    audio_features_artists = [
        'danceability_avg_artist', 'energy_avg_artist', 'loudness_avg_artist', 'speechiness_avg_artist',
        'acousticness_avg_artist', 'instrumentalness_avg_artist', 'liveness_avg_artist', 'valence_avg_artist'
    ]

    audio_features_playlists = [
        'acousticness_avg_playlist', 'danceability_avg_playlist', 'energy_avg_playlist', 'loudness_avg_playlist',
        'speechiness_avg_playlist', 'instrumentalness_avg_playlist', 'liveness_avg_playlist', 'valence_avg_playlist'
    ]

    final = []
    for x in file_list:
        mpd = pd.read_parquet(f'./data/mpd.slice.{x}.parquet')
        tracks = pd.read_csv(f'./data/track_feature_mpd.slice.{x}.csv')
        audio_features = pd.read_csv(f'./data/songs_mpd.slice.{x}.csv')
        artists = pd.read_csv(f'./data/artists_mpd.slice.{x}.csv')

        mpd['raw_track_uri'] = mpd['track_uri'].str.split(':')
        mpd['raw_track_uri'] = mpd['raw_track_uri'].str[2]
        mpd['raw_artist_uri'] = mpd['artist_uri'].str.split(':')
        mpd['raw_artist_uri'] = mpd['raw_artist_uri'].str[2]

        test_mpd = mpd[mpd['raw_track_uri'] == '0MYTcPXAAmeKBUpBgtAV0J']
        test_audio_features = audio_features[audio_features['raw_track_uri'] == '0MYTcPXAAmeKBUpBgtAV0J']
        test_tracks = tracks[tracks['raw_track_uri'] == '0MYTcPXAAmeKBUpBgtAV0J']

        merged_df = pd.merge(mpd, audio_features, on='raw_track_uri', how='outer', suffixes=('', '_audiofeature'))
        merged_df = merged_df.merge(tracks, on='raw_track_uri', how='outer', suffixes=('', '_trackfeature'))
        merged_df = merged_df.merge(artists, on='raw_artist_uri', how='outer', suffixes=('', '_artists'))
        merged_df = merged_df.drop_duplicates(subset=['raw_track_uri', 'pid', 'pos'])

        audio_features_columns = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness',
                                  'instrumentalness', 'liveness', 'valence']
        artist_audio_features_avg = merged_df.groupby('artist_name')[audio_features_columns].mean().reset_index()
        artist_audio_features_avg.columns = ['artist_name'] + [f'{col}_avg_artist' for col in audio_features_columns]
        merged_df = pd.merge(merged_df, artist_audio_features_avg, on='artist_name', how='left')

        playlist_audio_features_avg = merged_df.groupby('name')[audio_features_columns].mean().reset_index()
        playlist_audio_features_avg.columns = ['name'] + [f'{col}_avg_playlist' for col in audio_features_columns]
        merged_df = pd.merge(merged_df, playlist_audio_features_avg, on='name', how='left')

        # Apply the categorization function to each audio feature for songs, artists, and playlists
        for feature in audio_features_songs:
            merged_df['category_' + feature] = merged_df[feature].apply(lambda x: categorize_feature(x, feature))

        for feature in audio_features_artists:
            merged_df['category_' + feature] = merged_df[feature].apply(lambda x: categorize_feature(x, feature))

        for feature in audio_features_playlists:
            merged_df['category_' + feature] = merged_df[feature].apply(lambda x: categorize_feature(x, feature))

        # Normalize the audio features.
        merged_df = normalize_audio_features(merged_df)
        final.append(merged_df)

    final = pd.concat(final)

    cols_to_keep = [
        'pos', 'artist_name', 'track_name', 'duration_ms', 'album_name', 'pid', 'name', 'modified_at', 'num_artists',
        'num_albums', 'num_tracks', 'num_followers', 'num_edits', 'playlist_duration_ms', 'collaborative',
        'description',
        'num_playlists_track', 'num_playlists_artist', 'raw_track_uri', 'raw_artist_uri', 'danceability', 'energy',
        'key',
        'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
        'time_signature',
        'explicit', 'is_playable', 'track_popularity', 'followers', 'genres', 'name_artists', 'popularity',
        'type_artists',
        'uri_artists', 'danceability_avg_artist', 'energy_avg_artist', 'loudness_avg_artist', 'speechiness_avg_artist',
        'acousticness_avg_artist', 'instrumentalness_avg_artist', 'liveness_avg_artist', 'valence_avg_artist',
        'danceability_avg_playlist', 'energy_avg_playlist', 'loudness_avg_playlist', 'speechiness_avg_playlist',
        'acousticness_avg_playlist', 'instrumentalness_avg_playlist', 'liveness_avg_playlist', 'valence_avg_playlist',
        'category_danceability', 'category_energy', 'category_loudness', 'category_speechiness',
        'category_acousticness',
        'category_instrumentalness', 'category_liveness', 'category_valence', 'category_danceability_avg_artist',
        'category_energy_avg_artist', 'category_loudness_avg_artist', 'category_speechiness_avg_artist',
        'category_acousticness_avg_artist', 'category_instrumentalness_avg_artist', 'category_liveness_avg_artist',
        'category_valence_avg_artist', 'category_acousticness_avg_playlist', 'category_danceability_avg_playlist',
        'category_energy_avg_playlist', 'category_loudness_avg_playlist', 'category_speechiness_avg_playlist',
        'category_instrumentalness_avg_playlist', 'category_liveness_avg_playlist', 'category_valence_avg_playlist'

    ]

    final = final[cols_to_keep]
    final.to_csv('./processed_data/merged_dataset.csv', encoding='utf-8-sig', index=False)


if __name__ == '__main__':
    # df = pd.read_csv('./processed_data/spotify_splice_audio_features.csv')
    # final_df = add_song_occurrences(df=df)
    # final_df = add_artist_occurrences(df=final_df)
    # final_df = add_album_occurrences(df=final_df)
    # final_df = final_df.sort_values(by=['pid', 'pos'])
    # final_df.to_parquet(f'./processed_data/processed_spotify_splice_audio_features.parquet')
    # final_df.to_csv(f'./processed_data/processed_spotify_splice_audio_features.csv')

    join_data()

    # path = "./data/*.parquet"
    #
    # for fname in glob.glob(path):
    #     print(f'Processing {fname}')
    #     curr_df = pd.read_parquet(fname)
    #     curr_df = curr_df.drop_duplicates()
    #     curr_df = curr_df.sort_values(by=['pid', 'pos'])
    #
    #     # Add the calculated fields.
    #     final_df = add_song_occurrences(df=curr_df)
    #     final_df = add_artist_occurrences(df=final_df)
    #     final_df = add_album_occurrences(df=final_df)
    #     final_df = final_df.sort_values(by=['pid', 'pos'])
    #
    #     # Split fname to get the name of the file.
    #     new_fname = fname.split('\\')
    #     new_fname = new_fname[1]
    #     new_fname = new_fname.replace('.parquet', '')
    #
    #     final_df['raw_artist_uri'] = final_df['artist_uri'].str.split(':')
    #     final_df['raw_artist_uri'] = final_df['raw_artist_uri'].str[2]
    #
    #     final_df['raw_track_uri'] = final_df['track_uri'].str.split(':')
    #     final_df['raw_track_uri'] = final_df['raw_track_uri'].str[2]
    #
    #     # Output to csv and parquet. csv for tableau usage, parquet for Python usage.
    #     final_df.to_csv(f'./processed_data/processed_{new_fname}.csv', index=False)
    #     final_df.to_parquet(f'./processed_data/processed_{new_fname}.parquet')
    print()
