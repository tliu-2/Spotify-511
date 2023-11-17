import pandas as pd
import glob


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




if __name__ == '__main__':

    df = pd.read_csv('./processed_data/spotify_splice_audio_features.csv')
    final_df = add_song_occurrences(df=df)
    final_df = add_artist_occurrences(df=final_df)
    final_df = add_album_occurrences(df=final_df)
    final_df = final_df.sort_values(by=['pid', 'pos'])
    final_df.to_parquet(f'./processed_data/processed_spotify_splice_audio_features.parquet')
    final_df.to_csv(f'./processed_data/processed_spotify_splice_audio_features.csv')


    path = "./data/*.parquet"

    for fname in glob.glob(path):
        print(f'Processing {fname}')
        curr_df = pd.read_parquet(fname)
        curr_df = curr_df.drop_duplicates()
        curr_df = curr_df.sort_values(by=['pid', 'pos'])

        # Add the calculated fields.
        final_df = add_song_occurrences(df=curr_df)
        final_df = add_artist_occurrences(df=final_df)
        final_df = add_album_occurrences(df=final_df)
        final_df = final_df.sort_values(by=['pid', 'pos'])

        # Split fname to get the name of the file.
        new_fname = fname.split('\\')
        new_fname = new_fname[1]
        new_fname = new_fname.replace('.parquet', '')

        final_df['raw_artist_uri'] = final_df['artist_uri'].str.split(':')
        final_df['raw_artist_uri'] = final_df['raw_artist_uri'].str[2]

        # Output to csv and parquet. csv for tableau usage, parquet for Python usage.
        final_df.to_csv(f'./processed_data/processed_{new_fname}.csv', index=False)
        final_df.to_parquet(f'./processed_data/processed_{new_fname}.parquet')
    print()
