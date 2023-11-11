import pandas as pd
import json
import glob


if __name__ == '__main__':

    path = "./data/*.json"
    for fname in glob.glob(path):
        with open(fname) as data_file:
            data = json.load(data_file)

        # Rename the 'duration_ms' in the playlist metadata to avoid conflict with track 'duration_ms'
        for playlist in data['playlists']:
            playlist['playlist_duration_ms'] = playlist.pop('duration_ms')

        # Define the metadata fields to include, with 'duration_ms' changed to 'playlist_duration_ms'
        metadata_fields = [
            'pid', 'name', 'modified_at', 'num_artists', 'num_albums', 'num_tracks',
            'num_followers', 'num_edits', 'playlist_duration_ms', 'collaborative', 'description'
        ]

        # Normalize the JSON data with the updated metadata fields
        playlists_df = pd.json_normalize(
            data['playlists'],
            record_path='tracks',
            meta=metadata_fields,
            errors='ignore'
        )

        # Calculate how many playlists a track appears in
        track_playlist_counts = playlists_df.groupby('track_uri')['pid'].nunique().reset_index(name='num_playlists')

        # Calculate how many playlists feature an artist
        artist_playlist_counts = playlists_df.groupby('artist_uri')['pid'].nunique().reset_index(name='num_playlists')

        # Merge these counts back to the original dataframe if needed
        # For each track
        playlists_df = playlists_df.merge(track_playlist_counts, on='track_uri', how='left')

        # And for each artist
        playlists_df = playlists_df.merge(artist_playlist_counts, on='artist_uri', how='left', suffixes=('_track', '_artist'))


        # 'num_playlists_track' - indicates how many playlists each track appears in
        # 'num_playlists_artist' - indicates how many playlists feature each artist
        new_fname = fname.split('\\')
        new_fname = new_fname[1]
        new_fname = new_fname.replace('.json', '')
        playlists_df.to_csv(f'./data/{new_fname}.csv')
        playlists_df.to_parquet(f'./data/{new_fname}.parquet')
    print()