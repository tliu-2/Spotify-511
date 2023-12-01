import json

def replace_values(json1, json2):
    with open(json1, 'r') as file1, open(json2, 'r') as file2:
        data1 = json.load(file1)
        data2 = json.load(file2)

    for key, value in data1.items():
        if 'genre' in value:
            genres = value['genre']
            updated_genres = []
            for genre in genres:
                for new_key, new_genres in data2.items():
                    if genre in new_genres:
                        updated_genres.append(new_key)
                        break
                else:
                    updated_genres.append(genre)
            value['genre'] = updated_genres

    with open(json1, 'w') as file1:
        json.dump(data1, file1, indent=2)  



def convert_to_set(json1):
    with open(json1, 'r') as file1:
        data1 = json.load(file1)

    for key, value in data1.items():
        if 'genre' in value:
            genres = value['genre']
            print(genres)
            print(set(list(genres)))
            print(list(set(list(genres))))

            value['genre'] = list(set(list(genres)))

    with open(json1, 'w') as file1:
        json.dump(data1, file1, indent=2)  



# Replace 'path/to/your/json1.json' and 'path/to/your/json2.json' with your actual file paths
# replace_values('artists_scraped_all.json', 'genres_classified.json')
convert_to_set('artists_scraped_all.json')