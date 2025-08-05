import pandas as pd

movie_ratings = pd.read_csv("datasets\movie_rating.csv")

light_mood = ['Comedy', 'Animation',
       'Children', 'Romance', 'Fantasy','Musical']
dark_mood = ['Mystery','Crime','Action',  'Thriller', 'Horror', 'War','Sci-Fi',]
neutral_mood = ['Adventure','Drama','Western','Documentary','(no genres listed)', 'IMAX','Film-Noir']

filtered_df = movie_ratings[movie_ratings['genre'].isin(light_mood + dark_mood)].copy()

def map_mood(genre):
    if genre in light_mood:
        return "Light Mood"
    elif genre in dark_mood:
        return "Dark Mood"
    else:
        return "Neutral Mood"
    

filtered_df["mood"] = filtered_df['genre'].apply(map_mood)
filtered_df.drop(columns=['genre'], inplace=True)
filtered_df.to_csv("datasets\movie_rating_2moods.csv", index=False)