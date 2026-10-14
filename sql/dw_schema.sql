DROP DATABASE IF EXISTS music_dw;
CREATE DATABASE music_dw;
USE music_dw;

CREATE TABLE dim_artists (
    artist_id VARCHAR(255) PRIMARY KEY, 
    artist_name TEXT, 
    grammy_winner_flag BOOLEAN
);

CREATE TABLE dim_tracks (
    track_id VARCHAR(255) PRIMARY KEY, 
    track_name TEXT, 
    acoustic_quartile VARCHAR(50),
    danceability FLOAT,
    valence FLOAT,
    duration_ms INT
);

CREATE TABLE fact_track_performance (
    track_id VARCHAR(255), 
    artist_id VARCHAR(255), 
    popularity_score INT, 
    FOREIGN KEY (track_id) REFERENCES dim_tracks(track_id), 
    FOREIGN KEY (artist_id) REFERENCES dim_artists(artist_id)
);