# Where Are the Aliens? - Data Engineering & Analysis

## Project Overview

This project develops an ETL pipeline using PySpark to extract, transform, clean, and load a Kaggle dataset on UFO sightings. Data analysis is conducted on a global scale, including sentiment analysis of witness reports, with a specific focus on Australia.


## Links

https://www.kaggle.com/datasets/NUFORC/ufo-sightings

## Dataset Exploration

### Worldwide UFO Reports

#### Data Analysis

1) How does the frequency of UFO sightings differ over time?
2) What is the geographic distribution of UFO sightings worldwide?
3) Based on historical data, what are the most likely places to see a UFO and at what time?
4) What is the most common appearance of UFO's and for how long is each type seen?

#### Sentiment Analysis

8) Are the comments after a UFO sighting more negative or positive, on average?
9) What are the 20 most frequently used words in comments after UFO sightings by witnesses?
10) Are there commonalities between the profiles of people that are involved in UFO sightings?

### UFO Reports in Australia

#### Data Analysis

5) What is the geographic distribution of UFO sightings in Australia?
6) Based on historical data, what are the most likely places to see a UFO in Australia and at what time?
7) What is the most common appearance of UFO's and for how long is each type seen?

#### Australian Sentiment Analysis

8) Are the comments after a UFO sighting in Australia more negative or positive, on average?
9) What are the 20 most frequently used words in comments after UFO sightings in Australia by witnesses?
10) Are there commonalities between the profiles of people that are involved in UFO sightings in Australia?

## Technologies

- Pandas
- NumPy
- Matplotlib
- Seaborne
- SQLAlchemy
- PostgreSQL
- PySpark

## Summary of Dataset on Kaggle

```text
UFO Sightings
Reports of unidentified flying object reports in the last century

About Dataset
Context
This dataset contains over 80,000 reports of UFO sightings over the last century.

Content
There are two versions of this dataset: scrubbed and complete. The complete data includes entries where the location of the sighting was not found or blank (0.8146%) or have an erroneous or blank time (8.0237%). Since the reports date back to the 20th century, some older data might be obscured. Data contains city, state, time, description, and duration of each sighting.

Inspiration
What areas of the country are most likely to have UFO sightings?
Are there any trends in UFO sightings over time? Do they tend to be clustered or seasonal?
Do clusters of UFO sightings correlate with landmarks, such as airports or government research centers?
What are the most common UFO descriptions?
```
