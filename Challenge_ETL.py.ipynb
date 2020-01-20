{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import json\n",
    "import pandas as pd\n",
    "import numpy as np\n",
    "import re\n",
    "from sqlalchemy import create_engine \n",
    "import psycopg2\n",
    "from config import db_password\n",
    "import time"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Define a variable for the directory that's holding the data\n",
    "file_dir = \"C:/Users/krumb/Classwork/Movies_ETL\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "C:\\Users\\krumb\\Anaconda3\\envs\\PythonData\\lib\\site-packages\\IPython\\core\\interactiveshell.py:3051: DtypeWarning: Columns (10) have mixed types. Specify dtype option on import or set low_memory=False.\n",
      "  interactivity=interactivity, compiler=compiler, result=result)\n"
     ]
    }
   ],
   "source": [
    "kaggle_metadata = pd.read_csv(f\"{file_dir}/Data/movies_metadata.csv\")\n",
    "ratings = pd.read_csv(f\"{file_dir}/Data/ratings.csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "'C:/Users/krumb/Classwork/Movies_ETLwikipedia.movies.json'"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# To open the file in the directory instead of needing to type out entire directory every time\n",
    "# If I move files, I only need to update the file_dir rather than \n",
    "f\"{file_dir}wikipedia.movies.json\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Load the JSON file into a list of dictionaries\n",
    "with open(f\"{file_dir}/Data/wikipedia.movies.json\", mode=\"r\") as file:\n",
    "    wiki_movies_raw = json.load(file)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create a fiter expression only for movies with a director and IMDb link\n",
    "wiki_movies = [movie for movie in wiki_movies_raw\n",
    "                if (\"Director\" in movie or \"Directed by\" in movie) \n",
    "                and \"imdb_link\" in movie\n",
    "                and \"No. of episodes\" not in movie]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Handle alternative titles by creating an empty dictionary\n",
    "# loop through all alt. title keys\n",
    "# if they exist, remove the key-value pair and add to alt. titles dict.\n",
    "def clean_movie(movie):\n",
    "    movie = dict(movie) # create a non-destructive copy\n",
    "    alt_titles = {}\n",
    "    # combine alternate titles into one list\n",
    "    for key in ['Also known as','Arabic','Cantonese','Chinese','French',\n",
    "                'Hangul','Hebrew','Hepburn','Japanese','Literally',\n",
    "                'Mandarin','McCune–Reischauer','Original title','Polish',\n",
    "                'Revised Romanization','Romanized','Russian',\n",
    "                'Simplified','Traditional','Yiddish']:\n",
    "        if key in movie:\n",
    "            alt_titles[key] = movie[key]\n",
    "            movie.pop(key)\n",
    "    if len(alt_titles) > 0:\n",
    "        movie['alt_titles'] = alt_titles \n",
    "\n",
    "    # merger column names that mean the same thing\n",
    "    def change_column_name(old_name, new_name):\n",
    "        if old_name in movie:\n",
    "            movie[new_name] = movie.pop(old_name)\n",
    "    change_column_name('Adaptation by', 'Writer(s)')\n",
    "    change_column_name('Country of origin', 'Country')\n",
    "    change_column_name('Directed by', 'Director')\n",
    "    change_column_name('Distributed by', 'Distributor')\n",
    "    change_column_name('Edited by', 'Editor(s)')\n",
    "    change_column_name('Length', 'Running time')\n",
    "    change_column_name('Original release', 'Release date')\n",
    "    change_column_name('Music by', 'Composer(s)')\n",
    "    change_column_name('Produced by', 'Producer(s)')\n",
    "    change_column_name('Producer', 'Producer(s)')\n",
    "    change_column_name('Productioncompanies ', 'Production company(s)')\n",
    "    change_column_name('Productioncompany ', 'Production company(s)')\n",
    "    change_column_name('Released', 'Release Date')\n",
    "    change_column_name('Release Date', 'Release date')\n",
    "    change_column_name('Screen story by', 'Writer(s)')\n",
    "    change_column_name('Screenplay by', 'Writer(s)')\n",
    "    change_column_name('Story by', 'Writer(s)')\n",
    "    change_column_name('Theme music composer', 'Composer(s)')\n",
    "    change_column_name('Written by', 'Writer(s)')\n",
    "\n",
    "    return movie"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Make a list of cleanned movies with a list comprehension\n",
    "try:\n",
    "    clean_movies = [clean_movie(movie) for movie in wiki_movies]\n",
    "    wiki_movies_df = pd.DataFrame(clean_movies)\n",
    "except:\n",
    "    print (\"Unable to transform wikipedia data into DataFrame.\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [],
   "source": [
    "# \"(tt\\d{7}}\" the {7} says to match the last thing exactly 7 times\n",
    "# doing this to get each individual movie with an IMDb \n",
    "# need to put an 'r' in front of the quotes because there is a backslash being used\n",
    "wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\\d{7})')\n",
    "wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[['url', 0],\n",
       " ['year', 0],\n",
       " ['imdb_link', 0],\n",
       " ['title', 1],\n",
       " ['Based on', 4852],\n",
       " ['Starring', 184],\n",
       " ['Narrated by', 6752],\n",
       " ['Cinematography', 691],\n",
       " ['Release date', 32],\n",
       " ['Running time', 139],\n",
       " ['Country', 236],\n",
       " ['Language', 244],\n",
       " ['Budget', 2295],\n",
       " ['Box office', 1548],\n",
       " ['Director', 0],\n",
       " ['Distributor', 357],\n",
       " ['Editor(s)', 548],\n",
       " ['Composer(s)', 518],\n",
       " ['Producer(s)', 202],\n",
       " ['Production company(s)', 1678],\n",
       " ['Writer(s)', 199],\n",
       " ['Genre', 6923],\n",
       " ['Original language(s)', 6875],\n",
       " ['Original network', 6908],\n",
       " ['Executive producer(s)', 6936],\n",
       " ['Production location(s)', 6986],\n",
       " ['Picture format', 6969],\n",
       " ['Audio format', 6972],\n",
       " ['Voices of', 7031],\n",
       " ['Followed by', 7024],\n",
       " ['Created by', 7023],\n",
       " ['Preceded by', 7023],\n",
       " ['Suggested by', 7032],\n",
       " ['alt_titles', 7012],\n",
       " ['Recorded', 7031],\n",
       " ['Venue', 7032],\n",
       " ['Label', 7031],\n",
       " ['Animation by', 7031],\n",
       " ['Color process', 7032],\n",
       " ['imdb_id', 0]]"
      ]
     },
     "execution_count": 10,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# # Find null values\n",
    "[[column,wiki_movies_df[column].isnull().sum()] for column in wiki_movies_df.columns]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "metadata": {},
   "outputs": [],
   "source": [
    "# How to get rid of columns where 90% of the values are null\n",
    "wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]\n",
    "wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "metadata": {},
   "outputs": [],
   "source": [
    "# BOX OFFICE\n",
    "# Box office data should be numeric. Only want to look at rows that has defined data (aka drop missing values) \n",
    "box_office = wiki_movies_df[\"Box office\"].dropna()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Can see that some are stored as lists. We want to use a join() function and use a space as our joining character\n",
    "box_office = box_office.apply(lambda x: \" \".join(x) if type(x) == list else x)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create a regular expression to catch all of the box office values\n",
    "# Fixing pattern matches\n",
    "#   Some values have spaces in between the dollar sign and the number...just need to add '\\s*' after the $\n",
    "#   When million is misspelled. Just need to make the second i optional\n",
    "form_one = r\"\\$\\s*\\d+\\.?\\d*\\s*[mb]illi?on\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "3903"
      ]
     },
     "execution_count": 16,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Count how many box office values match our first form\n",
    "# To ignore whether letters are upper or lowercase, add the argument 'flags' and set it equal to 're.IGNORECASE'\n",
    "box_office.str.contains(form_one, flags=re.IGNORECASE).sum()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "1559"
      ]
     },
     "execution_count": 17,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Next, create a reg. ex. that matches the form '$123,456,789'\n",
    "# Fixing pattern matches\n",
    "#   Some values have spaces in between the dollar sign and the number...just need to add '\\s*'\n",
    "#   Some values use a period as a thousands separator, not a comma, add in '[,\\.]'\n",
    "#   Need to add a negatice lookahead because we don't want to catch values like '1.234 billion'\n",
    "form_two = r\"\\$\\s*\\d{1,3}(?:[,\\.]\\d{3})+(?!\\s[mb]illion)\"\n",
    "box_office.str.contains(form_two, flags=re.IGNORECASE).sum()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Want to see if any values aren't descibed by either form, and if any value is described by both\n",
    "matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE)\n",
    "matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Solve values given as a range\n",
    "box_office = box_office.str.replace(r\"\\$.*[---](?![a-z])\", \"$\", regex=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>0</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>$21.4 million</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>$2.7 million</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>$57,718,089</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>$7,331,647</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>$6,939,946</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>...</th>\n",
       "      <td>...</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7070</th>\n",
       "      <td>$19.4 million</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7071</th>\n",
       "      <td>$41.9 million</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7072</th>\n",
       "      <td>$76.1 million</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7073</th>\n",
       "      <td>$38.4 million</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7074</th>\n",
       "      <td>$5.5 million</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "<p>5485 rows × 1 columns</p>\n",
       "</div>"
      ],
      "text/plain": [
       "                  0\n",
       "0     $21.4 million\n",
       "1      $2.7 million\n",
       "2       $57,718,089\n",
       "3        $7,331,647\n",
       "4        $6,939,946\n",
       "...             ...\n",
       "7070  $19.4 million\n",
       "7071  $41.9 million\n",
       "7072  $76.1 million\n",
       "7073  $38.4 million\n",
       "7074   $5.5 million\n",
       "\n",
       "[5485 rows x 1 columns]"
      ]
     },
     "execution_count": 20,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Extract and convert the box office values. Only want to extract the parts of the string that match. Use 'str.extract()'\n",
    "box_office.str.extract(f\"({form_one}|{form_two})\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Need a function to turn the extracted values into a numeric value\n",
    "def parse_dollars(s):\n",
    "    # if s is not a string, return NaN\n",
    "    if type(s) != str:\n",
    "        return np.nan\n",
    "\n",
    "    # if input is of the form $###.# million\n",
    "    if re.match(r'\\$\\s*\\d+\\.?\\d*\\s*milli?on', s, flags=re.IGNORECASE):\n",
    "\n",
    "        # remove dollar sign and \" million\"\n",
    "        s = re.sub('\\$|\\s|[a-zA-Z]','', s)\n",
    "\n",
    "        # convert to float and multiply by a million\n",
    "        value = float(s) * 10**6\n",
    "\n",
    "        # return value\n",
    "        return value\n",
    "\n",
    "    # if input is of the form $###.# billion\n",
    "    elif re.match(r'\\$\\s*\\d+\\.?\\d*\\s*billi?on', s, flags=re.IGNORECASE):\n",
    "\n",
    "        # remove dollar sign and \" billion\"\n",
    "        s = re.sub('\\$|\\s|[a-zA-Z]','', s)\n",
    "\n",
    "        # convert to float and multiply by a billion\n",
    "        value = float(s) * 10**9\n",
    "\n",
    "        # return value\n",
    "        return value\n",
    "\n",
    "    # if input is of the form $###,###,###\n",
    "    elif re.match(r'\\$\\s*\\d{1,3}(?:[,\\.]\\d{3})+(?!\\s[mb]illion)', s, flags=re.IGNORECASE):\n",
    "\n",
    "        # remove dollar sign and commas\n",
    "        s = re.sub('\\$|,','', s)\n",
    "\n",
    "        # convert to float\n",
    "        value = float(s)\n",
    "\n",
    "        # return value\n",
    "        return value\n",
    "\n",
    "    # otherwise, return NaN\n",
    "    else:\n",
    "        return np.nan\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Now we can parse the box office values to numeric values\n",
    "#   Extract values from box_office using str.extract and apply parse_dollars to the first column of the dataframe\n",
    "wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "metadata": {},
   "outputs": [],
   "source": [
    "# No longer need the box office column, so we may drop it \n",
    "wiki_movies_df.drop('Box office', axis=1, inplace=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "metadata": {},
   "outputs": [],
   "source": [
    "# BUDGET\n",
    "# Create a budget variable\n",
    "budget = wiki_movies_df['Budget'].dropna()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Convert lists to strings\n",
    "budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Remove values between a $ and a hyphen\n",
    "budget = budget.str.replace(r'\\$.*[-—–](?![a-z])', '$', regex=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Parse the budget data\n",
    "matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)\n",
    "matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Remove citation references\n",
    "budget = budget.str.replace(r'\\[\\d+\\]\\s*', '')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Can now parse the budget values\n",
    "wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Drop the original budget column\n",
    "wiki_movies_df.drop('Budget', axis=1, inplace=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "metadata": {},
   "outputs": [],
   "source": [
    "# RELEASE DATE\n",
    "# Make a variable that holds non-null values of Release date in the dataframe, converting lists to strings\n",
    "release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "metadata": {},
   "outputs": [],
   "source": [
    "# What we'll be parsing:\n",
    "#   January 1, 2000....form_one\n",
    "#   2000-01-01....form_two\n",
    "#   January 2000....form_three\n",
    "#   four-digit year....form_four\n",
    "date_form_one = r\"(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s[123]\\d,\\s\\d{4}\"\n",
    "date_form_two = r\"\\d{4}.[01]\\d.[123]\\d\"\n",
    "date_form_three = r\"(?:January|February|March|April|May|June|July|August|September|October|November|December)\\s\\d{4}\"\n",
    "date_form_four = r\"\\d{4}\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Use the built-in to_datetime() method in Pandas\n",
    "# There are different date formats, want to set the 'infer_datetime_format' to 'True'\n",
    "wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "metadata": {},
   "outputs": [],
   "source": [
    "# RUNNING TIME\n",
    "# Make a variable that holds the non-null values of Running time in the dataframe\n",
    "running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Need to match all the hour + minute patterns with one regex. Must:\n",
    "#   Start with one digit\n",
    "#   Have an optional space after the digit and before \"h\"\n",
    "#   Capture all abbreviations of \"hour(s)\". Need to make every letter optional except the \"h\"\n",
    "#   Have an optional space after hours marker\n",
    "#   Have optional number of digits for minutes\n",
    "#   CODE = \\d+\\s*ho?u?r?s?\\s*\\d*\n",
    "#   Want to only extract digits, so add capture groups () around the digits. Also, add in the or statement | for the other form\n",
    "running_time_extract = running_time.str.extract(r'(\\d+)\\s*ho?u?r?s?\\s*(\\d*)|(\\d+)\\s*m')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "metadata": {},
   "outputs": [],
   "source": [
    "# The dataframe is all strings, so we need to convert to numeric values. Use 'to_numeric()'\n",
    "# Set the errors arguement to 'coerce', this will turn empty strings to NaN\n",
    "# We can then use 'fillna()' to change all NaN's to zero \n",
    "running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Apply a function that will convert the hour and minute capture groups to minutes\n",
    "wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 38,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Can now drop Running time from the dataset\n",
    "wiki_movies_df.drop('Running time', axis=1, inplace=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Keep rows where adult column is False, and drop adult column\n",
    "kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "0        False\n",
       "1        False\n",
       "2        False\n",
       "3        False\n",
       "4        False\n",
       "         ...  \n",
       "45461    False\n",
       "45462    False\n",
       "45463    False\n",
       "45464    False\n",
       "45465    False\n",
       "Name: video, Length: 45454, dtype: bool"
      ]
     },
     "execution_count": 40,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Convert data types\n",
    "kaggle_metadata['video'] == 'True'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 41,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Assign  it back to video\n",
    "kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 42,
   "metadata": {},
   "outputs": [],
   "source": [
    "# For numeric columns: use to_numeric(), and make sure errors= is set to 'raise' so we know of any data that can't be converted to numbers\n",
    "kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)\n",
    "kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')\n",
    "kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 43,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Convert release_date to datetime\n",
    "kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 26024289 entries, 0 to 26024288\n",
      "Data columns (total 4 columns):\n",
      "userId       26024289 non-null int64\n",
      "movieId      26024289 non-null int64\n",
      "rating       26024289 non-null float64\n",
      "timestamp    26024289 non-null int64\n",
      "dtypes: float64(1), int64(3)\n",
      "memory usage: 794.2 MB\n"
     ]
    }
   ],
   "source": [
    "# RATINGS DATA\n",
    "# Use null_counts with option True \n",
    "ratings.info(null_counts=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 45,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "0          2015-03-09 22:52:09\n",
       "1          2015-03-09 23:07:15\n",
       "2          2015-03-09 22:52:03\n",
       "3          2015-03-09 22:52:26\n",
       "4          2015-03-09 22:52:36\n",
       "                   ...        \n",
       "26024284   2009-10-31 23:26:04\n",
       "26024285   2009-10-31 23:33:52\n",
       "26024286   2009-10-31 23:29:24\n",
       "26024287   2009-11-01 00:06:30\n",
       "26024288   2009-10-31 23:30:58\n",
       "Name: timestamp, Length: 26024289, dtype: datetime64[ns]"
      ]
     },
     "execution_count": 45,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Use to_datetime() 'unix' as the time unit in seconds(s)\n",
    "pd.to_datetime(ratings['timestamp'], unit='s')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 46,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Dates look good, so assign to timestamp column\n",
    "ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 47,
   "metadata": {},
   "outputs": [],
   "source": [
    "# MERGING DATA\n",
    "# Print list of columns so we can see what is redundant\n",
    "movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 48,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Competing data:\n",
    "# Wiki                     Movielens                Resolution\n",
    "#--------------------------------------------------------------------------\n",
    "# title_wiki               title_kaggle            Drop wikipedia\n",
    "# running_time             runtime                 Keep kaggle data, but fill in zeros with wikipedia data                \n",
    "# budget_wiki              budget_kaggle           Keep kaggle data, but fill in zeros with wikipedia data\n",
    "# box_office               revenue                 Keep kaggle data, but fill in zeros with wikipedia data\n",
    "# release_date_wiki        release_date_kaggle\n",
    "# Language                 original_language       Drop wikipedia\n",
    "# Production company(s)    production_companies    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 49,
   "metadata": {},
   "outputs": [],
   "source": [
    "# PUT IT ALL TOGETHER\n",
    "movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 50,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Make a function that fills in missing data for a column pair, then drops the redundant column\n",
    "def fill_missing_kaggle_data(df, kaggle_column, wiki_column):\n",
    "    df[kaggle_column] = df.apply(\n",
    "        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]\n",
    "        , axis=1)\n",
    "    df.drop(columns=wiki_column, inplace=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 51,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Call the new function for the columns we will be filling in that have zeros\n",
    "fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')\n",
    "fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')\n",
    "fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 52,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Reorder the rows\n",
    "movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',\n",
    "                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',\n",
    "                       'genres','original_language','overview','spoken_languages','Country',\n",
    "                       'production_companies','production_countries','Distributor',\n",
    "                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'\n",
    "                      ]]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 53,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Rename columns\n",
    "movies_df.rename({'id':'kaggle_id',\n",
    "                  'title_kaggle':'title',\n",
    "                  'url':'wikipedia_url',\n",
    "                  'budget_kaggle':'budget',\n",
    "                  'release_date_kaggle':'release_date',\n",
    "                  'Country':'country',\n",
    "                  'Distributor':'distributor',\n",
    "                  'Producer(s)':'producers',\n",
    "                  'Director':'director',\n",
    "                  'Starring':'starring',\n",
    "                  'Cinematography':'cinematography',\n",
    "                  'Editor(s)':'editors',\n",
    "                  'Writer(s)':'writers',\n",
    "                  'Composer(s)':'composers',\n",
    "                  'Based on':'based_on'\n",
    "                 }, axis='columns', inplace=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 54,
   "metadata": {},
   "outputs": [],
   "source": [
    "rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \\\n",
    "                .rename({'userId':'count'}, axis=1) \\\n",
    "                .pivot(index='movieId',columns='rating', values='count')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 55,
   "metadata": {},
   "outputs": [],
   "source": [
    "rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 56,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Use a left merge\n",
    "movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 57,
   "metadata": {},
   "outputs": [],
   "source": [
    "movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 58,
   "metadata": {},
   "outputs": [],
   "source": [
    "# START DATA LOAD\n",
    "db_string = f\"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 59,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create a database engine \n",
    "engine = create_engine(db_string)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 60,
   "metadata": {},
   "outputs": [],
   "source": [
    "# IMPORT MOVIE DATA\n",
    "movies_df.to_sql(name='movies', con=engine, if_exists=\"replace\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "importing rows 0 to 100000...Done. 13.348315477371216 total seconds elapsed\n",
      "importing rows 100000 to 200000...Done. 25.272439002990723 total seconds elapsed\n",
      "importing rows 200000 to 300000...Done. 37.27848505973816 total seconds elapsed\n",
      "importing rows 300000 to 400000...Done. 48.7588574886322 total seconds elapsed\n",
      "importing rows 400000 to 500000...Done. 60.501654624938965 total seconds elapsed\n",
      "importing rows 500000 to 600000...Done. 72.109934091568 total seconds elapsed\n",
      "importing rows 600000 to 700000...Done. 84.25073885917664 total seconds elapsed\n",
      "importing rows 700000 to 800000...Done. 96.81627798080444 total seconds elapsed\n",
      "importing rows 800000 to 900000...Done. 109.11094546318054 total seconds elapsed\n",
      "importing rows 900000 to 1000000...Done. 120.97537970542908 total seconds elapsed\n",
      "importing rows 1000000 to 1100000...Done. 132.7213840484619 total seconds elapsed\n",
      "importing rows 1100000 to 1200000...Done. 144.3063883781433 total seconds elapsed\n",
      "importing rows 1200000 to 1300000...Done. 156.66049909591675 total seconds elapsed\n",
      "importing rows 1300000 to 1400000...Done. 168.30771398544312 total seconds elapsed\n",
      "importing rows 1400000 to 1500000...Done. 180.02833890914917 total seconds elapsed\n",
      "importing rows 1500000 to 1600000...Done. 192.17761850357056 total seconds elapsed\n",
      "importing rows 1600000 to 1700000...Done. 204.4549400806427 total seconds elapsed\n",
      "importing rows 1700000 to 1800000...Done. 217.18911576271057 total seconds elapsed\n",
      "importing rows 1800000 to 1900000...Done. 229.61690592765808 total seconds elapsed\n",
      "importing rows 1900000 to 2000000...Done. 243.36016416549683 total seconds elapsed\n",
      "importing rows 2000000 to 2100000...Done. 255.73115611076355 total seconds elapsed\n",
      "importing rows 2100000 to 2200000...Done. 268.17101764678955 total seconds elapsed\n",
      "importing rows 2200000 to 2300000...Done. 280.2088348865509 total seconds elapsed\n",
      "importing rows 2300000 to 2400000...Done. 292.2147841453552 total seconds elapsed\n",
      "importing rows 2400000 to 2500000...Done. 303.8346788883209 total seconds elapsed\n",
      "importing rows 2500000 to 2600000...Done. 315.9165463447571 total seconds elapsed\n",
      "importing rows 2600000 to 2700000...Done. 327.69705176353455 total seconds elapsed\n",
      "importing rows 2700000 to 2800000...Done. 339.37283730506897 total seconds elapsed\n",
      "importing rows 2800000 to 2900000...Done. 350.74342107772827 total seconds elapsed\n",
      "importing rows 2900000 to 3000000...Done. 362.36935925483704 total seconds elapsed\n",
      "importing rows 3000000 to 3100000...Done. 373.9892876148224 total seconds elapsed\n",
      "importing rows 3100000 to 3200000...Done. 385.65608263015747 total seconds elapsed\n",
      "importing rows 3200000 to 3300000...Done. 397.2720305919647 total seconds elapsed\n",
      "importing rows 3300000 to 3400000...Done. 408.98471760749817 total seconds elapsed\n",
      "importing rows 3400000 to 3500000...Done. 420.7073760032654 total seconds elapsed\n",
      "importing rows 3500000 to 3600000...Done. 432.1108901500702 total seconds elapsed\n",
      "importing rows 3600000 to 3700000...Done. 443.7807083129883 total seconds elapsed\n",
      "importing rows 3700000 to 3800000...Done. 455.259978055954 total seconds elapsed\n",
      "importing rows 3800000 to 3900000...Done. 467.0395760536194 total seconds elapsed\n",
      "importing rows 3900000 to 4000000...Done. 478.5458345413208 total seconds elapsed\n",
      "importing rows 4000000 to 4100000...Done. 490.23853158950806 total seconds elapsed\n",
      "importing rows 4100000 to 4200000...Done. 501.92852783203125 total seconds elapsed\n",
      "importing rows 4200000 to 4300000...Done. 515.3087315559387 total seconds elapsed\n",
      "importing rows 4300000 to 4400000...Done. 529.5576860904694 total seconds elapsed\n",
      "importing rows 4400000 to 4500000...Done. 542.719484090805 total seconds elapsed\n",
      "importing rows 4500000 to 4600000...Done. 556.086748123169 total seconds elapsed\n",
      "importing rows 4600000 to 4700000...Done. 569.6345295906067 total seconds elapsed\n",
      "importing rows 4700000 to 4800000...Done. 582.8960428237915 total seconds elapsed\n",
      "importing rows 4800000 to 4900000...Done. 596.1117107868195 total seconds elapsed\n",
      "importing rows 4900000 to 5000000...Done. 610.6089541912079 total seconds elapsed\n",
      "importing rows 5000000 to 5100000...Done. 624.1218283176422 total seconds elapsed\n",
      "importing rows 5100000 to 5200000...Done. 637.7663507461548 total seconds elapsed\n",
      "importing rows 5200000 to 5300000...Done. 651.3191196918488 total seconds elapsed\n",
      "importing rows 5300000 to 5400000...Done. 665.1680989265442 total seconds elapsed\n",
      "importing rows 5400000 to 5500000...Done. 678.5892462730408 total seconds elapsed\n",
      "importing rows 5500000 to 5600000...Done. 694.7958884239197 total seconds elapsed\n",
      "importing rows 5600000 to 5700000...Done. 709.3519785404205 total seconds elapsed\n",
      "importing rows 5700000 to 5800000...Done. 722.7880520820618 total seconds elapsed\n",
      "importing rows 5800000 to 5900000...Done. 736.0645625591278 total seconds elapsed\n",
      "importing rows 5900000 to 6000000...Done. 749.4916622638702 total seconds elapsed\n",
      "importing rows 6000000 to 6100000...Done. 763.3077259063721 total seconds elapsed\n",
      "importing rows 6100000 to 6200000...Done. 777.0469954013824 total seconds elapsed\n",
      "importing rows 6200000 to 6300000...Done. 790.4750967025757 total seconds elapsed\n",
      "importing rows 6300000 to 6400000...Done. 803.9271337985992 total seconds elapsed\n",
      "importing rows 6400000 to 6500000...Done. 817.1278445720673 total seconds elapsed\n",
      "importing rows 6500000 to 6600000...Done. 830.4362666606903 total seconds elapsed\n",
      "importing rows 6600000 to 6700000...Done. 843.7267329692841 total seconds elapsed\n",
      "importing rows 6700000 to 6800000...Done. 857.092001914978 total seconds elapsed\n",
      "importing rows 6800000 to 6900000...Done. 870.6407806873322 total seconds elapsed\n",
      "importing rows 6900000 to 7000000...Done. 884.2154891490936 total seconds elapsed\n",
      "importing rows 7000000 to 7100000...Done. 1693.7718329429626 total seconds elapsed\n",
      "importing rows 7100000 to 7200000...Done. 1707.5809953212738 total seconds elapsed\n",
      "importing rows 7200000 to 7300000...Done. 1720.9975187778473 total seconds elapsed\n",
      "importing rows 7300000 to 7400000...Done. 1733.6040208339691 total seconds elapsed\n",
      "importing rows 7400000 to 7500000...Done. 1745.8542459011078 total seconds elapsed\n",
      "importing rows 7500000 to 7600000...Done. 1757.9549224376678 total seconds elapsed\n",
      "importing rows 7600000 to 7700000...Done. 1770.2219183444977 total seconds elapsed\n",
      "importing rows 7700000 to 7800000...Done. 1817.5825552940369 total seconds elapsed\n",
      "importing rows 7800000 to 7900000...Done. 1862.9708783626556 total seconds elapsed\n",
      "importing rows 7900000 to 8000000...Done. 1909.575196504593 total seconds elapsed\n",
      "importing rows 8000000 to 8100000...Done. 1955.4982562065125 total seconds elapsed\n",
      "importing rows 8100000 to 8200000...Done. 2002.0697567462921 total seconds elapsed\n",
      "importing rows 8200000 to 8300000...Done. 2049.139028787613 total seconds elapsed\n",
      "importing rows 8300000 to 8400000...Done. 2095.1338906288147 total seconds elapsed\n",
      "importing rows 8400000 to 8500000...Done. 2142.6663377285004 total seconds elapsed\n",
      "importing rows 8500000 to 8600000...Done. 2189.2066929340363 total seconds elapsed\n",
      "importing rows 8600000 to 8700000...Done. 2236.263628721237 total seconds elapsed\n",
      "importing rows 8700000 to 8800000..."
     ]
    }
   ],
   "source": [
    "# LOAD IN THE RATINGS DATA\n",
    "# Create variable for number of rows imported\n",
    "rows_imported = 0\n",
    "# get the start_time from time.time()\n",
    "start_time = time.time()\n",
    "\n",
    "for data in pd.read_csv(f'{file_dir}/Data/ratings.csv', chunksize=100000):\n",
    "    print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')\n",
    "    data.to_sql(name='ratings', con=engine, if_exists='append')\n",
    "    rows_imported += len(data)\n",
    "\n",
    "    # add elapsed time to final print out\n",
    "    print(f'Done. {time.time() - start_time} total seconds elapsed')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
