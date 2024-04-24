# Youtube Scrapping
This project is aimed at scraping data from YouTube channels, playlists, videos, and comments using the YouTube API and storing it in a MySQL database. It provides functionality to fetch various types of data from YouTube and perform analysis on the collected data.

## Features
+ Data Fetching: Fetch details about YouTube channels, playlists, videos, and comments using the YouTube API.
- Database Integration: Store the fetched data in a MySQL database for future analysis and reference.
* User Interface: Interact with the application through a Streamlit-based user interface, allowing users to input channel and playlist IDs and view the retrieved data.
+ Data Analysis: Perform various data analysis tasks such as determining top-viewed videos, channels with the most videos, total likes and views, average video duration, and more.

## Technologies Used
+ Python: Programming language used for scripting and backend development.
_ Streamlit: Python library for creating interactive web applications.
* Pandas: Data manipulation and analysis library used for handling data in tabular form.
+ MySQL: Relational database management system used for storing and managing the collected data.
- Google API Client Library for Python: Python library for interacting with Google APIs, specifically the YouTube API.

  ## Setup
1) Clone the repository:
`git clone https://github.com/yourusername/youtube-scraping-project.git`
2) Install the required dependencies:
`pip install -r requirements.txt`
3) Set up MySQL database and configure database connection in the code.
4) Obtain Google API Key from the Google Cloud Console and replace Api_key variable in the code with your API key.
5) Run the application:
`streamlit run main.py`

## Usage
1) Navigate to the Streamlit UI in your web browser.
2) Enter the channel ID or playlist ID to fetch data.
3) Explore various options to fetch channel details, playlist details, video details, and comments.
4) Analyze the fetched data using the provided queries in the application.

## Contributors
Ryan Sam Daniel N (@ryan-sam-daniel)
