# NASA Historical Data Processing Pipeline

## Overview
This project consists of three main scripts designed to automate the process of downloading, processing, and analyzing historical environmental data from NASA. The pipeline involves:

1. **`downloader.py`** - Logs in to NASA's data portal using client cookies, downloads specified datasets, extracts relevant information, converts them into Pandas DataFrames, and deletes the original NetCDF (`.nc`) files to optimize disk space.
2. **`to_monthly_pm25.py`** - Converts the daily PM2.5 data into monthly averages for each location.
3. **`add_pm25.py`** - Merges the monthly PM2.5 data with `bol_cities_expanded.csv`.

---

## Installation & Requirements
Ensure that the following dependencies are installed:

```bash
pip install aiohttp pandas numpy aiofiles
```

---

## Usage

### Step 1: Download Data
Run the `downloader.py` script to fetch NASA data and process it into CSV format.

```bash
python downloader.py
```

**Functionality:**
- Reads URLs from a text file.
- Logs in using client cookies.
- Downloads NASA datasets asynchronously.
- Extracts relevant environmental variables from `.nc` files.
- Converts the extracted data into Pandas DataFrames.
- Deletes the original `.nc` files to free up disk space.

### Step 2: Convert Daily PM2.5 Data to Monthly Averages
Run the `to_monthly_pm25.py` script to aggregate daily PM2.5 readings into monthly averages.

```bash
python to_monthly_pm25.py
```

**Functionality:**
- Creates `PM25_monthly/` directory if it doesn't exist.
- Iterates through the daily PM2.5 datasets.
- Averages data for each month.
- Saves the monthly averages as CSV files.

### Step 3: Merge Monthly Data with City Data
Run the `add_pm25.py` script to merge the processed monthly PM2.5 data with city information in `bol_cities_expanded.csv`.

```bash
python add_pm25.py
```

---

## File Structure
```
.
├── downloader.py          # Downloads and processes NASA datasets
├── to_monthly_pm25.py     # Converts daily PM2.5 data to monthly averages
├── add_pm25.py            # Merges monthly PM2.5 data with city datasets
├── NASA_downloads/        # Directory where NASA datasets are temporarily stored
├── PM25_monthly/          # Directory for monthly PM2.5 averages
├── pollution_monthly/     # Intermediate directory for pollution data
├── bol_cities_expanded.csv # Input city dataset
├── utils.py               # Helper functions
└── undownloaded_urls.log  # Log file for failed downloads
```

---

## Logging
Errors and missing files are logged in:
- `monthlyData.log`: Logs issues encountered during data aggregation.
- `undownloaded_urls.log`: Logs failed NASA dataset downloads.

---

## Notes
- Ensure you replace `USE_YOUR_COOKIES` in `downloader.py` with your actual NASA cookies.
- The scripts are designed to handle missing data gracefully, but logs should be checked for any issues.
- The dataset spans from **2004-06-01 to 2014-01-01**.

---

## Future Improvements
- Automate cookie retrieval for seamless authentication.
- Implement parallel processing to improve performance.
- Extend support for additional datasets.

---

## License
This project is distributed under the MIT License.

