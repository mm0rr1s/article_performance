# Article Performance

This repository contains a script for a pipeline that assesses the most influential articles leading to user registration.

## Overview

The script processes a hitlog CSV file to identify the top 3 most influential articles that lead users to register on the website. The results are saved to an output CSV file.

## Usage

1. **Input File**: Ensure you have a hitlog CSV file with the following columns:
    - `page_name`: The name of the page.
    - `page_url`: The URL of the page.
    - `user_id`: The ID of the user.
    - `timestamp`: The timestamp of the page visit.

2. **Run the Script**: Execute the script to process the hitlog and generate the output file.
    ```sh
    python influential_articles.py
    ```

3. **Output File**: The script will generate an output CSV file with the top 3 influential articles.