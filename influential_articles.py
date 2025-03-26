import pandas as pd
import sys
from io import StringIO

def load_hitlog(file_path: str) -> pd.DataFrame:
    """Load the hitlog CSV file into a Pandas DataFrame."""
    return pd.read_csv(file_path)

def find_influential_articles(df: pd.DataFrame):
    """Identify the most influential articles leading to registration using Pandas."""
    
    df = df.sort_values(['user_id', 'timestamp']) 

     # Track if the user has reached registration so if user does not register, articles not counted
    df['before_registration'] = df.groupby('user_id')['page_url'].apply(lambda x: (x == '/register').cumsum() == 0)

    # Filter out user journeys that do not end in registration
    users_with_registration = df[df['page_url'] == '/register']['user_id'].unique()
    df = df[df['user_id'].isin(users_with_registration)]

    # Filter only articles that appear before registration
    articles_before_reg = df[(df['before_registration']) & (df['page_url'].str.startswith('/articles/'))]

    # Use a set to track unique articles for each user
    unique_articles = articles_before_reg.groupby('user_id').apply(lambda x: x.drop_duplicates(subset=['page_name', 'page_url']))

    # Count occurrences of each article
    article_counts = unique_articles.groupby(['page_name', 'page_url']).size().reset_index(name='total')

    # Sort the articles by their counts in descending order
    article_counts_sorted = article_counts.sort_values(by='total', ascending=False).head(3)
    
    return article_counts_sorted

def save_top_articles(article_counts: pd.DataFrame, output_file: str):
    """Save the top 3 influential articles to a CSV file."""
    article_counts.to_csv(output_file, index=False)

def main(input_file: str, output_file: str):
    """Main function to run the pipeline."""
    df = load_hitlog(input_file)
    article_counts = find_influential_articles(df)
    save_top_articles(article_counts, output_file)

if __name__ == "__main__":
    if len(sys.argv) == 3:
        input_file_path = sys.argv[1]
        output_file_path = sys.argv[2]
        main(input_file_path, output_file_path)  # Run the pipeline
        print("Pipeline executed successfully.")