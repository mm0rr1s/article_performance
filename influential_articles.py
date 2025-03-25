import pandas as pd
import unittest
from io import StringIO

# Define the input and output file paths
input_file_path = "c:/Users/Mary/Downloads/hitlog.csv"
output_file_path =  "c:/Users/Mary/Downloads/top_articles.csv"

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

# --- Unit Test Class ---
class TestPipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up a mock hitlog dataset."""
        self.test_csv = StringIO("""
        page_name,page_url,user_id,timestamp
        "Article 1","/articles/article-1","user1","2025-03-25 10:00:00"
        "Article 2","/articles/article-2","user1","2025-03-25 10:05:00"
        "Article 3","/articles/article-3","user1","2025-03-25 10:10:00"
        "Registration","/register","user1","2025-03-25 10:15:00"
        "Article 1","/articles/article-1","user2","2025-03-25 11:00:00"
        "Registration","/register","user2","2025-03-25 11:05:00"
        "Article 2","/articles/article-2","user3","2025-03-25 12:00:00"
        "Article 1","/articles/article-1","user3","2025-03-25 12:05:00"
        "Registration","/register","user3","2025-03-25 12:10:00"
        """.strip())
        self.df = pd.read_csv(self.test_csv)
    
    def test_load_hitlog(self):
        """Test loading the hitlog data."""
        df = load_hitlog(StringIO(self.test_csv.getvalue()))
        self.assertEqual(len(df), 9)  # 9 rows of data
    
    def test_find_influential_articles(self):
        """Test identifying influential articles."""
        article_counts = find_influential_articles(self.df)
        self.assertEqual(len(article_counts), 3)  # Top 3 articles should be identified

    def test_no_registration(self):
        """Test handling of data with no registrations."""
        no_reg_csv = StringIO("""
        page_name,page_url,user_id,timestamp
        "Article 1","/articles/article-1","user1","2025-03-25 10:00:00"
        "Article 2","/articles/article-2","user1","2025-03-25 10:05:00"
        """.strip())
        no_reg_df = pd.read_csv(no_reg_csv)
        article_counts = find_influential_articles(no_reg_df)
        self.assertTrue(article_counts.empty)  # Should return an empty DataFrame

    def test_no_articles(self):
        """Test handling of data with no articles."""
        no_articles_csv = StringIO("""
        page_name,page_url,user_id,timestamp
        "Home","/home","user1","2025-03-25 10:00:00"
        "Registration","/register","user1","2025-03-25 10:05:00"
        """.strip())
        no_articles_df = pd.read_csv(no_articles_csv)
        article_counts = find_influential_articles(no_articles_df)
        self.assertTrue(article_counts.empty)  # Should return an empty DataFrame

    def test_multiple_registrations(self):
        """Test handling of data with multiple registrations."""
        multiple_reg_csv = StringIO("""
        page_name,page_url,user_id,timestamp
        "Article 1","/articles/article-1","user1","2025-03-25 10:00:00"
        "Registration","/register","user1","2025-03-25 10:05:00"
        "Article 2","/articles/article-2","user1","2025-03-25 10:10:00"
        "Registration","/register","user1","2025-03-25 10:15:00"
        """.strip())
        multiple_reg_df = pd.read_csv(multiple_reg_csv)
        article_counts = find_influential_articles(multiple_reg_df)
        self.assertEqual(len(article_counts), 1)  # Only the first registration should be considered
    

if __name__ == "__main__":
    main(input_file_path, output_file_path)  # Run the pipeline
    # Run the unit tests after processing
    print("Running unit tests...")
    unittest.main()
    
