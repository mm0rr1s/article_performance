import pandas as pd
import pytest
from io import StringIO
from influential_articles import load_hitlog, find_influential_articles  

@pytest.fixture
def sample_hitlog():
    """Provide a sample dataset for testing."""
    test_csv = StringIO("""page_name,page_url,user_id,timestamp
    "Article 1","/articles/article-1","user1","2025-03-25 10:00:00"
    "Article 2","/articles/article-2","user1","2025-03-25 10:05:00"
    "Article 3","/articles/article-3","user1","2025-03-25 10:10:00"
    "Registration","/register","user1","2025-03-25 10:15:00"
    "Article 1","/articles/article-1","user2","2025-03-25 11:00:00"
    "Registration","/register","user2","2025-03-25 11:05:00"
    "Article 2","/articles/article-2","user3","2025-03-25 12:00:00"
    "Article 1","/articles/article-1","user3","2025-03-25 12:05:00"
    "Registration","/register","user3","2025-03-25 12:10:00"
    """)
    return pd.read_csv(test_csv)

def test_load_hitlog(sample_hitlog):
    """Test loading the hitlog data."""
    assert len(sample_hitlog) == 9  # 9 rows of data

def test_find_influential_articles(sample_hitlog):
    """Test identifying influential articles."""
    article_counts = find_influential_articles(sample_hitlog)
    assert len(article_counts) == 3  # Top 3 articles should be identified

def test_no_registration():
    """Test handling of data with no registrations."""
    no_reg_csv = StringIO("""page_name,page_url,user_id,timestamp
    "Article 1","/articles/article-1","user1","2025-03-25 10:00:00"
    "Article 2","/articles/article-2","user1","2025-03-25 10:05:00"
    """)
    no_reg_df = pd.read_csv(no_reg_csv)
    article_counts = find_influential_articles(no_reg_df)
    assert article_counts.empty  # Should return an empty DataFrame

def test_no_articles():
    """Test handling of data with no articles."""
    no_articles_csv = StringIO("""page_name,page_url,user_id,timestamp
    "Home","/home","user1","2025-03-25 10:00:00"
    "Registration","/register","user1","2025-03-25 10:05:00"
    """)
    no_articles_df = pd.read_csv(no_articles_csv)
    article_counts = find_influential_articles(no_articles_df)
    assert article_counts.empty  # Should return an empty DataFrame

def test_multiple_registrations():
    """Test handling of data with multiple registrations."""
    multiple_reg_csv = StringIO("""page_name,page_url,user_id,timestamp
    "Article 1","/articles/article-1","user1","2025-03-25 10:00:00"
    "Registration","/register","user1","2025-03-25 10:05:00"
    "Article 2","/articles/article-2","user1","2025-03-25 10:10:00"
    "Registration","/register","user1","2025-03-25 10:15:00"
    """)
    multiple_reg_df = pd.read_csv(multiple_reg_csv)
    article_counts = find_influential_articles(multiple_reg_df)
    assert len(article_counts) == 1 