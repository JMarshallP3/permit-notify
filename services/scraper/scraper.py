import logging
import requests
from bs4 import BeautifulSoup
from typing import Optional, List
from requests.exceptions import RequestException, Timeout, ConnectionError

logger = logging.getLogger(__name__)

class Scraper:
    """
    Web scraper class for permit notification system.
    Uses requests and BeautifulSoup to fetch and parse web content.
    """
    
    def __init__(self, base_url: Optional[str] = None):
        """
        Initialize the scraper.
        
        Args:
            base_url: Base URL for scraping operations
        """
        self.base_url = base_url
        self.logger = logger
        self.session = requests.Session()
        # Set a reasonable timeout and user agent
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def fetch_url(self, url: str) -> Optional[str]:
        """
        Fetch content from a given URL with error handling.
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content as string, or None if error occurred
        """
        try:
            self.logger.info(f"Fetching URL: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except Timeout:
            self.logger.error(f"Timeout error fetching {url}")
            print(f"Error: Request timed out for {url}")
            return None
        except ConnectionError:
            self.logger.error(f"Connection error fetching {url}")
            print(f"Error: Could not connect to {url}")
            return None
        except RequestException as e:
            self.logger.error(f"Request error fetching {url}: {e}")
            print(f"Error: Request failed for {url} - {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching {url}: {e}")
            print(f"Error: Unexpected error fetching {url} - {e}")
            return None
    
    def extract_titles_and_headings(self, html_content: str) -> List[str]:
        """
        Extract all <title> and <h1> tags from HTML content.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            List of title and heading text
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            titles_and_headings = []
            
            # Extract title tag
            title_tag = soup.find('title')
            if title_tag:
                titles_and_headings.append(f"Title: {title_tag.get_text().strip()}")
            
            # Extract all h1 tags
            h1_tags = soup.find_all('h1')
            for h1 in h1_tags:
                text = h1.get_text().strip()
                if text:  # Only add non-empty headings
                    titles_and_headings.append(f"H1: {text}")
            
            return titles_and_headings
        except Exception as e:
            self.logger.error(f"Error parsing HTML content: {e}")
            print(f"Error: Failed to parse HTML content - {e}")
            return []
    
    def run(self) -> List[str]:
        """
        Run the scraper service.
        Fetches the RRC Texas drilling permits page and extracts titles/headings.
        
        Returns:
            List of title and heading strings
        """
        url = "https://www.rrc.texas.gov/oil-gas/research-and-statistics/drilling-permits/"
        
        self.logger.info("Scraper service running")
        print("Scraper service running")
        
        # Fetch the URL
        html_content = self.fetch_url(url)
        if not html_content:
            print("Failed to fetch content from RRC Texas website")
            return []
        
        # Extract titles and headings
        titles_and_headings = self.extract_titles_and_headings(html_content)
        
        # Print results
        if titles_and_headings:
            print(f"\nFound {len(titles_and_headings)} titles and headings:")
            for item in titles_and_headings:
                print(f"  - {item}")
        else:
            print("No titles or headings found")
        
        return titles_and_headings
