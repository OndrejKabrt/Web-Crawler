import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import concurrent.futures
import threading
from queue import Queue, Empty
import time
import logging

class WebCrawler:
    def __init__(self, start_url, max_pages, max_depth, max_workers,timeout):
        """
        Initialize a sophisticated multithreaded web crawler
        
        :param start_url: Initial URL to begin crawling
        :param max_pages: Maximum number of pages to crawl
        :param max_depth: Maximum link traversal depth
        :param max_workers: Number of concurrent worker threads
        :param timeout: Request and queue timeout settings
        """
        # Crawler configuration
        self.start_url = start_url
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.max_workers = max_workers
        self.timeout = timeout
        
        # Thread-safe data structures
        self.url_queue = Queue()
        self.visited_urls = set()
        self.crawl_results = {}
        
        # Synchronization primitives
        self.visited_lock = threading.Lock()
        self.results_lock = threading.Lock()
        self.page_count_lock = threading.Lock()
        
        # Tracking variables
        self.total_pages_crawled = 0
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Initial queue population
        self.url_queue.put((start_url, 0))

    def is_valid_url(self, url):
        """
        Comprehensive URL validation
        
        :param url: URL to validate
        :return: Boolean indicating URL validity
        """
        try:
            parsed = urlparse(url)

            start_domain = urlparse(self.start_url).netloc

            file_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.mp4']
            is_file = any(url.lower().endswith(ext) for ext in file_extensions)
            
            # Detailed URL validation checks
            checks = [
                parsed.scheme in ['http', 'https'],  # Web protocols only
                parsed.netloc == start_domain,       # Same domain
                url not in self.visited_urls,        # Not already visited
                not url.endswith(('.jpg', '.jpeg', '.png', '.gif', '.pdf', '.mp4')),  # Ignore media
                len(url) < 200  # Prevent extremely long URLs
            ]
            
            return all(checks)
        
        except Exception as e:
            self.logger.warning(f"URL validation error: {e}")
            return False

    def extract_links(self, html_content, current_url):
        """
        Extract and normalize links from HTML
        
        :param html_content: Page HTML content
        :param current_url: Current page URL
        :return: List of absolute, valid links
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links = []
            
            for link in soup.find_all('a', href=True):
                # Resolve relative URLs to absolute
                absolute_url = urljoin(current_url, link['href'])
                
                if self.is_valid_url(absolute_url):
                    links.append(absolute_url)
            
            return links
        
        except Exception as e:
            self.logger.error(f"Link extraction error: {e}")
            return []

    def worker(self):
        """
        Worker thread method for crawling
        Handles URL processing and link discovery
        """
        while True:
            # Check if crawl limit reached
            with self.page_count_lock:
                if self.total_pages_crawled >= self.max_pages:
                    break
            
            try:
                # Get next URL with timeout
                current_url, depth = self.url_queue.get(timeout=self.timeout)
                
                # Skip if depth exceeded
                if depth > self.max_depth:
                    self.url_queue.task_done()
                    continue
                
                # Thread-safe visited check
                with self.visited_lock:
                    if current_url in self.visited_urls:
                        self.url_queue.task_done()
                        continue
                    self.visited_urls.add(current_url)
                
                try:
                    # Fetch webpage
                    response = requests.get(current_url, timeout=self.timeout)
                    
                    # Store page info
                    with self.results_lock:
                        self.crawl_results[current_url] = {
                            'title': self.extract_title(response.text),
                            'depth': depth
                        }
                    
                    # Extract and queue new links
                    discovered_links = self.extract_links(response.text, current_url)
                    
                    for link in discovered_links:
                        self.url_queue.put((link, depth + 1))
                    
                    # Increment page count
                    with self.page_count_lock:
                        self.total_pages_crawled += 1
                        self.logger.info(f"Crawled: {current_url}")
                    
                    # Politeness delay
                    time.sleep(0.5)
                
                except requests.RequestException as e:
                    self.logger.warning(f"Request failed for {current_url}: {e}")
                
                finally:
                    self.url_queue.task_done()
                
            except Empty:
                # No more URLs to process
                break
            
            except Exception as e:
                self.logger.error(f"Unexpected worker error: {e}")
                break

    def extract_title(self, html_content):
        """
        Extract page title safely
        
        :param html_content: Page HTML
        :return: Page title or default
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            return soup.title.string if soup.title else "No Title"
        except Exception:
            return "Title Extraction Failed"

    def crawl(self):
        """
        Execute multithreaded web crawling
        
        :return: Crawl results dictionary
        """
        self.logger.info(f"Starting crawl for {self.start_url}")
        
        # Create thread pool
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            # Submit worker tasks
            futures = [
                executor.submit(self.worker) 
                for _ in range(self.max_workers)
            ]
            
            # Wait for all threads to complete
            concurrent.futures.wait(futures)
        
        self.logger.info(f"Crawl completed. Total pages: {self.total_pages_crawled}")
        return self.crawl_results

# Example usage
if __name__ == "__main__":
    crawler = WebCrawler(
        start_url="https://example.com",
        max_pages=10,
        max_depth=2,
        max_workers=5
    )
    
    results = crawler.crawl()
    
    # Print crawl results
    for url, data in results.items():
        print(f"URL: {url}")
        print(f"Title: {data['title']}")
        print(f"Depth: {data['depth']}\n")