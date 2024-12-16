import pandas as pd
import unicodedata
from fundus import PublisherCollection, Crawler
from typing import Dict, List
import math
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_articles_for_publisher(publisher: str, num_articles: int, affiliation: str) -> List[Dict]:

    logging.info(f"Starting to scrape {num_articles} articles from {publisher} ({affiliation})")
    crawler = Crawler(getattr(PublisherCollection.us, publisher))
    articles = []
    article_count = 0
    
    for article in crawler.crawl(max_articles=num_articles):
        if not article.title or not article.plaintext:
            continue
            
        # Normalize and clean text fields
        title = unicodedata.normalize('NFKD', article.title)
        body = unicodedata.normalize('NFKD', article.plaintext)
        title = title.encode('ascii', 'ignore').decode('ascii')
        body = body.encode('ascii', 'ignore').decode('ascii')

        article_data = {
            'publisher': article.publisher,
            'publishing_date': article.publishing_date,
            'authors': article.authors,
            'headline': title,
            'body': body,
            'poli_affiliation': affiliation
        }
        articles.append(article_data)
        article_count += 1
    
    logging.info(f"Finished scraping {article_count} articles from {publisher} ({affiliation})")
    return articles

def main():
    publishers_politics = {
        "left": [
            "BusinessInsider", "CNBC", "LATimes",
            "RollingStone", "TechCrunch", "TheIntercept", "TheNation",
            "TheNewYorker", "VoiceOfAmerica"
        ],
        "center": [
            "APNews",
            "Reuters"
        ],
        "right": [
            "FoxNews",
            "FreeBeacon",
            "DailyMail",
            "WashingtonTimes",
            "NationalPost"
            
        ]
    }

    # Distribution requirements - total 10000 articles
    distribution = {
        "left": 4000,    
        "center": 2000,  
        "right": {
            "FoxNews": 2500,
            "others": 1500 
        }
    }

    all_articles = []

    articles_per_left = distribution["left"] // len(publishers_politics["left"])  # Integer division
    remaining_left = distribution["left"] % len(publishers_politics["left"])
    
    for i, publisher in enumerate(publishers_politics["left"]):
        extra = 1 if i < remaining_left else 0
        articles = get_articles_for_publisher(publisher, articles_per_left + extra, "left")
        all_articles.extend(articles)

    for publisher in publishers_politics["center"]:
        articles = get_articles_for_publisher(publisher, distribution["center"], "center")
        all_articles.extend(articles)

    fox_articles = get_articles_for_publisher("FoxNews", distribution["right"]["FoxNews"], "right")
    all_articles.extend(fox_articles)
    
    other_right_publishers = [p for p in publishers_politics["right"] if p != "FoxNews"]
    for publisher in other_right_publishers:
        articles = get_articles_for_publisher(publisher, distribution["right"]["others"], "right")
        all_articles.extend(articles)

    df = pd.DataFrame(all_articles)
    df.to_csv("balanced_articles_103.csv", index=True, encoding='utf-8')
    
    logging.info("\nArticle collection summary:")
    logging.info(df['publisher'].value_counts())
    logging.info(f"\nTotal articles collected: {len(df)}")

if __name__ == "__main__":
    main()