import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone
from pymongo import MongoClient

# 2️⃣ CONFIG
# ===== MongoDB =====
MONGODB_URI = "mongodb+srv://akhila:root@insta.yb94sys.mongodb.net/"
MONGODB_DB_NAME = "instagram"
COLLECTION_NAME = "daily_scrap"

# ===== ScrapeBee =====
SCRAPEBEE_API_KEY = "4OZI42TQMZDIYCYHC8SW796JM9748K2BFW5KBE2XR7AOI7GI1AWHWT4YVKVVVRQV9YF1YD7NKZ3BNVTU"
SCRAPEBEE_ENDPOINT = "https://app.scrapebee.com/api/v1/"

# 3️⃣ MongoDB Connection
client = MongoClient(MONGODB_URI)
db = client[MONGODB_DB_NAME]
collection = db[COLLECTION_NAME]

# 4️⃣ ScrapeBee Request Helper
def scrape_url(url):
    params = {
        "api_key": SCRAPEBEE_API_KEY,
        "url": url,
        "render_js": "true",  # IMPORTANT
        "json_response": "true"
    }
    
    session = requests.Session()
    # Retry up to 3 times with backoff (wait 2s, 4s...) if connection fails
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        res = session.get(SCRAPEBEE_ENDPOINT, params=params, timeout=60)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.ConnectionError:
        print(f"❌ Network Error: Could not connect to ScrapeBee. Check your internet connection or DNS.")
        raise

# 6️⃣ Save Data to MongoDB (ONE COLLECTION)
def save_posts(username, account, posts):
    if not posts:
        return

    # Use UTC date for daily snapshot consistency
    today_str = datetime.now(timezone.utc).date().isoformat()

    for post in posts:
        document = {
            "username": username,

            # ===== ACCOUNT SNAPSHOT =====
            "account": account,

            # ===== POST DATA =====
            "post_id": post.get("post_id"),
            "caption": post.get("caption"),
            "posted_at": post.get("posted_at"),
            "likes": post.get("likes"),
            "comments": post.get("comments"),
            "media": post.get("media"),
            "post_url": post.get("post_url"),

            # ===== META =====
            "scraped_date": today_str,
            "scraped_at": datetime.now(timezone.utc)
        }

        collection.update_one(
            {
                "post_id": document["post_id"],
                "scraped_date": document["scraped_date"]
            },
            {"$set": document},
            upsert=True
        )
    print(f"Saved {len(posts)} posts for {username}.")

def extract_posts(edges):
    posts = []
    for edge in edges:
        node = edge["node"]
        posts.append({
            "post_id": node["id"],
            "caption": (
                node["edge_media_to_caption"]["edges"][0]["node"]["text"]
                if node["edge_media_to_caption"]["edges"] else ""
            ),
            "posted_at": datetime.fromtimestamp(
                node["taken_at_timestamp"], tz=timezone.utc
            ),
            "likes": node["edge_liked_by"]["count"],
            "comments": node["edge_media_to_comment"]["count"],
            "media": node.get("display_url"),
            "post_url": f"https://www.instagram.com/p/{node['shortcode']}/"
        })
    return posts

# 5️⃣ Core Function – Account + ALL Posts
def scrape_instagram_account(username):
    base_url = f"https://www.instagram.com/{username}/"
    print(f"Scraping account: {username}")

    response = scrape_url(base_url)

    try:
        user = (
            response["data"]
            ["entry_data"]["ProfilePage"][0]
            ["graphql"]["user"]
        )
    except KeyError:
        print("❌ Instagram structure changed or blocked")
        return

    account_details = {
        "username": user["username"],
        "full_name": user.get("full_name"),
        "bio": user.get("biography"),
        "followers": user["edge_followed_by"]["count"],
        "following": user["edge_follow"]["count"],
        "posts_count": user["edge_owner_to_timeline_media"]["count"],
        "profile_pic": user.get("profile_pic_url_hd"),
    }

    posts_edge = user["edge_owner_to_timeline_media"]
    save_posts(username, account_details, extract_posts(posts_edge["edges"]))

    page_info = posts_edge["page_info"]

    # PAGINATION
    while page_info["has_next_page"]:
        time.sleep(1.5)
        cursor = page_info["end_cursor"]

        next_page_url = (
            f"https://www.instagram.com/{username}/"
            f"?__a=1&__d=dis&after={cursor}"
        )

        response = scrape_url(next_page_url)
        media = (
            response["data"]
            ["graphql"]["user"]
            ["edge_owner_to_timeline_media"]
        )

        save_posts(username, account_details, extract_posts(media["edges"]))
        page_info = media["page_info"]

    print("✅ Completed scraping")

# 7️⃣ Run It
if __name__ == "__main__":
    # Ensure indexes exist (Run once in MongoDB shell if needed)
    # db.daily_scrap.createIndex({ post_id: 1, scraped_date: 1 }, { unique: true })
    # db.daily_scrap.createIndex({ username: 1 })
    
    scrape_instagram_account("tdp.official")