import time
import logging
from datetime import datetime
from pymongo import MongoClient
from yt_dlp import YoutubeDL

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------- CLUSTER 2 CONFIG ----------------
MONGO_URI = "mongodb+srv://akhilamerugu:root@youtube.b3wyvwo.mongodb.net/mydb?authSource=admin&retryWrites=true&w=majority"
DB_NAME = "youtube"
COLLECTION_NAME = "dailyscraping"

# ---------------- YOUTUBE DATE FETCH ----------------
def get_published_date(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"

    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "no_warnings": True,
        "ignoreerrors": True
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info:
                return None

            upload_date = info.get("upload_date") or info.get("release_date")
            if upload_date:
                return datetime.strptime(upload_date, "%Y%m%d")

    except Exception as e:
        logger.error(f"yt-dlp error for {video_id}: {e}")

    return None

# ---------------- PROCESS ----------------
def main():
    logger.info("🚀 Starting backfill for Cluster 2")

    client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
    collection = client[DB_NAME][COLLECTION_NAME]

    videos = collection.find({}, {"video_id": 1})
    total = collection.count_documents({})

    for idx, video in enumerate(videos, start=1):
        video_id = (video.get("video_id") or "").strip()
        if not video_id:
            continue

        logger.info(f"[{idx}/{total}] Fetching date for {video_id}")

        published_date = get_published_date(video_id)

        if published_date:
            collection.update_one(
                {"video_id": video_id},
                {"$set": {"publishedAt": published_date}}
            )
            logger.info(f"✅ Updated {video_id} → {published_date}")
        else:
            logger.warning(f"❌ Date not found for {video_id}")

        time.sleep(0.5)

    client.close()
    logger.info("✅ Cluster 2 backfill completed")

if __name__ == "__main__":
    main()
