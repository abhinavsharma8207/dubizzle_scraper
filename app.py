from flask import Flask
from dubizzle_scraper import DubizzleScraper
import threading

app = Flask(__name__)


@app.route("/get_gcc_dubizzle_listings", methods=['GET'])
def get_gcc_dubizzle_listings():
    scraper = DubizzleScraper()
    scraper_thread = threading.Thread(target=scraper.get_dubizzle_listings)
    scraper_thread.start()
    return "Dubizzle Scraping Started"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=6123)
