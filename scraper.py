import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import logging
import xml.etree.ElementTree as ET
from typing import List, Dict

# Store log messages in ASCII format to avoid encoding issues in the terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class BinaAzScraper:
    def __init__(self):
        self.base_url = "https://bina.az"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept-Language": "az,ru,en-US;q=0.9,en;q=0.8",
            "Referer": "https://bina.az/"
        })

    def _delay(self):
        time.sleep(random.uniform(2, 5))

    def get_latest_listing_urls(self, limit: int = 20) -> List[str]:
        """Fetches the latest listing URLs using the sitemap."""
        logging.info("Reading sitemap...")
        try:
            # Read main sitemap index
            r = self.session.get(f"{self.base_url}/sitemap.xml")
            sitemaps = re.findall(
                r'https://bina.az/uploads/attachment/[^<]+_az1\.xml',
                r.text
            )

            if not sitemaps:
                logging.error("No sitemap found.")
                return []

            # Use the first sitemap (contains the most recent listings)
            r_sub = self.session.get(sitemaps[0])

            # Extract item URLs (items/{id})
            urls = re.findall(r'https://bina.az/items/\d+', r_sub.text)
            logging.info(f"Found {len(urls)} listing URLs from sitemap.")
            return list(set(urls))[:limit]

        except Exception as e:
            logging.error(f"Sitemap error: {e}")
            return []

    def get_phone_number(self, item_id: str) -> str:
        """Fetches phone number from the listing API."""
        try:
            url = f"{self.base_url}/items/{item_id}/phones"
            r = self.session.get(
                url,
                headers={"X-Requested-With": "XMLHttpRequest"}
            )
            if r.status_code == 200:
                data = r.json()
                return ", ".join(data.get('phones', []))
        except:
            pass
        return "Not found"

    def parse_item(self, url: str) -> Dict:
        """Parses a single listing page."""
        logging.info(f"Analyzing: {url}")
        self._delay()
        try:
            r = self.session.get(url, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')

            item_id = url.split('/')[-1]

            # Title
            title = soup.find("h1", class_="product-title")
            title_text = title.text.strip() if title else ""

            # Price section
            price_val = soup.find("span", class_="price-val")
            price_cur = soup.find("span", class_="price-cur")
            price_per = soup.find("span", class_="price-per")

            price = price_val.text.strip() if price_val else ""
            currency = price_cur.text.strip() if price_cur else ""
            price_type = price_per.text.strip() if price_per else "Total"

            # Properties (Category, Area, Rooms, etc.)
            properties = {}
            for row in soup.select(".product-properties__i"):
                label = row.find(class_=re.compile("label|name"))
                value = row.find(class_=re.compile("value"))
                if label and value:
                    properties[label.text.strip().lower()] = value.text.strip()

            # Deal type (Rent / Sale) extracted from breadcrumbs
            breadcrumbs = [
                a.text.strip()
                for a in soup.select(".product-breadcrumbs__i-link")
            ]

            deal_type = "Unknown"
            if any("kirayə" in b.lower() for b in breadcrumbs):
                deal_type = "Rent"
            elif any("satış" in b.lower() for b in breadcrumbs):
                deal_type = "Sale"

            # Update date
            date_text = ""
            stats = soup.select(".product-statistics__i-text")
            for s in stats:
                if "Yeniləndi" in s.text:
                    date_text = s.text.replace("Yeniləndi:", "").strip()

            # Location (strict extraction strategy)
            location = ""

            # 1. Map button
            map_btn = soup.find("a", class_="open_map")
            if map_btn:
                location = map_btn.text.strip()

            # 2. Breadcrumbs fallback
            if not location and breadcrumbs:
                t = breadcrumbs[-1]
                # If last breadcrumb contains deal type, it's not a location
                if any(x in t.lower() for x in ["satış", "kirayə"]):
                    location = breadcrumbs[-2] if len(breadcrumbs) > 1 else ""
                else:
                    location = t

            # 3. Product location class fallback
            if not location:
                loc_el = soup.select_one(".product-location")
                if loc_el:
                    location = loc_el.text.strip()

            return {
                "Title": title_text,
                "Updated date": date_text,
                "Category": properties.get("kateqoriya", ""),
                "Building type": properties.get(
                    "tikili növü",
                    properties.get("lahiyə", "")
                ),
                "Renovation": properties.get("təmir", ""),
                "Area": properties.get("sahə", ""),
                "Rooms": properties.get("otaq sayı", ""),
                "Deal type": deal_type,
                "Price": price,
                "Currency": currency,
                "Price type": price_type,
                "Phone": self.get_phone_number(item_id),
                "Location": location,
                "Listing ID": item_id
            }

        except Exception as e:
            logging.error(f"Error ({url}): {e}")
            return None

    def start(self, count: int = 10):
        urls = self.get_latest_listing_urls(limit=count)
        if not urls:
            logging.error("No listings found.")
            return

        all_results = []
        batch_size = 500
        excel_file = "bina_az_data.xlsx"

        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i: i + batch_size]
            batch_results = []

            logging.info(
                f"Starting batch {i // batch_size + 1}/"
                f"{((len(urls) - 1) // batch_size) + 1} "
                f"({len(batch_urls)} listings)..."
            )

            for url in batch_urls:
                data = self.parse_item(url)
                if data:
                    batch_results.append(data)
                    all_results.append(data)

            # Excel batch append logic
            if batch_results:
                try:
                    df_batch = pd.DataFrame(batch_results)

                    # Check if file exists without importing os
                    file_exists = False
                    try:
                        with open(excel_file, "rb"):
                            file_exists = True
                    except FileNotFoundError:
                        file_exists = False

                    if file_exists:
                        # Append mode
                        with pd.ExcelWriter(
                            excel_file,
                            mode='a',
                            engine='openpyxl',
                            if_sheet_exists='overlay'
                        ) as writer:
                            if "Sheet1" in writer.sheets:
                                start_row = writer.sheets["Sheet1"].max_row
                            else:
                                start_row = 0
                            df_batch.to_excel(
                                writer,
                                index=False,
                                header=False,
                                startrow=start_row
                            )
                    else:
                        # Create new file
                        df_batch.to_excel(excel_file, index=False)

                    logging.info(
                        f"Batch {i // batch_size + 1} "
                        f"successfully written/appended to Excel."
                    )

                except PermissionError:
                    logging.error(
                        "ERROR: Excel file is currently open. "
                        "Batch could not be written."
                    )
                except Exception as e:
                    logging.error(f"Excel batch write error: {e}")

        # Final CSV output containing all collected listings
        if all_results:
            try:
                df = pd.DataFrame(all_results)
                df.to_csv(
                    "bina_az_data.csv",
                    index=False,
                    encoding='utf-8-sig'
                )
                logging.info(
                    f"SUCCESS! Total {len(all_results)} listings "
                    f"saved to bina_az_data.csv"
                )
            except Exception as e:
                logging.info(f"CSV save error: {e}")
        else:
            logging.warning("No data collected.")

if __name__ == "__main__":
    scraper = BinaAzScraper()
    scraper.start(count=30000)
