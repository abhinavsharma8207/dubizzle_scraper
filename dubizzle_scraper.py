from concurrent.futures import as_completed
import requests
from bs4 import BeautifulSoup
from requests_futures.sessions import FuturesSession
import json
from database import *
from utilities import *
from datetime import datetime as dt
import logger
import sys
from urllib.parse import urlparse
from urllib.parse import parse_qs

PROXY_HOST = ''  # rotating proxy or host
PROXY_PORT = ''  # port
PROXY_USER = ''  # username
PROXY_PASS = ''  # password
time_interval_arr = ["seconds", "second", "hours", "hour", "minutes", "minute"]


class DubizzleScraper:

    def __init__(self):
        url = "http://{}:{}@{}:{}".format(PROXY_USER, PROXY_PASS, PROXY_HOST, PROXY_PORT)
        proxies = {'http': url}
        self.proxy = proxies
        self.urls = ["https://uae.dubizzle.com/motors/used-cars/"]
        self.phone_request_headers = {
            'authority': 'dubai.dubizzle.com',
            'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
            'accept-language': 'en',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'content-type': 'application/json;charset=UTF-8',
            'accept': 'application/json',
            'cache-control': 'must-revalidate, max-age=0, no-cache, no-store',
            'x-access-token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpZCI6IjlhZjBlODc5LWI0ZWYtNGYzYi05MjE1LWEzYmJkNjVmZTRkMCIsImlzcyI6ImR1Yml6emxlLmNvbSIsImF1ZCI6WyJkdWJpenpsZS5jb20iXSwic3ViIjoiOWFmMGU4NzktYjRlZi00ZjNiLTkyMTUtYTNiYmQ2NWZlNGQwIiwiZXhwIjoxNjM1OTM4MTkxLCJpYXQiOjE2MzU4NTE3OTEsImp0aSI6ImY1OGVlNjI3LWJlZmQtNDczMy05YjMxLTJlNDRhY2VhOTk0NSIsInR5cCI6IkFjY2VzcyBKV1QgVG9rZW4iLCJmbGFncyI6eyJsb2dnZWRfaW4iOmZhbHNlLCJpc19zdGFmZiI6ZmFsc2UsImlzX3N1cGVydXNlciI6ZmFsc2UsImlzX3Byb3BlcnR5X2FnZW50IjpmYWxzZSwiaXNfbGFuZGxvcmQiOmZhbHNlLCJpc19tb3RvcnNfYWdlbnQiOmZhbHNlLCJpc19qb2JzX2FnZW50IjpmYWxzZSwiaGFzX2NhbGxfdHJhY2tpbmciOmZhbHNlfSwidXNlcl9kYXRhIjp7InVzZXJfaWQiOm51bGwsImdlbmRlciI6bnVsbCwibmF0aW9uYWxpdHkiOm51bGwsImVkdWNhdGlvbiI6bnVsbCwicm9sZSI6bnVsbCwiZG9iIjpudWxsLCJhZ2UiOm51bGwsImZpcnN0X25hbWUiOiIiLCJsYXN0X25hbWUiOiIiLCJlbWFpbCI6IiIsInBob25lIjoiIn19.QpKAXsOKKNTJv3GXrHY7kMtMAoMPugvlhai_wc631RR7NKto0RfCPjv27MHtnOkempq4z8ZTkwWgetaWkzUuVXZxWA4LAuSa6-XNgrgIGl6111wnF5P7j2midT_JziWCswTZOmVHfrkL0D7jWf1s45emt9X9kTtONyhIAqpXc4RPcmeJ1xD3w7S9ihMQABR36NLyY5I4YYJEGxMvelcSn6MnOmmvM66hxRnPCH3VnSlZCI6A3MDVF-ANeU2L1oQ9QyjMWMIvASW7MdKji4XJH36I0kL3ryHIBsvt_-63pKCL_gvYx07VK22w63KzYqYW9fnXruyd7ywTC7Vu0nEBGOftNqArjFtMaHbrp2qp3SYPwLSE3AzP_RUl2-yl3IWnTrHZFLK2Bl93inp96WZH_Er0h-DaFAXDNZCAGCrZ3xcFP8hU4eTW5tqpYtMb87NZ2AeTD3If9pFfzFWGPzwroMmdpKlFjSQwBIlwgtgNRCKO8Q0dVQBS4CxuuXlxWtaHbEKoM8HdTgK7vgk6XFcxcrOfTY4jhU5kYe1K7aNx-u6trSko4ico54PUVn9VbNrT2pb6IFuHYrAL78WrIHRo_Uh0EcxOLqgKWXyMJE24iW6K3EAey7gYsazR2SaeA5vh3pEa5CuCGg9oaoFV8UwLKWMBc-Si1HxdzLkllB9KkAE',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://dubai.dubizzle.com/motors/used-cars/ferrari/roma/2021/11/2/2021-ferrari-roma-gcc-spec-with-warranty-a-2-782---6e854560e3db476ea1b2d844d9103370/?back=L21vdG9ycy91c2VkLWNhcnMv&pos=5',
            'cookie': 'visid_incap_2413658=n/HEyHcjSvWvgGRGrFCwMkfjOWEAAAAAQUIPAAAAAACREV8nlTBawtGMseK6fdyU; _gac_UA-125984715-1=1.1631183700.Cj0KCQjw4eaJBhDMARIsANhrQAC08vs2ozop6DzY5rmCKL8svSCfgkbXt3aYoKhsVG7HgnXdHiRk6esaArNZEALw_wcB; laquesis=; laquesisff=da-2161#dcon-2344#dcon-2559#dcon-2565#dcon-2566#dcon-2576#dcon-2586#dcon-2640#dcon-2641#dpb-1459#dpv-2748#dpv-3000#dpv-3598#dub-194#mgp-1018; laquesissu=; _gcl_au=1.1.1594302182.1631183700; _gac_UA-205528691-1=1.1631183700.Cj0KCQjw4eaJBhDMARIsANhrQAC08vs2ozop6DzY5rmCKL8svSCfgkbXt3aYoKhsVG7HgnXdHiRk6esaArNZEALw_wcB; _rtb_user_id=bc2246ee-bef6-4af5-c6d5-4e41eef964fb; __gads=ID=4ca1a097dbd56a38:T=1631183700:S=ALNI_MaohUHqpFXB32xWbV22NlJJd8hAqg; _fbp=fb.1.1631183700908.1551992440; _gcl_aw=GCL.1631183701.Cj0KCQjw4eaJBhDMARIsANhrQAC08vs2ozop6DzY5rmCKL8svSCfgkbXt3aYoKhsVG7HgnXdHiRk6esaArNZEALw_wcB; csrftoken=peC1eDfIDLhXiKHS0ZeiB8S8uus6geyo; _cc_id=64fb1a670d2d04ad8643b956822a0933; USER_DATA=%7B%22attributes%22%3A%5B%7B%22key%22%3A%22USER_ATTRIBUTE_UNIQUE_ID%22%2C%22value%22%3Anull%7D%5D%2C%22subscribedToOldSdk%22%3Afalse%2C%22deviceUuid%22%3A%229b1d7cc2-5e6e-41bf-9803-7529fbe6e2c6%22%2C%22deviceAdded%22%3Atrue%7D; ldTd=true; nlbi_2413658=12AFJX/BzDIknKcyYBO7kwAAAACbSzU5zUz3OWQPOCR/ZEXX; ias=0; moe_uuid=9b1d7cc2-5e6e-41bf-9803-7529fbe6e2c6; sid=283evers4p7dpf1rf2dhb5u4y48fvbc1; _pbjs_userid_consent_data=3524755945110770; panoramaId_expiry=1635920719995; panoramaId=eab4d71f9bf22fd4459063abc6f64945a702c7fa46ff2f84fa820daa4edebca1; incap_ses_773_2413658=6kk/Z2+sy00/bzSSCkC6ChOfemEAAAAASqvf/+pzM4SAQjHjRIViUA==; _gid=GA1.2.2121445630.1635763445; OPT_IN_SHOWN_TIME=1635763450315; SOFT_ASK_STATUS=%7B%22actualValue%22%3A%22shown%22%2C%22MOE_DATA_TYPE%22%3A%22string%22%7D; default_site=2; SETUP_TIME=1635763522376; incap_ses_775_2413658=vhgcV+mQZyFbkQGj6VrBCsHdgGEAAAAAqQxbsfHOlO8LxDMD6Oc00A==; incap_ses_771_2413658=0tYTaAJ9/S8WFCNbcSWzCg3sgGEAAAAAncoYeBMhHgcBmneCxqbSCg==; cto_bundle=tMOhYV9XSG5kU3B2cDQ1SE8zamwxUjM5Nm93eFlxbyUyRnRVbEE5S1QlMkI0aG5hUzltZ095V1JtJTJGSHViazJBOHB6Q29RUXUxTU02JTJGMzVXbTc1WSUyRmdKSlJKQU0lMkJpVFhpdFhFeHFNSFVybEhJJTJGSFFlcWFsVHNMSXVzZmx6U2F4R2tlUUk2RVc5REl6OG83eEJ4bXV5ZXRLWlp4VGxwQSUzRCUzRA; _ga_LRML1YM9GH=GS1.1.1635855050.27.1.1635855073.0; incap_ses_776_2413658=8c/lMIZJIk4PnuhEb+jECnwfgmEAAAAAgCx2elYI8gteqzx5+EM94w==; lqstatus=1635919073||||; nlbi_2413658_2147483646=FzMbCq7/OlOIGzjDYBO7kwAAAADgjcvkDDT3leJxIA1tS52J; reese84=3:GpsO6qgajV3+wprsieWKHQ==:4ndQe7EOamq7MHpx6ZIB1SvR3xyXs0xVpo+goUifXAsiHx6oUfS9bk6EgPTUFYIJgPFWpDIUuuwYryYQceaDrbt+l4gfiX1+uIr1HyQAEXIRqPVxlo8jOElSeAOhTgqxj5+/SsfV/wt+3IidHk+etSMsYR0K+eGd994mTT5g+pmuOFru11yjx/PPloy7nL8LCMXBqbHilqj/KCTuJKWg4Enhhktc+itQNLYN5ZsVf1kBBXsuADJLyQLY6SiaCWji0hlcPBcIBIW1uYTojfTrfEU391KLnAxi/16htLWsOQ+zY7O8WQTyy1og1GJlt0Ge7x2VL11IK3yHfT7/F1LvxmmBn9o1d4f77aYKrjm3RJqN4+7WVh9taXKMAOM6QwihEod+WsoluZUhOLBzaC2gmRYsgn7wfAeD3qP5/lHcJ7T2T/2q0KBPZlDcLlCK0AdkSiUIMdUnRtrOntUAmPwnZFNIHB8tC+Cc+TYHyo14iP0=:DEv3Vm2j/a+k8Ew0tepi8Qy+E5+6cL9/P9GmDW8E5MY=; _ga=GA1.2.1208754115.1631183700; _gat_UA-205528691-1=1; onap=17bca1fffdfx4d31785-24-17ce45f9604x7262bec4-1-1635920842; _gat_clientNinja=1',
        }
        self.headers = {
            "authority": "dubai.dubizzle.com",
            "cache-control": "max-age=0",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "sec-fetch-site": "none",
            "sec-fetch-mode": "navigate",
            "sec-fetch-user": "?1",
            "sec-fetch-dest": "document",
            "accept-language": "en-US,en;q=0.9",
            "Cookie": "default_site=2; laquesis=\"mb-1777@a\"; sid=jg6lldqfqey8tmxjds3218n2szqg47e4; "
                      "laquesisff=da-2161#dcon-2344#dcon-2559#dcon-2565#dcon-2566#dcon-2576#dcon-2586#dcon-2640#dcon"
                      "-2641#dpb-1459#dpv-2748#dpv-3000#dpv-3598#dub-194#mgp-1018; lqstatus=1601292265; ias=0; "
                      "skybar_sess_False=2"
        }

    def get_dubizzle_listings(self):
        try:
            recent_dubizzle_ids = []
            cars_listings = []
            self.populate_list_of_url_pages()
            lst_splited = chunks(self.urls, 5)
            for lst in lst_splited:
                with FuturesSession() as session:
                    futures = [session.get(url, proxies=self.proxy, headers=self.headers) for url
                               in lst]
                    for future in as_completed(futures):
                        if future.result().status_code == 200:
                            soup = BeautifulSoup(future.result().content, 'html5lib')
                            all_ads = soup.findAll('div',
                                                   attrs={'class': 'list-item-wrapper'})
                            for source in all_ads:
                                car_listing = dict()
                                listing_item = source.findAll('div', attrs={'class': 'listing-item'})
                                car_listing["dubizzle_id"] = listing_item[0]["data-id"]
                                listing_block = listing_item[0].findAll('div', attrs={'class': 'block item-title'})
                                listing_anchor = listing_block[0].findAll('a')
                                car_listing["link"] = listing_anchor[0]["href"]
                                self.phone_request_headers["referer"] = car_listing["link"]
                                cars_listings.append(car_listing)
                                recent_dubizzle_ids.append(car_listing["dubizzle_id"])
                    add_futures = []
                    for car_listing in cars_listings:
                        add_future = session.get(car_listing["link"], proxies=self.proxy, headers=self.headers)
                        add_future.car_listing = car_listing
                        add_futures.append(add_future)
                    for addFuture in as_completed(add_futures):
                        if addFuture.result().status_code == 200:
                            soup = BeautifulSoup(addFuture.result().content, 'html5lib')
                            res = soup.find('script', attrs={'id': '__NEXT_DATA__'})
                            try:
                                advt_object = json.loads(res.contents[0])
                            except Exception as e:
                                print("unable to parse json")
                                continue
                            if "props" in advt_object:
                                if "pageProps" in advt_object["props"]:
                                    ad_ops = advt_object["props"]["pageProps"]["0"]["payload"]["ad_ops"]
                                    ad_listing = advt_object["props"]["pageProps"]["0"]["payload"]["listing"]
                                    addFuture.car_listing["make"] = ad_ops["make"]
                                    addFuture.car_listing["model"] = ad_ops["model"]
                                    addFuture.car_listing["city"] = ad_ops["emirate"]
                                    addFuture.car_listing["country"] = "UAE"
                                    addFuture.car_listing["location"] = ad_ops["loc"]
                                    addFuture.car_listing["price"] = ad_ops["price_aed"]
                                    addFuture.car_listing["description"] = ad_listing["description"]
                                    addFuture.car_listing["title"] = ad_listing["name"]
                                    uuid = ad_listing["uuid"]
                                    legacy_id = advt_object["props"]["pageProps"]["0"]["payload"]["leads"]["chat"][
                                        "listing_metadata"]["cL4Id"]
                                    primary_details = ad_listing["details"]["primary"]
                                    secondary_details = ad_listing["details"]["secondary"]
                                    try:
                                        addFuture.car_listing["badge_description"] = \
                                            next(item for item in primary_details if item["slug"] == "motors_trim")[
                                                "value"] if next(
                                                item for item in primary_details if
                                                item["slug"] == "motors_trim") else None
                                    except StopIteration:
                                        addFuture.car_listing["badge_description"] = None
                                    try:
                                        addFuture.car_listing["mileage"] = \
                                            next(item for item in primary_details if item["slug"] == "kilometers")[
                                                "value"] if next(
                                                item for item in primary_details if
                                                item["slug"] == "kilometers") else None
                                    except StopIteration:
                                        addFuture.car_listing["mileage"] = None
                                    try:
                                        addFuture.car_listing["warranty"] = \
                                            next(item for item in primary_details if item["slug"] == "warranty")[
                                                "value"] if next(
                                                item for item in primary_details if
                                                item["slug"] == "warranty") else None
                                    except StopIteration:
                                        addFuture.car_listing["warranty"] = None
                                    try:
                                        addFuture.car_listing["color"] = \
                                            next(item for item in primary_details if item["slug"] == "color")[
                                                "value"] if next(
                                                item for item in primary_details if item["slug"] == "color") else None
                                    except StopIteration:
                                        addFuture.car_listing["color"] = None
                                    try:
                                        addFuture.car_listing["year"] = \
                                            next(item for item in primary_details if item["slug"] == "year")[
                                                "value"] if next(
                                                item for item in primary_details if item["slug"] == "year") else None
                                    except StopIteration:
                                        addFuture.car_listing["year"] = None
                                    try:
                                        addFuture.car_listing["doors"] = \
                                            next(item for item in primary_details if item["slug"] == "doors")[
                                                "value"] if next(
                                                item for item in primary_details if item["slug"] == "doors") else None
                                    except StopIteration:
                                        addFuture.car_listing["doors"] = None
                                    try:
                                        addFuture.car_listing["body_condition"] = \
                                            next(
                                                item for item in secondary_details if item["slug"] == "body_condition")[
                                                "value"] if next(item for item in secondary_details if
                                                                 item["slug"] == "body_condition") else None
                                    except StopIteration:
                                        addFuture.car_listing["body_condition"] = None
                                    try:
                                        addFuture.car_listing["mechanical_condition"] = next(
                                            item for item in secondary_details if
                                            item["slug"] == "mechanical_condition")["value"] if next(
                                            item for item in secondary_details if
                                            item["slug"] == "mechanical_condition") else None
                                    except StopIteration:
                                        addFuture.car_listing["mechanical_condition"] = None
                                    try:
                                        addFuture.car_listing["seller_type"] = \
                                            next(item for item in secondary_details if item["slug"] == "seller_type")[
                                                "value"] if next(item for item in secondary_details if
                                                                 item["slug"] == "seller_type") else None
                                    except StopIteration:
                                        addFuture.car_listing["seller_type"] = None
                                    try:
                                        addFuture.car_listing["body_type"] = \
                                            next(item for item in secondary_details if item["slug"] == "body_type")[
                                                "value"] if next(item for item in secondary_details if
                                                                 item["slug"] == "body_type") else None
                                    except StopIteration:
                                        addFuture.car_listing["body_type"] = None
                                    try:
                                        addFuture.car_listing["cylinders"] = \
                                            next(item for item in secondary_details if
                                                 item["slug"] == "no_of_cylinders")[
                                                "value"] if next(item for item in secondary_details if
                                                                 item["slug"] == "no_of_cylinders") else None
                                    except StopIteration:
                                        addFuture.car_listing["cylinders"] = None
                                    try:
                                        addFuture.car_listing["transmission"] = \
                                            next(item for item in secondary_details if
                                                 item["slug"] == "transmission_type")[
                                                "value"] if next(item for item in secondary_details if
                                                                 item["slug"] == "transmission_type") else None
                                    except StopIteration:
                                        addFuture.car_listing["transmission"] = None
                                    try:
                                        addFuture.car_listing["specs"] = \
                                            next(
                                                item for item in secondary_details if item["slug"] == "regional_specs")[
                                                "value"] if next(item for item in secondary_details if
                                                                 item["slug"] == "regional_specs") else None
                                    except StopIteration:
                                        addFuture.car_listing["specs"] = None
                                    try:
                                        addFuture.car_listing["hp"] = \
                                            next(item for item in secondary_details if item["slug"] == "horsepower")[
                                                "value"] if next(item for item in secondary_details if
                                                                 item["slug"] == "horsepower") else None
                                    except StopIteration:
                                        addFuture.car_listing["hp"] = None
                                    try:
                                        addFuture.car_listing["fuel_type"] = \
                                            next(item for item in secondary_details if item["slug"] == "fuel_type")[
                                                "value"] if next(
                                                item for item in secondary_details if
                                                item["slug"] == "fuel_type") else None
                                    except StopIteration:
                                        addFuture.car_listing["fuel_type"] = None
                                    addFuture.car_listing["created_on"] = dt.fromtimestamp(
                                        ad_listing["posted_timestamp"])
                                    addFuture.car_listing["vin"] = ad_listing["car_report"]["params"]["vin"]
                                    if "body_type" in ad_listing["car_report"]["params"] and ad_listing["car_report"][
                                        "has_report"]:
                                        addFuture.car_listing["body_type"] = ad_listing["car_report"]["params"][
                                            "body_type"]
                                    if "trim" in ad_listing["car_report"]["params"] and ad_listing["car_report"][
                                        "has_report"]:
                                        addFuture.car_listing["trim"] = ad_listing["car_report"]["params"][
                                            "trim"]
                                    if "make" in ad_listing["car_report"]["params"] and ad_listing["car_report"][
                                        "has_report"]:
                                        addFuture.car_listing["make"] = ad_listing["car_report"]["params"][
                                            "make"]
                                    if "model" in ad_listing["car_report"]["params"] and ad_listing["car_report"][
                                        "has_report"]:
                                        addFuture.car_listing["model"] = ad_listing["car_report"]["params"][
                                            "model"]
                                    if "year" in ad_listing["car_report"]["params"] and ad_listing["car_report"][
                                        "has_report"]:
                                        addFuture.car_listing["year"] = ad_listing["car_report"]["params"][
                                            "year"]
                                    if addFuture.car_listing["year"]:
                                        try:
                                            adcode, adcode_short = get_adcode(addFuture.car_listing)
                                            addFuture.car_listing["adcode"] = adcode
                                            addFuture.car_listing["adcode_short"] = adcode_short
                                        except:
                                            pass
                                    else:
                                        addFuture.car_listing["adcode"] = None
                                        addFuture.car_listing["adcode_short"] = None

                                    if uuid is not None and legacy_id is not None:
                                        try:
                                            phone_obj_response = requests.get(
                                                "https://dubai.dubizzle.com/api/v4/leads/{}/{}/listing-profile/".format(
                                                    legacy_id, uuid),
                                                headers=self.phone_request_headers, proxies=self.proxy)
                                            addFuture.car_listing["phone"] = phone_obj_response.json()["phone_number"]
                                        except:
                                            addFuture.car_listing["phone"] = None
                        try:
                            create_dubizzle_listing([addFuture.car_listing])
                        except:
                            logger.log("Error in get_dubizzle_listings: " + str(e))
                            logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
                            continue
            removed_on_dubizzle_ids = get_dubizzle_listings_by_dubizzle_ids_for_removing(recent_dubizzle_ids)
            update_dubizzle_listing_for_removed_on(removed_on_dubizzle_ids)
        except Exception as e:
            logger.log("Error in get_dubizzle_listings: " + str(e))
            logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    def populate_list_of_url_pages(self):
        try:
            r = requests.get('https://uae.dubizzle.com/motors/used-cars/', proxies=self.proxy, headers=self.headers)
            soup = BeautifulSoup(r.content, 'html5lib')
            last_page_anchor = soup.findAll('a', attrs={
                'id': 'last_page'})
            last_page_url_string = last_page_anchor[0].attrs['href']
            parsed_url = urlparse(last_page_url_string)
            total_pages = int(parse_qs(parsed_url.query)['page'][0])
            x = range(2, total_pages)
            for n in x:
                self.urls.append("https://uae.dubizzle.com/motors/used-cars/?page={}".format(n))
        except Exception as e:
            logger.log("Error in populate_list_of_url_pages: " + str(e))
            logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))



