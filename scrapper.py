import os
import sys
import requests
from bs4 import BeautifulSoup
import re
import datetime
import psycopg2
import smtplib
import ssl
import logging
import argparse

timestamp_of_script = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())

# A very basic logger that dumps information into a file.
log_file = os.path.join(os.getcwd(), "WebScrapper.log")
logger = logging.getLogger("WebScrapper")
logger.setLevel(logging.INFO)
file_logger = logging.FileHandler(log_file, mode='a')
file_logger.setLevel(logging.INFO)
logger.addHandler(file_logger)




# These are the web pages I decided to scrape for information. The information we need to scrape the data is:
# The URL of the web page, the class where the name of the GPU is stored, the class where the price is stored, and
# the class where the buy button is stored (this is our availability condition, as unless there is stock it wont appear)
pages_dictionary = {"coolmod": ["https://www.coolmod.com/asus-turbo-geforce-rtx-3090-24gb-gddr6x-tarjeta-grafica"
                                 "-precio", "product-first-part", "text-price-total", "button-buy"],
                     "coolmod2": [
                         "https://www.coolmod.com/evga-geforce-rtx-3090-xc3-black-gaming-24gb-gddr6x-tarjeta-grafica-precio",
                         "product-first-part", "text-price-total", "button-buy"],
                     "coolmod3": [
                         "https://www.coolmod.com/evga-geforce-rtx-3090-xc3-gaming-24gb-gddr6x-tarjeta-grafica-precio",
                         "product-first-part", "text-price-total", "button-buy"],
                     "coolmod4": [
                         "https://www.coolmod.com/evga-geforce-rtx-3090-xc3-ultra-gaming-24gb-gddr6x-tarjeta-grafica-precio",
                         "product-first-part", "text-price-total", "button-buy"],
                    "ibertronica": ["https://www.ibertronica.es/asus-rtx-3090-turbo-24gb-gddr6x",
                                     "mb-3 h2 product-title", "col-6 ng-tns-c1-1 ng-star-inserted",
                                     "btn btn-outline-primary btn-block m-0 mb-3"],
                    "xtremmedia": ["https://www.xtremmedia.com/Asus_Turbo_GeForce_RTX_3090_24GB_GDDR6X.html",
                                    "ficha-titulo", "offerDetails article-list-pvp", "article-carrito2", "precio"],
                    "xtremmedia2": [
                         "https://www.xtremmedia.com/EVGA_GeForce_RTX_3090_XC3_Ultra_Gaming_24GB_GDDR6X.html",
                         "ficha-titulo", "offerDetails article-list-pvp", "article-carrito2", "precio"],
                    "pccomponentes": ["https://www.pccomponentes.com/asus-turbo-geforce-rtx-3090-24gb-gddr6x", "h4",
                                       "baseprice",
                                       "btn btn-primary btn-lg buy GTM-addToCart buy-button js-article-buy"],
                    "pccomponentes2": [
                         "https://www.pccomponentes.com/evga-geforce-rtx-3090-xc3-black-gaming-24gb-gdddr6x", "h4",
                         "baseprice", "btn btn-primary btn-lg buy GTM-addToCart buy-button js-article-buy"],
                    "pccomponentes3": ["https://www.pccomponentes.com/evga-geforce-rtx-3090-xc3-gaming-24gb-gddr6x",
                                        "h4", "baseprice",
                                        "btn btn-primary btn-lg buy GTM-addToCart buy-button js-article-buy"],
                    "pccomponentes4": [
                         "https://www.pccomponentes.com/evga-geforce-rtx-3090-xc3-ultra-gaming-24gb-gddr6x", "h4",
                         "baseprice", "btn btn-primary btn-lg buy GTM-addToCart buy-button js-article-buy"]}





# Note for docker:
# Since we have a postgres client in local and it uses port 5432, we must bind another port when creating our container.
# In this case : docker run -d -p 4321:5432 ...... and so on

def get_product_details(urls, name_class, price_class, instock_class, alternate_price_class=None):
    """ Receives 4-5 inputs, and returns a dictionary with the scrapped information.
        The function extracts the relevant information of the url provided (price, name, availability),
        it then cleans and formats the information so that it can be dumped into a relational DB"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/88.0.4324.104 Safari/537.36 "
    }
    details = {"date_of_scrapping": "", "seller": "", "name": "", "price": 0, "in_stock": False, "deal": False,
               "url": ""}
    if urls == "":
        logger.warning(f"URL parameter is empty, skipping this k-v pair")
        details = None
    else:
        try:
            page = requests.get(urls, headers=headers)
            page.raise_for_status()  # to check if we got a correct response (200) else it raises an Exception.
            soup = BeautifulSoup(page.content, features="html.parser")
            timestamp = '{:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now())
            seller_raw = re.sub('^.*w\.', '', urls)
            name = soup.find(class_=name_class)
            price = soup.find(class_=price_class)
            in_stock = soup.find(class_=instock_class)
            if alternate_price_class is not None and price is None:
                price = soup.find(class_=alternate_price_class)
            details["date_of_scrapping"] = timestamp
            if "ibertronica" in seller_raw:
                details["seller"] = re.sub('\.es.*', '', seller_raw)
            else:
                details["seller"] = re.sub('\.com.*', '', seller_raw)
            if name is not None:
                details["name"] = name.get_text()
                details["name"] = re.sub("GeForce", "", details["name"])
                details["name"] = re.sub("®", "", details["name"])
                details["name"] = re.sub(" - {2}Tarjeta Gráfica", "", details["name"])
                details["name"] = re.sub(" {2}", " ", details["name"])
                details["name"] = re.sub("DDD", "DD", details["name"])
                details["name"] = details["name"].upper()
                details["name"] = re.sub("ASUS TURBO RTX 3090", "ASUS RTX 3090 TURBO", details["name"])
                details["url"] = urls
            else:
                details = None
                logger.warning(f"URL: {urls} not scrapped because the name of the product was not found @ {timestamp}")
                return details
            if price is not None:
                details["price"] = int(re.sub('[^0-9]', '', price.get_text())[0:4])
            if in_stock is not None:
                details["in_stock"] = True
            if int(details["price"]) <= 1800:
                details["deal"] = True
            logger.info(f"{urls} scrapped successfully @ {timestamp}")
        except Exception as ex:
            logger.warning(f"Exception caught @ get_product_details :{ex}")
            details = None
    return details


def iterate_webpages(dictionary):
    """ Helper function to iterate over our pages directory using the get_products_details function"""
    if not dictionary:
        logger.warning(f"Nothing to scrap, ending script")
        sys.exit(1)
    sql_information_list = []
    for key in dictionary:
        query = get_product_details(*dictionary[key])
        if query is not None:
            sql_information_list.append(query)
    if not sql_information_list:
        logger.warning(f"No information was scrapped, terminating {timestamp_of_script}")
        sys.exit(1)
    return sql_information_list


def create_message(scrapped_data):
    """ A simple function that creates the message to be sent in an email if the conditions are met."""
    message = ""
    for dic in scrapped_data:
        if dic["in_stock"] and dic["deal"]:
            line = f"The item sold by {dic['seller']} is on sale for {dic['price']} euros @ {dic['url']}\n"
            message += line
    return message


def send_email(message, config):
    """ This function sends the actual email should the conditions be met."""
    try:
        with open(config) as reader:
            lines = reader.read().splitlines()
        port = 465  # For SSL
        smtp_server = lines[0]
        sender_email = lines[1]
        password = lines[2]
        receiver_email = lines[3]
        print(smtp_server, sender_email, password, receiver_email)

        message_to_send = f"Subject: Price Alert \n\n {message}"
        message_to_send = re.sub(r'[^\x00-\x7F]+', ' ', message_to_send)  # Quick and dirty regex to remove non ascii chars.

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message_to_send)
    except Exception as ex:
        logger.warning(f"Exception caught when trying to send an email @ send_email():{ex}")


def do_insert(rec, config):
    """ This function inserts the scrapped data into our Postgres DB, should an exception occur the function will
        rollback the transaction and continue with the rest."""
    try:
        with open(config) as reader:
            lines = reader.read().splitlines()
        db_name = lines[0]
        username = lines[1]
        password = lines[2]
        ip_address = lines[3]
        port = lines[4]
        conn = psycopg2.connect(dbname=db_name, user=username, password=password, host=ip_address, port=port)
        cur = conn.cursor()
    except Exception as ex:
        logger.warning(f"Exception caught when reading config file @ do_insert():{ex}")
        sys.exit(1)

    for dictionary in rec:
        try:
            cols = dictionary.keys()
            cols_str = ','.join(cols)
            values_to_insert = [dictionary[k] for k in cols]
            values_wildcards = ','.join(['%s' for i in range(len(values_to_insert))])  # -> %s,%s,%s,%s,%s,%s,%s
            sql_str = f"INSERT INTO scrapped_data ({cols_str}) VALUES ({values_wildcards}) ON CONFLICT DO NOTHING"
            cur.execute(sql_str, values_to_insert)
            conn.commit()
        except Exception as ex:
            conn.rollback()
            logger.warning(f"Exception caught @ do_insert():{ex}")
            continue


def main():
    scrapped_data = iterate_webpages(pages_dictionary)
    email = create_message(scrapped_data)
    if email:
        send_email(email, config_path)
    do_insert(scrapped_data, pg_config_path)
    logger.info(f"We are done! @ {timestamp_of_script}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("email_config_file",
                        type=str,
                        help="a text file with email_config parameters for sending the email")
    parser.add_argument("postgres_config_file",
                        type=str,
                        help="a text file with email_config parameters connecting to our postgres db")
    args = parser.parse_args()
    pwd = os.getcwd()
    config_path = os.path.join(pwd, args.email_config_file)
    pg_config_path = os.path.join(pwd, args.postgres_config_file)

    main()

