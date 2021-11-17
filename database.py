import re
import sys
import mysql.connector
from datetime import datetime
import logger

DB_HOST_DS = ""
DB_USER_DS = ""
DB_PASS_DS = ""
DB_DATABASE_DS = ""


def create_dubizzle_listing(dubizzle_listings):
    try:
        remove_duplicate_dubizzle_listings_by_dubizzle_id(dubizzle_listings[0]["dubizzle_id"])
        connection_ds = mysql.connector.connect(
            host=DB_HOST_DS,
            user=DB_USER_DS,
            password=DB_PASS_DS,
            database=DB_DATABASE_DS
        )
        cursor = connection_ds.cursor()
        val = []
        sql = "INSERT INTO gcc_dubizzle_listings (dubizzle_id, link, make, model, phone, city, country," \
              "location, price, description, title, badge_description, mileage, warranty, color, year, doors, " \
              "body_condition, " \
              "mechanical_condition, seller_type, body_type, cylinders,transmission, hp, " \
              "fuel_type, vin, " \
              "adcode, adcode_short, specs, created_on) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s " \
              ") on duplicate key update make=values(make),model=values(model),city=values(city)," \
              "country=values(country),location=values(location),price=values(price),description=values(description)," \
              "title=values(title),badge_description=values(badge_description),mileage=values(mileage)," \
              "warranty=values(warranty),color=values(color)," \
              "year=values(year),doors=values(doors),body_condition=values(body_condition)," \
              "mechanical_condition=values(mechanical_condition),seller_type=values(seller_type),body_type=values(" \
              "body_type)," \
              "cylinders=values(cylinders),transmission=values(transmission),transmission=values(transmission)," \
              "hp=values(" \
              "hp),fuel_type=values(fuel_type),vin=values(vin),adcode=values(adcode),adcode_short=values(adcode_short),specs=values(specs)," \
              "updated_on='{}'".format(
            str(datetime.now().replace(microsecond=0)))
        for listing in dubizzle_listings:
            record = (listing["dubizzle_id"], listing["link"], listing["make"], listing["model"], listing["phone"],
                      listing["city"], listing["country"],
                      listing["location"],
                      listing["price"], listing["description"], listing["title"], listing["badge_description"],
                      listing["mileage"],
                      listing["warranty"], listing["color"],
                      listing["year"], listing["doors"], listing["body_condition"], listing["mechanical_condition"],
                      listing["seller_type"],
                      listing["body_type"], listing["cylinders"], listing["transmission"],
                      listing["hp"], listing["fuel_type"],
                      listing["vin"], listing["adcode"], listing["adcode_short"], listing["specs"],
                      str(listing["created_on"]))
            val.append(record)
        cursor.executemany(sql, val)
        connection_ds.commit()
        logger.log(str(cursor.rowcount) + " was inserted or updated", "log")
        logger.log("create_gumtree_listings sql " + cursor.statement, "log")
    except Exception as e:
        logger.log("Error in create_gumtree_listing: " + str(e))
        logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


def update_dubizzle_listing_for_removed_on(dubizzle_ids):
    try:
        connection_ds = mysql.connector.connect(
            host=DB_HOST_DS,
            user=DB_USER_DS,
            password=DB_PASS_DS,
            database=DB_DATABASE_DS
        )
        cursor = connection_ds.cursor()
        converted_list = [str(element) for element in dubizzle_ids]
        joined_list = ",".join(converted_list)
        sql = "UPDATE gcc_dubizzle_listings SET removed_on = '{}' WHERE dubizzle_Id IN ({})".format(
            str(datetime.now().replace(microsecond=0)), joined_list)
        cursor.execute(sql)
        connection_ds.commit()
        print(cursor.rowcount, "was updated.")
    except Exception as e:
        logger.log("Error in update_dubizzle_listing_for_removed_on: " + str(e))
        logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


def get_adcode(record):
    try:
        if "badge_description" in record:
            if record["badge_description"] is not None:
                mapping_sql = "select * from listings_addata_mapping where make ='{}' and listing_model ='{}' and trim ='{}' and source ='{}';".format(
                    record["make"], record["model"], record["badge_description"], "dubizzle_listings")
                connection_ds = mysql.connector.connect(
                    host=DB_HOST_DS,
                    user=DB_USER_DS,
                    password=DB_PASS_DS,
                    database=DB_DATABASE_DS
                )
                cursor = connection_ds.cursor(buffered=True)
                cursor.execute(mapping_sql)
                for row in cursor:
                    addata_model = row[3]
                    addata_sql = "select * from ADData where make_description = '{}' and model_description = '{}' and year = {}".format(
                        record["make"], addata_model, record["year"])
                    cursor_addata = connection_ds.cursor()
                    cursor_addata.execute(addata_sql)
                    addata_ids = [addata_row[0] for addata_row in cursor_addata]
                    match_found, vehicle_key, shortcode = process_text_for_addcode(record, addata_ids)
                    if match_found:
                        return vehicle_key, shortcode
                match_found, vehicle_key, shortcode = process_text_for_addcode(record)
                return vehicle_key, shortcode
        else:
            match_found, vehicle_key, shortcode = process_text_for_addcode(record)
            return vehicle_key, shortcode
    except Exception as e:
        print("Error in get_adcode: " + str(e) + str(record[0]))
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


def get_dubizzle_listings_by_dubizzle_ids_for_removing(dubizzle_ids):
    try:
        connection_ds = mysql.connector.connect(
            host=DB_HOST_DS,
            user=DB_USER_DS,
            password=DB_PASS_DS,
            database=DB_DATABASE_DS
        )
        cursor = connection_ds.cursor()
        format_strings = ','.join(['%s'] * len(dubizzle_ids))
        cursor.execute(
            "Select dubizzle_id from gcc_dubizzle_listings WHERE dubizzle_id not IN (%s) and removed_on is null" % format_strings,
            tuple(dubizzle_ids))
        return [item[0] for item in cursor.fetchall()]
    except Exception as e:
        logger.log("Error in get_gumtree_listings_by_gumtree_ids: " + str(e))
        logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


def remove_duplicate_dubizzle_listings_by_dubizzle_id(dubizzle_id):
    try:
        connection_ds = mysql.connector.connect(
            host=DB_HOST_DS,
            user=DB_USER_DS,
            password=DB_PASS_DS,
            database=DB_DATABASE_DS
        )
        cursor = connection_ds.cursor()
        cursor.execute(
            "Select listing_id from gcc_dubizzle_listings WHERE dubizzle_id = {}".format(dubizzle_id))
        duplicates = [item[0] for item in cursor.fetchall()]
        format_strings = ','.join(['%s'] * len(duplicates))
        cursor.execute(
            "delete from gcc_dubizzle_listings WHERE listing_id IN (%s)" % format_strings,
            tuple(duplicates))
        connection_ds.commit()
    except Exception as e:
        logger.log("Error in remove_duplicate_dubizzle_listings_by_dubizzle_id: " + str(e))
        logger.log("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    finally:
        if cursor is not None:
            cursor.close()
            connection_ds.close()


def process_text_for_addcode(row, add_dataids=None):
    adshort_code = None
    joined_list = ""
    row_title = row['title'].replace(":", " ", )
    row_link_array = row['link'].split("/")
    row_link = row_link_array[5] + " " + row_link_array[6]
    # row_title_arr = row['title'].split(":")
    match_string = row_title + " " + row_link + " " + row['description'] + " " + row['make'] + " " + row[
        'model']
    match_string = match_string.replace("'", "")
    match_string = match_string.replace('"', '')
    match_string = match_string + " " + row['badge_description']
    match_string = match_string + " " + row['body_type']
    if add_dataids:
        converted_list = [str(element) for element in add_dataids]
        joined_list = ",".join(converted_list)
        addata_sql = "Select model_description, make_description, badge_description, bodystyle_description, vehicle_key from ADData WHERE make_description = '{}' and year = {} and ID IN ({}) and MATCH(make_description, model_description, description, badge_description, bodystyle_description) AGAINST('{}' IN NATURAL LANGUAGE MODE);".format(
            row["make"], row["year"], joined_list, match_string)
    else:
        addata_sql = "Select model_description, make_description, badge_description, bodystyle_description, vehicle_key from ADData WHERE make_description = '{}' and year = {} and MATCH(make_description, model_description, description, badge_description, bodystyle_description) AGAINST('{}' IN NATURAL LANGUAGE MODE);".format(
            row["make"], row["year"], match_string)
    try:
        connection_ds = mysql.connector.connect(
            host=DB_HOST_DS,
            user=DB_USER_DS,
            password=DB_PASS_DS,
            database=DB_DATABASE_DS
        )
        cursor = connection_ds.cursor(buffered=True)
        cursor.execute(addata_sql)
        counter = 0;
        for adrow in cursor:
            counter = counter + 1;
            addata_modeldescription = adrow[0]
            addata_makedescription = adrow[1]
            addata_badgedescription = adrow[2]
            addata_bodystyledescription = adrow[3]
            badge_desArr = addata_badgedescription.split(" ")
            if (re.search(addata_modeldescription, match_string, re.IGNORECASE) or re.search(
                    addata_modeldescription.replace(" ", ""), match_string, re.IGNORECASE)) is not None and re.search(
                    addata_makedescription, match_string, re.IGNORECASE):
                vehicle_key = adrow[4]
                adshort_code = vehicle_key[0:-4]
                if (re.search(addata_badgedescription, match_string, re.IGNORECASE) or re.search(badge_desArr[0],
                                                                                                 match_string,
                                                                                                 re.IGNORECASE) and re.search(
                    addata_bodystyledescription, match_string, re.IGNORECASE)):
                    return True, vehicle_key, adshort_code

            if add_dataids is None and counter == cursor.rowcount and adshort_code is not None:
                return False, None, adshort_code

        return False, None, None

    except Exception as e:
        print("Error in process_text_for_addcode: " + str(e))
        print("sql error:  " + addata_sql + " " + joined_list)
        print("Error on line {}".format(sys.exc_info()[-1].tb_lineno))



