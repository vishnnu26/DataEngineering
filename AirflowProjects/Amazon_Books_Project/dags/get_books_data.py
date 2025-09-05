import pandas as pd
import re
import requests

from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from io import BytesIO
from tempfile import NamedTemporaryFile
from typing import Dict, List, Tuple

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

today: str = str(date.today())
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0"}

def get_books_in_page_given(all_books: List) -> List[Dict]:
      books_data_list: List = []
      for current_book in all_books:
            current_book_page_url: str = "http://books.toscrape.com/catalogue/" + current_book.h3.a.get("href")
            
            currency, total_price = (
                        text[0], float(text[1:])
            # Walrus Operator
            ) if (text:= current_book.find("p", class_ = "price_color").get_text(strip=True)) else (None, None)
            
            # (book_detail_page_ := requests.get(current_book_page_url)).encoding = "utf-8"
            (book_detail_page := requests.get(current_book_page_url, headers=headers, timeout=10)).encoding = "utf-8"
            current_book_soup = BeautifulSoup(book_detail_page.text, "html.parser")

            # Cases where the P-tag is missing. Thus going ahead by using walrus operator:
            description = (
                              desc_p.get_text(strip=True)
                              if (desc_div := current_book_soup.find("div", id="product_description")) and 
                                 (desc_p := desc_div.find_next_sibling("p"))
                              else None
                        )
            
            counts_text = current_book_soup.find("th", string="Availability").find_next_sibling("td").get_text(strip=True)
            match = re.search(r"\((\d+)\s+available\)", counts_text)

            # Build dictionary
            book_info: Dict = {
                              "title": current_book.h3.a.get("title"),
                              "url": current_book_page_url,
                              "rating": current_book.p.get("class")[1],
                              "availability": current_book.find("p", class_="instock availability").get_text(strip=True),
                              "currency": currency,
                              "price": total_price,
                              "description" : description,
                              "count": int(match.group(1)) if match else None
                        }
            
            books_data_list.append(book_info)
      return books_data_list

def insert_data_into_s3(final_result_list: List[Dict], columns: List[str]) -> None:
      try:
            minio_connect = S3Hook(aws_conn_id = "aws_connection_key")
            books_dataframe = pd.DataFrame(final_result_list, columns = columns)
            
            with NamedTemporaryFile(suffix = ".csv", mode = "w") as tempfile:
                  books_dataframe.to_csv(tempfile.name, index = False)
                  
                  minio_connect.load_file(
                        filename = tempfile.name,
                        bucket_name = "tutorialsbucket",
                        key = f"books_to_scrape/{today}.csv",
                        replace = True
                  )

            print("Written into S3 successfully")
      except Exception as err:
            raise err

def get_all_books_and_write_to_s3(ti, page_number = "page-1.html") -> None:

      final_result_list: List = []
      
      while page_number:
            url = f"http://books.toscrape.com/catalogue/{page_number}"
            resp = requests.get(url, headers=headers, timeout=10)

            # FORCE CORRECT ENCODING.
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            all_books = soup.find_all("li", class_ = "col-xs-6 col-sm-4 col-md-3 col-lg-3")
            
            final_result_list.extend(get_books_in_page_given(all_books))
            print(f"page number - {page_number} completed")
            
            page_number = (
                  next_page.a.get("href")
                  if (next_page := soup.find("li", class_ = "current").find_next_sibling("li", class_ = "next"))
                  else None
            )

      columns = [keys for keys in final_result_list[0].keys()] if final_result_list else []
      ti.xcom_push(key = "columns", value = columns)
      insert_data_into_s3(final_result_list, columns)


def insert_into_postgres_database(ti) -> None:
      postgres_conn = None
      try:
            minio_connect = S3Hook(aws_conn_id = "aws_connection_key")
            # books_dataframe = pd.DataFrame(final_result_list, columns = columns)

            s3_client = minio_connect.get_conn()

            minio_object = s3_client.get_object(Bucket = "tutorialsbucket", 
                                                Key = f"books_to_scrape/{today}.csv"
                                          )
            
            file_bytes = minio_object["Body"].read()
            books_data_from_s3 = pd.read_csv(BytesIO(file_bytes))
            
            columns: List = ti.xcom_pull(task_ids = "fetch_all_books_from_site", key = "columns")

            all_books: List[Tuple] = [tuple(value) for value in books_data_from_s3[columns].values]
            
            postgres_hook = PostgresHook(postgres_conn_id = "postgres_connection_key")
            
            postgres_conn = postgres_hook.get_conn()
            cursor = postgres_conn.cursor()

            insert_query: str = """
                                   INSERT INTO books_to_scrape (title, url, rating, availability, 
                                                                currency, price, description, count)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                              """
            cursor.executemany(insert_query, all_books)
            postgres_conn.commit()

            print("INSERTION COMPLETED -- NO.OF ROWS ==>", len(all_books))

      except Exception as err:
            raise err
      
      finally:
            if postgres_conn:
                  postgres_conn.close()
                  cursor.close()