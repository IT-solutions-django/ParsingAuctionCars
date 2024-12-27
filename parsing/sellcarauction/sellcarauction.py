import requests
import random
import time
from utils.log import logger
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pytz
from bs4 import BeautifulSoup
import re

novosibirsk_tz = pytz.timezone("Asia/Novosibirsk")


def get_novosibirsk_time():
    return datetime.now(novosibirsk_tz)


Base = declarative_base()


class SellCarAuction(Base):
    __tablename__ = 'site_sellcarauction'

    id = Column(Integer, primary_key=True)
    id_car = Column(String)
    car_mark = Column(String)
    car_model = Column(String)
    images = Column(String)
    main_image = Column(String)
    year = Column(Integer)
    millage = Column(Integer)
    transmission = Column(String)
    car_fuel = Column(String)
    color = Column(String)
    price = Column(String)
    created_at = Column(DateTime(timezone=True), nullable=False, default=get_novosibirsk_time)
    updated_at = Column(DateTime(timezone=True), onupdate=get_novosibirsk_time, nullable=False,
                        default=get_novosibirsk_time)


engine = create_engine('sqlite:///cars_auction.db')
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
]


def fetch_data_from_api(url, page, sessionid):
    try:
        headers = {
            'User-Agent': random.choice(user_agents),
            'Cookie': f'JSESSIONID={sessionid}',
            'Referer': 'https://www.sellcarauction.co.kr/newfront/receive/rc/receive_rc_list.do'
        }

        params = {
            'i_iNowPageNo': page,
        }

        response = requests.post(url, headers=headers, data=params, verify=False)

        if response.status_code == 200:
            data = response.content
            soup = BeautifulSoup(data, 'html.parser')
            pagination = soup.find('ul', class_='pagination')
            active_page_tag = pagination.find('li', class_='active')
            if active_page_tag:
                active_page = active_page_tag.get_text(strip=True)
                if str(page) != active_page:
                    return None, True
            else:
                return None, True
            if not data:
                return None, True
            return data, False
        elif response.status_code == 302:
            logger.error("Ошибка 302: Неавторизованный доступ. Завершаем выполнение.")
            return None, True
        else:
            logger.error(f"Ошибка запроса: {response.status_code} на странице {page}")
            return None, False
    except Exception as e:
        logger.exception(f"Ошибка при запросе к API на странице {page}: {e}")
        return None, False


def save_data_to_db(id_car, car_mark, car_model, main_image, images, year, millage, price, color, car_fuel,
                    transmission):
    try:
        existing_car = session.query(SellCarAuction).filter_by(id_car=id_car).first()
        if existing_car:
            return

        new_record = SellCarAuction(
            id_car=id_car,
            car_mark=car_mark,
            car_model=car_model,
            main_image=main_image,
            images=images,
            year=year,
            millage=millage,
            price=price,
            color=color,
            car_fuel=car_fuel,
            transmission=transmission
        )

        session.add(new_record)
        session.commit()
    except Exception as e:
        logger.exception(f"Ошибка сохранения данных в БД: {e}")
        return


def parse_params(data):
    try:
        soup = BeautifulSoup(data, 'html.parser')
        car_cards = soup.find_all('div', class_='car_one')

        for card in car_cards:
            title = card.find('div', class_='car-title')
            title_tag = title.find('a', onclick=re.compile(r'carInfo'))
            if title_tag:
                full_car_name = title_tag.get_text(strip=True)
                if full_car_name:
                    full_name_split = full_car_name.split(" ")
                    car_mark = full_name_split[0]
                    car_model = " ".join(full_name_split[1:])
                else:
                    car_mark = None
                    car_model = None

                onclick_value = title_tag.get('onclick', '')
                match = re.search(r"carInfo\('([^']+)'\)", onclick_value)
                id_car = match.group(1) if match else None
            else:
                car_mark = None
                car_model = None
                id_car = None

            car_list = card.find('div', class_='car-list').find_all('li')
            year = car_list[0].get_text(strip=True) if len(car_list) > 0 else None
            millage = car_list[1].get_text(strip=True) if len(car_list) > 1 else None
            transmission = car_list[2].get_text(strip=True) if len(car_list) > 2 else None
            car_fuel = car_list[3].get_text(strip=True) if len(car_list) > 3 else None
            color = car_list[4].get_text(strip=True) if len(car_list) > 4 else None

            price_tag = card.find('strong', class_='car_list_item', string='시작가 : ')
            price = price_tag.find_next_sibling('strong').get_text(strip=True) if price_tag else None

            image_tag = card.find('div', class_='car-image').find('img')
            main_image = images = image_tag['src'] if image_tag else None

            save_data_to_db(id_car, car_mark, car_model, main_image, images, year, millage, price, color, car_fuel,
                            transmission)
    except Exception as e:
        logger.exception(f"Ошибка обработки данных: {e}")


def main():
    url = "https://www.sellcarauction.co.kr/newfront/receive/rc/receive_rc_list.do"
    jsessionid = "E0B9DF5D0961F87E5C7B530754F53465"

    page = 1
    while True:
        try:
            data, is_empty = fetch_data_from_api(url, page, jsessionid)

            if is_empty:
                logger.info(f"Данные отсутствуют на странице {page}. Завершаем выполнение.")
                break

            if not data:
                page += 1
                continue

            parse_params(data)

            delay = random.randint(10, 15)
            time.sleep(delay)

            page += 1
        except Exception as e:
            logger.exception(f"Ошибка в основном цикле на странице {page}: {e}")
            break


if __name__ == '__main__':
    main()
