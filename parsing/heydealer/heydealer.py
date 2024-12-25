import requests
import random
import time
from utils.log import logger
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pytz

novosibirsk_tz = pytz.timezone("Asia/Novosibirsk")


def get_novosibirsk_time():
    return datetime.now(novosibirsk_tz)


Base = declarative_base()


class Heydealer(Base):
    __tablename__ = 'site_heydealer'

    id = Column(Integer, primary_key=True)
    id_car = Column(String)
    car_mark = Column(String)
    car_model = Column(String)
    images = Column(String)
    main_image = Column(String)
    year = Column(Integer)
    millage = Column(Integer)
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
            'Cookie': f'sessionid={sessionid}'
        }

        params = {
            'page': page,
            'type': 'auction',
            'is_subscribed': 'false',
            'is_retried': 'false',
            'is_previously_bid': 'false',
            'order': 'default'
        }

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            if not data:
                return None, True
            return data, False
        elif response.status_code == 401:
            logger.error("Ошибка 401: Неавторизованный доступ. Завершаем выполнение.")
            return None, True
        else:
            logger.error(f"Ошибка запроса: {response.status_code} на странице {page}")
            return None, False
    except Exception as e:
        logger.exception(f"Ошибка при запросе к API на странице {page}: {e}")
        return None, False


def save_data_to_db(id_car, car_mark, car_model, main_image, images, year, millage):
    try:
        existing_car = session.query(Heydealer).filter_by(id_car=id_car).first()
        if existing_car:
            return

        new_record = Heydealer(
            id_car=id_car,
            car_mark=car_mark,
            car_model=car_model,
            main_image=main_image,
            images=images,
            year=year,
            millage=millage
        )

        session.add(new_record)
        session.commit()
    except Exception as e:
        logger.exception(f"Ошибка сохранения данных в БД: {e}")
        return


def parse_params(data):
    try:
        id_car = data.get("hash_id", None)
        detail = data.get("detail", None)
        if detail:
            full_name = detail.get("model_part_name", None)
            if full_name:
                full_name_split = full_name.split(" ")
                car_mark = full_name_split[0]
                car_model = " ".join(full_name_split[1:])
            else:
                car_mark = None
                car_model = None

            main_image = detail.get("main_image_url", None)
            all_images = detail.get("image_urls", None)
            if all_images:
                images = ",".join(all_images)
            else:
                images = None

            year = detail.get("year", None)
            millage = detail.get("mileage", None)
        else:
            car_mark = None
            car_model = None
            main_image = None
            images = None
            year = None
            millage = None

        save_data_to_db(id_car, car_mark, car_model, main_image, images, year, millage)
    except Exception as e:
        logger.exception(f"Ошибка обработки данных: {e}")


def main():
    url = "https://api.heydealer.com/v2/dealers/web/cars/"
    sessionid = "9wccxvgv0asmj57mjo8mzne59g8eq0jx"

    page = 1
    while True:
        try:
            data, is_empty = fetch_data_from_api(url, page, sessionid)

            if is_empty:
                logger.info(f"Данные отсутствуют на странице {page}. Завершаем выполнение.")
                break

            if not data:
                page += 1
                continue

            for data_elem in data:
                parse_params(data_elem)

            delay = random.randint(10, 15)
            time.sleep(delay)

            page += 1
        except Exception as e:
            logger.exception(f"Ошибка в основном цикле на странице {page}: {e}")
            break


if __name__ == '__main__':
    main()
