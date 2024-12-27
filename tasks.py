from celery import Celery
from celery.schedules import crontab
from parsing.heydealer.heydealer_script import start_parse as heydealer
from parsing.sellcarauction.sellcarauction_script import start_parse as sellcarauction
from utils.log import logger

app = Celery('tasks', broker='redis://redis:6379/0', backend='redis://redis:6379/0')


def run_parsers():
    # logger.info("Обработка первого сайта")
    # heydealer()
    # logger.info("Обработка первого сайта завершилась")

    logger.info("Обработка второго сайта")
    sellcarauction()
    logger.info("Обработка второго сайта завершилась")


@app.task
def run_all_parsers():
    logger.info("Парсинг начался")

    run_parsers()

    logger.info("Парсинг завершился")


app.conf.beat_schedule = {
    'run-every-day-at-23am': {
        'task': 'tasks.run_all_parsers',
        'schedule': crontab(hour=23, minute=15),
    },
}
app.conf.timezone = 'Asia/Novosibirsk'
