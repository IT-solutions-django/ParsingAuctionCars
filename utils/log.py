import logging

logger = logging.getLogger('logs_app')
handler = logging.FileHandler('/app/app.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
