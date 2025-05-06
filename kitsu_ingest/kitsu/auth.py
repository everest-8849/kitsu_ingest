import os
import logging
import gazu
from dotenv import load_dotenv

def kitsu_login():
    load_dotenv()
    kitsu_server = os.getenv('KITSU_SERVER')
    kitsu_email = os.getenv('KITSU_EMAIL')
    kitsu_password = os.getenv('KITSU_PASSWORD')

    if not all([kitsu_server, kitsu_email, kitsu_password]):
        raise EnvironmentError("Missing one of KITSU_SERVER, KITSU_EMAIL, or KITSU_PASSWORD in .env")

    logging.info(f"Connecting to Kitsu server: {kitsu_server}")
    try:
        gazu.set_host(kitsu_server)
        gazu.log_in(kitsu_email, kitsu_password)
        logging.info("Successfully logged in to Kitsu")
        return True
    except Exception as e:
        raise RuntimeError("Kitsu login failed. Check your credentials or host address.") from e