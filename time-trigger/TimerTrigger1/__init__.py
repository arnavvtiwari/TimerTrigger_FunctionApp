import datetime
import logging
from TimerTrigger1.scripts.connection import db_connection, remove_null, push_to_storage
import azure.functions as func
import pandas as pd

def main(mytimer: func.TimerRequest) -> None:
    df = db_connection()
    df = remove_null(df)
    push_to_storage(df)
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
