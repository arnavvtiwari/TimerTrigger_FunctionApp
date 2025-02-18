import logging
from HttpTrigger2.scripts.connection import  read_file, insert_data
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if (name == 'csv'):
        read_file()
        return func.HttpResponse("CSV read successfully!", status_code=200)
    elif (name == 'insert'):
        insert_data()
        return func.HttpResponse("Data inserted successfully!", status_code=200)
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
