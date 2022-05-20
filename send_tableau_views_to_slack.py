import logging
import os

from datetime import datetime
import pandas as pd
from slack_sdk import WebClient
import tableauserverclient as TSC

from drgn.data_lake.config import env_config

views = {
    "key_metrics_1": {
        "display_name": "Daily Portfolio Reporting - Key Metrics",
        "id": {
            "dev": "xxxxx-xxxxx",
            "stg": "xxxxx-xxxxx",
            "prod": "xxxxx-xxxxx",
        },
        "description": "Daily Metrics",
    },
    "key_metrics_2": {
        "display_name": "Daily Portfolio Reporting - Key Metrics II",
        "id": {
            "dev": "xxxxx-xxxxx",
            "stg": "xxxxx-xxxxx",
            "prod": "xxxxx-xxxxx",
        },
        "description": "Daily Metrics II",
    }
}

channels = {
    "daily-portfolio-view": {
        "id": {
            "dev": "xxxxx-xxxxx",
            "stg": "xxxxx-xxxxx",
            "prod": "xxxxx-xxxxx",
        }
    }
}

tableau_server_url = {
    "dev": "xxxxx-xxxxx",
    "stg": "xxxxx-xxxxx",
    "prod": "xxxxx-xxxxx",
}

logger = logging.getLogger()
logger.setLevel(logging.INFO)
_DATE_STRING = str(datetime.date(datetime.now()))
_DISPLAY_NAME_VAR = 'display_name'
_ID_VAR = 'id'
_DESCRIPTION_VAR = 'description'
CHANNEL = 'daily-portfolio-view'
_ENV = env_config['bucket_default']['default_bucket_pattern'].split('-')[0]
_SLACK_BOT_TOKEN = env_config['slack']['bot_token']
_TABLEAU_ADMIN_USER = env_config['tableau']['admin_user']
_TABLEAU_ADMIN_PASSWORD = env_config['tableau']['admin_password']

def get_tableau_view(
        tableau_view:str,
        _date_string:str,
        server:object,
):
    filepath = f"./{_date_string} {tableau_view[_DISPLAY_NAME_VAR]}.png"
    view = server.views.get_by_id(tableau_view[_ID_VAR][_ENV])
    server.views.populate_image(view)
    logging.info(f"Download started for {tableau_view[_DISPLAY_NAME_VAR]} on {_date_string}.")
    with open(filepath, 'wb') as f:
        f.write(view.image)
    logging.info(f"Download completed for {tableau_view[_DISPLAY_NAME_VAR]} on {_date_string}.")

def send_image_to_slack(
        channels:dict,
        tableau_view:str,
        _date_string:str,
        client:object,
):
    tableau_view_display_name = f"{_date_string} {tableau_view[_DISPLAY_NAME_VAR]}"
    logging.info(f"Uploaded started for {tableau_view_display_name} in Slack channel {CHANNEL}.")
    client.files_upload(
        channels=channels[CHANNEL][_ID_VAR][_ENV],
        file=f"{tableau_view_display_name}.png",
        title=f"{tableau_view_display_name}",
        initial_comment=f"<!here> is the {tableau_view[_DESCRIPTION_VAR]} report on {_DATE_STRING}.",
    )
    logging.info(f"Uploaded completed for {tableau_view_display_name} in Slack channel {CHANNEL}.")

def task_data_extraction_data2990_daily_portfolio_report_automation(
        views:dict,
        _date_string:str,
):
    logging.info(f"Authentication for Slack Bot started.")
    client = WebClient(token=_SLACK_BOT_TOKEN, timeout=300)
    client.api_call(api_method='auth.test')
    logging.info(f"Authentication for Slack Bot successful.")

    logging.info(f"Initialization for Tableau connection started.")
    tableau_auth = TSC.TableauAuth(_TABLEAU_ADMIN_USER, _TABLEAU_ADMIN_PASSWORD, site_id='Publisher')
    server = TSC.server(tableau_server_url[_ENV], use_server_version=True)
    logging.info(f"Authentication for Tableau started.")
    server.auth.sign_in(tableau_auth)
    logging.info(f"Authentication for Tableau successful.")

    for tableau_view in views.values():
        get_tableau_view(tableau_view, _DATE_STRING, server)
        send_image_to_slack(channels, tableau_view, _DATE_STRING, client)