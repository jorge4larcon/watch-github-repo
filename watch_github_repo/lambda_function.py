"""
This is an AWS Lambda function that watches specific files of a public GitHub
repository since a given date, if it detects new changes, it notifies the user
via Telegram (using a Telegram bot).
"""
from __future__ import annotations  # https://www.python.org/dev/peps/pep-0563/

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import boto3
import jinja2

from utils import console_logger, utc2datetime, datetime2utc


LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
TELEGRAM_API_URL = 'https://api.telegram.org/bot'
TELEGRAM_MSG_TEMPLATE_FILE = 'telegram-msg.j2'


logger = console_logger(__name__, LOG_LEVEL)


@dataclass
class Commit:
    """Basic representation of a commit."""
    message: str
    timestamp: datetime
    url: str

    @staticmethod
    def from_api_dict(api_dict: dict) -> Commit:
        """
        Retrieves a 'Commit' object using a dict.
        """
        try:
            message = api_dict['commit']['message']
            timestamp = utc2datetime(api_dict['commit']['committer']['date'])
            url = api_dict['html_url']
            return Commit(message, timestamp, url)
        except KeyError as e:
            logger.exception('Seems that the Github API is not using the way '
                             'to represent commits in JSON format they used '
                             'to.')
            raise e


def get_last_check_date(s3: boto3.session.Session.resource, bucket: str,
                        key: str) -> datetime:
    """Retrives the last check date from a text file in an S3 bucket."""
    try:
        s3_obj = s3.Object(bucket, key)
        date_str = s3_obj.get()['Body'].read().decode('UTF-8').strip()
        return utc2datetime(date_str)
    except Exception:
        logger.exception('Unable to retrieve the object %s to obtain the last '
                         ' check date, using "now" as the last check date.',
                         f's3://{bucket}/{key}')
        return datetime.now()


def write_check_date(check_date: datetime, s3: boto3.session.Session.resource,
                     bucket: str, key: str):
    """Saves the check date in iso format in a text file in an S3 bucket."""
    check_date_str = datetime2utc(check_date)
    object_path = f's3://{bucket}/{key}'
    try:
        s3_obj = s3.Object(bucket, key)
        response = s3_obj.put(Body=check_date_str)
        response_metadata = response.get('ResponseMetadata')
        if response_metadata.get('HTTPStatusCode') == 200:
            logger.info('The check date was saved successfully in %s',
                        object_path)
        else:
            logger.error('Unable to save the check date in %s', object_path)
    except Exception:
        logger.exception('Unable to save the check date in %s', object_path)


def get_github_commits(repo_url: str, files_to_watch: List[str],
                       since: datetime) -> List[dict]:
    """
    Retrieves the Github commits that contain the specified files since an
    specific date.
    """
    query = {'path': files_to_watch, 'since': since.isoformat()}
    params = urllib.parse.urlencode(query, doseq=True,
                                    quote_via=urllib.parse.quote)
    url = f'{repo_url}?{params}'
    commits: List[dict] = []
    try:
        with urllib.request.urlopen(url) as response:
            commits = json.loads(response.read())
    except Exception:
        logger.exception('Unable to retrieve the Github repository commits.')
    commits = list(map(Commit.from_api_dict, commits))
    return commits


def make_telegram_msg(commits: List[dict], watched_files: List[str],
                      project_name: str, template_file: str) -> str:
    """Creates the text message that will be sent via Telegram."""
    template_loader = jinja2.FileSystemLoader(searchpath='.')
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template(template_file)
    return template.render(commits=commits, watched_files=watched_files,
                           project_name=project_name)


def send_telegram_msg(msg: str, chat_id: str, token: str):
    """Sends a text message to an specific Telegram chat."""
    msg = urllib.parse.urlencode({
        'chat_id': chat_id, 'text': msg, 'disable_web_page_preview': True})
    msg = msg.encode('ascii')
    url = f'{TELEGRAM_API_URL}{token}/sendMessage'
    request = urllib.request.Request(url=url, data=msg, method='POST')
    try:
        logger.info('Notifying the boss via Telegram...')
        with urllib.request.urlopen(request) as response:
            parsed_response = json.loads(response.read())
            logger.info('Telegram response received: %s', parsed_response)
            if parsed_response.get('ok'):
                logger.info('The boss has been notified via Telegram.')
            else:
                logger.error('There was a problem notifying the boss via '
                             'Telegram O_o.')
    except urllib.error.URLError:
        logger.exception('There was a problem sending the Telegram message!')


def watch_files(s3_bucket: str, s3_obj_key: str, github_repo_api_url: str,
                files_to_watch: List[str], project_name: str,
                telegram_msg_template: str, telegram_chat_id: str,
                telegram_token: str):
    """Orchestrates all the operation of watching files of the repository."""
    logger.info('Retrieving the last check date from "%s"...',
                f's3://{s3_bucket}/{s3_obj_key}')
    s3 = boto3.resource('s3')
    last_check_date = get_last_check_date(s3, s3_bucket, s3_obj_key)
    logger.info('Retrieving the commits that contain the files %s since: %s',
                ','.join(files_to_watch),
                last_check_date.strftime('%d/%b/%Y, %I:%M %p'))
    commits = get_github_commits(github_repo_api_url, files_to_watch,
                                 last_check_date)
    if not commits:
        logger.info('There are no recent commmits that include the files the '
                    'boss is interested on.')
        return
    five_min_ago = datetime.now() - timedelta(minutes=5)
    write_check_date(five_min_ago, s3, s3_bucket, s3_obj_key)
    msg = make_telegram_msg(commits, files_to_watch, project_name,
                            telegram_msg_template)
    logger.info('Notifying about %s commit(s).', len(commits))
    send_telegram_msg(msg, telegram_chat_id, telegram_token)


def lambda_handler(event, _context):
    """AWS Lambda funtion handler."""
    watch_files(
        event['s3_bucket'],
        event['check_date_file'],
        event['github_repo_api_url'],
        event['files_to_watch'],
        event['project_name'],
        TELEGRAM_MSG_TEMPLATE_FILE,
        event['telegram_chat_id'],
        event['telegram_bot_token']
    )
