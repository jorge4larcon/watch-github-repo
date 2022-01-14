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
from datetime import datetime
from typing import List

import jinja2

from utils import console_logger, utc2datetime


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
            logger.critical('Seems that the Github API is not using the way '
                            'to represent commits in JSON format they used '
                            'to. The key `%s` is not present in the commit '
                            'response. GitHub response: %s', e, api_dict)
            raise e


def ok():
    """
    Returns a dict representing that the execution of the Lambda function was
    successful.
    """
    return dict(ok=True)


def error():
    """
    Returns a dict representing that the execution of the Lambda function was
    unsuccessful.
    """
    return dict(ok=False)


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
    with urllib.request.urlopen(url) as response:
        commits = json.loads(response.read())
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
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read())


def watch_files(github_repo_api_url: str, files_to_watch: List[str],
                project_name: str, telegram_msg_template: str,
                telegram_chat_id: str, telegram_token: str,
                check_since: datetime):
    """Orchestrates all the operation of watching files of the repository."""
    logger.info('Retrieving the commits that contain the files %s since: %s',
                ','.join(files_to_watch),
                check_since.strftime('%d/%b/%Y, %I:%M %p'))
    commits = get_github_commits(github_repo_api_url, files_to_watch,
                                 check_since)
    if not commits:
        logger.info('There are no recent commmits that include the files the '
                    'boss is interested on. No notifications will be sent.')
        return ok()
    msg = make_telegram_msg(commits, files_to_watch, project_name,
                            telegram_msg_template)
    logger.info('Notifying about %s commit(s).', len(commits))
    response = send_telegram_msg(msg, telegram_chat_id, telegram_token)
    try:
        if not response['ok']:
            logger.critical('There was a problem notifying the boss via '
                            'Telegram O_o, this is the Telegram response: %s',
                            response)
            return error()
        logger.info('The boss has been notified via Telegram.')
        return ok()
    except KeyError as e:
        logger.critical('Seems that the Telegram API response has changed, the'
                        'key: `%s` was not found in the JSON response '
                        'received. Telegram response received: %s', e,
                        response)
        raise e


def lambda_handler(event, _context):
    """AWS Lambda funtion handler."""
    response: dict
    try:
        response = watch_files(
            event['github_repo_api_url'],
            event['files_to_watch'],
            event['project_name'],
            TELEGRAM_MSG_TEMPLATE_FILE,
            event['telegram_chat_id'],
            event['telegram_bot_token'],
            utc2datetime(event['check_since'])
        )
    except Exception:
        logger.exception('Unexpected exception occurred.')
        response = error()
    return response
