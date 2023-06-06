import requests
import json

__all__ = ['slack_msg']

def slack_msg(webhook,msg,logger=None):
    """
    Send a message to a slack channel
    Args:
        webhook (str): Slack webhook URL
        msg (str): Message to send
        logger (logging.Logger): Logger to use
    Returns:
        None
    """
    
    response = requests.post(
        url=webhook,
        data=json.dumps(msg),
        headers={'Content-Type': 'application/json'})

    if logger:
        logger.info('Slack response: %s', response.text)