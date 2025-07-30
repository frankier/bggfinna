"""Configurable retry decorators using tenacity."""

import os
import logging
from typing import Callable, Any
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_fixed, 
    wait_chain,
    retry_if_exception_type,
    retry_if_result,
    before_sleep_log,
    after_log
)
import requests
from tqdm import tqdm


# Configuration from environment variables
MAX_RETRIES = int(os.environ.get('BGGFINNA_MAX_RETRIES', '3'))
BASE_DELAY = float(os.environ.get('BGGFINNA_BASE_DELAY', '1.0'))
BGG_202_DELAY = float(os.environ.get('BGGFINNA_BGG_202_DELAY', '2.0'))


def log_retry_attempt(retry_state):
    """Log retry attempts with context information."""
    exception = retry_state.outcome.exception() if retry_state.outcome.failed else None
    attempt_num = retry_state.attempt_number
    
    if exception:
        if hasattr(retry_state.args[0], '__name__'):
            func_name = retry_state.args[0].__name__
        else:
            func_name = "API call"
        tqdm.write(f"Request failed for {func_name} (attempt {attempt_num}/{MAX_RETRIES}): {exception}")


def log_success_after_retries(retry_state):
    """Log successful completion after retries."""
    if retry_state.attempt_number > 1:
        if hasattr(retry_state.args[0], '__name__'):
            func_name = retry_state.args[0].__name__
        else:
            func_name = "API call"
        tqdm.write(f"Successfully completed {func_name} after {retry_state.attempt_number - 1} retries")


def is_bgg_202_response(result):
    """Check if result is a requests Response with status 202 (BGG processing)."""
    return (
        hasattr(result, 'status_code') and 
        result.status_code == 202
    )


# BGG API retry decorator with special 202 handling
bgg_api_retry = retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_chain(
        # First handle 202 responses with longer delay
        wait_fixed(BGG_202_DELAY),
        # Then regular delay for other failures
        wait_fixed(BASE_DELAY)
    ),
    retry=(
        retry_if_exception_type(requests.exceptions.RequestException) |
        retry_if_result(is_bgg_202_response)
    ),
    before_sleep=log_retry_attempt,
    after=log_success_after_retries
)


# Finna API retry decorator  
finna_api_retry = retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(BASE_DELAY),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=log_retry_attempt,
    after=log_success_after_retries
)


# Generic API retry decorator
generic_api_retry = retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(BASE_DELAY),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=log_retry_attempt,
    after=log_success_after_retries
)