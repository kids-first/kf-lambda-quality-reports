import pytest
import service


def test_handler():
    """
    Test the service handler
    """
    service.handler({}, {})
