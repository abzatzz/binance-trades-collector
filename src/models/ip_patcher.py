import socket
import urllib3.util.connection
import requests
from aiohttp import TCPConnector, ClientSession
import asyncio
import sys


def patch_ip(ip, logger):
    """Patch socket, urllib3, and aiohttp to use the specified IP and run tests."""

    def patched_create_connection(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                                  source_address=None, socket_options=None):
        if source_address is None:
            source_address = (ip, 0)
        return urllib3.util.connection._orig_create_connection(
            address, timeout, source_address, socket_options
        )

    if not hasattr(urllib3.util.connection, "_orig_create_connection"):
        urllib3.util.connection._orig_create_connection = urllib3.util.connection.create_connection

    urllib3.util.connection.create_connection = patched_create_connection

    original_tcp_connector_init = TCPConnector.__init__

    def patched_tcp_connector_init(self, *args, **kwargs):
        if 'local_addr' not in kwargs:
            kwargs['local_addr'] = (ip, 0)
        original_tcp_connector_init(self, *args, **kwargs)

    TCPConnector.__init__ = patched_tcp_connector_init

    def is_site_accessible(url):
        """Check if the site is accessible."""
        try:
            response = requests.get(url, timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    async def run_tests():
        async def test_socket_patch(expected_ip):
            try:
                test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_sock.bind((expected_ip, 0))
                test_sock.connect(("api.ipify.org", 80))
                local_addr = test_sock.getsockname()
                test_sock.close()
                return f"Socket: {'Success' if local_addr[0] == expected_ip else 'Failed'}"
            except Exception:
                return "Socket: Failed"

        async def test_urllib3_patch(expected_ip):
            try:
                response = requests.get("https://api.ipify.org?format=json", timeout=5)
                external_ip = response.json().get('ip')
                return f"urllib3: {'Success' if external_ip == expected_ip else 'Failed'}"
            except Exception:
                return "urllib3: Failed"

        async def test_aiohttp_patch(expected_ip):
            try:
                connector = TCPConnector()
                async with ClientSession(connector=connector) as session:
                    async with session.get("https://api.ipify.org?format=json") as response:
                        external_ip = (await response.json()).get('ip')
                        return f"aiohttp: {'Success' if external_ip == expected_ip else 'Failed'}"
            except Exception:
                return "aiohttp: Failed"

        async def test_binance_async_client(expected_ip):
            try:
                from binance import AsyncClient
                client = await AsyncClient.create()
                try:
                    response = await client.session.get("https://api.ipify.org?format=json")
                    external_ip = (await response.json()).get('ip')
                    return f"Binance: {'Success' if external_ip == expected_ip else 'Failed'}"
                finally:
                    await client.close_connection()
            except Exception:
                return "Binance: Failed"

        logger.info(f"Patching to IP: {ip}")
        results = await asyncio.gather(
            test_socket_patch(ip),
            test_urllib3_patch(ip),
            test_aiohttp_patch(ip),
            test_binance_async_client(ip)
        )

        if any("Failed" in result for result in results):
            logger.critical(" | ".join(results))
            sys.exit(1)
        else:
            logger.info(" | ".join(results))

    # Check if api.ipify.org is accessible
    if is_site_accessible("https://api.ipify.org?format=json"):
        asyncio.run(run_tests())
    else:
        logger.critical("api.ipify.org is not accessible. Skipping tests.")