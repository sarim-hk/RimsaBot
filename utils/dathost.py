import aiohttp
from aiohttp import BasicAuth
from typing import Any

class DatHostAPIWrapper:
    def __init__(self, cfg: dict[str, str]):
        self.server_id: str = cfg["DATHOST_SERVER_ID"]
        self.username: str = cfg["DATHOST_USERNAME"]
        self.password: str = cfg["DATHOST_PASSWORD"]

    async def start_server(self) -> int:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://dathost.net/api/0.1/game-servers/{self.server_id}/start",
                auth = BasicAuth(self.username, self.password)
            ) as response:
                return response.status

    async def stop_server(self) -> int:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://dathost.net/api/0.1/game-servers/{self.server_id}/stop",
                auth = BasicAuth(self.username, self.password)
            ) as response:
                return response.status

    async def get_server_status(self) -> dict[str, Any]:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://dathost.net/api/0.1/game-servers/{self.server_id}",
                auth = BasicAuth(self.username, self.password)
            ) as response:
                return await response.json()
