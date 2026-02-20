import aiohttp
from aiohttp import BasicAuth
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands
from bot import RimsaBot

class DatHost(commands.Cog):
    def __init__(self, bot: RimsaBot):
        self.bot = bot
        self.cfg = bot.cfg
        self.APIWrapper = DatHostAPIWrapper(bot.cfg)

    @app_commands.command(name="start_server", description="Start the server.")
    async def start_server(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        
        startserver_responsecode: int = await self.APIWrapper.async_startserver()
        if startserver_responsecode != 200:
            raise RuntimeError(f"Couldn't start server! {startserver_responsecode}")
        
        result: dict[str, Any] = await self.APIWrapper.async_getserver()
        server_ip = result.get("custom_domain")
        port = result.get("ports", {}).get("game")
        
        if not server_ip or not port:
            raise RuntimeError(f"Couldn't find server ip or port! {server_ip} {port}")

        await interaction.followup.send(f"Server started.\n`connect {server_ip}:{port}`")

class DatHostAPIWrapper:
    def __init__(self, cfg: dict[str, str]):
        self.server_id: str = cfg["DATHOST_SERVER_ID"]
        self.username: str = cfg["DATHOST_USERNAME"]
        self.password: str = cfg["DATHOST_PASSWORD"]

    async def async_startserver(self) -> int:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://dathost.net/api/0.1/game-servers/{self.server_id}/start",
                auth = BasicAuth(self.username, self.password)
            ) as response:
                return response.status

    async def async_getserver(self) -> dict[str, Any]:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://dathost.net/api/0.1/game-servers/{self.server_id}",
                auth = BasicAuth(self.username, self.password)
            ) as response:
                return await response.json()

async def setup(bot: RimsaBot):
    await bot.add_cog(DatHost(bot))
