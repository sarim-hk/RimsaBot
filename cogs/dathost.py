
import discord
from discord import app_commands
from discord.ext import commands
from typing import Any
from bot import RimsaBot
from utils.dathost import DatHostAPIWrapper

class DatHost(commands.Cog):
    def __init__(self, bot: RimsaBot):
        self.bot = bot
        self.cfg = bot.cfg
        self.APIWrapper = DatHostAPIWrapper(bot.cfg)

    @app_commands.command(name="start_server", description="Start (or restart) the DatHost server.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def start_server(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        
        startserver_responsecode: int = await self.APIWrapper.async_startserver()
        if startserver_responsecode != 200:
            await interaction.followup.send(f"Couldn't start server! {startserver_responsecode}")
            raise RuntimeError(f"Couldn't start server! {startserver_responsecode}")
        
        result: dict[str, Any] = await self.APIWrapper.async_getserver()
        server_ip = result.get("custom_domain")
        port = result.get("ports", {}).get("game")
        
        if not server_ip or not port:
            await interaction.followup.send(f"Couldn't find server ip or port! {server_ip} {port}")
            raise RuntimeError(f"Couldn't find server ip or port! {server_ip} {port}")
        
        await interaction.followup.send(f"Server started.\n`connect {server_ip}:{port}`")

    @app_commands.command(name="stop_server", description="Stop the DatHost server.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def stop_server(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        
        stop_server_responsecode: int = await self.APIWrapper.async_stopserver()
        if stop_server_responsecode != 200:
            await interaction.followup.send(f"Couldn't stop server! {stop_server_responsecode}")
            raise RuntimeError(f"Couldn't stop server! {stop_server_responsecode}")
        else:
            await interaction.followup.send(f"Server stopped.")

    @app_commands.command(name="check_server", description="Check the status of the DatHost server.")
    async def check_server(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
                
        result: dict[str, Any] = await self.APIWrapper.async_getserver()

        server_on = result.get("on")
        if not server_on:
            await interaction.followup.send(f"The server is offline.")
            return
        
        server_status = result.get("status", {})
        if not server_status:
            await interaction.followup.send(f"Status doesn't exist but server is on! {server_status}")
            raise RuntimeError(f"Status doesn't exist but server is on! {server_status}")
        
        map_name: str = server_status[1].get("value")
        players_online: str = server_status[2].get("value")[0]
        await interaction.followup.send(f"Server status:\n`{players_online} players online on {map_name}`")
        
async def setup(bot: RimsaBot):
    await bot.add_cog(DatHost(bot))
