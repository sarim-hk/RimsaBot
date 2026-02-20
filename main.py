import logging
import json
from bot import RimsaBot

if __name__ == "__main__":
    with open("config.json", "r") as f:
        cfg = json.load(f)
    
    # might have to update this logic depending on what the config holds in future
    if any(not v for v in cfg.values()):
        raise RuntimeError("Missing or invalid config values!")

    handler = logging.FileHandler(
        filename="discord.log",
        encoding="utf-8",
        mode="w"
    )

    bot = RimsaBot(cfg)
    bot.run(
        cfg["DISCORD_TOKEN"],
        log_handler=handler,
        log_level=logging.DEBUG
    )
