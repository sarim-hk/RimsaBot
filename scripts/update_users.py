import asyncio
import json

import discord

from utils.database import Database


async def main() -> None:
    with open("config.json", encoding="utf-8") as file:
        config = json.load(file)

    database = Database(config)
    client = discord.Client(intents=discord.Intents.none())

    try:
        cursor = database.connection.cursor(dictionary=True)
        try:
            cursor.execute("""
                SELECT UserID
                FROM Users

                UNION

                SELECT ReactorID AS UserID
                FROM Reactions

                UNION

                SELECT AuthorID AS UserID
                FROM Reactions

                ORDER BY UserID
            """)
            users = cursor.fetchall()
        finally:
            cursor.close()

        print(f"Updating {len(users)} users...")

        await client.login(config["DISCORD_TOKEN"])

        updated = 0
        skipped = 0

        for row in users:
            user_id = int(row["UserID"])

            try:
                user = await client.fetch_user(user_id)
                username = user.global_name or user.name
                avatar_url = user.display_avatar.url

                database.insert_user(user.id, username, avatar_url)
                updated += 1
                print(f"Updated {user_id}: {username}")

            except discord.NotFound:
                skipped += 1
                print(f"Skipped {user_id}: user no longer exists")
            except discord.HTTPException as error:
                skipped += 1
                print(f"Skipped {user_id}: Discord request failed ({error})")

        print(f"Finished: {updated} updated, {skipped} skipped.")

    finally:
        database.connection.close()
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
