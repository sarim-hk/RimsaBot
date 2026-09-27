from mysql.connector import MySQLConnection
from typing import cast
import discord

DEFAULT_MAX_MESSAGE_AGE: int = 21600    # 6 hours

class Database:
    def __init__(self, cfg: dict[str, str]):
        self.connection = MySQLConnection(
            host=cfg["MYSQL_SERVER"],
            database=cfg["MYSQL_DB"],
            user=cfg["MYSQL_USERNAME"],
            password=cfg["MYSQL_PASSWORD"]
            )

        if not self.connection.is_connected():
            raise ConnectionError("Could not connect to the database.")

        self.create_tables()

    def create_tables(self) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Reactions (
                ReactionID int UNSIGNED AUTO_INCREMENT PRIMARY KEY NOT NULL,
                EmojiID bigint UNSIGNED NULL,
                EmojiName VARCHAR(32) NOT NULL,
                ReactorID bigint UNSIGNED NOT NULL,
                AuthorID bigint UNSIGNED NOT NULL,
                GuildID bigint UNSIGNED NOT NULL,
                ChannelID bigint UNSIGNED NOT NULL,
                MessageID bigint UNSIGNED NOT NULL,
                Timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS GuildConfig (
                GuildID bigint UNSIGNED PRIMARY KEY NOT NULL,
                MaxMessageAge int UNSIGNED NOT NULL DEFAULT {DEFAULT_MAX_MESSAGE_AGE}
            )""")

            self.connection.commit() 

        except Exception:
            self.connection.rollback()
            raise

        finally:
            cursor.close()

    def get_max_message_age(self, guild_id: int) -> int:
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                SELECT MaxMessageAge
                FROM GuildConfig
                WHERE GuildID = %s
            """, (guild_id,))

            row = cursor.fetchone()
            if row is None:
                return DEFAULT_MAX_MESSAGE_AGE
            
            return cast(int, row[0])

        except Exception:
            self.connection.rollback()
            raise

        finally:
            cursor.close()

    def set_max_message_age(self, age: int, guild_id: int) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO GuildConfig (GuildID, MaxMessageAge)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE MaxMessageAge = %s
            """, (guild_id, age, age))
            self.connection.commit()

        except Exception:
            self.connection.rollback()
            raise

        finally:
            cursor.close()

    def insert_guild_config(self, guild_id: int) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT IGNORE INTO GuildConfig (GuildID)
                VALUES (%s)
            ;""",
            (guild_id,))
            self.connection.commit()

        except Exception:
            self.connection.rollback()
            raise

        finally:
            cursor.close()        

    def insert_reaction(self, payload: discord.RawReactionActionEvent) -> None:
        emoji_id = None
        if payload.emoji.is_custom_emoji():
            emoji_id = payload.emoji.id

        emoji_name = payload.emoji.name
        reactor_id = payload.user_id
        author_id = payload.message_author_id
        guild_id = payload.guild_id
        channel_id = payload.channel_id
        message_id = payload.message_id

        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO Reactions
                (EmojiID, EmojiName, ReactorID, AuthorID, GuildID, ChannelID, MessageID)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ;""",
            (emoji_id, emoji_name, reactor_id, author_id, guild_id, channel_id, message_id))
            self.connection.commit()

        except Exception:
            self.connection.rollback()
            raise

        finally:
            cursor.close()

    def remove_reaction(self, payload: discord.RawReactionActionEvent) -> None:
        emoji_id = None
        if payload.emoji.is_custom_emoji():
            emoji_id = payload.emoji.id

        emoji_name = payload.emoji.name
        reactor_id = payload.user_id
        message_id = payload.message_id
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                DELETE FROM Reactions
                WHERE (EmojiID = %s OR (EmojiID IS NULL AND %s IS NULL))
                AND EmojiName = %s
                AND ReactorID = %s
                AND MessageID = %s
            ;""",
            (emoji_id, emoji_id, emoji_name, reactor_id, message_id))
            self.connection.commit()

        except Exception:
            self.connection.rollback()
            raise

        finally:
            cursor.close()        

    def close(self):
        if self.connection.is_connected():
            self.connection.close()
