from mysql.connector import Error, MySQLConnection
from functools import wraps
from typing import Any, Callable, cast
import discord

DEFAULT_MAX_MESSAGE_AGE: int = 21600    # 6 hours

def requires_connection(method: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(method)
    def wrapper(self: "Database", *args: Any, **kwargs: Any) -> Any:
        try:
            # ping the db to see if the connection is up, attempt reconnection if not
            self.connection.ping(reconnect=True, attempts=3, delay=1)
        except Error:
            # ping() with reconnect=True might not always work so try explicitly aswell.
            self.connection.reconnect(attempts=3, delay=1)

        if not self.connection.is_connected():
            raise ConnectionError("Could not reconnect to the database.")

        return method(self, *args, **kwargs)

    return wrapper

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

    @requires_connection
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

    @requires_connection
    def get_user_reactions_received(self, user_id: int, guild_id: int) -> list[dict[str, Any]]:
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute("""
                SELECT COUNT(*) AS Total, EmojiID, EmojiName
                FROM Reactions
                WHERE AuthorID = %s AND GuildID = %s
                GROUP BY EmojiID, EmojiName
                ORDER BY Total DESC
            """, (user_id, guild_id))

            rows = cursor.fetchall()
            return cast(list[dict[str, Any]], rows)
            
        except Exception:
            self.connection.rollback()
            raise

        finally:
            cursor.close()        

    @requires_connection
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

    @requires_connection
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

    @requires_connection
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

    @requires_connection
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

    @requires_connection
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
