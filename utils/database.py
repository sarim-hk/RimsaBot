from mysql.connector import MySQLConnection
import discord


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

    def create_tables(self):
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
            self.connection.commit() 

        except Exception:
            self.connection.rollback()
            raise

        finally:
            cursor.close()

    def insert_reaction(self, payload: discord.RawReactionActionEvent):
        emoji_id = None
        if payload.emoji.is_custom_emoji():
            emoji_id = payload.emoji.id

        emoji_name = payload.emoji.name
        reactor_id = payload.user_id
        author_id = payload.message_author_id
        guild_id = payload.guild_id
        channel_id = payload.channel_id
        message_id = payload.message_id

        if author_id is None or guild_id is None:
            return

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

    def remove_reaction(self, payload: discord.RawReactionActionEvent):
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
