from enum import StrEnum
import argparse
import dotenv
import datetime
from typing import Dict, Optional
import json
from pathlib import Path
import discord
import os
from rich.logging import RichHandler
from logging import getLogger, INFO, DEBUG, Formatter
from logging.handlers import RotatingFileHandler

dotenv.load_dotenv()

# Configure the root logger
logger = getLogger()
logger.setLevel(DEBUG)

# Create handlers
rich_handler = RichHandler()
file_handler = RotatingFileHandler("app.log", maxBytes=5 * 1024 * 1024, backupCount=6)

# Set logging level for handlers
rich_handler.setLevel(DEBUG)
file_handler.setLevel(INFO)

# Create a logging format
formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(rich_handler)
logger.addHandler(file_handler)


print("discord token: " + len(os.environ["DISCORD_TOKEN"]) * "#")

GUILD_ID = int(os.environ["GUILD_ID"])
SAPPHIRE_ID = 678344927997853742


class Events(StrEnum):
    MESSAGE = "message"
    RAW_REACTION_ADD = "raw_reaction_add"
    REACTION_ADD = "reaction_add"
    MEMBER_JOIN = "member_join"
    MEMBER_REMOVE = "member_remove"
    INVITE_CREATE = "invite_create"


class JsonlDataWriter:
    def __init__(self, outfile: Path):
        self.outfile = outfile
        if outfile.is_dir():
            logger.error("cannot log to %s: is directory", outfile)
            raise ValueError("cannot log to %s: is directory")

        if not outfile.exists():
            outfile.touch()

    def log(self, event_name: Events, data: Optional[Dict] = None):
        server_ts = datetime.datetime.now().timestamp()
        with open(self.outfile, "a", encoding="utf-8") as outfile:
            logged_data = {"timestamp": server_ts, "event": event_name}
            if data:
                logged_data["data"] = data
            json.dump(logged_data, outfile)
            outfile.write("\n")


class StatCollectorClient(discord.Client):
    def __init__(self, guild_id: int, data_writer: JsonlDataWriter, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.guild_id = guild_id
        self.data_writer = data_writer

    async def on_message(self, message: discord.Message):
        if message.author.id == SAPPHIRE_ID:
            return

        # just note the channel
        logger.debug("received message from user: %s", message.author.id)
        self.data_writer.log(
            Events.MESSAGE,
            data={
                "author_id": message.author.id,
                "author_name": message.author.name,
                "channel_id": message.channel.id,
                "channel_name": message.channel.name,
                "current_member_count": self.__get_current_member_count(),
            },
        )

    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        logger.debug("received a reaction add: %s", payload.emoji.name)
        channel = self.get_channel(payload.channel_id)
        self.data_writer.log(
            Events.REACTION_ADD,
            data={
                "channel_id": channel.id,
                "channel_name": channel.name,
                "emoji": payload.emoji.name,
                "member_name": payload.member.name,
                "member_id": payload.member.id,
                "current_member_count": self.__get_current_member_count(),
            },
        )

    async def on_member_join(self, member: discord.Member):
        logger.debug("new member joined: %s", member.id)
        self.data_writer.log(
            Events.MEMBER_JOIN,
            data={
                "member_id": member.id,
                "current_member_count": self.__get_current_member_count(),
            },
        )

    async def on_raw_member_remove(self, payload: discord.RawMemberRemoveEvent):
        logger.debug("member removed: %s", payload.user.id)
        self.data_writer.log(
            Events.MEMBER_REMOVE,
            data={
                "member_id": payload.user.id,
                "current_member_count": self.__get_current_member_count(),
            },
        )

    async def on_invite_create(self, invite: discord.Invite):
        logger.debug("invite created")
        self.data_writer.log(
            Events.INVITE_CREATE,
            data={
                "invite_id": invite.id,
                "inviter_id": invite.inviter.id,
                "inviter_name": invite.inviter.name,
                "current_member_count": self.__get_current_member_count(),
            },
        )

    def __get_current_member_count(self) -> int:
        guild = self.get_guild(self.guild_id)
        if not guild.member_count:
            logger.error("member count is not available!")
            raise RuntimeError("member count is not available!")
        return guild.member_count

    async def on_ready(self):
        logger.info("application is ready")


if __name__ == "__main__":
    default_outfile = Path(__file__).resolve().parent / "data.jsonl"
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-log",
        default=default_outfile,
        help="Path to the file which will write out the data file",
        type=Path,
    )
    args = parser.parse_args()
    writer = JsonlDataWriter(args.output_log)
    client = StatCollectorClient(GUILD_ID, writer, intents=discord.Intents.all())
    client.run(os.environ["DISCORD_TOKEN"])
