import os
import re
import discord
from discord.ext import commands
from userscrape.cfg import DiscordCfg, UserCfg
import asyncio
from typing import Dict
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath, WindowsPath


class DiscordBot():
    def __init__(self, cfg: DiscordCfg):
        self.token = cfg.bot_token
        self.channel_id = cfg.channel_id
        self.bot = commands.Bot(command_prefix='!')
        self.codes: Dict[str, str] = dict()

        @self.bot.event
        async def on_ready():
            print(f'Decord bot ({self.bot.user.name}) has connected')

        @self.bot.command(name='code', help='Parses the validation code and passes it to the scraper')
        async def code(ctx: commands.Context, email: str = None, code: str = None):
            if code is None:
                await ctx.channel.send('Please attach the validation code to the command')
                return
            if email is None:
                await ctx.channel.send('Please provider a email')
                return
            if email not in self.codes:
                return ctx.channel.send(f'Code for {email} has not been requested')

            # codePattern = re.compile('^\d{6}$')
            # if not codePattern.search(str(code)):
            #     await ctx.channel.send('Please provide the validation code that is 6 digits')
            #     return

            self.codes[email] = code
            await ctx.channel.send(f'Thanks for providing the validation code for {email}. Scraping resumes!')

    async def request_code(self, user: UserCfg, msg: str = None, file: PurePath = None):
        await self.bot.wait_until_ready()
        self.codes[user.email] = None
        channel = self.channel()

        msg = msg or "please provide the validation code"
        userMention = f'<@{user.notify_discord_user_id}>' if user.notify_discord_user_id != None else 'someone'
        dFile = None if file == None else discord.File(file.as_posix())
        await channel.send(f'{userMention} - {msg}. Response with: !code {user.email} <code>', file=dFile)

        code = None
        while True:
            code = self.codes.get(user.email, None)
            if code != None:
                del self.codes[user.email]
                return code
            await asyncio.sleep(1)
        return code

    def channel(self) -> discord.TextChannel:
        channel = self.bot.get_channel(self.channel_id)
        if(channel == None):
            raise EnvironmentError(f'channel {self.channel_id} not found')
        return channel

    async def msg(self, msg: str, localFile: PurePath = None):
        channel = self.channel()
        if(localFile != None):
            await channel.send(msg, file=discord.File(localFile.as_posix()))
        else:
            await channel.send(msg)

    async def start_in_backround(self):
        asyncio.create_task(self.bot.start(self.token))  # run as a background task and return once it has been started
        await self.bot.wait_until_ready()

    async def close(self):
        await self.bot.close()
