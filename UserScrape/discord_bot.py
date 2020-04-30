# bot.py
import os
import re

from dotenv import load_dotenv
from discord.ext import commands


class DiscordBot:

    def __init__(self):
        load_dotenv()
        self.TOKEN = os.getenv('DISCORD_TOKEN')
        self.bot = commands.Bot(command_prefix='!')
        self.code = None

        @self.bot.event
        async def on_ready():
            print(f'{self.bot.user.name} has connected to Discord!')

        @self.bot.command(name='code', help='Parses the validation code and passes it to the scraper')
        async def get_code(ctx, code=None):
            if code is None:
                response = "Please attach the validation code to the command"
                await ctx.channel.send(response)
            pattern = re.compile("^\d{6}$")
            if pattern.search(str(code)):
                response = "Thanks for providing the validation code. Scraping resumes."
                self.code = code
                await ctx.channel.send(response)
                await self.bot.close()
            else:
                response = "Please provide the validation code in the following form: !code <6-digit-code>"
                await ctx.channel.send(response)

        self.bot.run(self.TOKEN)

    def get_code(self):
        return self.code

