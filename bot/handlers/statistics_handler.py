from aiogram import Router
from aiogram.filters import CommandStart, Command
from aiogram.types import Message

router_commands = Router()


@router_commands.message(CommandStart())
async def start_command(message: Message) -> None:
    await message.answer(text=f"Assalomu aleykum {message.from_user.full_name} Boss !")


@router_commands.message(Command(commands=['lid_count']))
async def lid_count_command(message: Message) -> None:
    pass

@router_commands.message(Command(commands=['statistics']))
async def statistics_command(message: Message) -> None:
    pass


@router_commands.message(Command(commands=['help']))
async def help_command(message: Message) -> None:
    pass
