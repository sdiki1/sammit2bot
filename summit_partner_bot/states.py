from aiogram.fsm.state import State, StatesGroup


class SupportFlow(StatesGroup):
    waiting_for_question = State()

