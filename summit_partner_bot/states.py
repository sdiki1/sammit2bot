from aiogram.fsm.state import State, StatesGroup


class SupportFlow(StatesGroup):
    waiting_for_question = State()


class FeedbackFlow(StatesGroup):
    waiting_for_feedback = State()


class AccessRequestFlow(StatesGroup):
    waiting_access_code = State()
    waiting_partner_inn = State()
    waiting_partner_company = State()
    waiting_partner_contact_name = State()
    waiting_partner_phone = State()
    waiting_name = State()
    waiting_phone = State()


class NavigationFlow(StatesGroup):
    waiting_category_choice = State()
    waiting_subcategory_choice = State()
    waiting_link_choice = State()
    waiting_material_choice = State()


class BoothBookingFlow(StatesGroup):
    waiting_booth = State()
    waiting_company = State()
    waiting_contact_name = State()
    waiting_phone = State()
    waiting_comment = State()
