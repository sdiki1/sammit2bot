from aiogram.fsm.state import State, StatesGroup


class ConsentFlow(StatesGroup):
    waiting_agreement = State()


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
    waiting_partner_email = State()
    waiting_partner_booth = State()
    waiting_name = State()
    waiting_phone = State()
    waiting_email = State()
    waiting_company = State()
    waiting_consent = State()


class NoCodeRegistrationFlow(StatesGroup):
    waiting_contact = State()
    waiting_email = State()
    waiting_full_name = State()
    waiting_company = State()
    waiting_inn = State()
    waiting_booth = State()
    waiting_consent = State()


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
    waiting_email = State()


class ApplicationLinkFlow(StatesGroup):
    waiting_start = State()


class PublicContactFlow(StatesGroup):
    waiting_contact = State()


class PartnerStartFlow(StatesGroup):
    waiting_choice = State()
    waiting_apply_choice = State()
    waiting_map_confirm = State()


class PartnerApplicationFlow(StatesGroup):
    waiting_full_name = State()
    waiting_phone = State()
    waiting_email = State()
    waiting_company = State()
    waiting_inn = State()
    waiting_comment = State()
    waiting_consent = State()


class ExpertStartFlow(StatesGroup):
    waiting_choice = State()


class ExpertApplicationFlow(StatesGroup):
    waiting_full_name = State()
    waiting_phone = State()
    waiting_email = State()
    waiting_company = State()
    waiting_format = State()
    waiting_format_other = State()
    waiting_topic = State()
    waiting_description = State()
    waiting_audience = State()
    waiting_experience = State()
    waiting_links = State()
    waiting_consent = State()


class InfluencerStartFlow(StatesGroup):
    waiting_choice = State()


class InfluencerApplicationFlow(StatesGroup):
    waiting_full_name = State()
    waiting_phone = State()
    waiting_email = State()
    waiting_social = State()
    waiting_platforms = State()
    waiting_topic = State()
    waiting_audience = State()
    waiting_geo = State()
    waiting_collab = State()
    waiting_formats = State()
    waiting_terms = State()
    waiting_experience = State()
    waiting_comment = State()
    waiting_consent = State()
