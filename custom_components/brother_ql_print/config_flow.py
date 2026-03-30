"""config_flow.py — Brother QL Print setup wizard."""
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback

DOMAIN = "brother_ql_print"

# All label sizes supported by brother_ql
LABEL_SIZES = [
    "12", "29", "38", "50", "54", "62", "102",
    "17x54", "17x87", "23x23", "29x42", "29x90",
    "39x90", "39x48", "52x29", "62x29", "62x100",
    "102x51", "102x152", "d12", "d24", "d58",
]

QL_MODELS = [
    "QL-500", "QL-550", "QL-560", "QL-570", "QL-580N",
    "QL-650TD", "QL-700", "QL-710W", "QL-720NW",
    "QL-800", "QL-810W", "QL-820NWB",
    "QL-1050", "QL-1060N", "QL-1100", "QL-1110NWB", "QL-1115NWB",
]


def _schema(defaults: dict) -> vol.Schema:
    return vol.Schema({
        vol.Required("label", default=defaults.get("label", "62")):
            vol.In(LABEL_SIZES),
        vol.Optional("model", default=defaults.get("model", "QL-700")):
            vol.In(QL_MODELS),
    })


class BrotherQLConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        if user_input is not None:
            return self.async_create_entry(
                title=f"Brother {user_input.get('model','QL')} ({user_input.get('label','?')}mm)",
                data=user_input,
            )
        return self.async_show_form(
            step_id="user",
            data_schema=_schema({}),
            description_placeholders={
                "note": "Connect your Brother QL printer via USB before saving."
            },
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return BrotherQLOptionsFlow(config_entry)


class BrotherQLOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, entry):
        self._entry = entry

    async def async_step_init(self, user_input=None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        defaults = {**self._entry.data, **self._entry.options}
        return self.async_show_form(
            step_id="init",
            data_schema=_schema(defaults),
        )
