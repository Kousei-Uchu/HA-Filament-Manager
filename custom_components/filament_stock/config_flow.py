"""config_flow.py — Setup wizard for Filament Stock Manager."""
import os
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback

DOMAIN = "filament_stock"


class FilamentStockConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        errors = {}
        if user_input is not None:
            for k in ("tag_label_path","sticker_label_path","egg_label_path","small_sticker_label_path"):
                p = user_input.get(k, "")
                if p and not os.path.isfile(p):
                    errors[k] = "file_not_found"
            if not errors:
                return self.async_create_entry(title="Filament Stock", data=user_input)

        return self.async_show_form(step_id="user", errors=errors, data_schema=vol.Schema({
            vol.Required("tag_label_path", description="Absolute path to tag_label.lbx"): str,
            vol.Required("sticker_label_path", description="Absolute path to mini_label.lbx"): str,
            vol.Required("egg_label_path", description="Absolute path to egg_label.lbx"): str,
            vol.Required("small_sticker_label_path", description="Absolute path to mini_priceless_label.lbx"): str,
            vol.Optional("shopify_url", default=""): str,
            vol.Optional("shopify_token", default=""): str,
            vol.Optional("square_token", default=""): str,
            vol.Optional("square_location_id", default=""): str,
            vol.Optional("price_refresh_hour", default=3): vol.All(int, vol.Range(min=0, max=23)),
        }))

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return FilamentStockOptionsFlow(config_entry)


class FilamentStockOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, entry):
        self._entry = entry

    async def async_step_init(self, user_input=None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        cur = {**self._entry.data, **self._entry.options}
        return self.async_show_form(step_id="init", data_schema=vol.Schema({
            vol.Required("tag_label_path",          default=cur.get("tag_label_path","")): str,
            vol.Required("sticker_label_path",      default=cur.get("sticker_label_path","")): str,
            vol.Required("egg_label_path",          default=cur.get("egg_label_path","")): str,
            vol.Required("small_sticker_label_path",default=cur.get("small_sticker_label_path","")): str,
            vol.Optional("shopify_url",             default=cur.get("shopify_url","")): str,
            vol.Optional("shopify_token",           default=cur.get("shopify_token","")): str,
            vol.Optional("square_token",            default=cur.get("square_token","")): str,
            vol.Optional("square_location_id",      default=cur.get("square_location_id","")): str,
            vol.Optional("price_refresh_hour",      default=cur.get("price_refresh_hour",3)): vol.All(int,vol.Range(min=0,max=23)),
        }))
