"""
brother_ql_print — Home Assistant custom integration.

Provides:
  • brother_ql_print.print_label service
      template (str)  — absolute path to .lbx file
      quantity (int)  — labels to print (1-500)
      fields   (dict) — {FieldName: text_value} merge fields

Rendering: bundled printing.py (LBX → Pillow → brother_ql).
Works on Linux/HA OS — no Windows COM / b-pac dependency.
"""
from __future__ import annotations

import logging
import os

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv

_LOGGER = logging.getLogger(__name__)
DOMAIN   = "brother_ql_print"
PLATFORMS: list[str] = []

PRINT_SCHEMA = vol.Schema({
    vol.Required("template"): cv.string,
    vol.Required("quantity"): vol.All(vol.Coerce(int), vol.Range(min=1, max=500)),
    vol.Required("fields"):   dict,
})


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    # Pull config; merge options if reconfigured
    config     = {**entry.data, **entry.options}
    label_type = config.get("label", "62")
    model      = config.get("model", "QL-700")

    hass.data[DOMAIN][entry.entry_id] = {"label": label_type, "model": model}

    async def handle_print(call: ServiceCall):
        template = call.data["template"]
        qty      = int(call.data["quantity"])
        fields   = dict(call.data["fields"])

        # Validate the template path exists
        if not os.path.isfile(template):
            _LOGGER.error(
                "brother_ql_print: template file not found: %s", template
            )
            return

        _LOGGER.info(
            "brother_ql_print: printing %d × '%s'  fields=%s",
            qty, os.path.basename(template), list(fields.keys()),
        )

        try:
            await hass.async_add_executor_job(
                _do_print, template, fields, qty, label_type, model
            )
            _LOGGER.info("brother_ql_print: print job sent successfully")
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error("brother_ql_print: print failed — %s", exc, exc_info=True)

    hass.services.async_register(
        DOMAIN,
        "print_label",
        handle_print,
        schema=PRINT_SCHEMA,
    )

    return True


def _do_print(template: str, fields: dict, quantity: int, label: str, model: str):
    """
    Blocking function — runs in executor thread.
    Renders the .lbx, scales for the target tape, sends to USB printer.
    """
    from .printing import render_lbx_to_image, prepare_image, print_image

    img = render_lbx_to_image(template, fields)
    img = prepare_image(img, label)
    print_image(img, label=label, quantity=quantity, preview=False, model_override=model)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.services.async_remove(DOMAIN, "print_label")
    hass.data[DOMAIN].pop(entry.entry_id, None)
    return True
