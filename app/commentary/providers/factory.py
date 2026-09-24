from __future__ import annotations

import logging
import os
from configparser import ConfigParser

from app.commentary.providers.base import CommentaryProvider
from app.commentary.providers.fallback import FallbackCommentaryProvider
from app.commentary.providers.llm import LLMCommentaryProvider
from app.commentary.providers.noop import NoopCommentaryProvider
from app.commentary.providers.template import TemplateCommentaryProvider

_LLM_NAMES = ("llm", "openai", "openai-compatible")
_DEFAULT_BASE_URL = "https://api.openai.com/v1"


def _create_llm_provider(config: ConfigParser, logger: logging.Logger | None) -> CommentaryProvider:
    api_key_env = config.get("commentary", "apiKeyEnv", fallback="OPENAI_API_KEY")
    api_key = os.getenv(api_key_env, "")
    base_url = config.get("commentary", "baseUrl", fallback=_DEFAULT_BASE_URL)

    # A remote OpenAI endpoint without a key is almost certainly a misconfig;
    # degrade to templates instead of failing every call.
    if not api_key and "openai.com" in base_url:
        if logger is not None:
            logger.warning(
                "Commentary provider 'llm' selected but env var %s is unset; using templates.",
                api_key_env,
            )
        return TemplateCommentaryProvider()

    llm = LLMCommentaryProvider(
        api_key=api_key,
        base_url=base_url,
        model=config.get("commentary", "model", fallback="gpt-4.1-nano"),
        timeout=config.getfloat("commentary", "timeoutSeconds", fallback=8.0),
        temperature=config.getfloat("commentary", "temperature", fallback=0.7),
        max_tokens=config.getint("commentary", "maxTokens", fallback=60),
        logger=logger,
    )

    if config.getboolean("commentary", "fallbackToTemplate", fallback=True):
        return FallbackCommentaryProvider(llm, TemplateCommentaryProvider())
    return llm


def create_commentary_provider(
    config: ConfigParser,
    logger: logging.Logger | None = None,
) -> CommentaryProvider:
    """Build the configured commentary provider, defaulting to templates."""
    provider = config.get("commentary", "provider", fallback="template").strip().lower()

    if provider == "noop":
        return NoopCommentaryProvider()
    if provider in _LLM_NAMES:
        return _create_llm_provider(config, logger)

    if provider != "template" and logger is not None:
        logger.warning("Unknown commentary provider '%s'; falling back to 'template'.", provider)

    return TemplateCommentaryProvider()
