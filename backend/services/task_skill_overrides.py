"""Internal markers for one-turn Task skill overrides."""

TEMP_SKILLS_GENERATION_KEY = "_ccm_temporary_skills_generation"


def clear_temporary_skills_marker(metadata: dict | None) -> dict:
    """Return Task metadata without the internal one-turn ownership marker."""
    cleaned = dict(metadata or {})
    cleaned.pop(TEMP_SKILLS_GENERATION_KEY, None)
    return cleaned
