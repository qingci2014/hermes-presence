"""Hermes native directory-plugin entrypoint."""
def register(ctx):
    from .hermes_presence.plugin import register as register_presence
    return register_presence(ctx)
