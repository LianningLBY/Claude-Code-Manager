"""disable the legacy source-seeded administrator

Revision ID: e4c9f2a71b03
Revises: d8f0a1b2c3d4
Create Date: 2026-07-27

"""

from typing import Sequence, Union
import secrets

from alembic import op
import bcrypt
import sqlalchemy as sa


revision: str = "e4c9f2a71b03"
down_revision: Union[str, None] = "d8f0a1b2c3d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# This value was shipped by older releases and is already public.  It is kept
# here only as a migration fingerprint: accounts that changed their password
# must not be disabled merely because they retained the historical email.
_LEGACY_DEFAULT_PASSWORD = b"admin123456"


def upgrade() -> None:
    # Older releases created this shared identity with a publicly known
    # password.  Invalidate it during upgrade as well as removing the seeding
    # code, otherwise an existing installation would remain exposed forever.
    # AUTH_TOKEN/no-auth deployments retain their administrator recovery path;
    # when this was the only active user, the next verified registration is
    # promoted by the registration transaction.
    users = sa.table(
        "users",
        sa.column("id", sa.Integer()),
        sa.column("email", sa.String()),
        sa.column("password_hash", sa.String()),
        sa.column("is_active", sa.Boolean()),
    )
    connection = op.get_bind()
    candidates = connection.execute(
        sa.select(users.c.id, users.c.password_hash).where(
            users.c.email == "admin@apexin.ai"
        )
    ).mappings().all()
    for candidate in candidates:
        stored_hash = candidate["password_hash"]
        try:
            still_uses_default = bool(stored_hash) and bcrypt.checkpw(
                _LEGACY_DEFAULT_PASSWORD,
                stored_hash.encode(),
            )
        except (TypeError, ValueError):
            still_uses_default = False
        if not still_uses_default:
            continue

        random_password = secrets.token_urlsafe(48).encode()
        replacement_hash = bcrypt.hashpw(
            random_password,
            bcrypt.gensalt(),
        ).decode()
        # Fence on the observed hash so a concurrent password change cannot be
        # overwritten between inspection and invalidation.
        connection.execute(
            users.update()
            .where(
                users.c.id == candidate["id"],
                users.c.password_hash == stored_hash,
            )
            .values(
                password_hash=replacement_hash,
                is_active=sa.false(),
            )
        )


def downgrade() -> None:
    # Security credential invalidation is intentionally irreversible.
    pass
