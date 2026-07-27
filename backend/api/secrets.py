from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.api.deps import require_admin
from backend.models.secret import Secret
from backend.schemas.secret import SecretCreate, SecretUpdate, SecretResponse

router = APIRouter(
    prefix="/api/secrets",
    tags=["secrets"],
    dependencies=[Depends(require_admin)],
)


@router.get("", response_model=list[SecretResponse])
async def list_secrets(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Secret).order_by(Secret.name))
    return list(result.scalars().all())


@router.post("", response_model=SecretResponse, status_code=201)
async def create_secret(body: SecretCreate, db: AsyncSession = Depends(get_db)):
    secret = Secret(name=body.name, content=body.content)
    db.add(secret)
    await db.commit()
    await db.refresh(secret)
    return secret


@router.get("/{secret_id}", response_model=SecretResponse)
async def get_secret(secret_id: int, db: AsyncSession = Depends(get_db)):
    secret = await db.get(Secret, secret_id)
    if not secret:
        raise HTTPException(404, "Secret not found")
    return secret


@router.put("/{secret_id}", response_model=SecretResponse)
async def update_secret(secret_id: int, body: SecretUpdate, db: AsyncSession = Depends(get_db)):
    secret = await db.get(Secret, secret_id)
    if not secret:
        raise HTTPException(404, "Secret not found")
    data = body.model_dump(exclude_unset=True)
    for key, val in data.items():
        setattr(secret, key, val)
    await db.commit()
    await db.refresh(secret)
    return secret


@router.delete("/{secret_id}")
async def delete_secret(secret_id: int, db: AsyncSession = Depends(get_db)):
    secret = await db.get(Secret, secret_id)
    if not secret:
        raise HTTPException(404, "Secret not found")
    await db.delete(secret)
    await db.commit()
    return {"ok": True}
