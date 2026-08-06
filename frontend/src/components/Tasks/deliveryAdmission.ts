import type { DeliveryRunCreate } from '../../api/client';

type DeliveryAdmissionDraft = Omit<DeliveryRunCreate, 'idempotency_key'>;

interface StoredAdmission {
  version: 1;
  fingerprint: string;
  idempotencyKey: string;
}

const STORAGE_PREFIX = 'cc_pending_delivery_admission_v1';
const inMemoryAdmissions = new Map<string, StoredAdmission>();

function newIdempotencyKey(): string {
  const nonce = globalThis.crypto?.randomUUID?.()
    || `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `delivery-${nonce}`.slice(0, 128);
}

function fingerprint(draft: DeliveryAdmissionDraft): string {
  const entries = Object.entries(draft)
    .filter(([, value]) => value !== undefined)
    .sort(([left], [right]) => left.localeCompare(right));
  return JSON.stringify(Object.fromEntries(entries));
}

function storageKey(scope: string): string {
  return `${STORAGE_PREFIX}:${scope}`;
}

function readStored(scope: string): StoredAdmission | null {
  try {
    const raw = globalThis.sessionStorage?.getItem(storageKey(scope));
    if (!raw) return inMemoryAdmissions.get(scope) ?? null;
    const value: unknown = JSON.parse(raw);
    if (
      !value
      || typeof value !== 'object'
      || (value as StoredAdmission).version !== 1
      || typeof (value as StoredAdmission).fingerprint !== 'string'
      || typeof (value as StoredAdmission).idempotencyKey !== 'string'
      || !(value as StoredAdmission).idempotencyKey
      || (value as StoredAdmission).idempotencyKey.length > 128
    ) {
      return inMemoryAdmissions.get(scope) ?? null;
    }
    const admission = value as StoredAdmission;
    inMemoryAdmissions.set(scope, admission);
    return admission;
  } catch {
    return inMemoryAdmissions.get(scope) ?? null;
  }
}

function writeStored(scope: string, value: StoredAdmission): void {
  inMemoryAdmissions.set(scope, value);
  try {
    globalThis.sessionStorage?.setItem(storageKey(scope), JSON.stringify(value));
  } catch {
    // Storage can be unavailable in privacy-restricted browsers. The backend
    // remains idempotent for retries made before the component is reloaded.
  }
}

/**
 * Bind one exact Delivery admission request to a browser-tab durable key.
 *
 * sessionStorage survives refresh/response loss but remains isolated between
 * tabs. A changed request receives a fresh key, avoiding an idempotency
 * conflict with the server's immutable request hash.
 */
export function prepareDeliveryAdmission(
  scope: string,
  draft: DeliveryAdmissionDraft,
): DeliveryRunCreate {
  const requestFingerprint = fingerprint(draft);
  const stored = readStored(scope);
  const idempotencyKey = stored?.fingerprint === requestFingerprint
    ? stored.idempotencyKey
    : newIdempotencyKey();
  writeStored(scope, {
    version: 1,
    fingerprint: requestFingerprint,
    idempotencyKey,
  });
  return { ...draft, idempotency_key: idempotencyKey };
}

/** Clear only the exact acknowledged request, preserving newer tab state. */
export function acknowledgeDeliveryAdmission(
  scope: string,
  request: DeliveryRunCreate,
): void {
  const { idempotency_key: idempotencyKey, ...draft } = request;
  const stored = readStored(scope);
  if (
    stored?.idempotencyKey !== idempotencyKey
    || stored.fingerprint !== fingerprint(draft)
  ) {
    return;
  }
  try {
    globalThis.sessionStorage?.removeItem(storageKey(scope));
  } catch {
    // A stale key is safe: a changed request rotates it, and an exact request
    // is deliberately replayed to the already-created Run.
  }
  inMemoryAdmissions.delete(scope);
}
