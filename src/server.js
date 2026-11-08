// ============================================================================
// Webhook Testing & Simulation Laboratory
// A single-file Express + fetch based webhook sender for learning how to
// build robust webhook *receivers*. This process sends HTTP requests to
// receiver endpoints that YOU configure. It does not receive webhooks.
// ============================================================================

import express from 'express';
import crypto from 'node:crypto';

// ----------------------------------------------------------------------------
// 1. CONFIG
// ----------------------------------------------------------------------------

const CONFIG = {
  port: parseInt(process.env.PORT || '3000', 10),
  apiKey: process.env.MANAGEMENT_API_KEY || '',
  defaultTimeoutMs: parseInt(process.env.DEFAULT_TIMEOUT_MS || '10000', 10),
  maxTimeoutMs: parseInt(process.env.MAX_TIMEOUT_MS || '60000', 10),
  maxRetries: parseInt(process.env.MAX_RETRIES_CAP || '10', 10),
  maxConcurrency: parseInt(process.env.MAX_CONCURRENCY_CAP || '25', 10),
  maxStoredEvents: parseInt(process.env.MAX_STORED_EVENTS || '2000', 10),
  maxStoredDeliveries: parseInt(process.env.MAX_STORED_DELIVERIES || '4000', 10),
  maxStoredLifecycles: parseInt(process.env.MAX_STORED_LIFECYCLES || '1000', 10),
  maxPayloadBytes: parseInt(process.env.MAX_PAYLOAD_BYTES || '1048576', 10), // 1MB bound for "large payload" tests
  maxTargets: parseInt(process.env.MAX_TARGETS || '20', 10),
  schedulerMinIntervalMs: parseInt(process.env.SCHEDULER_MIN_INTERVAL_MS || '2000', 10),
  pollingMinIntervalMs: parseInt(process.env.POLLING_MIN_INTERVAL_MS || '2000', 10),
};

if (!CONFIG.apiKey) {
  // Not fatal - server still boots - but management routes will refuse all
  // requests until an API key is set, which is safer than running open.
  console.warn(
    '[SERVER] WARNING: MANAGEMENT_API_KEY is not set. All /api/* management ' +
    'routes will reject requests until you set it via environment variable.'
  );
}

// ----------------------------------------------------------------------------
// 2. LOGGER
// ----------------------------------------------------------------------------

const LOG_CATEGORIES = new Set([
  'SERVER', 'EVENT', 'DELIVERY', 'RETRY', 'SECURITY', 'SCHEDULER', 'POLLING', 'ERROR',
]);

function log(category, message, extra) {
  const cat = LOG_CATEGORIES.has(category) ? category : 'SERVER';
  const ts = new Date().toISOString();
  const line = `${ts} [${cat}] ${message}`;
  if (extra !== undefined) {
    console.log(line, typeof extra === 'string' ? extra : JSON.stringify(extra));
  } else {
    console.log(line);
  }
}

// ----------------------------------------------------------------------------
// 3. SMALL UTILITIES
// ----------------------------------------------------------------------------

function uuid() {
  return crypto.randomUUID();
}

function nowIso() {
  return new Date().toISOString();
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randFloat(min, max, decimals = 2) {
  const v = Math.random() * (max - min) + min;
  return parseFloat(v.toFixed(decimals));
}

function randBool(trueChance = 0.5) {
  return Math.random() < trueChance;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const FIRST_NAMES = ['Alex', 'Jordan', 'Taylor', 'Morgan', 'Casey', 'Riley', 'Jamie', 'Avery', 'Quinn', 'Drew'];
const LAST_NAMES = ['Smith', 'Johnson', 'Lee', 'Brown', 'Garcia', 'Miller', 'Davis', 'Wilson', 'Clark', 'Walker'];
const DOMAINS_EMAIL = ['example.com', 'mail.test', 'inbox.dev', 'usertest.io'];
const CURRENCIES = ['USD', 'EUR', 'GBP', 'CAD', 'AUD'];
const PRODUCT_NAMES = ['Wireless Mouse', 'Mechanical Keyboard', 'USB-C Hub', 'Noise Cancelling Headphones', 'Standing Desk Mat', '4K Monitor', 'Laptop Stand', 'Webcam'];
const COUNTRIES = ['US', 'GB', 'DE', 'FR', 'CA', 'AU', 'JP'];
const FILE_TYPES = ['invoice.pdf', 'report.docx', 'dataset.csv', 'profile.png', 'contract.pdf', 'archive.zip'];

function randomName() {
  return `${pick(FIRST_NAMES)} ${pick(LAST_NAMES)}`;
}

function randomEmail() {
  return `${pick(FIRST_NAMES).toLowerCase()}.${pick(LAST_NAMES).toLowerCase()}${randInt(1, 999)}@${pick(DOMAINS_EMAIL)}`;
}

function randomAmount() {
  return randFloat(1, 2500, 2);
}

function safeJsonParse(str) {
  try {
    return { ok: true, value: JSON.parse(str) };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

function clampInt(value, min, max, fallback) {
  const n = parseInt(value, 10);
  if (Number.isNaN(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

// ----------------------------------------------------------------------------
// 4. IN-MEMORY STORES (bounded ring buffers)
// ----------------------------------------------------------------------------

class BoundedStore {
  constructor(limit) {
    this.limit = limit;
    this.items = [];
    this.byId = new Map();
  }
  add(item) {
    this.items.push(item);
    this.byId.set(item.id, item);
    if (this.items.length > this.limit) {
      const removed = this.items.shift();
      if (removed && this.byId.get(removed.id) === removed) {
        this.byId.delete(removed.id);
      }
    }
    return item;
  }
  get(id) {
    return this.byId.get(id);
  }
  all() {
    return this.items;
  }
  recent(n = 50) {
    return this.items.slice(-n).reverse();
  }
  count() {
    return this.items.length;
  }
}

const store = {
  events: new BoundedStore(CONFIG.maxStoredEvents),
  deliveries: new BoundedStore(CONFIG.maxStoredDeliveries),
  lifecycles: new BoundedStore(CONFIG.maxStoredLifecycles),
  targets: new Map(), // id -> target config
};

const stats = {
  eventsGenerated: 0,
  deliveriesAttempted: 0,
  deliveriesSucceeded: 0,
  deliveriesFailed: 0,
  retriesPerformed: 0,
  securityScenariosRun: 0,
  lifecyclesStarted: 0,
  lifecyclesCompleted: 0,
  replaysSent: 0,
  pollsSent: 0,
  startedAt: nowIso(),
};

// ----------------------------------------------------------------------------
// 5. EVENT GENERATION ENGINE
// ----------------------------------------------------------------------------
// Each domain maps event-type -> payload builder. Builders accept an
// optional `entity` object so lifecycle sequences can share the same
// underlying entity id across multiple events.

const EVENT_DOMAINS = {
  payment: {
    types: [
      'payment.created', 'payment.processing', 'payment.successful',
      'payment.failed', 'payment.refunded', 'payment.cancelled',
    ],
    build(type, entity = {}) {
      const id = entity.id || `pay_${uuid()}`;
      const amount = entity.amount ?? randomAmount();
      const currency = entity.currency || pick(CURRENCIES);
      return {
        payment_id: id,
        amount,
        currency,
        customer_email: entity.customer_email || randomEmail(),
        payment_method: pick(['card', 'bank_transfer', 'wallet']),
        status: type.split('.')[1],
        failure_reason: type === 'payment.failed' ? pick(['insufficient_funds', 'card_declined', 'expired_card']) : undefined,
        refund_amount: type === 'payment.refunded' ? amount : undefined,
      };
    },
  },
  order: {
    types: [
      'order.created', 'order.confirmed', 'order.shipped',
      'order.delivered', 'order.cancelled',
    ],
    build(type, entity = {}) {
      const id = entity.id || `order_${uuid()}`;
      const items = entity.items || Array.from({ length: randInt(1, 4) }, () => ({
        sku: `SKU-${randInt(1000, 9999)}`,
        name: pick(PRODUCT_NAMES),
        quantity: randInt(1, 3),
        unit_price: randomAmount(),
      }));
      return {
        order_id: id,
        customer_email: entity.customer_email || randomEmail(),
        items,
        total: entity.total ?? items.reduce((s, i) => s + i.quantity * i.unit_price, 0),
        currency: entity.currency || pick(CURRENCIES),
        status: type.split('.')[1],
        shipping_country: pick(COUNTRIES),
        tracking_number: type === 'order.shipped' || type === 'order.delivered' ? `TRK${randInt(100000, 999999)}` : undefined,
      };
    },
  },
  user: {
    types: [
      'user.registered', 'user.login', 'user.login.failed',
      'user.password.changed', 'user.account.locked',
    ],
    build(type, entity = {}) {
      const id = entity.id || `user_${uuid()}`;
      return {
        user_id: id,
        email: entity.email || randomEmail(),
        name: entity.name || randomName(),
        ip_address: `${randInt(1, 255)}.${randInt(0, 255)}.${randInt(0, 255)}.${randInt(1, 255)}`,
        user_agent: pick(['Mozilla/5.0', 'curl/8.4.0', 'PostmanRuntime/7.36', 'okhttp/4.12']),
        reason: type === 'user.login.failed' ? pick(['bad_password', 'mfa_failed', 'unknown_user']) : undefined,
        locked_until: type === 'user.account.locked' ? new Date(Date.now() + 30 * 60000).toISOString() : undefined,
      };
    },
  },
  subscription: {
    types: [
      'subscription.created', 'subscription.renewed', 'subscription.upgraded',
      'subscription.downgraded', 'subscription.cancelled', 'subscription.payment_failed',
    ],
    build(type, entity = {}) {
      const id = entity.id || `sub_${uuid()}`;
      return {
        subscription_id: id,
        customer_email: entity.customer_email || randomEmail(),
        plan: entity.plan || pick(['free', 'starter', 'pro', 'enterprise']),
        billing_cycle: pick(['monthly', 'annual']),
        amount: randomAmount(),
        currency: pick(CURRENCIES),
        status: type.split('.')[1],
        next_billing_date: new Date(Date.now() + 30 * 86400000).toISOString(),
      };
    },
  },
  inventory: {
    types: ['inventory.updated', 'inventory.low_stock', 'inventory.out_of_stock'],
    build(type, entity = {}) {
      const sku = entity.sku || `SKU-${randInt(1000, 9999)}`;
      const quantity = type === 'inventory.out_of_stock' ? 0 : type === 'inventory.low_stock' ? randInt(1, 5) : randInt(10, 500);
      return {
        sku,
        product_name: entity.product_name || pick(PRODUCT_NAMES),
        warehouse: pick(['WH-EAST', 'WH-WEST', 'WH-EU', 'WH-APAC']),
        quantity,
        threshold: 10,
      };
    },
  },
  file: {
    types: ['file.uploaded', 'file.processing', 'file.processed', 'file.processing.failed'],
    build(type, entity = {}) {
      const id = entity.id || `file_${uuid()}`;
      return {
        file_id: id,
        filename: entity.filename || pick(FILE_TYPES),
        size_bytes: entity.size_bytes ?? randInt(1024, 10 * 1024 * 1024),
        content_type: pick(['application/pdf', 'image/png', 'text/csv', 'application/zip', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document']),
        uploaded_by: entity.uploaded_by || randomEmail(),
        error: type === 'file.processing.failed' ? pick(['corrupt_file', 'unsupported_format', 'virus_detected']) : undefined,
        processed_url: type === 'file.processed' ? `https://files.example-storage.test/${id}` : undefined,
      };
    },
  },
  message: {
    types: ['message.sent', 'message.delivered', 'message.failed', 'message.read'],
    build(type, entity = {}) {
      const id = entity.id || `msg_${uuid()}`;
      return {
        message_id: id,
        channel: pick(['email', 'sms', 'push', 'slack']),
        to: entity.to || randomEmail(),
        subject: pick(['Your order update', 'Security alert', 'Weekly digest', 'Password reset']),
        status: type.split('.')[1],
        error: type === 'message.failed' ? pick(['bounced', 'invalid_recipient', 'provider_timeout']) : undefined,
      };
    },
  },
  banking: {
    types: [
      'transaction.initiated', 'transaction.completed', 'transaction.failed',
      'transaction.reversed', 'account.balance_updated',
    ],
    build(type, entity = {}) {
      const id = entity.id || `txn_${uuid()}`;
      return {
        transaction_id: id,
        account_id: entity.account_id || `acct_${uuid()}`,
        amount: randomAmount(),
        currency: pick(CURRENCIES),
        type: pick(['debit', 'credit']),
        status: type.split('.')[1] || type.split('.')[0],
        balance_after: randFloat(0, 50000, 2),
      };
    },
  },
  shipping: {
    types: [
      'shipment.created', 'shipment.in_transit', 'shipment.out_for_delivery',
      'shipment.delivered', 'shipment.delayed', 'shipment.lost',
    ],
    build(type, entity = {}) {
      const id = entity.id || `ship_${uuid()}`;
      return {
        shipment_id: id,
        tracking_number: entity.tracking_number || `TRK${randInt(100000, 999999)}`,
        carrier: pick(['UPS', 'FedEx', 'DHL', 'USPS']),
        origin: pick(COUNTRIES),
        destination: pick(COUNTRIES),
        status: type.split('.')[1],
        estimated_delivery: new Date(Date.now() + randInt(1, 7) * 86400000).toISOString(),
      };
    },
  },
  repository: {
    types: [
      'repo.push', 'repo.pull_request.opened', 'repo.pull_request.merged',
      'repo.issue.opened', 'repo.issue.closed', 'repo.star.created',
    ],
    build(type, entity = {}) {
      return {
        repository: entity.repository || `octo-org/${pick(['webhook-lab', 'api-service', 'data-pipeline', 'frontend-app'])}`,
        actor: entity.actor || pick(FIRST_NAMES).toLowerCase() + randInt(1, 99),
        ref: 'refs/heads/main',
        pr_number: type.startsWith('repo.pull_request') ? randInt(1, 500) : undefined,
        issue_number: type.startsWith('repo.issue') ? randInt(1, 500) : undefined,
        commit_sha: type === 'repo.push' ? crypto.randomBytes(20).toString('hex') : undefined,
      };
    },
  },
  generic: {
    types: ['app.event.custom', 'app.heartbeat', 'app.feature_flag.changed', 'app.config.updated'],
    build(type, entity = {}) {
      return {
        app: entity.app || 'demo-app',
        detail: pick(['value_changed', 'routine_check', 'manual_trigger']),
        metadata: { env: pick(['production', 'staging']), version: `1.${randInt(0, 50)}.${randInt(0, 20)}` },
      };
    },
  },
};

function listEventTypes() {
  const result = [];
  for (const [domain, def] of Object.entries(EVENT_DOMAINS)) {
    for (const type of def.types) result.push({ domain, type });
  }
  return result;
}

function findDomainForType(type) {
  for (const [domain, def] of Object.entries(EVENT_DOMAINS)) {
    if (def.types.includes(type)) return domain;
  }
  return null;
}

/**
 * Build a fully formed event envelope (id, type, timestamps, correlation)
 * wrapping a domain-specific payload.
 */
function createEvent({ type, domain, entity, correlationId, sequence } = {}) {
  let resolvedDomain = domain;
  if (!resolvedDomain) resolvedDomain = type ? findDomainForType(type) : pick(Object.keys(EVENT_DOMAINS));
  if (!resolvedDomain || !EVENT_DOMAINS[resolvedDomain]) {
    throw new Error(`Unknown domain: ${domain}`);
  }
  const def = EVENT_DOMAINS[resolvedDomain];
  const resolvedType = type && def.types.includes(type) ? type : pick(def.types);
  const payload = def.build(resolvedType, entity || {});

  const event = {
    id: `evt_${uuid()}`,
    type: resolvedType,
    domain: resolvedDomain,
    created_at: nowIso(),
    correlation_id: correlationId || `corr_${uuid()}`,
    sequence: sequence ?? 0,
    data: payload,
  };
  stats.eventsGenerated += 1;
  store.events.add(event);
  log('EVENT', `Generated ${event.type} (${event.domain}) id=${event.id}`);
  return event;
}

function generateRandomEvent() {
  const domain = pick(Object.keys(EVENT_DOMAINS));
  return createEvent({ domain });
}

// ----------------------------------------------------------------------------
// 6. LIFECYCLE ENGINE
// ----------------------------------------------------------------------------
// A lifecycle is an ordered sequence of event types that share one entity.
// Supports: complete, fail-at-stage, pending (stop early), skip-a-stage,
// out-of-order delivery, duplicate delivery, and "resume after interruption"
// (i.e. continue a previously pending lifecycle).

const LIFECYCLE_DEFINITIONS = {
  payment: {
    success: ['payment.created', 'payment.processing', 'payment.successful'],
    failure: ['payment.created', 'payment.processing', 'payment.failed'],
  },
  order: {
    success: ['order.created', 'order.confirmed', 'order.shipped', 'order.delivered'],
    failure: ['order.created', 'order.confirmed', 'order.cancelled'],
  },
  file: {
    success: ['file.uploaded', 'file.processing', 'file.processed'],
    failure: ['file.uploaded', 'file.processing', 'file.processing.failed'],
  },
  subscription: {
    success: ['subscription.created', 'subscription.renewed'],
    failure: ['subscription.created', 'subscription.payment_failed', 'subscription.cancelled'],
  },
  shipping: {
    success: ['shipment.created', 'shipment.in_transit', 'shipment.out_for_delivery', 'shipment.delivered'],
    failure: ['shipment.created', 'shipment.in_transit', 'shipment.lost'],
  },
};

/**
 * Plan the sequence of event types for a lifecycle run, applying the
 * requested mutation (fail/pending/skip/duplicate/out_of_order).
 */
function planLifecycleStages(domain, outcome, mutation) {
  const defs = LIFECYCLE_DEFINITIONS[domain];
  if (!defs) throw new Error(`No lifecycle defined for domain: ${domain}`);
  let stages = [...(defs[outcome] || defs.success)];

  if (mutation === 'pending') {
    // Stop after the first stage only - entity remains "in limbo".
    stages = stages.slice(0, 1);
  } else if (mutation === 'skip') {
    if (stages.length > 2) stages.splice(randInt(1, stages.length - 2), 1);
  } else if (mutation === 'duplicate') {
    const idx = randInt(0, stages.length - 1);
    stages.splice(idx, 0, stages[idx]);
  } else if (mutation === 'out_of_order') {
    // Shuffle the middle stages, keep first/last anchored.
    if (stages.length > 2) {
      const middle = stages.slice(1, -1);
      for (let i = middle.length - 1; i > 0; i--) {
        const j = randInt(0, i);
        [middle[i], middle[j]] = [middle[j], middle[i]];
      }
      stages = [stages[0], ...middle, stages[stages.length - 1]];
    }
  }
  return stages;
}

/**
 * Run (or resume) a lifecycle: generates each stage event and delivers it
 * to the target, waiting `delayMs` between stages.
 *
 * options:
 *   domain, targetId, outcome ('success'|'failure'|'random'),
 *   mutation ('none'|'pending'|'skip'|'duplicate'|'out_of_order'),
 *   delayMs, resumeOf (lifecycle id to continue)
 */
async function runLifecycle(options) {
  const {
    domain,
    targetId,
    outcome = 'success',
    mutation = 'none',
    delayMs = 1000,
    resumeOf = null,
    securityScenario = null,
  } = options;

  const target = store.targets.get(targetId);
  if (!target) throw new Error(`Unknown target: ${targetId}`);

  let lifecycleRecord;
  let stages;
  let entity;
  let correlationId;
  let startIndex = 0;

  if (resumeOf) {
    const prior = store.lifecycles.get(resumeOf);
    if (!prior) throw new Error(`Unknown lifecycle to resume: ${resumeOf}`);
    const defs = LIFECYCLE_DEFINITIONS[prior.domain];
    stages = defs[prior.outcome] || defs.success;
    entity = prior.entity;
    correlationId = prior.correlationId;
    startIndex = prior.completedStages.length;
    lifecycleRecord = prior;
    lifecycleRecord.resumed = true;
    log('EVENT', `Resuming lifecycle ${resumeOf} from stage ${startIndex}`);
  } else {
    const resolvedOutcome = outcome === 'random' ? pick(['success', 'failure']) : outcome;
    stages = planLifecycleStages(domain, resolvedOutcome, mutation);
    entity = { id: `${domain}_${uuid()}` };
    correlationId = `corr_${uuid()}`;
    lifecycleRecord = {
      id: `life_${uuid()}`,
      domain,
      outcome: resolvedOutcome,
      mutation,
      targetId,
      entity,
      correlationId,
      plannedStages: stages,
      completedStages: [],
      status: 'running',
      started_at: nowIso(),
      resumed: false,
    };
    store.lifecycles.add(lifecycleRecord);
    stats.lifecyclesStarted += 1;
    log('EVENT', `Starting lifecycle ${lifecycleRecord.id} domain=${domain} outcome=${resolvedOutcome} mutation=${mutation} stages=${stages.join(' -> ')}`);
  }

  for (let i = startIndex; i < stages.length; i++) {
    const type = stages[i];
    const event = createEvent({ type, domain, entity, correlationId, sequence: i });

    let deliveryOptions = { method: 'POST' };
    if (securityScenario) {
      deliveryOptions = buildSecurityScenarioRequest(securityScenario, event, target);
    } else {
      deliveryOptions = buildStandardRequest(event, target);
    }

    await deliverWithRetry(target, event, deliveryOptions, { lifecycleId: lifecycleRecord.id });
    lifecycleRecord.completedStages.push(type);

    if (i < stages.length - 1) await sleep(delayMs);
  }

  if (mutation === 'pending' && !resumeOf) {
    lifecycleRecord.status = 'pending';
    log('EVENT', `Lifecycle ${lifecycleRecord.id} left pending after stage 0 (call resume to continue)`);
  } else {
    lifecycleRecord.status = 'completed';
    lifecycleRecord.completed_at = nowIso();
    stats.lifecyclesCompleted += 1;
    log('EVENT', `Lifecycle ${lifecycleRecord.id} completed with status=completed`);
  }

  return lifecycleRecord;
}


// ----------------------------------------------------------------------------
// 7. SIGNATURE ENGINE (HMAC)
// ----------------------------------------------------------------------------

/**
 * Compute an HMAC signature over the exact raw body string that will be
 * sent on the wire. Signing the already-serialized body (not re-serialized
 * JSON) is the single most important correctness rule for HMAC webhooks.
 */
function computeHmac(secret, rawBody, algorithm = 'sha256') {
  const algo = algorithm === 'sha512' ? 'sha512' : 'sha256';
  return crypto.createHmac(algo, secret).update(rawBody, 'utf8').digest('hex');
}

/**
 * Build the signed-content string used by many real providers:
 * `${timestamp}.${rawBody}` - this binds the timestamp into the signature
 * so an attacker can't replay an old signature with a new timestamp.
 */
function buildSignedContent(timestamp, rawBody) {
  return `${timestamp}.${rawBody}`;
}

function formatSignatureHeader(format, signature) {
  // format is a template like "sha256={sig}" or "{sig}"
  return (format || 'sha256={sig}').replace('{sig}', signature);
}

/**
 * Produce a complete, valid signature header set for a target+body.
 */
function signRequest(target, rawBody, timestampOverride) {
  const algorithm = target.security?.algorithm || 'sha256';
  const secret = target.security?.secret || '';
  const timestamp = timestampOverride || Math.floor(Date.now() / 1000).toString();
  const signedContent = target.security?.timestampBinding === false ? rawBody : buildSignedContent(timestamp, rawBody);
  const signature = computeHmac(secret, signedContent, algorithm);
  const headerName = target.security?.signatureHeader || 'X-Webhook-Signature';
  const timestampHeaderName = target.security?.timestampHeader || 'X-Webhook-Timestamp';
  const headerValue = formatSignatureHeader(target.security?.signatureFormat, signature);
  const headers = { [headerName]: headerValue };
  if (target.security?.timestampBinding !== false) {
    headers[timestampHeaderName] = timestamp;
  }
  return { headers, signature, timestamp, signedContent };
}

// ----------------------------------------------------------------------------
// 8. REQUEST CONSTRUCTION
// ----------------------------------------------------------------------------

function serializeBody(data, contentType) {
  if (contentType === 'application/x-www-form-urlencoded') {
    const params = new URLSearchParams();
    const flat = flattenForForm(data);
    for (const [k, v] of Object.entries(flat)) params.append(k, String(v));
    return params.toString();
  }
  if (contentType === 'text/plain') {
    return typeof data === 'string' ? data : JSON.stringify(data);
  }
  // default: application/json
  return JSON.stringify(data);
}

function flattenForForm(obj, prefix = '') {
  const out = {};
  for (const [k, v] of Object.entries(obj || {})) {
    if (v === undefined) continue;
    const key = prefix ? `${prefix}[${k}]` : k;
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      Object.assign(out, flattenForForm(v, key));
    } else {
      out[key] = Array.isArray(v) ? JSON.stringify(v) : v;
    }
  }
  return out;
}

/**
 * Build the standard (non-adversarial) webhook request for an event
 * against a target, including a valid HMAC signature if the target has
 * security configured.
 */
function buildStandardRequest(event, target, overrides = {}) {
  const method = overrides.method || target.defaultMethod || 'POST';
  const contentType = overrides.contentType || target.defaultContentType || 'application/json';
  const envelope = {
    id: event.id,
    type: event.type,
    created_at: event.created_at,
    correlation_id: event.correlation_id,
    data: event.data,
  };
  const rawBody = overrides.rawBody !== undefined ? overrides.rawBody : serializeBody(envelope, contentType);

  const headers = {
    'Content-Type': contentType,
    'X-Webhook-Event-Id': event.id,
    'X-Webhook-Event-Type': event.type,
    'User-Agent': 'Webhook-Lab-Simulator/1.0',
    ...(overrides.headers || {}),
  };

  if (target.security?.enabled) {
    const { headers: sigHeaders } = signRequest(target, rawBody, overrides.timestampOverride);
    Object.assign(headers, sigHeaders);
  }

  if (target.authToken) {
    headers['Authorization'] = `Bearer ${target.authToken}`;
  }

  return {
    method,
    url: overrides.url || target.url,
    headers,
    body: rawBody,
    timeoutMs: overrides.timeoutMs || target.timeoutMs || CONFIG.defaultTimeoutMs,
    note: overrides.note || 'standard_delivery',
  };
}


// ----------------------------------------------------------------------------
// 9. DELIVERY ENGINE
// ----------------------------------------------------------------------------
// Performs the actual HTTP request with fetch(), measures duration,
// captures everything needed for learning, and never logs secrets.

const SENSITIVE_HEADER_NAMES = new Set([
  'authorization', 'x-webhook-signature', 'x-api-key', 'cookie', 'set-cookie',
]);

function redactHeaders(headers) {
  const out = {};
  for (const [k, v] of Object.entries(headers || {})) {
    out[k] = SENSITIVE_HEADER_NAMES.has(k.toLowerCase()) ? '[REDACTED]' : v;
  }
  return out;
}

/**
 * Perform a single HTTP delivery attempt. Always does a REAL network call -
 * nothing here fakes a response. Returns a delivery record.
 */
async function deliverOnce(target, event, request, meta = {}) {
  const attemptNumber = meta.attemptNumber || 1;
  const controller = new AbortController();
  const timeoutMs = Math.min(request.timeoutMs || CONFIG.defaultTimeoutMs, CONFIG.maxTimeoutMs);
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  const record = {
    id: `del_${uuid()}`,
    event_id: event?.id || null,
    target_id: target.id,
    lifecycle_id: meta.lifecycleId || null,
    scenario: request.note || 'standard_delivery',
    attempt_number: attemptNumber,
    method: request.method,
    url: request.url,
    request_headers: redactHeaders(request.headers),
    request_body_preview: typeof request.body === 'string' ? request.body.slice(0, 2000) : null,
    request_body_bytes: typeof request.body === 'string' ? Buffer.byteLength(request.body) : 0,
    started_at: nowIso(),
    timestamp: Date.now(),
    duration_ms: null,
    success: false,
    outcome: null, // 'ok' | 'http_error' | 'timeout' | 'network_error'
    response_status: null,
    response_headers: null,
    response_body_preview: null,
    error_message: null,
  };

  const startedAt = Date.now();
  stats.deliveriesAttempted += 1;

  try {
    const fetchOptions = {
      method: request.method,
      headers: request.headers,
      signal: controller.signal,
    };
    if (request.method !== 'GET' && request.method !== 'HEAD' && request.body !== undefined) {
      fetchOptions.body = request.body;
    }

    const res = await fetch(request.url, fetchOptions);
    const duration = Date.now() - startedAt;
    const respHeaders = {};
    res.headers.forEach((v, k) => { respHeaders[k] = v; });

    let bodyText = '';
    try {
      bodyText = await res.text();
    } catch {
      bodyText = '[unreadable response body]';
    }

    record.duration_ms = duration;
    record.response_status = res.status;
    record.response_headers = respHeaders;
    record.response_body_preview = bodyText.slice(0, 2000);
    record.success = res.ok;
    record.outcome = 'ok';

    log('DELIVERY', `${request.method} ${request.url} -> ${res.status} (${duration}ms) attempt=${attemptNumber} scenario=${record.scenario}`);
  } catch (err) {
    const duration = Date.now() - startedAt;
    record.duration_ms = duration;
    record.success = false;
    if (err.name === 'AbortError') {
      record.outcome = 'timeout';
      record.error_message = `Request timed out after ${timeoutMs}ms`;
      log('DELIVERY', `${request.method} ${request.url} -> TIMEOUT after ${timeoutMs}ms attempt=${attemptNumber}`);
    } else {
      record.outcome = 'network_error';
      record.error_message = err.message;
      log('DELIVERY', `${request.method} ${request.url} -> NETWORK ERROR: ${err.message} attempt=${attemptNumber}`);
    }
  } finally {
    clearTimeout(timer);
  }

  if (record.success) stats.deliveriesSucceeded += 1; else stats.deliveriesFailed += 1;
  store.deliveries.add(record);
  return record;
}

// ----------------------------------------------------------------------------
// 10. RETRY ENGINE
// ----------------------------------------------------------------------------

const RETRYABLE_STATUS_CODES = new Set([408, 429, 500, 502, 503, 504]);

function computeBackoffDelay(attempt, strategy, baseDelayMs, maxDelayMs) {
  let delay;
  if (strategy === 'exponential') {
    delay = baseDelayMs * Math.pow(2, attempt - 1);
  } else {
    delay = baseDelayMs; // fixed
  }
  // small jitter to avoid thundering herd
  delay = delay + randInt(0, Math.floor(baseDelayMs * 0.25));
  return Math.min(delay, maxDelayMs);
}

/**
 * Deliver a single request, retrying according to the target's retry
 * policy (or an override). Each attempt is recorded independently.
 */
async function deliverWithRetry(target, event, request, meta = {}) {
  const policy = {
    enabled: true,
    maxAttempts: 3,
    strategy: 'exponential', // 'fixed' | 'exponential'
    baseDelayMs: 500,
    maxDelayMs: 30000,
    retryOnNetworkError: true,
    retryStatusCodes: [...RETRYABLE_STATUS_CODES],
    respectRetryAfter: true,
    ...(target.retryPolicy || {}),
    ...(meta.retryOverride || {}),
  };
  policy.maxAttempts = clampInt(policy.maxAttempts, 1, CONFIG.maxRetries, 3);

  let lastRecord = null;
  for (let attempt = 1; attempt <= policy.maxAttempts; attempt++) {
    lastRecord = await deliverOnce(target, event, request, { ...meta, attemptNumber: attempt });

    if (lastRecord.success) return lastRecord;
    if (!policy.enabled) return lastRecord;
    if (attempt === policy.maxAttempts) {
      log('RETRY', `Exhausted retries (${policy.maxAttempts}) for event=${event?.id} target=${target.id}`);
      return lastRecord;
    }

    const isNetworkIssue = lastRecord.outcome === 'network_error' || lastRecord.outcome === 'timeout';
    const isRetryableStatus = lastRecord.response_status && policy.retryStatusCodes.includes(lastRecord.response_status);

    if (isNetworkIssue && !policy.retryOnNetworkError) return lastRecord;
    if (!isNetworkIssue && !isRetryableStatus) return lastRecord; // non-retryable status, stop

    let delay = computeBackoffDelay(attempt, policy.strategy, policy.baseDelayMs, policy.maxDelayMs);
    const retryAfterHeader = lastRecord.response_headers?.['retry-after'];
    if (policy.respectRetryAfter && retryAfterHeader) {
      const retryAfterSec = parseInt(retryAfterHeader, 10);
      if (!Number.isNaN(retryAfterSec)) delay = Math.min(retryAfterSec * 1000, policy.maxDelayMs);
    }

    stats.retriesPerformed += 1;
    log('RETRY', `Retrying event=${event?.id} target=${target.id} in ${delay}ms (attempt ${attempt + 1}/${policy.maxAttempts})`);
    await sleep(delay);
  }
  return lastRecord;
}


// ----------------------------------------------------------------------------
// 11. SECURITY SCENARIO ENGINE
// ----------------------------------------------------------------------------
// Builds deliberately adversarial-but-SAFE requests against a target the
// user has explicitly configured. Every scenario below only manipulates
// the request this process sends to that one receiver - nothing here
// probes, scans, or attacks any third-party system.

const SECURITY_SCENARIOS = [
  'valid_hmac', 'missing_signature', 'incorrect_signature', 'incorrect_secret',
  'modified_payload_after_signing', 'invalid_timestamp_format', 'expired_timestamp',
  'future_timestamp', 'malformed_signature_format', 'unexpected_method',
  'missing_content_type', 'incorrect_content_type', 'malformed_json', 'empty_body',
  'missing_required_fields', 'unexpected_fields', 'incorrect_data_types',
  'large_payload', 'unexpected_nested_json', 'unicode_edge_cases', 'header_casing',
  'missing_authorization_header', 'invalid_bearer_token', 'secret_rotation',
];

function buildSecurityScenarioRequest(scenarioName, event, target) {
  stats.securityScenariosRun += 1;
  const base = buildStandardRequest(event, target);
  const envelope = { id: event.id, type: event.type, created_at: event.created_at, correlation_id: event.correlation_id, data: event.data };

  log('SECURITY', `Building scenario "${scenarioName}" for event=${event.id} target=${target.id}`);

  switch (scenarioName) {
    case 'valid_hmac':
      return { ...base, note: 'security:valid_hmac' };

    case 'missing_signature': {
      const headers = { ...base.headers };
      const sigHeader = target.security?.signatureHeader || 'X-Webhook-Signature';
      delete headers[sigHeader];
      return { ...base, headers, note: 'security:missing_signature' };
    }

    case 'incorrect_signature': {
      const headers = { ...base.headers };
      const sigHeader = target.security?.signatureHeader || 'X-Webhook-Signature';
      headers[sigHeader] = formatSignatureHeader(target.security?.signatureFormat, crypto.randomBytes(32).toString('hex'));
      return { ...base, headers, note: 'security:incorrect_signature' };
    }

    case 'incorrect_secret': {
      const rawBody = base.body;
      const wrongSecretTarget = { ...target, security: { ...target.security, secret: `${target.security?.secret || ''}_wrong` } };
      const { headers: sigHeaders } = signRequest(wrongSecretTarget, rawBody);
      return { ...base, headers: { ...base.headers, ...sigHeaders }, note: 'security:incorrect_secret' };
    }

    case 'modified_payload_after_signing': {
      // Sign the real body, then send a different body - simulates
      // tampering in transit or at a misbehaving proxy.
      const tampered = { ...envelope, data: { ...envelope.data, _tampered: true, amount: 999999.99 } };
      const tamperedBody = JSON.stringify(tampered);
      return { ...base, body: tamperedBody, note: 'security:modified_payload_after_signing' };
    }

    case 'invalid_timestamp_format': {
      const headers = { ...base.headers };
      const tsHeader = target.security?.timestampHeader || 'X-Webhook-Timestamp';
      headers[tsHeader] = 'not-a-timestamp';
      return { ...base, headers, note: 'security:invalid_timestamp_format' };
    }

    case 'expired_timestamp': {
      const oldTs = Math.floor((Date.now() - 24 * 3600 * 1000) / 1000).toString();
      const rawBody = base.body;
      const { headers: sigHeaders } = signRequest(target, rawBody, oldTs);
      return { ...base, headers: { ...base.headers, ...sigHeaders }, note: 'security:expired_timestamp' };
    }

    case 'future_timestamp': {
      const futureTs = Math.floor((Date.now() + 24 * 3600 * 1000) / 1000).toString();
      const rawBody = base.body;
      const { headers: sigHeaders } = signRequest(target, rawBody, futureTs);
      return { ...base, headers: { ...base.headers, ...sigHeaders }, note: 'security:future_timestamp' };
    }

    case 'malformed_signature_format': {
      const headers = { ...base.headers };
      const sigHeader = target.security?.signatureHeader || 'X-Webhook-Signature';
      headers[sigHeader] = 'this-is-not-a-valid-signature-format;;;';
      return { ...base, headers, note: 'security:malformed_signature_format' };
    }

    case 'unexpected_method': {
      const method = pick(['PUT', 'PATCH', 'DELETE', 'GET'].filter((m) => m !== base.method));
      return { ...base, method, note: 'security:unexpected_method' };
    }

    case 'missing_content_type': {
      const headers = { ...base.headers };
      delete headers['Content-Type'];
      return { ...base, headers, note: 'security:missing_content_type' };
    }

    case 'incorrect_content_type': {
      const headers = { ...base.headers, 'Content-Type': 'image/png' };
      return { ...base, headers, note: 'security:incorrect_content_type' };
    }

    case 'malformed_json': {
      const body = base.body.slice(0, Math.max(5, base.body.length - 5)) + '{{{invalid';
      return { ...base, body, note: 'security:malformed_json' };
    }

    case 'empty_body':
      return { ...base, body: '', headers: { ...base.headers, 'Content-Length': '0' }, note: 'security:empty_body' };

    case 'missing_required_fields': {
      const stripped = { id: event.id, type: event.type }; // drop data, timestamps, correlation
      return { ...base, body: JSON.stringify(stripped), note: 'security:missing_required_fields' };
    }

    case 'unexpected_fields': {
      const extra = { ...envelope, unexpected_field_1: 'surprise', __proto__inject: 'test', nested_surprise: { a: 1, b: [1, 2, 3] } };
      return { ...base, body: JSON.stringify(extra), note: 'security:unexpected_fields' };
    }

    case 'incorrect_data_types': {
      const wrongTypes = { ...envelope, data: { ...envelope.data, amount: 'not-a-number', quantity: 'five', id_as_number: 12345 } };
      return { ...base, body: JSON.stringify(wrongTypes), note: 'security:incorrect_data_types' };
    }

    case 'large_payload': {
      const padding = 'x'.repeat(Math.min(CONFIG.maxPayloadBytes - 2000, 500000));
      const big = { ...envelope, padding_for_testing: padding };
      return { ...base, body: JSON.stringify(big), note: 'security:large_payload' };
    }

    case 'unexpected_nested_json': {
      const deep = { ...envelope, data: { ...envelope.data, nested: buildDeepObject(8) } };
      return { ...base, body: JSON.stringify(deep), note: 'security:unexpected_nested_json' };
    }

    case 'unicode_edge_cases': {
      const weird = {
        ...envelope,
        data: {
          ...envelope.data,
          unicode_name: 'Zoë Müller 田中太郎 🚀🔥 \u0000\u200B',
          rtl_text: 'مرحبا بالعالم',
          emoji_flood: '😀'.repeat(50),
          zero_width: 'a\u200Bb\u200Cc',
        },
      };
      return { ...base, body: JSON.stringify(weird), note: 'security:unicode_edge_cases' };
    }

    case 'header_casing': {
      const headers = {};
      for (const [k, v] of Object.entries(base.headers)) {
        // randomly mangle casing per header
        headers[randomizeCasing(k)] = v;
      }
      return { ...base, headers, note: 'security:header_casing' };
    }

    case 'missing_authorization_header': {
      const headers = { ...base.headers };
      delete headers['Authorization'];
      return { ...base, headers, note: 'security:missing_authorization_header' };
    }

    case 'invalid_bearer_token': {
      const headers = { ...base.headers, Authorization: `Bearer invalid_${crypto.randomBytes(8).toString('hex')}` };
      return { ...base, headers, note: 'security:invalid_bearer_token' };
    }

    case 'secret_rotation': {
      // Sign with a "new" rotated secret while the receiver may still
      // expect the old one - useful for testing graceful key rollover.
      const rotatedTarget = { ...target, security: { ...target.security, secret: `${target.security?.secret || ''}_rotated_${Date.now()}` } };
      const { headers: sigHeaders } = signRequest(rotatedTarget, base.body);
      return { ...base, headers: { ...base.headers, ...sigHeaders }, note: 'security:secret_rotation' };
    }

    default:
      throw new Error(`Unknown security scenario: ${scenarioName}`);
  }
}

function buildDeepObject(depth) {
  if (depth <= 0) return { leaf: true, value: randInt(1, 100) };
  return { level: depth, child: buildDeepObject(depth - 1), siblings: [randInt(1, 10), randInt(1, 10)] };
}

function randomizeCasing(str) {
  return str.split('').map((c) => (randBool() ? c.toUpperCase() : c.toLowerCase())).join('');
}


// ----------------------------------------------------------------------------
// 12. DUPLICATE DELIVERY & REPLAY
// ----------------------------------------------------------------------------

/**
 * Send N duplicate deliveries of the same event, with a configurable
 * variant: exact duplicates, same-id-modified-payload, or
 * same-payload-different-id. Can be sent sequentially or concurrently.
 */
async function sendDuplicateDeliveries(target, event, { count = 2, variant = 'exact', concurrent = false } = {}) {
  const requests = [];
  for (let i = 0; i < count; i++) {
    let thisEvent = event;
    let note = `duplicate:${variant}`;
    if (variant === 'same_id_modified_payload' && i > 0) {
      thisEvent = { ...event, data: { ...event.data, _duplicate_variant_index: i, amount: randomAmount() } };
    } else if (variant === 'same_payload_different_id' && i > 0) {
      thisEvent = { ...event, id: `evt_${uuid()}` };
    }
    const request = buildStandardRequest(thisEvent, target, { note });
    requests.push({ event: thisEvent, request });
  }

  if (concurrent) {
    return Promise.all(requests.map(({ event: e, request }) => deliverWithRetry(target, e, request)));
  }
  const results = [];
  for (const { event: e, request } of requests) {
    results.push(await deliverWithRetry(target, e, request));
  }
  return results;
}

/**
 * Replay a previously recorded delivery. Supports reusing the original
 * event id/signature/timestamp verbatim (true replay attack simulation),
 * or just resending the logical event as a fresh request.
 */
async function replayDelivery(deliveryId, { mode = 'exact_replay' } = {}) {
  const original = store.deliveries.get(deliveryId);
  if (!original) throw new Error(`Unknown delivery: ${deliveryId}`);
  const target = store.targets.get(original.target_id);
  if (!target) throw new Error(`Target no longer exists: ${original.target_id}`);
  const event = original.event_id ? store.events.get(original.event_id) : null;

  stats.replaysSent += 1;

  if (mode === 'exact_replay') {
    // Resend byte-for-byte the same headers/body as originally sent,
    // including the (now stale) signature and timestamp if present.
    const request = {
      method: original.method,
      url: original.url,
      headers: { ...original.request_headers }, // note: redacted sensitive headers are lost here on purpose
      body: original.request_body_preview,
      timeoutMs: target.timeoutMs,
      note: 'replay:exact_replay',
    };
    return deliverWithRetry(target, event, request, {});
  }

  if (mode === 'fresh_signature') {
    if (!event) throw new Error('Original event no longer in memory; cannot rebuild a fresh request');
    const request = buildStandardRequest(event, target, { note: 'replay:fresh_signature' });
    return deliverWithRetry(target, event, request, {});
  }

  if (mode === 'reused_old_signature_new_timestamp') {
    if (!event) throw new Error('Original event no longer in memory');
    const rawBody = JSON.stringify({ id: event.id, type: event.type, created_at: event.created_at, correlation_id: event.correlation_id, data: event.data });
    const headers = { 'Content-Type': 'application/json', 'X-Webhook-Event-Id': event.id, 'X-Webhook-Event-Type': event.type };
    const sigHeader = target.security?.signatureHeader || 'X-Webhook-Signature';
    headers[sigHeader] = original.request_headers?.[sigHeader] || formatSignatureHeader(target.security?.signatureFormat, 'stale');
    const request = { method: 'POST', url: target.url, headers, body: rawBody, timeoutMs: target.timeoutMs, note: 'replay:reused_old_signature_new_timestamp' };
    return deliverWithRetry(target, event, request, {});
  }

  throw new Error(`Unknown replay mode: ${mode}`);
}

// ----------------------------------------------------------------------------
// 13. ORDERING & CONCURRENCY
// ----------------------------------------------------------------------------

/**
 * Deliver a batch of events to a target under a chosen ordering/concurrency
 * mode: 'sequential', 'concurrent' (bounded by concurrencyLimit), or
 * 'random' (shuffled order, sequential execution).
 */
async function deliverBatch(target, events, { mode = 'sequential', concurrencyLimit = 5, delayMs = 0, securityScenario = null } = {}) {
  let ordered = [...events];
  if (mode === 'random') {
    for (let i = ordered.length - 1; i > 0; i--) {
      const j = randInt(0, i);
      [ordered[i], ordered[j]] = [ordered[j], ordered[i]];
    }
  }

  const buildFor = (event) => (securityScenario ? buildSecurityScenarioRequest(securityScenario, event, target) : buildStandardRequest(event, target));

  if (mode === 'concurrent') {
    const limit = clampInt(concurrencyLimit, 1, CONFIG.maxConcurrency, 5);
    const results = new Array(ordered.length);
    let cursor = 0;
    async function worker() {
      while (cursor < ordered.length) {
        const idx = cursor++;
        const event = ordered[idx];
        results[idx] = await deliverWithRetry(target, event, buildFor(event));
      }
    }
    const workers = Array.from({ length: Math.min(limit, ordered.length) }, () => worker());
    await Promise.all(workers);
    return results;
  }

  // sequential or random (random just changes order, still sequential sends)
  const results = [];
  for (const event of ordered) {
    results.push(await deliverWithRetry(target, event, buildFor(event)));
    if (delayMs > 0) await sleep(delayMs);
  }
  return results;
}

// ----------------------------------------------------------------------------
// 14. POLLING SIMULATION
// ----------------------------------------------------------------------------

const pollingState = {
  running: false,
  intervalHandle: null,
  config: null,
  pollCount: 0,
  missedCount: 0,
  lastResults: [],
};

function startPolling({ targetUrl, intervalMs, queryParams = {}, missRate = 0 }) {
  if (pollingState.running) throw new Error('Polling is already running. Stop it before starting a new session.');
  const interval = clampInt(intervalMs, CONFIG.pollingMinIntervalMs, 3600000, 10000);

  pollingState.config = { targetUrl, intervalMs: interval, queryParams, missRate };
  pollingState.running = true;
  pollingState.pollCount = 0;
  pollingState.missedCount = 0;
  pollingState.lastResults = [];

  log('POLLING', `Starting polling of ${targetUrl} every ${interval}ms`);

  pollingState.intervalHandle = setInterval(async () => {
    if (randBool(missRate)) {
      pollingState.missedCount += 1;
      log('POLLING', `Simulated missed polling interval (#${pollingState.missedCount})`);
      return;
    }
    try {
      const url = new URL(targetUrl);
      for (const [k, v] of Object.entries(queryParams || {})) url.searchParams.set(k, String(v));
      const startedAt = Date.now();
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), CONFIG.defaultTimeoutMs);
      let result;
      try {
        const res = await fetch(url.toString(), { method: 'GET', signal: controller.signal });
        const body = await res.text();
        result = { status: res.status, duration_ms: Date.now() - startedAt, body_preview: body.slice(0, 500), timestamp: nowIso() };
      } finally {
        clearTimeout(timer);
      }
      pollingState.pollCount += 1;
      pollingState.lastResults.push(result);
      if (pollingState.lastResults.length > 50) pollingState.lastResults.shift();
      stats.pollsSent += 1;
      log('POLLING', `Poll #${pollingState.pollCount} -> ${result.status} (${result.duration_ms}ms)`);
    } catch (err) {
      pollingState.pollCount += 1;
      const result = { status: null, error: err.message, timestamp: nowIso() };
      pollingState.lastResults.push(result);
      log('POLLING', `Poll failed: ${err.message}`);
    }
  }, interval);

  return pollingState.config;
}

function stopPolling() {
  if (!pollingState.running) return false;
  clearInterval(pollingState.intervalHandle);
  pollingState.intervalHandle = null;
  pollingState.running = false;
  log('POLLING', `Stopped polling after ${pollingState.pollCount} polls (${pollingState.missedCount} simulated misses)`);
  return true;
}

// ----------------------------------------------------------------------------
// 15. SCHEDULER
// ----------------------------------------------------------------------------

const schedulerState = {
  running: false,
  intervalHandle: null,
  config: null,
  runCount: 0,
};

function startScheduler({ targetId, intervalMs, mode = 'random_events', lifecycleDomains = null }) {
  if (schedulerState.running) throw new Error('Scheduler is already running. Stop it before starting a new session.');
  const target = store.targets.get(targetId);
  if (!target) throw new Error(`Unknown target: ${targetId}`);
  const interval = clampInt(intervalMs, CONFIG.schedulerMinIntervalMs, 3600000, 15000);

  schedulerState.config = { targetId, intervalMs: interval, mode, lifecycleDomains };
  schedulerState.running = true;
  schedulerState.runCount = 0;

  log('SCHEDULER', `Starting scheduler target=${targetId} mode=${mode} interval=${interval}ms`);

  let busy = false; // guards against overlapping ticks if one run is slow
  schedulerState.intervalHandle = setInterval(async () => {
    if (busy) {
      log('SCHEDULER', 'Previous scheduler tick still running; skipping this tick to avoid overlap.');
      return;
    }
    busy = true;
    try {
      schedulerState.runCount += 1;
      await runSchedulerTick(target, mode, lifecycleDomains);
    } catch (err) {
      log('ERROR', `Scheduler tick failed: ${err.message}`);
    } finally {
      busy = false;
    }
  }, interval);

  return schedulerState.config;
}

async function runSchedulerTick(target, mode, lifecycleDomains) {
  if (mode === 'random_events') {
    const event = generateRandomEvent();
    const request = buildStandardRequest(event, target);
    await deliverWithRetry(target, event, request);
  } else if (mode === 'random_lifecycles') {
    const domains = lifecycleDomains && lifecycleDomains.length ? lifecycleDomains : Object.keys(LIFECYCLE_DEFINITIONS);
    const domain = pick(domains);
    await runLifecycle({ domain, targetId: target.id, outcome: 'random', mutation: pick(['none', 'none', 'skip', 'out_of_order']), delayMs: 800 });
  } else if (mode === 'security_scenarios') {
    const event = generateRandomEvent();
    const scenario = pick(SECURITY_SCENARIOS);
    const request = buildSecurityScenarioRequest(scenario, event, target);
    await deliverWithRetry(target, event, request);
  } else if (mode === 'duplicate_deliveries') {
    const event = generateRandomEvent();
    await sendDuplicateDeliveries(target, event, { count: randInt(2, 3), variant: pick(['exact', 'same_id_modified_payload', 'same_payload_different_id']), concurrent: randBool(0.4) });
  } else if (mode === 'mixed') {
    const choice = pick(['random_events', 'random_lifecycles', 'security_scenarios', 'duplicate_deliveries']);
    await runSchedulerTick(target, choice, lifecycleDomains);
  } else {
    throw new Error(`Unknown scheduler mode: ${mode}`);
  }
}

function stopScheduler() {
  if (!schedulerState.running) return false;
  clearInterval(schedulerState.intervalHandle);
  schedulerState.intervalHandle = null;
  schedulerState.running = false;
  log('SCHEDULER', `Stopped scheduler after ${schedulerState.runCount} ticks`);
  return true;
}


// ----------------------------------------------------------------------------
// 16. SCENARIO CATALOGUE (for /api/scenarios)
// ----------------------------------------------------------------------------

const SCENARIO_CATALOGUE = {
  beginner: [
    { id: 'basic_json_webhook', description: 'Send a single JSON webhook for a random event type.', endpoint: 'POST /api/simulate/event' },
    { id: 'simple_post_request', description: 'Standard POST delivery with default headers.', endpoint: 'POST /api/simulate/event' },
    { id: 'different_http_methods', description: 'Send the same event via PUT/PATCH/GET instead of POST.', endpoint: 'POST /api/simulate/event (body.method)' },
    { id: 'custom_headers', description: 'Attach custom headers to a delivery.', endpoint: 'POST /api/simulate/event (body.headers)' },
    { id: 'query_parameters', description: 'Append query parameters to the target URL.', endpoint: 'POST /api/simulate/event (body.queryParams)' },
    { id: 'different_content_types', description: 'Send JSON, form-urlencoded, or plain text bodies.', endpoint: 'POST /api/simulate/event (body.contentType)' },
    { id: 'multiple_event_types', description: 'Trigger specific event types across all domains.', endpoint: 'GET /api/events then POST /api/simulate/event' },
  ],
  intermediate: [
    { id: 'hmac_verification', description: 'Send a correctly HMAC-signed request.', endpoint: 'POST /api/simulate/security {"scenario":"valid_hmac"}' },
    { id: 'timestamp_verification', description: 'Send expired/future timestamp signatures.', endpoint: 'POST /api/simulate/security {"scenario":"expired_timestamp"}' },
    { id: 'duplicate_events', description: 'Send exact or modified duplicate deliveries.', endpoint: 'POST /api/simulate/replay or security duplicate scenarios' },
    { id: 'idempotency_testing', description: 'Resend the same event id with the same or different payload.', endpoint: 'POST /api/simulate/replay' },
    { id: 'retry_handling', description: 'Force retryable status codes and observe backoff.', endpoint: 'Configure receiver to return 500/503, then POST /api/simulate/event' },
    { id: 'http_status_handling', description: 'Observe how your receiver responds across many real status codes.', endpoint: 'POST /api/simulate/event' },
    { id: 'delayed_delivery', description: 'Delay delivery by a configurable amount.', endpoint: 'POST /api/simulate/event (body.delayMs)' },
    { id: 'event_lifecycle_processing', description: 'Run a full multi-stage lifecycle (e.g. order.created -> delivered).', endpoint: 'POST /api/simulate/lifecycle' },
    { id: 'multiple_concurrent_events', description: 'Fire several events at once.', endpoint: 'POST /api/simulate/lifecycle (mode=concurrent) or custom batch' },
    { id: 'out_of_order_events', description: 'Shuffle lifecycle stage order.', endpoint: 'POST /api/simulate/lifecycle {"mutation":"out_of_order"}' },
  ],
  advanced: [
    { id: 'replay_attacks', description: 'Replay an old delivery verbatim, including stale signature/timestamp.', endpoint: 'POST /api/simulate/replay {"mode":"exact_replay"}' },
    { id: 'invalid_signature_attacks', description: 'Send wrong/missing/malformed signatures.', endpoint: 'POST /api/simulate/security' },
    { id: 'signature_rotation', description: 'Sign with a rotated secret to test graceful key rollover.', endpoint: 'POST /api/simulate/security {"scenario":"secret_rotation"}' },
    { id: 'event_id_collisions', description: 'Send different payloads under the same event id.', endpoint: 'POST /api/simulate/replay {"mode":"reused_old_signature_new_timestamp"}' },
    { id: 'payload_tampering', description: 'Sign one payload, send a modified one.', endpoint: 'POST /api/simulate/security {"scenario":"modified_payload_after_signing"}' },
    { id: 'race_condition_testing', description: 'Send concurrent duplicate deliveries.', endpoint: 'POST /api/simulate/event (duplicate concurrent variant)' },
    { id: 'network_failures', description: 'Point the target at an unreachable host/port to see real connection failures.', endpoint: 'Configure a target with a bad URL' },
    { id: 'timeout_handling', description: 'Set an aggressive timeoutMs against a slow receiver.', endpoint: 'POST /api/targets (timeoutMs) then simulate' },
    { id: 'retry_exhaustion', description: 'Force maxAttempts retries against an always-failing receiver.', endpoint: 'POST /api/simulate/event with retryOverride' },
    { id: 'malformed_payloads', description: 'Send invalid JSON / wrong types / missing fields.', endpoint: 'POST /api/simulate/security' },
    { id: 'large_payload_handling', description: 'Send a large but bounded JSON payload.', endpoint: 'POST /api/simulate/security {"scenario":"large_payload"}' },
    { id: 'event_ordering_problems', description: 'Random/out-of-order batch delivery.', endpoint: 'POST /api/simulate/lifecycle {"mutation":"out_of_order"}' },
    { id: 'clock_skew', description: 'Future-dated signature timestamps.', endpoint: 'POST /api/simulate/security {"scenario":"future_timestamp"}' },
    { id: 'poison_events', description: 'Unicode/encoding edge cases and deeply nested JSON.', endpoint: 'POST /api/simulate/security {"scenario":"unicode_edge_cases"}' },
    { id: 'dead_letter_tracking', description: 'Inspect exhausted-retry deliveries via the deliveries API.', endpoint: 'GET /api/deliveries?success=false' },
    { id: 'secret_rotation_during_delivery', description: 'Simulate the receiver rotating secrets mid-flight.', endpoint: 'POST /api/simulate/security {"scenario":"secret_rotation"}' },
    { id: 'provider_retry_behavior', description: 'Full retry engine with exponential backoff and Retry-After support.', endpoint: 'POST /api/simulate/event with retryOverride.strategy=exponential' },
  ],
};

// ----------------------------------------------------------------------------
// 17. EXPRESS APP
// ----------------------------------------------------------------------------

const app = express();
app.use(express.json({ limit: `${CONFIG.maxPayloadBytes}b` }));

app.use((req, res, next) => {
  log('SERVER', `${req.method} ${req.path}`);
  next();
});

function requireApiKey(req, res, next) {
  if (!CONFIG.apiKey) {
    return res.status(503).json({ ok: false, error: 'Management API is disabled: MANAGEMENT_API_KEY is not configured on the server.' });
  }
  const provided = req.get('X-API-Key') || (req.get('Authorization') || '').replace(/^Bearer\s+/i, '');
  if (!provided || provided !== CONFIG.apiKey) {
    return res.status(401).json({ ok: false, error: 'Missing or invalid API key. Provide it via X-API-Key header.' });
  }
  next();
}

function sendOk(res, data, status = 200) {
  res.status(status).json({ ok: true, ...data });
}

function sendErr(res, status, message) {
  res.status(status).json({ ok: false, error: message });
}

function asyncRoute(fn) {
  return (req, res) => {
    fn(req, res).catch((err) => {
      log('ERROR', `Route error: ${err.message}`);
      sendErr(res, 400, err.message);
    });
  };
}


// ----------------------------------------------------------------------------
// 18. ROUTES: PUBLIC
// ----------------------------------------------------------------------------

app.get('/health', (req, res) => {
  sendOk(res, {
    status: 'healthy',
    uptime_seconds: process.uptime(),
    started_at: stats.startedAt,
    apiKeyConfigured: Boolean(CONFIG.apiKey),
  });
});

// All routes below require the management API key.
app.use('/api', requireApiKey);

// ----------------------------------------------------------------------------
// 19. ROUTES: TARGETS
// ----------------------------------------------------------------------------

app.post('/api/targets', asyncRoute(async (req, res) => {
  if (store.targets.size >= CONFIG.maxTargets) return sendErr(res, 400, `Maximum of ${CONFIG.maxTargets} targets reached.`);
  const { name, url, defaultMethod, defaultContentType, timeoutMs, authToken, security, retryPolicy } = req.body || {};
  if (!url || typeof url !== 'string') return sendErr(res, 400, 'url is required.');
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    return sendErr(res, 400, 'url is not a valid absolute URL.');
  }
  if (!['http:', 'https:'].includes(parsed.protocol)) return sendErr(res, 400, 'url must use http or https.');

  const target = {
    id: `tgt_${uuid()}`,
    name: name || parsed.hostname,
    url,
    defaultMethod: defaultMethod || 'POST',
    defaultContentType: defaultContentType || 'application/json',
    timeoutMs: clampInt(timeoutMs, 100, CONFIG.maxTimeoutMs, CONFIG.defaultTimeoutMs),
    authToken: authToken || null,
    security: security ? {
      enabled: security.enabled !== false,
      algorithm: security.algorithm === 'sha512' ? 'sha512' : 'sha256',
      secret: security.secret || crypto.randomBytes(24).toString('hex'),
      signatureHeader: security.signatureHeader || 'X-Webhook-Signature',
      timestampHeader: security.timestampHeader || 'X-Webhook-Timestamp',
      signatureFormat: security.signatureFormat || 'sha256={sig}',
      timestampBinding: security.timestampBinding !== false,
    } : { enabled: false },
    retryPolicy: retryPolicy || null,
    created_at: nowIso(),
  };
  store.targets.set(target.id, target);
  log('SERVER', `Created target ${target.id} (${target.name}) -> ${target.url}`);
  sendOk(res, { target: redactTarget(target) }, 201);
}));

app.get('/api/targets', (req, res) => {
  sendOk(res, { targets: [...store.targets.values()].map(redactTarget) });
});

app.delete('/api/targets/:id', (req, res) => {
  const existed = store.targets.delete(req.params.id);
  if (!existed) return sendErr(res, 404, 'Target not found.');
  log('SERVER', `Deleted target ${req.params.id}`);
  sendOk(res, { deleted: true });
});

function redactTarget(target) {
  return {
    ...target,
    authToken: target.authToken ? '[REDACTED]' : null,
    security: target.security ? { ...target.security, secret: target.security.secret ? '[REDACTED]' : null } : target.security,
  };
}

// ----------------------------------------------------------------------------
// 20. ROUTES: EVENTS / DELIVERIES / STATS / SCENARIOS
// ----------------------------------------------------------------------------

app.get('/api/scenarios', (req, res) => {
  sendOk(res, { catalogue: SCENARIO_CATALOGUE, securityScenarioIds: SECURITY_SCENARIOS, eventDomains: Object.keys(EVENT_DOMAINS), eventTypes: listEventTypes() });
});

app.get('/api/events', (req, res) => {
  const limit = clampInt(req.query.limit, 1, 500, 50);
  sendOk(res, { events: store.events.recent(limit), total_stored: store.events.count(), total_generated: stats.eventsGenerated });
});

app.get('/api/deliveries', (req, res) => {
  const limit = clampInt(req.query.limit, 1, 500, 50);
  let items = store.deliveries.recent(limit * 3); // over-fetch then filter
  if (req.query.success === 'true') items = items.filter((d) => d.success);
  if (req.query.success === 'false') items = items.filter((d) => !d.success);
  if (req.query.target_id) items = items.filter((d) => d.target_id === req.query.target_id);
  items = items.slice(0, limit);
  sendOk(res, { deliveries: items, total_stored: store.deliveries.count() });
});

app.get('/api/stats', (req, res) => {
  sendOk(res, {
    stats: { ...stats, currentTargets: store.targets.size, storedEvents: store.events.count(), storedDeliveries: store.deliveries.count(), storedLifecycles: store.lifecycles.count() },
    scheduler: { running: schedulerState.running, config: schedulerState.config, runCount: schedulerState.runCount },
    polling: { running: pollingState.running, config: pollingState.config, pollCount: pollingState.pollCount, missedCount: pollingState.missedCount },
  });
});

// ----------------------------------------------------------------------------
// 21. ROUTES: SIMULATION
// ----------------------------------------------------------------------------

function getTargetOrThrow(targetId) {
  const target = store.targets.get(targetId);
  if (!target) throw new Error(`Unknown target: ${targetId}`);
  return target;
}

app.post('/api/simulate/random', asyncRoute(async (req, res) => {
  const { targetId } = req.body || {};
  const target = getTargetOrThrow(targetId);
  const event = generateRandomEvent();
  const request = buildStandardRequest(event, target);
  const delivery = await deliverWithRetry(target, event, request);
  sendOk(res, { event, delivery });
}));

app.post('/api/simulate/event', asyncRoute(async (req, res) => {
  const {
    targetId, type, domain, method, contentType, headers, queryParams,
    delayMs, retryOverride, duplicate,
  } = req.body || {};
  const target = getTargetOrThrow(targetId);
  const event = createEvent({ type, domain });

  if (delayMs) await sleep(clampInt(delayMs, 0, 120000, 0));

  let url = target.url;
  if (queryParams && typeof queryParams === 'object') {
    const u = new URL(target.url);
    for (const [k, v] of Object.entries(queryParams)) u.searchParams.set(k, String(v));
    url = u.toString();
  }

  if (duplicate && duplicate.count > 1) {
    const results = await sendDuplicateDeliveries(target, event, duplicate);
    return sendOk(res, { event, deliveries: results });
  }

  const request = buildStandardRequest(event, target, { method, contentType, headers, url });
  const delivery = await deliverWithRetry(target, event, request, { retryOverride });
  sendOk(res, { event, delivery });
}));

app.post('/api/simulate/lifecycle', asyncRoute(async (req, res) => {
  const { targetId, domain, outcome, mutation, delayMs, resumeOf, securityScenario } = req.body || {};
  if (!resumeOf && !LIFECYCLE_DEFINITIONS[domain]) {
    return sendErr(res, 400, `domain must be one of: ${Object.keys(LIFECYCLE_DEFINITIONS).join(', ')}`);
  }
  getTargetOrThrow(targetId);
  const lifecycle = await runLifecycle({
    domain, targetId, outcome, mutation,
    delayMs: clampInt(delayMs, 0, 60000, 1000),
    resumeOf, securityScenario,
  });
  sendOk(res, { lifecycle });
}));

app.post('/api/simulate/security', asyncRoute(async (req, res) => {
  const { targetId, scenario, type, domain } = req.body || {};
  const target = getTargetOrThrow(targetId);
  if (!SECURITY_SCENARIOS.includes(scenario)) {
    return sendErr(res, 400, `scenario must be one of: ${SECURITY_SCENARIOS.join(', ')}`);
  }
  const event = createEvent({ type, domain });
  const request = buildSecurityScenarioRequest(scenario, event, target);
  const delivery = await deliverWithRetry(target, event, request);
  sendOk(res, { event, delivery, scenario });
}));

app.post('/api/simulate/replay', asyncRoute(async (req, res) => {
  const { deliveryId, mode } = req.body || {};
  if (!deliveryId) return sendErr(res, 400, 'deliveryId is required.');
  const delivery = await replayDelivery(deliveryId, { mode });
  sendOk(res, { delivery });
}));

app.post('/api/simulate/batch', asyncRoute(async (req, res) => {
  const { targetId, count, domain, mode, concurrencyLimit, delayMs, securityScenario } = req.body || {};
  const target = getTargetOrThrow(targetId);
  const n = clampInt(count, 1, 50, 5);
  const events = Array.from({ length: n }, () => createEvent({ domain }));
  const results = await deliverBatch(target, events, { mode, concurrencyLimit, delayMs, securityScenario });
  sendOk(res, { events, deliveries: results });
}));

// ----------------------------------------------------------------------------
// 22. ROUTES: SCHEDULER
// ----------------------------------------------------------------------------

app.post('/api/scheduler/start', (req, res) => {
  try {
    const config = startScheduler(req.body || {});
    sendOk(res, { scheduler: config });
  } catch (err) {
    sendErr(res, 400, err.message);
  }
});

app.post('/api/scheduler/stop', (req, res) => {
  const stopped = stopScheduler();
  sendOk(res, { stopped });
});

// ----------------------------------------------------------------------------
// 23. ROUTES: POLLING
// ----------------------------------------------------------------------------

app.post('/api/polling/start', (req, res) => {
  try {
    const { targetUrl, intervalMs, queryParams, missRate } = req.body || {};
    if (!targetUrl) return sendErr(res, 400, 'targetUrl is required.');
    const config = startPolling({ targetUrl, intervalMs, queryParams, missRate: Math.min(Math.max(missRate || 0, 0), 0.9) });
    sendOk(res, { polling: config });
  } catch (err) {
    sendErr(res, 400, err.message);
  }
});

app.post('/api/polling/stop', (req, res) => {
  const stopped = stopPolling();
  sendOk(res, { stopped });
});

app.get('/api/polling/results', (req, res) => {
  sendOk(res, { running: pollingState.running, pollCount: pollingState.pollCount, missedCount: pollingState.missedCount, results: pollingState.lastResults });
});

// ----------------------------------------------------------------------------
// 24. ROUTES: CONFIG
// ----------------------------------------------------------------------------

app.get('/api/config', (req, res) => {
  sendOk(res, { config: { ...CONFIG, apiKey: CONFIG.apiKey ? '[CONFIGURED]' : '[NOT SET]' } });
});

app.post('/api/config', (req, res) => {
  // Most config is environment-driven for safety (so a stray API call can't
  // silently loosen limits). This endpoint lets you adjust the handful of
  // values that are safe to change at runtime.
  const { maxRetries, maxConcurrency } = req.body || {};
  if (maxRetries !== undefined) CONFIG.maxRetries = clampInt(maxRetries, 1, 20, CONFIG.maxRetries);
  if (maxConcurrency !== undefined) CONFIG.maxConcurrency = clampInt(maxConcurrency, 1, 50, CONFIG.maxConcurrency);
  log('SERVER', `Runtime config updated: maxRetries=${CONFIG.maxRetries} maxConcurrency=${CONFIG.maxConcurrency}`);
  sendOk(res, { config: { maxRetries: CONFIG.maxRetries, maxConcurrency: CONFIG.maxConcurrency } });
});

// ----------------------------------------------------------------------------
// 25. 404 + ERROR HANDLING
// ----------------------------------------------------------------------------

app.use((req, res) => {
  sendErr(res, 404, `No such route: ${req.method} ${req.path}`);
});

app.use((err, req, res, next) => {
  log('ERROR', `Unhandled error: ${err.message}`);
  if (res.headersSent) return next(err);
  sendErr(res, 500, 'Internal server error.');
});


// ----------------------------------------------------------------------------
// 26. STARTUP, GRACEFUL SHUTDOWN, UNCAUGHT ERRORS
// ----------------------------------------------------------------------------

const server = app.listen(CONFIG.port, '0.0.0.0', () => {
  log('SERVER', `Webhook Testing Laboratory listening on 0.0.0.0:${CONFIG.port}`);
  log('SERVER', `Management API key ${CONFIG.apiKey ? 'is configured' : 'is NOT set - /api/* routes will reject requests'}`);
});

function shutdown(signal) {
  log('SERVER', `Received ${signal}, shutting down gracefully...`);
  stopScheduler();
  stopPolling();
  server.close(() => {
    log('SERVER', 'HTTP server closed. Bye.');
    process.exit(0);
  });
  // Force-exit if close takes too long.
  setTimeout(() => {
    log('ERROR', 'Graceful shutdown timed out, forcing exit.');
    process.exit(1);
  }, 10000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

process.on('uncaughtException', (err) => {
  log('ERROR', `Uncaught exception: ${err.stack || err.message}`);
  // Do not crash the whole process over one bad simulated request - this
  // service's entire purpose is to send deliberately weird requests.
});

process.on('unhandledRejection', (reason) => {
  log('ERROR', `Unhandled rejection: ${reason instanceof Error ? reason.stack : reason}`);
});

export default app;
