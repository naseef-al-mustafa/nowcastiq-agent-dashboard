/**
 * NowcastIQ Lead Intelligence Agent
 * 
 * Runs daily at 9AM UTC. Processes all 5 Attio lists,
 * creates/escalates tasks, flags inconsistencies, and sends digest.
 * 
 * Usage:
 *   node nowcastiq-agent.mjs           # Full daily run
 *   node nowcastiq-agent.mjs --dry-run # Preview only, no writes
 *   node nowcastiq-agent.mjs --list 3.0 # Single list only
 */

import https from 'https';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─────────────────────────────────────────────
// VERSION — bump this on every code change
// ─────────────────────────────────────────────
const VERSION = '1.0.0';

// ─────────────────────────────────────────────
// CREDIT / TOKEN TRACKER
// Haiku pricing (as of 2026): $0.80/M input, $4.00/M output
// ─────────────────────────────────────────────
const CREDIT_TRACKER = {
  calls: 0,
  inputTokens: 0,
  outputTokens: 0,
  // Haiku rates per million tokens
  RATES: { input: 0.80, output: 4.00 },

  add(inputTokens, outputTokens) {
    this.calls++;
    this.inputTokens += (inputTokens || 0);
    this.outputTokens += (outputTokens || 0);
  },

  costUSD() {
    return (
      (this.inputTokens / 1_000_000) * this.RATES.input +
      (this.outputTokens / 1_000_000) * this.RATES.output
    );
  },

  summary() {
    return {
      calls: this.calls,
      inputTokens: this.inputTokens,
      outputTokens: this.outputTokens,
      totalTokens: this.inputTokens + this.outputTokens,
      estimatedCostUSD: parseFloat(this.costUSD().toFixed(6)),
      estimatedCostUSDFormatted: `$${this.costUSD().toFixed(4)}`,
    };
  },

  report() {
    const s = this.summary();
    return [
      `Claude API Usage (this run):`,
      `  Calls made:    ${s.calls}`,
      `  Input tokens:  ${s.inputTokens.toLocaleString()}`,
      `  Output tokens: ${s.outputTokens.toLocaleString()}`,
      `  Total tokens:  ${s.totalTokens.toLocaleString()}`,
      `  Est. cost:     ${s.estimatedCostUSDFormatted} (Haiku @ $0.80/$4.00 per M tokens)`,
    ].join('\n');
  },
};

// ─────────────────────────────────────────────
// CONFIGURATION — fill in before deploying
// ─────────────────────────────────────────────
const CONFIG = {
  attioApiKey: process.env.ATTIO_API_KEY,
  anthropicApiKey: process.env.ANTHROPIC_API_KEY,
  notificationEmail: process.env.NOTIFY_EMAIL || 'naseef@leopardlabs.ai',
  dryRun: process.argv.includes('--dry-run'),
  singleList: process.argv.includes('--list')
    ? process.argv[process.argv.indexOf('--list') + 1]
    : null,
};

// ─────────────────────────────────────────────
// CONFIRMED ATTIO IDs (validated against live workspace)
// ─────────────────────────────────────────────
const ATTIO = {
  // Workspace members
  members: {
    naseef: 'ff838889-41cd-4885-8d1d-6740f74c58e7',
    diana:  '7d506f47-0e0e-4bff-8de4-27742bdaf74c',
    ed:     '154eea89-3ffc-4e74-a46e-160ae54efa3e',
    // Andreas has no workspace account — escalation is a field value only
  },

  // Lists
  lists: {
    directOutreach: 'd3574819-7ce2-475a-8d07-7edcf66e7175',  // 1.0 — People
    newsletter:     '8a538ac4-9d48-42b7-bbe9-a3b4a284eb3c',  // B1  — People
    deals:          'e78c57b4-8de0-482b-9c45-c8e9a534165a',  // 2.0 — Deals
    activeTrials:   '43856dbc-9cc5-41d4-ae0a-d8204e67dad9',  // 3.0 — Trials
    awaitingTrials: '0cc83187-0486-4b05-8547-7895913b2270',  // 3.1 — Trials
  },

  // Segment values → T-tier
  segments: {
    T1: ['Hedge Fund'],
    T2: ['Family Office', 'Asset Management', 'Bank'],
    T3: ['Commodity House', 'Energy Company'],
    // All others (Private Equity, Investment Company, Investment Bank,
    // Pension Fund, Digital Assets, Consultants, Fintech, Financial Media,
    // Education, Venture Capital, No Segment, Other) → no tier, email loops only
  },

  // Trial health status IDs
  trialHealth: {
    neutral:  '209d4232-e46b-4650-915b-e072d648ee81',
    healthy:  'c295ebb2-8b84-4b30-8607-a58f0e243381',
    atRisk:   '23587ca4-e3d5-43a4-adf8-5265b1ed8299',
    critical: 'f84494e7-1495-4530-a1d1-ef8d08f03e5c',
  },

  // Trial result status IDs
  trialResult: {
    active:         'fd1df13b-d807-46ea-a125-de9d51ffaa93',
    awaitingOutcome:'43f48b9b-3e13-4369-a7df-1e22906aa84b',
    won:            '07365610-09bf-45fe-84b3-d5cb71b635c2',
    lost:           '0998de05-5c72-48bb-b97e-a64e568c5672',
    cancelled:      '65721935-caa1-473a-8e75-d8b3abf97851', // skip processing
  },

  // User activation stage IDs
  activationStage: {
    invited:    '17691c9b-747a-4a50-ab2f-a0ad60f83cb8',
    signedUp:   '9bd1b7e0-1245-47cd-94a7-2da54c5166b2',
    activated:  '868cb846-8005-43b3-b812-7331db8100c2',
    adopted:    '5dca49c0-90b8-4b8b-b522-1102dcbd41f5',
    embedded:   '496b445d-fcee-41da-a8ca-7dabf8654d23',
    inactive:   'b7807943-ee3f-416b-8ac8-df13999b40ae',
  },

  // Deal stage IDs
  dealStage: {
    unresponsive:    '3b317a9b-eef7-4dea-8761-643090597c14',
    discovery:       '757ed2ef-34df-4238-beb1-7510d2208698',
    trialStarted:    '825eb109-4428-4ad5-9d3e-7e82ee24bfe5',
    trialEnded:      '04122de4-34e9-4f31-8762-6964bbfd33bd',
    procurement:     '8c9dfa9b-7402-4bfe-b22e-b2e366367af3',
    dealWon:         '25b7cc01-a9cd-40d0-a38f-599c873f45cc',
    expansion:       '50d886cc-4698-434b-b7de-d64e5418fe1d',
    dealLost:        '7ebd9a02-a2bc-4057-9af2-76a55e91d14c',
  },
};

// ─────────────────────────────────────────────
// OVERRIDES STORE
// Persists confirmed "deliberate override" records so agent
// never re-flags them. Stored in overrides.json next to script.
// ─────────────────────────────────────────────
const OVERRIDES_FILE = path.join(__dirname, 'overrides.json');

// ─────────────────────────────────────────────
// RULES — loaded from rules.json on every run
// Edit rules.json to change behaviour without touching code.
// See RULES_GUIDE.md for documentation.
// ─────────────────────────────────────────────
const RULES_FILE = path.join(__dirname, 'rules.json');

function loadRules() {
  try {
    if (fs.existsSync(RULES_FILE)) {
      const raw = JSON.parse(fs.readFileSync(RULES_FILE, 'utf8'));
      console.log(`✅ Rules loaded from rules.json (v${raw._version || 'unknown'})`);
      return raw;
    }
  } catch (e) {
    console.warn(`⚠️  Could not load rules.json: ${e.message}. Using built-in defaults.`);
  }
  // Built-in defaults — match what's in rules.json
  return {
    trials: {
      callForDemo:       { enabled: true, triggerWithinDays: 3,  owner: 'diana',  priority: 'URGENT' },
      midPointCheck:     { enabled: true, triggerOnDayMin: 6, triggerOnDayMax: 8, owner: 'naseef', priority: 'HIGH' },
      feedbackAndPricing:{ enabled: true, triggerDaysBeforeEnd: 3, owner: 'diana', priority: 'HIGH' },
      finalCheckIn:      { enabled: true, triggerDaysBeforeEnd: 2, owner: 'naseef', priority: 'URGENT' },
      escalation:        { staleTaskDays: 5, considerClosingDays: 10 },
    },
    deals: {
      trackProcurement: { enabled: true, owner: 'naseef', priority: 'HIGH',   defaultDueDays: 7 },
      trackExpansion:   { enabled: true, owner: 'naseef', priority: 'MEDIUM', defaultDueDays: 14 },
      reengagement:     { enabled: true, owner: 'naseef', priority: 'MEDIUM' },
      closingSoon:      { enabled: true, triggerWithinDays: 7, owner: 'naseef', priority: 'HIGH' },
    },
    leads: {
      directOutreach: { enabled: true, owner: 'naseef', priority: 'HIGH' },
      newsletter:     { enabled: true, owner: 'naseef', priority: 'HIGH' },
    },
    universal: {
      noTeamActivityDays: 14, noTeamActivityOwner: 'naseef', noTeamActivityPriority: 'MEDIUM',
    },
  };
}

// Resolve owner name to workspace member ID
function ownerToId(ownerName) {
  const map = { naseef: ATTIO.members.naseef, diana: ATTIO.members.diana, ed: ATTIO.members.ed };
  return map[ownerName?.toLowerCase()] || ATTIO.members.naseef;
}

// Module-level rules — loaded once per run at top of runDailySync
let RULES = null;

function loadOverrides() {
  try {
    if (fs.existsSync(OVERRIDES_FILE)) {
      return JSON.parse(fs.readFileSync(OVERRIDES_FILE, 'utf8'));
    }
  } catch (e) {
    console.warn('Warning: could not load overrides.json:', e.message);
  }
  return {};
}

function saveOverride(listId, recordId, reason) {
  const overrides = loadOverrides();
  if (!overrides[listId]) overrides[listId] = {};
  overrides[listId][recordId] = {
    confirmedAt: new Date().toISOString(),
    reason: reason || 'manual override confirmed',
  };
  if (!CONFIG.dryRun) {
    fs.writeFileSync(OVERRIDES_FILE, JSON.stringify(overrides, null, 2));
  }
  console.log(`  ✅ Override saved: ${recordId} in list ${listId}`);
}

function isOverrideConfirmed(listId, recordId) {
  const overrides = loadOverrides();
  return !!(overrides[listId]?.[recordId]);
}

// ─────────────────────────────────────────────
// RUN LOG
// Writes a structured JSON log per run to ./logs/
// Used for error monitoring and pilot review.
// ─────────────────────────────────────────────
const LOGS_DIR = path.join(__dirname, 'logs');

function initRunLog() {
  if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });
  return {
    runId: `run-${Date.now()}`,
    startedAt: new Date().toISOString(),
    completedAt: null,
    status: 'running',  // running | completed | failed
    listsProcessed: {},
    totalTasksCreated: 0,
    totalFlagsRaised: 0,
    totalOutOfCriteriaFlags: 0,
    totalErrors: 0,
    errors: [],
    warnings: [],
    dryRun: CONFIG.dryRun,
  };
}

function writeRunLog(runLog) {
  if (CONFIG.dryRun) return;
  try {
    const filename = path.join(LOGS_DIR, `${runLog.runId}.json`);
    fs.writeFileSync(filename, JSON.stringify(runLog, null, 2));

    // Also maintain a rolling summary (last 14 runs) for pilot review
    const summaryFile = path.join(LOGS_DIR, 'pilot-summary.json');
    let summary = [];
    if (fs.existsSync(summaryFile)) {
      summary = JSON.parse(fs.readFileSync(summaryFile, 'utf8'));
    }
    summary.unshift({
      runId: runLog.runId,
      date: runLog.startedAt?.split('T')[0],
      status: runLog.status,
      tasks: runLog.totalTasksCreated,
      flags: runLog.totalFlagsRaised,
      outOfCriteria: runLog.totalOutOfCriteriaFlags,
      errors: runLog.totalErrors,
    });
    summary = summary.slice(0, 14); // keep 14 runs
    fs.writeFileSync(summaryFile, JSON.stringify(summary, null, 2));
  } catch (e) {
    console.warn('Warning: could not write run log:', e.message);
  }
}

// ─────────────────────────────────────────────
// CRITERIA VALIDATORS
// Each list has a function that checks whether a given record
// actually meets the criteria for being in that list.
// Returns { valid: bool, reason: string }
// ─────────────────────────────────────────────
const CRITERIA_VALIDATORS = {

  // List 1.0: Must be a person with company segment T1/T2/T3 AND lead_status = Connected
  directOutreach: async (record, recordId) => {
    const leadStatus = getAttrValue(record, 'lead_status');
    if (leadStatus !== 'Connected') {
      return { valid: false, reason: `lead_status is "${leadStatus}", expected "Connected"` };
    }
    // Check linked company segment
    const companyRef = record?.values?.company;
    const companyId = Array.isArray(companyRef)
      ? companyRef[0]?.target_record_id
      : companyRef?.target_record_id;
    if (!companyId) {
      return { valid: false, reason: 'No linked company — cannot verify segment' };
    }
    const company = await getRecord('companies', companyId);
    const segment = getAttrValue(company, 'segment');
    const tier = getSegmentTier(segment);
    if (!tier) {
      return { valid: false, reason: `Company segment "${segment}" is not T1/T2/T3` };
    }
    return { valid: true, reason: `${tier} (${segment}) + Connected` };
  },

  // List B1: Newsletter invitation — any person is valid (Naseef controls manually)
  newsletter: async (record, recordId) => {
    return { valid: true, reason: 'B1 membership is manually controlled by Naseef' };
  },

  // List 2.0: Must be a deal in Commercial/Procurement, Expansion/Resigning, or Deal Lost
  deals: async (record, recordId) => {
    const stage = getAttrValue(record, 'stage');
    const VALID_STAGES = ['Commercial/Procurement', 'Expansion/Resigning', 'Deal Lost'];
    if (!VALID_STAGES.includes(stage)) {
      return { valid: false, reason: `Deal stage is "${stage}", expected one of: ${VALID_STAGES.join(', ')}` };
    }
    return { valid: true, reason: `Stage: ${stage}` };
  },

  // List 3.0: Must be a trial with result = Active
  activeTrials: async (record, recordId) => {
    const result = getAttrValue(record, 'trial_result_3');
    if (result !== 'Active') {
      return { valid: false, reason: `trial_result_3 is "${result}", expected "Active"` };
    }
    return { valid: true, reason: 'Active trial' };
  },

  // List 3.1: Must be a trial with result = Awaiting Outcome
  awaitingTrials: async (record, recordId) => {
    const result = getAttrValue(record, 'trial_result_3');
    if (result !== 'Awaiting Outcome') {
      return { valid: false, reason: `trial_result_3 is "${result}", expected "Awaiting Outcome"` };
    }
    return { valid: true, reason: 'Awaiting Outcome trial' };
  },
};

// Check a record against its list's criteria. If it fails and isn't
// already confirmed as a deliberate override, create a one-time flag task.
async function checkCriteriaAndFlagIfNeeded(listKey, listId, objectSlug, recordId, recordName, runLog) {
  // Already confirmed override — treat as normal
  if (isOverrideConfirmed(listId, recordId)) return true;

  const validatorKey = {
    '1.0': 'directOutreach',
    'B1':  'newsletter',
    '2.0': 'deals',
    '3.0': 'activeTrials',
    '3.1': 'awaitingTrials',
  }[listKey];

  if (!validatorKey || !CRITERIA_VALIDATORS[validatorKey]) return true;

  let record;
  try {
    record = await getRecord(objectSlug, recordId);
  } catch (e) {
    console.warn(`  ⚠️  Could not fetch record ${recordId} for criteria check: ${e.message}`);
    return true; // fail open — don't block processing
  }

  const { valid, reason } = await CRITERIA_VALIDATORS[validatorKey](record, recordId);

  if (!valid) {
    console.log(`  ⚠️  Out-of-criteria: ${recordName} in List ${listKey} — ${reason}`);
    runLog.totalOutOfCriteriaFlags++;
    runLog.warnings.push({ type: 'out_of_criteria', list: listKey, record: recordName, reason });

    await createTask(
      `[AGENT] ⚠️ Out-of-Criteria Entry — Please Review\n\nRecord: ${recordName}\nList: ${listKey}\nReason does not match criteria: ${reason}\n\nIs this a deliberate override?\n• If YES: reply "override confirmed for ${recordId}" in agent chat — it will never be flagged again\n• If NO: remove the entry from List ${listKey} in Attio\n\nThis flag will only appear ONCE per record.`,
      ATTIO.members.naseef,
      isoDateInDays(1),
      objectSlug,
      recordId
    );

    // Still return true — process the record normally even while awaiting confirmation
    return true;
  }

  return true;
}
async function attioRequest(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'api.attio.com',
      path: `/v2${path}`,
      method,
      headers: {
        'Authorization': `Bearer ${CONFIG.attioApiKey}`,
        'Content-Type': 'application/json',
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (res.statusCode >= 400) {
            reject(new Error(`Attio API error ${res.statusCode}: ${JSON.stringify(parsed)}`));
          } else {
            resolve(parsed);
          }
        } catch (e) {
          reject(new Error(`Failed to parse response: ${data}`));
        }
      });
    });

    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

// Paginate through all entries in a list
async function getAllListEntries(listId) {
  const entries = [];
  let offset = 0;
  const limit = 50;

  while (true) {
    const res = await attioRequest('GET', `/lists/${listId}/entries?limit=${limit}&offset=${offset}`);
    entries.push(...(res.data || []));
    if (!res.has_more) break;
    offset += limit;
  }

  return entries;
}

// Fetch a single record by object + ID
async function getRecord(objectSlug, recordId) {
  const res = await attioRequest('GET', `/objects/${objectSlug}/records/${recordId}`);
  return res.data;
}

// Update a list entry's attributes
async function updateListEntry(listId, entryId, attributes) {
  if (CONFIG.dryRun) {
    console.log(`[DRY RUN] Would update entry ${entryId}:`, JSON.stringify(attributes, null, 2));
    return;
  }
  await attioRequest('PATCH', `/lists/${listId}/entries/${entryId}`, { data: { values: attributes } });
}

// ─────────────────────────────────────────────
// ROLLBACK JOURNAL
// Every task created is logged to rollback/<run-id>.json
// Run `node nowcastiq-agent.mjs --rollback <run-id>` to undo a run.
// ─────────────────────────────────────────────
const ROLLBACK_DIR = path.join(__dirname, 'rollback');
let CURRENT_RUN_ID = null; // set at start of runDailySync

function logTaskToRollback(taskId, content, linkedObject, linkedRecordId) {
  if (CONFIG.dryRun || !CURRENT_RUN_ID || !taskId) return;
  try {
    if (!fs.existsSync(ROLLBACK_DIR)) fs.mkdirSync(ROLLBACK_DIR, { recursive: true });
    const file = path.join(ROLLBACK_DIR, `${CURRENT_RUN_ID}.json`);
    let journal = [];
    if (fs.existsSync(file)) journal = JSON.parse(fs.readFileSync(file, 'utf8'));
    journal.push({
      taskId,
      createdAt: new Date().toISOString(),
      contentPreview: content.substring(0, 80),
      linkedObject: linkedObject || null,
      linkedRecordId: linkedRecordId || null,
    });
    fs.writeFileSync(file, JSON.stringify(journal, null, 2));
  } catch (e) {
    console.warn('Warning: could not write rollback journal:', e.message);
  }
}

// Create an Attio task
async function createTask(content, assigneeId, deadlineAt, linkedObject, linkedRecordId) {
  if (CONFIG.dryRun) {
    console.log(`[DRY RUN] Would create task: "${content.substring(0, 60)}..." → assignee: ${assigneeId}`);
    return { task_id: 'dry-run-id' };
  }

  const body = {
    data: {
      content,
      is_completed: false,
      ...(assigneeId && { assignees: [{ workspace_member_id: assigneeId }] }),
      ...(deadlineAt && { deadline_at: deadlineAt }),
      ...(linkedObject && linkedRecordId && {
        linked_records: [{ target_object: linkedObject, target_record_id: linkedRecordId }],
      }),
    },
  };

  const res = await attioRequest('POST', '/tasks', body);
  const taskId = res.data?.task_id || res.task_id;

  // Log to rollback journal
  logTaskToRollback(taskId, content, linkedObject, linkedRecordId);

  return res.data || res;
}

// Get all open tasks linked to a record
async function getOpenTasksForRecord(objectSlug, recordId) {
  const res = await attioRequest(
    'GET',
    `/tasks?linked_record_object=${objectSlug}&linked_record_id=${recordId}&is_completed=false&limit=50`
  );
  return res.data || [];
}

// ─────────────────────────────────────────────
// CLAUDE API CLIENT
// ─────────────────────────────────────────────
async function claudeAnalyse(systemPrompt, userContent, maxTokens = 1000) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: maxTokens,
      system: systemPrompt,
      messages: [{ role: 'user', content: userContent }],
    });

    const options = {
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-api-key': CONFIG.anthropicApiKey,
        'anthropic-version': '2023-06-01',
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          // Track token usage
          if (parsed.usage) {
            CREDIT_TRACKER.add(parsed.usage.input_tokens, parsed.usage.output_tokens);
          }
          resolve(parsed.content?.[0]?.text || '');
        } catch (e) {
          reject(new Error(`Failed to parse Claude response: ${data}`));
        }
      });
    });

    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function getSegmentTier(segmentValue) {
  if (ATTIO.segments.T1.includes(segmentValue)) return 'T1';
  if (ATTIO.segments.T2.includes(segmentValue)) return 'T2';
  if (ATTIO.segments.T3.includes(segmentValue)) return 'T3';
  return null; // No tier — email loops only
}

// ─────────────────────────────────────────────
// PRODUCT USAGE CONTEXT
// Fetches usage events from the product_usage object.
// last_activity_date and signup date are read from the
// user record (or people record for non-trialists) directly.
//
// Matching rules (confirmed from live data):
//   all events → match by user_6 record reference (user record_id)
//   Signup event has no user_6 link — not queried here
//
// Signup date sources:
//   - users object: user_signup_date (api_slug confirmed)
//   - people object: user_signup_date (api_slug, titled "User Demo Date")
//     → used for leads who haven't started a trial (no user record yet)
// ─────────────────────────────────────────────

// users.product_usage object ID (confirmed live)
const PRODUCT_USAGE_OBJECT_ID = '4a9abd41-2596-46d5-948e-98a10a8cd928';

// Events we care about for lead/trial context
const USAGE_EVENTS_OF_INTEREST = [
  'Session Started',
  'Article Viewed',
  'Email Viewed',
  'Portfolio Viewed',
];

async function getProductUsageContext(userId, userRecord) {
  // userId      = Attio users record_id (for product_usage matching via user_6)
  // userRecord  = already-fetched user record (to read last_activity_date + user_signup_date directly)
  // NOTE: signup date and last activity are stored on the user record itself —
  //       no need to query product_usage for those fields.

  const context = {
    sessions: [],
    articlesViewed: [],
    emailsViewed: 0,
    portfolioViewed: 0,
    signupDate: getAttrValue(userRecord, 'user_signup_date'),
    lastActivityDate: getAttrValue(userRecord, 'last_activity_date'),
    totalEvents: 0,
    raw: [],
  };

  try {
    // ── Query: All activity events by user_6 record reference ────────
    // Covers: Session Started, Article Viewed, Email Viewed, Portfolio Viewed
    // Signup is NOT queried here — date already read from user record above.
    let offset = 0;
    const limit = 50;
    let hasMore = true;
    const allEvents = [];

    const userFilter = {
      attribute: 'user_6',
      op: 'eq',
      value: {
        object_id: PRODUCT_USAGE_OBJECT_ID,
        record_id: userId,
      },
    };

    while (hasMore) {
      const res = await attioRequest(
        'POST',
        `/objects/product_usage/records/query`,
        {
          filter: userFilter,
          limit,
          offset,
          sorts: [{ attribute: 'created_at', direction: 'desc' }],
        }
      );
      const records = res.data || [];
      allEvents.push(...records);
      hasMore = res.has_more && records.length === limit;
      offset += limit;
      // Cap at 200 events per user to avoid runaway queries
      if (allEvents.length >= 200) break;
    }

    context.totalEvents = allEvents.length;
    context.raw = allEvents;

    // ── Parse events into structured context ────────────────────────
    // lastActivityDate already populated from user.last_activity_date above.
    // signupDate already populated from user.user_signup_date above.

    for (const record of allEvents) {
      const attrs = record.attributes || {};
      const event = attrs.event;
      const createdAt = attrs.created_at;
      const dateStr = typeof createdAt === 'string'
        ? createdAt.replace(/^[A-Za-z]+,\s*/, '').split(' ')[0]
        : null;

      switch (event) {
        case 'Session Started':
          context.sessions.push({
            date: attrs.session_start_date || dateStr,
            sourceUrl: attrs.source_url || null,
            utmSource: attrs.utm_source || null,
            utmCampaign: attrs.utm_campaign || null,
          });
          break;

        case 'Article Viewed':
          context.articlesViewed.push({
            date: dateStr,
            articleName: attrs.article_name || 'Unknown article',
            articleCategory: attrs.article_category || null,
            utmSource: attrs.utm_source || null,
          });
          break;

        case 'Email Viewed':
          context.emailsViewed++;
          break;

        case 'Portfolio Viewed':
          context.portfolioViewed++;
          break;
      }
    }

    // lastActivityDate already set from user record — no override needed here

    // ── Deduplicate articles (same article viewed multiple times) ───
    const articleCounts = {};
    for (const a of context.articlesViewed) {
      const key = a.articleName;
      if (!articleCounts[key]) {
        articleCounts[key] = { ...a, viewCount: 0 };
      }
      articleCounts[key].viewCount++;
    }
    context.articlesViewed = Object.values(articleCounts)
      .sort((a, b) => b.viewCount - a.viewCount)
      .slice(0, 10); // top 10 most viewed

    // Sort sessions newest first, keep last 10
    context.sessions = context.sessions
      .sort((a, b) => (b.date || '') > (a.date || '') ? 1 : -1)
      .slice(0, 10);

  } catch (err) {
    console.warn(`  ⚠️  Could not fetch product usage for user ${userId}: ${err.message}`);
  }

  return context;
}

// Format product usage context as a concise summary string for task descriptions
function formatUsageSummary(usage) {
  if (!usage || usage.totalEvents === 0) return 'No product usage data found.';

  const lines = [];

  if (usage.signupDate) lines.push(`📅 Signed up: ${usage.signupDate}`);
  if (usage.lastActivityDate) lines.push(`🕐 Last activity: ${usage.lastActivityDate}`);

  if (usage.sessions.length > 0) {
    const sessionDates = [...new Set(usage.sessions.map(s => s.date).filter(Boolean))].slice(0, 5);
    lines.push(`📊 Sessions: ${usage.sessions.length} total (recent: ${sessionDates.join(', ')})`);

    // Highlight email-driven sessions
    const emailSessions = usage.sessions.filter(s => s.utmSource === 'notifications' || s.utmMedium === 'email');
    if (emailSessions.length > 0) {
      lines.push(`   └─ ${emailSessions.length} session(s) driven by email notifications`);
    }
  } else {
    lines.push(`📊 Sessions: 0 (user has not started any sessions)`);
  }

  if (usage.articlesViewed.length > 0) {
    lines.push(`📰 Articles viewed: ${usage.articlesViewed.length} unique`);
    const topArticles = usage.articlesViewed.slice(0, 3);
    for (const a of topArticles) {
      const name = a.articleName.replace(' | NowcastIQ', '').substring(0, 60);
      lines.push(`   └─ "${name}"${a.viewCount > 1 ? ` (×${a.viewCount})` : ''}`);
    }
  } else {
    lines.push(`📰 Articles: none viewed`);
  }

  if (usage.emailsViewed > 0) lines.push(`✉️  Emails opened: ${usage.emailsViewed}`);
  if (usage.portfolioViewed > 0) lines.push(`💼 Portfolio views: ${usage.portfolioViewed}`);

  return lines.join('\n');
}

function getAttrValue(record, slug) {
  if (!record?.values) return null;
  const attr = record.values[slug];
  if (attr === undefined || attr === null) return null;

  // Attio REST API returns attributes as arrays of value objects
  if (Array.isArray(attr)) {
    if (attr.length === 0) return null;
    const val = attr[0];
    if (val?.status?.title) return val.status.title;
    if (val?.status?.id)    return val.status.id;
    if (val?.option?.title) return val.option.title;
    if (val?.option?.id)    return val.option.id;
    if (val?.target_record_id) return val.target_record_id;
    if (val?.workspace_membership_id) return val.workspace_membership_id;
    if (val?.value !== undefined) return val.value;
    if (typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean') return val;
    return null;
  }

  // Some fields return flat values (e.g. from get-records-by-ids MCP tool)
  if (typeof attr === 'string' || typeof attr === 'number' || typeof attr === 'boolean') return attr;
  if (attr?.status?.title) return attr.status.title;
  if (attr?.option?.title) return attr.option.title;
  if (attr?.target_record_id) return attr.target_record_id;
  if (attr?.workspace_membership_id) return attr.workspace_membership_id;
  return null;
}

// Extract linked record IDs from a record attribute (handles both array and single)
function getLinkedRecordIds(record, slug) {
  if (!record?.values) return [];
  const attr = record.values[slug];
  if (!attr) return [];
  const arr = Array.isArray(attr) ? attr : [attr];
  return arr
    .map(v => v?.target_record_id || v?.record_id)
    .filter(Boolean);
}

function daysAgo(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  return Math.floor((Date.now() - d.getTime()) / 86400000);
}

function daysUntil(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  return Math.ceil((d.getTime() - Date.now()) / 86400000);
}

function isoDateInDays(days) {
  const d = new Date(Date.now() + days * 86400000);
  return d.toISOString();
}

function today() {
  return new Date().toISOString().split('T')[0];
}

// ─────────────────────────────────────────────
// TASK DEDUPLICATION
// Check if an open outreach task already exists for a record.
// If yes, may escalate instead of creating new.
// ─────────────────────────────────────────────
async function checkAndEscalateIfStale(objectSlug, recordId, recordName, taskTypeLabel) {
  const openTasks = await getOpenTasksForRecord(objectSlug, recordId);
  if (openTasks.length === 0) return false;

  // Read escalation thresholds from RULES if loaded, otherwise use defaults
  const staledays = RULES?.trials?.escalation?.staleTaskDays ?? 5;
  const closeDays = RULES?.trials?.escalation?.considerClosingDays ?? 10;

  for (const task of openTasks) {
    const createdDaysAgo = daysAgo(task.created_at);
    if (createdDaysAgo === null) continue;

    if (createdDaysAgo >= closeDays) {
      const updatedContent = `${task.content}\n\n⚠️ [AUTO] No response after ${closeDays} days (${today()}). Consider closing this lead.`;
      if (!CONFIG.dryRun) {
        await attioRequest('PATCH', `/tasks/${task.task_id}`, { data: { content: updatedContent } });
      }
      console.log(`  ↑ Escalated (day ${closeDays}) existing task for ${recordName}`);
      return true;
    } else if (createdDaysAgo >= staledays) {
      const updatedContent = `${task.content}\n\n🔴 [AUTO] No response after ${staledays} days (${today()}). Follow up urgently.`;
      if (!CONFIG.dryRun) {
        await attioRequest('PATCH', `/tasks/${task.task_id}`, { data: { content: updatedContent } });
      }
      console.log(`  ↑ Escalated (day ${staledays}) existing task for ${recordName}`);
      return true;
    }

    console.log(`  → Skipping new task for ${recordName}: existing open task is ${createdDaysAgo}d old`);
    return true;
  }

  return false;
}

// ─────────────────────────────────────────────
// PEOPLE SIGNUP DATE (for non-users in Lists 1.0 / B1)
// Signup date for non-users is stored on the people record as "joined date".
// The exact slug needs confirming — agent tries known candidates gracefully.
// UPDATE PEOPLE_SIGNUP_DATE_SLUG below once you confirm the field name in Attio.
// ─────────────────────────────────────────────
const PEOPLE_SIGNUP_DATE_SLUG = 'date_joined'; // ⚠️ CONFIRM: check your Attio people fields

function getPeopleSignupDate(personRecord) {
  // Try confirmed slug first, then fallback candidates
  const candidates = [PEOPLE_SIGNUP_DATE_SLUG, 'date_joined', 'joined_date', 'signup_date', 'created_date'];
  for (const slug of candidates) {
    const val = getAttrValue(personRecord, slug);
    if (val) return { date: val, slug };
  }
  return null;
}

// ─────────────────────────────────────────────
// LIST 1.0 — Direct Outreach (People)
// Rule: one task per entry, ever. Notify if completed but still in list.
// ─────────────────────────────────────────────
async function processDirectOutreach(runLog) {
  console.log('\n📋 LIST 1.0 — Direct Outreach');
  const entries = await getAllListEntries(ATTIO.lists.directOutreach);
  const results = { tasks: [], flags: [] };
  runLog.listsProcessed['1.0'] = { entries: entries.length, tasks: 0, flags: 0 };

  for (const entry of entries) {
    const personId = entry.parent_record?.record_id;
    if (!personId) continue;

    const person = await getRecord('people', personId);
    const name = getAttrValue(person, 'name') || personId;

    // Criteria check — first time only per record
    await checkCriteriaAndFlagIfNeeded('1.0', ATTIO.lists.directOutreach, 'people', personId, name, runLog);
    const followUpStatus = entry.values?.follow_up_status?.[0]?.option?.title;
    const messageSent = entry.values?.message_sent?.[0]?.value;

    // Check for existing open tasks
    const openTasks = await getOpenTasksForRecord('people', personId);
    const hasOpenTask = openTasks.length > 0;

    if (followUpStatus === 'Closed') {
      // Flag: closed outreach but still in list
      results.flags.push({
        severity: 'MEDIUM',
        message: `${name} — outreach closed but still in List 1.0. Remove entry or create new task manually.`,
        recordId: personId,
        object: 'people',
      });
      console.log(`  ⚠️  Flag: ${name} — outreach closed, still in list`);
      continue;
    }

    // Skip if already has open task
    if (hasOpenTask) {
      const escalated = await checkAndEscalateIfStale('people', personId, name, 'Direct Outreach');
      if (escalated) continue;
    }

    // Create Direct Outreach task — include signup date context for non-users
    const signupInfo = getPeopleSignupDate(person);
    const signupLine = signupInfo
      ? `📅 Joined NowcastIQ: ${signupInfo.date}`
      : '📅 Joined NowcastIQ: unknown (no date_joined field found — confirm slug)';

    const task = await createTask(
      `[List 1.0] Direct Outreach — ${name}\nPersona in list. Initiate direct contact.\n\nAttio entry status: ${followUpStatus || 'To Be Contacted'}\n${signupLine}\nLead status: ${getAttrValue(person, 'lead_status') || 'unknown'}\nLead intent: ${getAttrValue(person, 'lead_intent') || 'unknown'}`,
      ATTIO.members.naseef,
      isoDateInDays(1),
      'people',
      personId
    );

    results.tasks.push({ type: 'Direct Outreach', name, priority: 'HIGH', owner: 'Naseef' });
    console.log(`  ✅ Task: Direct Outreach → ${name}`);
  }

  return results;
}

// ─────────────────────────────────────────────
// LIST B1 — Newsletter (People)
// Rule: flag if Signal Response = "interested" type. Naseef ONLY.
// ─────────────────────────────────────────────
async function processNewsletter(runLog) {
  console.log('\n📋 LIST B1 — Newsletter Invitation');
  const entries = await getAllListEntries(ATTIO.lists.newsletter);
  const results = { tasks: [], flags: [] };
  runLog.listsProcessed['B1'] = { entries: entries.length, tasks: 0, flags: 0 };

  const POSITIVE_RESPONSES = [
    'Replied — asked market question',
    'Replied — requested brief · Replied — booked call',
    'Replied — forwarded internally',
  ];

  for (const entry of entries) {
    const personId = entry.parent_record?.record_id;
    if (!personId) continue;

    const person = await getRecord('people', personId);
    const name = getAttrValue(person, 'name') || personId;

    await checkCriteriaAndFlagIfNeeded('B1', ATTIO.lists.newsletter, 'people', personId, name, runLog);
    const signalResponse = entry.values?.signal_response?.[0]?.option?.title;
    const followUpStatus = entry.values?.follow_up_status?.[0]?.option?.title;
    const messageSent = entry.values?.message_sent?.[0]?.value;

    // Only create task if they responded positively or haven't been contacted
    if (!messageSent) {
      // Not yet messaged — standard newsletter outreach task
      const openTasks = await getOpenTasksForRecord('people', personId);
      if (openTasks.length === 0) {
        await createTask(
          `[B1] Newsletter Invitation — ${name}\nSend Macro Brief newsletter invitation. Message not yet sent.\n\nOwner: Naseef ONLY`,
          ATTIO.members.naseef,
          isoDateInDays(1),
          'people',
          personId
        );
        results.tasks.push({ type: 'Newsletter Invitation', name, priority: 'HIGH', owner: 'Naseef' });
        console.log(`  ✅ Task: Newsletter → ${name} (not yet messaged)`);
      }
      continue;
    }

    if (POSITIVE_RESPONSES.includes(signalResponse)) {
      // Positive signal — flag for Naseef follow-up
      const openTasks = await getOpenTasksForRecord('people', personId);
      if (openTasks.length === 0) {
        await createTask(
          `[B1] Newsletter Follow-up — ${name}\nPositive response detected: "${signalResponse}"\nFollow up to progress the conversation.\n\nOwner: Naseef ONLY`,
          ATTIO.members.naseef,
          isoDateInDays(1),
          'people',
          personId
        );
        results.tasks.push({ type: 'Newsletter Follow-up', name, priority: 'HIGH', owner: 'Naseef' });
        console.log(`  ✅ Task: Newsletter Follow-up → ${name} (${signalResponse})`);
      }
    }
  }

  return results;
}

// ─────────────────────────────────────────────
// LIST 2.0 — Deals (Expansion / Procurement / Churned)
// ─────────────────────────────────────────────
async function processDeals(runLog) {
  console.log('\n📋 LIST 2.0 — Deals');
  const entries = await getAllListEntries(ATTIO.lists.deals);
  const results = { tasks: [], flags: [] };
  runLog.listsProcessed['2.0'] = { entries: entries.length, tasks: 0, flags: 0 };

  for (const entry of entries) {
    const dealId = entry.parent_record?.record_id;
    if (!dealId) continue;

    // Fetch parent deal to get stage
    const deal = await getRecord('deals', dealId);
    const dealName = getAttrValue(deal, 'name') || dealId;
    const stage = getAttrValue(deal, 'stage');
    const stageTitle = stage || '';

    await checkCriteriaAndFlagIfNeeded('2.0', ATTIO.lists.deals, 'deals', dealId, dealName, runLog);
    const estimatedClose = getAttrValue(deal, 'estimated_close_date');
    const nextActionDue = getAttrValue(entry, 'next_action_due_date');
    const taskStatus = getAttrValue(entry, 'task_status');
    const comments = getAttrValue(entry, 'comments') || '';

    console.log(`  Processing deal: ${dealName} (stage: ${stageTitle})`);

    // Skip non-relevant stages
    const SKIP_STAGES = ['Discovery', 'Trial Started', 'Trial Ended', 'Unresponsive/Aged'];
    if (SKIP_STAGES.includes(stageTitle)) {
      console.log(`  → Skipping ${dealName}: stage "${stageTitle}" not actionable in List 2.0`);
      continue;
    }

    const openTasks = await getOpenTasksForRecord('deals', dealId);
    const hasOpenTask = openTasks.length > 0;
    const rd = RULES.deals;

    // Deal closing soon — configurable threshold
    if (rd.closingSoon.enabled && estimatedClose) {
      const daysToClose = daysUntil(estimatedClose);
      if (daysToClose !== null && daysToClose <= rd.closingSoon.triggerWithinDays && daysToClose > 0) {
        if (!hasOpenTask) {
          await createTask(
            `[2.0] 🚨 Deal Closing Soon — ${dealName}\nExpected close: ${estimatedClose} (${daysToClose} days)\nStage: ${stage}\nPrepare for close.\n\nComments: ${comments}`,
            ownerToId(rd.closingSoon.owner),
            isoDateInDays(1),
            'deals',
            dealId
          );
          results.tasks.push({ type: 'Prep Deal Close', name: dealName, priority: rd.closingSoon.priority, owner: rd.closingSoon.owner });
          console.log(`  ✅ Task: Prep Deal Close → ${dealName} (closes in ${daysToClose}d)`);
          continue;
        }
      }
    }

    if (hasOpenTask) {
      await checkAndEscalateIfStale('deals', dealId, dealName, 'Deal Tracking');
      continue;
    }

    // Determine task type by deal stage title
    if (rd.trackProcurement.enabled && stageTitle === 'Commercial/Procurement') {
      await createTask(
        `[2.0] Track Procurement — ${dealName}\nDeal in Commercial/Procurement stage.\nNext: support legal/procurement process.\n\n${estimatedClose ? `Expected close: ${estimatedClose}` : 'No close date set.'}\n\nComments: ${comments}`,
        ownerToId(rd.trackProcurement.owner),
        estimatedClose ? new Date(estimatedClose).toISOString() : isoDateInDays(rd.trackProcurement.defaultDueDays),
        'deals',
        dealId
      );
      results.tasks.push({ type: 'Track Procurement', name: dealName, priority: rd.trackProcurement.priority, owner: rd.trackProcurement.owner });
      console.log(`  ✅ Task: Track Procurement → ${dealName}`);

    } else if (rd.trackExpansion.enabled && stageTitle === 'Expansion/Resigning') {
      await createTask(
        `[2.0] Track Expansion — ${dealName}\nDeal in Expansion/Resigning stage.\nNext: support expansion progress.\n\n${estimatedClose ? `Expected close: ${estimatedClose}` : 'No close date set.'}\n\nComments: ${comments}`,
        ownerToId(rd.trackExpansion.owner),
        estimatedClose ? new Date(estimatedClose).toISOString() : isoDateInDays(rd.trackExpansion.defaultDueDays),
        'deals',
        dealId
      );
      results.tasks.push({ type: 'Track Expansion', name: dealName, priority: rd.trackExpansion.priority, owner: rd.trackExpansion.owner });
      console.log(`  ✅ Task: Track Expansion → ${dealName}`);

    } else if (rd.reengagement.enabled && stageTitle === 'Deal Lost') {
      if (!nextActionDue) {
        await createTask(
          `[2.0] ⚠️ Set Re-engagement Date — ${dealName}\nDeal marked Lost. No re-engagement date set.\nReview and decide when/if to re-approach.\n\nComments: ${comments}`,
          ownerToId(rd.reengagement.owner),
          isoDateInDays(1),
          'deals',
          dealId
        );
        results.flags.push({
          severity: 'HIGH',
          message: `${dealName} — Deal Lost, no re-engagement date set`,
          recordId: dealId,
          object: 'deals',
        });
        console.log(`  ⚠️  Flag + Task: Set Re-engagement Date → ${dealName}`);
      } else {
        const daysToReengage = daysUntil(nextActionDue);
        if (daysToReengage !== null && daysToReengage <= 0) {
          await createTask(
            `[2.0] Re-engagement Contact — ${dealName}\nRe-engagement date reached (${nextActionDue}).\nTime to re-approach this churned/lost deal.\n\nComments: ${comments}`,
            ownerToId(rd.reengagement.owner),
            isoDateInDays(1),
            'deals',
            dealId
          );
          results.tasks.push({ type: 'Re-engagement Contact', name: dealName, priority: rd.reengagement.priority, owner: rd.reengagement.owner });
          console.log(`  ✅ Task: Re-engagement Contact → ${dealName}`);
        } else {
          console.log(`  → ${dealName}: re-engagement scheduled in ${daysToReengage}d, skipping`);
        }
      }
    }
  }

  return results;
}

// ─────────────────────────────────────────────
// TRIAL PROCESSING — shared logic for Lists 3.0 & 3.1
// ─────────────────────────────────────────────
async function processTrial(entry, listId, listName) {
  const trialId = entry.parent_record?.record_id;
  if (!trialId) return null;

  const trial = await getRecord('trials', trialId);
  const trialName = getAttrValue(trial, 'name') || trialId;

  // Skip cancelled trials — notify if asked
  const trialResultTitle = getAttrValue(trial, 'trial_result_3');
  if (trialResultTitle === 'Cancelled') {
    console.log(`  → Skipping ${trialName}: trial result is Cancelled`);
    return { skipped: true, reason: 'cancelled', name: trialName };
  }

  // Get linked company to determine segment/tier
  // trial.company is a direct record reference (not an array) in some API responses
  const companyRef = trial?.values?.company;
  let companyId = null;
  if (Array.isArray(companyRef)) {
    companyId = companyRef[0]?.target_record_id;
  } else if (companyRef?.target_record_id) {
    companyId = companyRef.target_record_id;
  } else if (typeof companyRef === 'string') {
    companyId = companyRef;
  }
  let tier = null;
  let companyName = null;
  let companyRecord = null;

  if (companyId) {
    companyRecord = await getRecord('companies', companyId);
    companyName = getAttrValue(companyRecord, 'name');
    const segment = companyRecord?.values?.segment?.[0]?.option?.title;
    tier = getSegmentTier(segment);
  }

  const trialStart = getAttrValue(trial, 'trial_start_date');
  const trialEnd = getAttrValue(trial, 'trial_end_date');
  const storedHealth = getAttrValue(trial, 'trial_health_6');
  const comments = getAttrValue(trial, 'comments') || '';
  const entryComments = getAttrValue(entry, 'comments') || '';

  const trialAgeInDays = trialStart ? daysAgo(trialStart) : null;
  const daysToEnd = trialEnd ? daysUntil(trialEnd) : null;

  console.log(`  Trial: ${companyName || trialName} | Tier: ${tier || 'No Tier'} | Age: ${trialAgeInDays}d | Ends: ${daysToEnd !== null ? daysToEnd + 'd' : 'unknown'}`);

  const result = {
    tasks: [],
    flags: [],
    name: companyName || trialName,
    tier,
    trialId,
    companyId,
    trialAgeInDays,
    daysToEnd,
  };

  // ── HEALTH CHECK (flag discrepancies, never write) ──────────────────
  let calculatedHealth = null;
  const usersAttr = trial?.values?.users;
  const usersList = Array.isArray(usersAttr) ? usersAttr : (usersAttr ? [usersAttr] : []);
  const userIds = usersList.map(u => u?.target_record_id || u?.record_id).filter(Boolean);

  if (userIds.length === 0) {
    result.flags.push({
      severity: 'MEDIUM',
      message: `${result.name} — Trial has no linked users. Cannot assess activation health.`,
      recordId: trialId,
      object: 'trials',
    });
  } else if (trialAgeInDays !== null && trialAgeInDays >= 2) {
    const userRecords = await Promise.all(
      userIds.map(id =>
        attioRequest('GET', `/objects/users/records/${id}`)
          .then(r => r.data || r)
          .catch(() => null)
      )
    );
    const validUsers = userRecords.filter(Boolean);
    const ACTIVE_STAGE_TITLES = ['Activated', 'Adopted', 'Embedded'];
    const stages = validUsers.map(u => getAttrValue(u, 'activation_stage')).filter(Boolean);
    const hasAnyActive = stages.some(s => ACTIVE_STAGE_TITLES.includes(s));
    const allInactive = stages.length > 0 && stages.every(s => s === 'Inactive');
    const allEarly = stages.length > 0 && stages.every(s => ['Signed Up', 'Invited'].includes(s));

    if (hasAnyActive)       calculatedHealth = 'Healthy (Activated/Adopted)';
    else if (allInactive)   calculatedHealth = 'Critical (Inactive)';
    else if (allEarly)      calculatedHealth = 'At Risk (Stalled)';

    if (calculatedHealth && storedHealth && storedHealth !== calculatedHealth) {
      result.flags.push({
        severity: 'HIGH',
        message: `${result.name} — Trial health discrepancy. Stored: "${storedHealth}" but users indicate: "${calculatedHealth}". Please review and update manually in Attio.`,
        recordId: trialId,
        object: 'trials',
      });
      console.log(`  ⚠️  Flag: Health mismatch for ${result.name} (stored: ${storedHealth}, calculated: ${calculatedHealth})`);
    }
  }

  // ── PRODUCT USAGE CONTEXT ──────────────────────────────────────────
  // last_activity_date and user_signup_date are read directly from the user
  // record (stored fields on users object — no product_usage query needed
  // for those). Activity events (sessions, articles, emails) still queried
  // from product_usage via user_6 record reference.
  let usageSummary = 'No product usage data available.';
  if (userIds.length > 0) {
    try {
      const primaryUserRecord = await attioRequest('GET', `/objects/users/records/${userIds[0]}`)
        .then(r => r.data || r).catch(() => null);

      if (primaryUserRecord) {
        const usage = await getProductUsageContext(userIds[0], primaryUserRecord);
        usageSummary = formatUsageSummary(usage);
        console.log(`  📊 Usage: ${usage.sessions.length} sessions, ${usage.articlesViewed.length} articles, last active: ${usage.lastActivityDate || 'unknown'}, signed up: ${usage.signupDate || 'unknown'}`);
      }
    } catch (err) {
      console.warn(`  ⚠️  Product usage fetch failed for ${result.name}: ${err.message}`);
    }
  }

  // ── TIER-GATED TASKS (T1/T2/T3 only) ──────────────────────────────
  if (tier) {
    const openTasks = await getOpenTasksForRecord('trials', trialId);
    const r = RULES.trials;

    // Task 1: Call for Demo — configurable window from trial start (List 3.0 only)
    if (r.callForDemo.enabled && listId === ATTIO.lists.activeTrials
        && trialAgeInDays !== null && trialAgeInDays <= r.callForDemo.triggerWithinDays) {
      if (openTasks.length === 0) {
        await createTask(
          `[3.0] 🚨 Call for Demo — ${result.name} (${tier})\nTrial started ${trialAgeInDays} days ago.\nCall within ${r.callForDemo.triggerWithinDays * 24}h of trial start for onboarding/demo.\n\nHealth: ${storedHealth || 'Unknown'}\nComments: ${comments}\nEntry notes: ${entryComments}\n\n── PRODUCT USAGE ──\n${usageSummary}`,
          ownerToId(r.callForDemo.owner),
          isoDateInDays(1),
          'trials',
          trialId
        );
        result.tasks.push({ type: 'Call for Demo', name: result.name, priority: r.callForDemo.priority, owner: r.callForDemo.owner });
        console.log(`  ✅ Task: Call for Demo → ${result.name} [${r.callForDemo.owner}]`);
      } else {
        await checkAndEscalateIfStale('trials', trialId, result.name, 'Call for Demo');
      }
    }

    // Task 2: Trial Mid-Point Check — configurable day range
    if (r.midPointCheck.enabled
        && trialAgeInDays !== null
        && trialAgeInDays >= r.midPointCheck.triggerOnDayMin
        && trialAgeInDays <= r.midPointCheck.triggerOnDayMax) {
      const midpointTask = openTasks.find(t => t.content?.includes('Mid-Point'));
      if (!midpointTask) {
        await createTask(
          `[3.0/3.1] Trial Mid-Point Check — ${result.name} (${tier})\nTrial is ${trialAgeInDays} days old. Check in with lead.\nAssess engagement, answer questions, identify blockers.\n\nHealth: ${storedHealth || 'Unknown'}\nComments: ${comments}\n\n── PRODUCT USAGE ──\n${usageSummary}`,
          ownerToId(r.midPointCheck.owner),
          isoDateInDays(1),
          'trials',
          trialId
        );
        result.tasks.push({ type: 'Trial Mid-Point Check', name: result.name, priority: r.midPointCheck.priority, owner: r.midPointCheck.owner });
        console.log(`  ✅ Task: Mid-Point Check → ${result.name} [${r.midPointCheck.owner}]`);
      }
    }

    // Task 3: Call for Feedback & Pricing — configurable days before end
    if (r.feedbackAndPricing.enabled
        && daysToEnd !== null && daysToEnd <= r.feedbackAndPricing.triggerDaysBeforeEnd && daysToEnd > 0) {
      const feedbackTask = openTasks.find(t => t.content?.includes('Feedback & Pricing'));
      if (!feedbackTask) {
        await createTask(
          `[3.0/3.1] Call for Feedback & Pricing — ${result.name} (${tier})\nTrial ends in ${daysToEnd} days (${trialEnd}).\nCall to gather feedback and discuss pricing/conversion.\n\nHealth: ${storedHealth || 'Unknown'}\nComments: ${comments}\n\n── PRODUCT USAGE ──\n${usageSummary}`,
          ownerToId(r.feedbackAndPricing.owner),
          isoDateInDays(1),
          'trials',
          trialId
        );
        result.tasks.push({ type: 'Call for Feedback & Pricing', name: result.name, priority: r.feedbackAndPricing.priority, owner: r.feedbackAndPricing.owner });
        console.log(`  ✅ Task: Feedback & Pricing → ${result.name} [${r.feedbackAndPricing.owner}]`);
      }
    }

    // Task 4: Final Trial Check-in — configurable days before end
    if (r.finalCheckIn.enabled
        && daysToEnd !== null && daysToEnd <= r.finalCheckIn.triggerDaysBeforeEnd && daysToEnd > 0) {
      const finalTask = openTasks.find(t => t.content?.includes('Final Trial Check-in'));
      if (!finalTask) {
        await createTask(
          `[3.0/3.1] 🚨 Final Trial Check-in — ${result.name} (${tier})\nTrial ends in ${daysToEnd} days (${trialEnd}).\nFinal push — conversion or structured close.\n\nHealth: ${storedHealth || 'Unknown'}\nComments: ${comments}\n\n── PRODUCT USAGE ──\n${usageSummary}`,
          ownerToId(r.finalCheckIn.owner),
          isoDateInDays(1),
          'trials',
          trialId
        );
        result.tasks.push({ type: 'Final Trial Check-in', name: result.name, priority: r.finalCheckIn.priority, owner: r.finalCheckIn.owner });
        console.log(`  ✅ Task: Final Check-in → ${result.name} [${r.finalCheckIn.owner}]`);
      }
    }
  }

  // ── FEEDBACK CALL — ALL trials (regardless of tier) ────────────────
  const rf = RULES.trials.feedbackAndPricing;
  if (!tier && rf.enabled && daysToEnd !== null && daysToEnd <= rf.triggerDaysBeforeEnd && daysToEnd > 0) {
    const openTasks = await getOpenTasksForRecord('trials', trialId);
    const feedbackTask = openTasks.find(t => t.content?.includes('Feedback & Pricing'));
    if (!feedbackTask) {
      await createTask(
        `[3.0/3.1] Call for Feedback & Pricing — ${result.name} (No Tier)\nTrial ends in ${daysToEnd} days (${trialEnd}).\nGather feedback and discuss pricing.\n\nComments: ${comments}\n\n── PRODUCT USAGE ──\n${usageSummary}`,
        ownerToId(rf.owner),
        isoDateInDays(1),
        'trials',
        trialId
      );
      result.tasks.push({ type: 'Call for Feedback & Pricing', name: result.name, priority: rf.priority, owner: rf.owner });
      console.log(`  ✅ Task: Feedback & Pricing (No Tier) → ${result.name} [${rf.owner}]`);
    }
  }

  // ── LIST 3.1 — OUTCOME CHECKS ──────────────────────────────────────
  if (listId === ATTIO.lists.awaitingTrials) {
    const result31 = await checkTrialOutcome(trial, trialId, result.name, trialEnd);
    result.flags.push(...result31.flags);
    result.tasks.push(...result31.tasks);
  }

  return result;
}

// ─────────────────────────────────────────────
// TRIAL OUTCOME LOGIC (List 3.1)
// ─────────────────────────────────────────────
async function checkTrialOutcome(trial, trialId, trialName, trialEnd) {
  const flags = [];
  const tasks = [];
  const currentResult = getAttrValue(trial, 'trial_result_3');

  // Skip if already resolved or cancelled
  if (['Won', 'Lost', 'Cancelled'].includes(currentResult)) return { flags, tasks };

  // Trial ended but no outcome set
  const daysEnded = trialEnd ? daysAgo(trialEnd) : null;
  if (daysEnded !== null && daysEnded > 7) {
    // Check linked deal first
    const dealRef = trial?.values?.deal;
    const dealId = Array.isArray(dealRef)
      ? dealRef[0]?.target_record_id
      : dealRef?.target_record_id;

    let resolvedOutcome = null;

    if (dealId) {
      const deal = await getRecord('deals', dealId);
      const stageTitle = getAttrValue(deal, 'stage');
      if (stageTitle === 'Deal Won 🎉')  resolvedOutcome = 'Won';
      else if (stageTitle === 'Deal Lost') resolvedOutcome = 'Lost';
    }

    if (resolvedOutcome) {
      flags.push({
        severity: 'HIGH',
        message: `${trialName} — Trial ended ${daysEnded}d ago. Linked deal stage indicates "${resolvedOutcome}" but trial result is still "${currentResult}". Update trial result manually in Attio.`,
        recordId: trialId,
        object: 'trials',
      });
      console.log(`  ⚠️  Flag: Trial outcome determinable (${resolvedOutcome}) but not set → ${trialName}`);
    } else {
      flags.push({
        severity: 'HIGH',
        message: `${trialName} — Trial ended ${daysEnded}d ago with no outcome set. No conclusive deal stage found. Manually set trial result to Won or Lost in Attio.`,
        recordId: trialId,
        object: 'trials',
      });
      console.log(`  ⚠️  Flag: Trial ended ${daysEnded}d ago, no outcome → ${trialName}`);
    }
  }

  return { flags, tasks };
}

// ─────────────────────────────────────────────
// LIST 3.0 — Active Trialists
// ─────────────────────────────────────────────
async function processActiveTrials(runLog) {
  console.log('\n📋 LIST 3.0 — Active Trialists');
  const entries = await getAllListEntries(ATTIO.lists.activeTrials);
  const allResults = { tasks: [], flags: [], skipped: [] };
  runLog.listsProcessed['3.0'] = { entries: entries.length, tasks: 0, flags: 0 };

  for (const entry of entries) {
    const trialId = entry.parent_record?.record_id;
    if (trialId) await checkCriteriaAndFlagIfNeeded('3.0', ATTIO.lists.activeTrials, 'trials', trialId, trialId, runLog);
    const result = await processTrial(entry, ATTIO.lists.activeTrials, '3.0');
    if (!result) continue;
    if (result.skipped) { allResults.skipped.push(result); continue; }
    allResults.tasks.push(...(result.tasks || []));
    allResults.flags.push(...(result.flags || []));
  }

  return allResults;
}

async function processAwaitingTrials(runLog) {
  console.log('\n📋 LIST 3.1 — Awaiting Outcome');
  const entries = await getAllListEntries(ATTIO.lists.awaitingTrials);
  const allResults = { tasks: [], flags: [], skipped: [] };
  runLog.listsProcessed['3.1'] = { entries: entries.length, tasks: 0, flags: 0 };

  for (const entry of entries) {
    const trialId = entry.parent_record?.record_id;
    if (trialId) await checkCriteriaAndFlagIfNeeded('3.1', ATTIO.lists.awaitingTrials, 'trials', trialId, trialId, runLog);
    const result = await processTrial(entry, ATTIO.lists.awaitingTrials, '3.1');
    if (!result) continue;
    if (result.skipped) { allResults.skipped.push(result); continue; }
    allResults.tasks.push(...(result.tasks || []));
    allResults.flags.push(...(result.flags || []));
  }

  return allResults;
}

// ─────────────────────────────────────────────
// AI-ASSISTED CONTEXT ANALYSIS
// Called when Naseef provides context (email, transcript, note)
// via the chat interface. Not called during routine daily run.
// ─────────────────────────────────────────────
export async function analyseContextAndDecideTasks(leadInfo, newContext) {
  const systemPrompt = `You are a lead management assistant for NowcastIQ, a financial data analytics platform.
You help manage a sales pipeline of institutional finance leads.

Given information about a lead and new context (email, call notes, transcript excerpt), decide:
1. Which existing open tasks are now COMPLETE?
2. What NEW tasks should be created?
3. Should lead_status or lead_intent change?

Always respond in valid JSON. Be concise and action-oriented.
Today's date: ${today()}.`;

  const userContent = `LEAD: ${JSON.stringify(leadInfo, null, 2)}

NEW CONTEXT:
${newContext}

Respond with JSON:
{
  "completedTaskIds": ["id1", "id2"],
  "newTasks": [
    {
      "type": "string",
      "priority": "URGENT|HIGH|MEDIUM|LOW",
      "dueDays": 1,
      "owner": "naseef|diana|ed",
      "context": "why this task"
    }
  ],
  "statusUpdate": {
    "lead_status": "new value or null",
    "lead_intent": "High Intent|Medium Intent|Low Intent or null",
    "reason": "why"
  },
  "contradictionWithNotes": "describe clash if any or null",
  "reasoning": "brief explanation"
}`;

  const response = await claudeAnalyse(systemPrompt, userContent, 1000);

  try {
    const clean = response.replace(/```json|```/g, '').trim();
    return JSON.parse(clean);
  } catch (e) {
    console.error('Failed to parse Claude response:', response);
    throw e;
  }
}

// ─────────────────────────────────────────────
// DAILY COMPLETION DIGEST
// Uses Claude to assess which open tasks may be complete
// based on recent activity patterns.
// ─────────────────────────────────────────────
async function generateCompletionDigest(allTasks, allFlags, runLog) {
  const urgentTasks = allTasks.filter(t => t.priority === 'URGENT');
  const highTasks = allTasks.filter(t => t.priority === 'HIGH');
  const mediumTasks = allTasks.filter(t => t.priority === 'MEDIUM');
  const flagCount = allFlags.length;
  const errorCount = runLog?.totalErrors || 0;
  const outOfCriteriaCount = runLog?.totalOutOfCriteriaFlags || 0;

  const summary = [
    `NowcastIQ Daily Lead Intelligence Digest — ${today()}`,
    `Run ID: ${runLog?.runId || 'unknown'}`,
    ``,
    `🚨 URGENT (${urgentTasks.length}): ${urgentTasks.map(t => `${t.name} (${t.type})`).join(', ') || 'none'}`,
    `⚡ HIGH (${highTasks.length}): ${highTasks.map(t => `${t.name} (${t.type})`).join(', ') || 'none'}`,
    `📌 MEDIUM (${mediumTasks.length}): ${mediumTasks.map(t => `${t.name} (${t.type})`).join(', ') || 'none'}`,
    ``,
    `⚠️  Inconsistency flags: ${flagCount}`,
    flagCount > 0 ? allFlags.map(f => `  • [${f.severity}] ${f.message}`).join('\n') : '',
    outOfCriteriaCount > 0 ? `\n🔍 Out-of-criteria entries flagged: ${outOfCriteriaCount} (first-time only — check tasks)` : '',
    errorCount > 0 ? `\n❌ Agent errors this run: ${errorCount} — check error tasks` : '',
    ``,
    `Total tasks created/escalated: ${allTasks.length}`,
    `Claude API: ${CREDIT_TRACKER.summary().calls} call(s), ${CREDIT_TRACKER.summary().totalTokens.toLocaleString()} tokens, est. ${CREDIT_TRACKER.summary().estimatedCostUSDFormatted}`,
    `Run completed: ${new Date().toISOString()}`,
    `Agent version: ${VERSION}`,
  ].filter(l => l !== undefined).join('\n');

  if (allTasks.length > 0 || flagCount > 0 || errorCount > 0) {
    await createTask(
      summary,
      ATTIO.members.naseef,
      isoDateInDays(0),
      null,
      null
    );
  }

  return summary;
}

// ─────────────────────────────────────────────
// MAIN DAILY RUN
// ─────────────────────────────────────────────
async function runDailySync() {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`NowcastIQ Agent — Daily Run — ${new Date().toISOString()}`);
  if (CONFIG.dryRun) console.log('*** DRY RUN MODE — no writes will occur ***');
  console.log(`${'='.repeat(60)}`);

  if (!CONFIG.attioApiKey) throw new Error('ATTIO_API_KEY environment variable not set');
  if (!CONFIG.anthropicApiKey) throw new Error('ANTHROPIC_API_KEY environment variable not set');

  const runLog = initRunLog();
  CURRENT_RUN_ID = runLog.runId;
  RULES = loadRules(); // load fresh from rules.json every run
  const allTasks = [];
  const allFlags = [];
  const cancelledTrials = [];

  try {
    const lists = {
      '1.0': processDirectOutreach,
      'B1':  processNewsletter,
      '2.0': processDeals,
      '3.0': processActiveTrials,
      '3.1': processAwaitingTrials,
    };

    for (const [listKey, fn] of Object.entries(lists)) {
      if (CONFIG.singleList && CONFIG.singleList !== listKey) continue;
      try {
        const result = await fn(runLog);
        const tasks = result.tasks || [];
        const flags = result.flags || [];
        allTasks.push(...tasks);
        allFlags.push(...flags);
        if (result.skipped) cancelledTrials.push(...result.skipped);

        // Update per-list stats in run log
        if (runLog.listsProcessed[listKey]) {
          runLog.listsProcessed[listKey].tasks = tasks.length;
          runLog.listsProcessed[listKey].flags = flags.length;
        }
      } catch (err) {
        const errMsg = `Error processing List ${listKey}: ${err.message}`;
        console.error(`\n❌ ${errMsg}`);
        runLog.totalErrors++;
        runLog.errors.push({ list: listKey, error: err.message, stack: err.stack?.split('\n')[1] || '' });

        // Create a high-priority error task in Attio so it surfaces immediately
        await createTask(
          `[AGENT ERROR] ❌ List ${listKey} processing failed\n\n${err.message}\n\nThis list was skipped. Check logs for details.\nRun ID: ${runLog.runId}`,
          ATTIO.members.naseef,
          isoDateInDays(0),
          null,
          null
        ).catch(() => {}); // don't let error task creation break the run
      }
    }

    // Totals
    runLog.totalTasksCreated = allTasks.length;
    runLog.totalFlagsRaised = allFlags.length;
    runLog.completedAt = new Date().toISOString();
    runLog.status = runLog.totalErrors > 0 ? 'completed_with_errors' : 'completed';

    // Generate daily digest
    const digest = await generateCompletionDigest(allTasks, allFlags, runLog);

    console.log('\n' + '='.repeat(60));
    console.log('DAILY DIGEST:');
    console.log(digest);

    if (cancelledTrials.length > 0) {
      console.log(`\n📝 Note: ${cancelledTrials.length} cancelled trial(s) skipped during this run.`);
      cancelledTrials.forEach(t => console.log(`  • ${t.name}`));
    }

    if (runLog.totalErrors > 0) {
      console.log(`\n⚠️  ${runLog.totalErrors} error(s) occurred — error tasks created in Attio.`);
    }

    console.log('='.repeat(60));

    // Credit usage report
    const creditSummary = CREDIT_TRACKER.summary();
    runLog.claudeUsage = creditSummary;
    runLog.version = VERSION;
    console.log('\n' + CREDIT_TRACKER.report());

    // Write run log to disk
    writeRunLog(runLog);

    return { tasks: allTasks, flags: allFlags, digest, runLog };

  } catch (err) {
    runLog.status = 'failed';
    runLog.totalErrors++;
    runLog.errors.push({ fatal: true, error: err.message, stack: err.stack?.split('\n')[1] || '' });
    runLog.completedAt = new Date().toISOString();
    writeRunLog(runLog);

    // Try to create a fatal error task
    await createTask(
      `[AGENT FATAL ERROR] ❌ Agent run failed completely\n\n${err.message}\n\nRun ID: ${runLog.runId}\nCheck logs/${runLog.runId}.json for full details.`,
      ATTIO.members.naseef,
      isoDateInDays(0),
      null,
      null
    ).catch(() => {});

    console.error('\n❌ Fatal agent error:', err.message);
    throw err;
  }
}

// ─────────────────────────────────────────────
// OVERRIDE CONFIRMATION HANDLER
// Call this when Naseef replies "override confirmed for <recordId>"
// Persists the override so the record is never flagged again.
// ─────────────────────────────────────────────
export function confirmOverride(listId, recordId, reason) {
  saveOverride(listId, recordId, reason);
  console.log(`Override confirmed for ${recordId} in list ${listId}`);
}

// ─────────────────────────────────────────────
// SELF-HEALTH CHECK
// Run with: node nowcastiq-agent.mjs --health
// Verifies API connectivity and list access without processing.
// ─────────────────────────────────────────────
async function runHealthCheck() {
  console.log('\n🏥 NowcastIQ Agent Health Check\n');
  const checks = [];

  // 1. Attio API connectivity
  try {
    const r = await attioRequest('GET', '/lists?limit=1');
    checks.push({ check: 'Attio API', status: '✅', detail: 'Connected' });
  } catch (e) {
    checks.push({ check: 'Attio API', status: '❌', detail: e.message });
  }

  // 2. All 5 lists accessible
  const listChecks = [
    ['List 1.0', ATTIO.lists.directOutreach],
    ['List B1',  ATTIO.lists.newsletter],
    ['List 2.0', ATTIO.lists.deals],
    ['List 3.0', ATTIO.lists.activeTrials],
    ['List 3.1', ATTIO.lists.awaitingTrials],
  ];
  for (const [name, id] of listChecks) {
    try {
      const entries = await getAllListEntries(id);
      checks.push({ check: name, status: '✅', detail: `${entries.length} entries` });
    } catch (e) {
      checks.push({ check: name, status: '❌', detail: e.message });
    }
  }

  // 3. Overrides file
  const overrides = loadOverrides();
  const overrideCount = Object.values(overrides).reduce((n, v) => n + Object.keys(v).length, 0);
  checks.push({ check: 'Overrides file', status: '✅', detail: `${overrideCount} confirmed override(s)` });

  // 4. Run logs
  try {
    const summaryFile = path.join(LOGS_DIR, 'pilot-summary.json');
    if (fs.existsSync(summaryFile)) {
      const summary = JSON.parse(fs.readFileSync(summaryFile, 'utf8'));
      const lastRun = summary[0];
      checks.push({ check: 'Last run', status: lastRun?.status === 'completed' ? '✅' : '⚠️', detail: `${lastRun?.date} — ${lastRun?.status} (${lastRun?.tasks} tasks, ${lastRun?.errors} errors)` });
    } else {
      checks.push({ check: 'Last run', status: 'ℹ️', detail: 'No runs yet' });
    }
  } catch (e) {
    checks.push({ check: 'Last run', status: '⚠️', detail: 'Could not read log' });
  }

  // Print results
  for (const c of checks) {
    console.log(`  ${c.status} ${c.check}: ${c.detail}`);
  }

  const failures = checks.filter(c => c.status === '❌').length;
  console.log(`\n${failures === 0 ? '✅ All checks passed' : `❌ ${failures} check(s) failed`}\n`);
  return failures === 0;
}

// ─────────────────────────────────────────────
// ROLLBACK RUNNER
// Deletes all tasks created in a specific run.
// Usage: node nowcastiq-agent.mjs --rollback run-1234567890
// ─────────────────────────────────────────────
async function runRollback(runId) {
  const file = path.join(ROLLBACK_DIR, `${runId}.json`);
  if (!fs.existsSync(file)) {
    console.error(`❌ No rollback journal found for run ID: ${runId}`);
    console.error(`   Available rollback files:`);
    if (fs.existsSync(ROLLBACK_DIR)) {
      fs.readdirSync(ROLLBACK_DIR).forEach(f => console.error(`   • ${f.replace('.json', '')}`));
    } else {
      console.error('   (rollback directory does not exist yet)');
    }
    process.exit(1);
  }

  const journal = JSON.parse(fs.readFileSync(file, 'utf8'));
  console.log(`\n${'='.repeat(60)}`);
  console.log(`NowcastIQ Agent — Rollback Run: ${runId}`);
  console.log(`Tasks to delete: ${journal.length}`);
  console.log(`${'='.repeat(60)}\n`);

  if (journal.length === 0) {
    console.log('No tasks to delete. Run was empty.');
    return;
  }

  // Preview
  journal.forEach((entry, i) => {
    console.log(`  ${i + 1}. Task ${entry.taskId} — "${entry.contentPreview}..."`);
  });

  console.log('\nProceed? This will DELETE all tasks listed above. (yes/no)');
  const answer = await new Promise(resolve => {
    process.stdin.once('data', d => resolve(d.toString().trim().toLowerCase()));
  });

  if (answer !== 'yes') {
    console.log('Rollback cancelled.');
    process.exit(0);
  }

  let deleted = 0, failed = 0;
  for (const entry of journal) {
    try {
      await attioRequest('DELETE', `/tasks/${entry.taskId}`);
      console.log(`  ✅ Deleted: ${entry.taskId} — "${entry.contentPreview}"`);
      deleted++;
    } catch (e) {
      console.log(`  ❌ Failed:  ${entry.taskId} — ${e.message}`);
      failed++;
    }
  }

  console.log(`\nRollback complete: ${deleted} deleted, ${failed} failed.`);

  // Archive the rollback file so it can't be used again
  const archiveFile = path.join(ROLLBACK_DIR, `${runId}.rolled-back.json`);
  fs.renameSync(file, archiveFile);
  console.log(`Journal archived to: ${archiveFile}`);
}

// ─────────────────────────────────────────────
// ENTRY POINT
// ─────────────────────────────────────────────
const args = process.argv.slice(2);

if (args.includes('--version')) {
  console.log(`NowcastIQ Agent v${VERSION}`);
  process.exit(0);

} else if (args.includes('--rollback')) {
  const runId = args[args.indexOf('--rollback') + 1];
  if (!runId) {
    console.error('Usage: node nowcastiq-agent.mjs --rollback <run-id>');
    console.error('Find run IDs in: rollback/ directory or logs/pilot-summary.json');
    process.exit(1);
  }
  if (!CONFIG.attioApiKey) {
    console.error('ATTIO_API_KEY not set');
    process.exit(1);
  }
  runRollback(runId).catch(err => { console.error(err); process.exit(1); });

} else if (args.includes('--health')) {
  runHealthCheck().then(ok => process.exit(ok ? 0 : 1)).catch(err => {
    console.error(err);
    process.exit(1);
  });

} else {
  runDailySync().catch(err => {
    console.error(err);
    process.exit(1);
  });
}
