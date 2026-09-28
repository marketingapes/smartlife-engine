"""
SmartLife Engine — lead follow-up sequencer for SmartLife Insurance Quote.
Marketing Apes × First Benefit Solutions. Fully separate from legal-side infrastructure.

Conducts: instant SMS (Twilio) → instant email (SES) → Sofia AI call (Vapi) → multi-day
nurture ladder, with business-hours awareness, STOP/unsubscribe kill-switch across all
channels, and a portal-first Postgres schema (this DB is the future FBS portal backbone).
"""
import asyncio
import json
import os
import secrets
from datetime import datetime, timedelta, timezone, time as dtime
from zoneinfo import ZoneInfo

import asyncpg
import boto3
import httpx
import phonenumbers
from fastapi import FastAPI, Request, Response, HTTPException

# ---------------------------------------------------------------- config
ENV = os.environ
DATABASE_URL = ENV["DATABASE_URL"]
WEBHOOK_TOKEN = ENV.get("WEBHOOK_TOKEN", "")

TWILIO_ACCOUNT_SID = ENV.get("TWILIO_ACCOUNT_SID", "")
TWILIO_API_KEY = ENV.get("TWILIO_API_KEY", "")
TWILIO_API_SECRET = ENV.get("TWILIO_API_SECRET", "")
TWILIO_MESSAGING_SERVICE_SID = ENV.get("TWILIO_MESSAGING_SERVICE_SID", "")
SMS_FROM = ENV.get("SMS_FROM", "")  # fail-closed: no default sender; send_sms refuses when unset

AWS_REGION = ENV.get("SES_REGION", "us-east-2")
SES_CONFIG_SET = ENV.get("SES_CONFIG_SET", "smartlife-fe")
EMAIL_FROM = ENV.get("EMAIL_FROM", '"Sofia Ella" <sofia@smartlifeinsurancequote.com>')

VAPI_ENABLED = ENV.get("VAPI_ENABLED", "false").lower() == "true"
VAPI_API_KEY = ENV.get("VAPI_API_KEY", "")
VAPI_ASSISTANT_OUTBOUND = ENV.get("VAPI_ASSISTANT_OUTBOUND", "1143e258-3dd2-4097-96ed-b437f9090c62")
VAPI_PHONE_ID = ENV.get("VAPI_PHONE_ID", "912c226b-31e5-4131-98de-7eb956b5860a")

TEAM_ALERT_SMS = [n.strip() for n in ENV.get("TEAM_ALERT_SMS", "").split(",") if n.strip()]
TEAM_ALERT_EMAIL = [e.strip() for e in ENV.get("TEAM_ALERT_EMAIL", "").split(",") if e.strip()]
PUBLIC_BASE_URL = ENV.get("PUBLIC_BASE_URL", "https://smartlife-engine.onrender.com")

CALLTOOLS_KEY = ENV.get("CALLTOOLS_KEY", "")
CALLTOOLS_BASE = ENV.get("CALLTOOLS_BASE", "https://app.calltools.io/api")

PT = ZoneInfo("America/Los_Angeles")
BUSINESS_START, BUSINESS_END = dtime(7, 0), dtime(16, 0)      # Mon-Fri 7a-4p PT (team hours)
SMS_START, SMS_END = dtime(8, 0), dtime(17, 45)               # conservative TCPA window in PT
CALL_START, CALL_END = dtime(7, 0), dtime(17, 45)

STOP_WORDS = {"stop", "stopall", "unsubscribe", "cancel", "end", "quit", "revoke", "optout", "opt out"}

SMS_COPY = {
    "sms1": "SmartLife Insurance Quote: Hi {first_name}, we got your request for final expense info. A licensed agent will call you shortly from this number. Reply YES for a call ASAP. Reply STOP to opt out.",
    "sms2": "Hi {first_name}, SmartLife here - our licensed agent tried to reach you about your final expense request. What's a good time today? Reply with a time or YES for a call now.",
    "sms3": "{first_name}, most folks are surprised how affordable final expense coverage is at their age. Your info request is still open - reply YES and a licensed agent will call you.",
    "sms4": "SmartLife Insurance Quote: {first_name}, we're closing out your coverage request. If you still want your options, reply YES today. Otherwise we won't text again. Reply STOP to opt out.",
}
EMAIL_TEMPLATES = {
    "email1": "smartlife-fe-1-instant",
    "email2": "smartlife-fe-2-education",
    "email3": "smartlife-fe-3-cost",
    "email4": "smartlife-fe-4-objections",
    "email5": "smartlife-fe-5-closeout",
}
# step -> (channel, delay from lead creation)
SEQUENCE = [
    ("sms1", "sms", timedelta(seconds=0)),
    ("email1", "email", timedelta(seconds=30)),
    ("call1", "call", timedelta(minutes=2)),
    ("sms2", "sms", timedelta(days=1)),
    ("email2", "email", timedelta(days=1, minutes=30)),
    ("sms3", "sms", timedelta(days=3)),
    ("email3", "email", timedelta(days=3, minutes=30)),
    ("email4", "email", timedelta(days=5)),
    ("email5", "email", timedelta(days=8)),
    ("sms4", "sms", timedelta(days=8, minutes=30)),
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT NOT NULL DEFAULT 'unknown',
  first_name TEXT, last_name TEXT,
  phone TEXT, email TEXT,
  state TEXT, age_band TEXT,
  status TEXT NOT NULL DEFAULT 'in_sequence',
  unsub_token TEXT UNIQUE,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS leads_phone_idx ON leads(phone);
CREATE INDEX IF NOT EXISTS leads_email_idx ON leads(email);
CREATE TABLE IF NOT EXISTS steps (
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT NOT NULL REFERENCES leads(id),
  step TEXT NOT NULL,
  channel TEXT NOT NULL,
  due_at TIMESTAMPTZ NOT NULL,
  done_at TIMESTAMPTZ,
  canceled_at TIMESTAMPTZ,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS steps_due_idx ON steps(due_at) WHERE done_at IS NULL AND canceled_at IS NULL;
CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT REFERENCES leads(id),
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS events_lead_idx ON events(lead_id, ts);
CREATE TABLE IF NOT EXISTS suppressions (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  phone TEXT, email TEXT, reason TEXT
);
CREATE INDEX IF NOT EXISTS supp_phone_idx ON suppressions(phone);
CREATE INDEX IF NOT EXISTS supp_email_idx ON suppressions(email);
CREATE TABLE IF NOT EXISTS callbacks (
  id BIGSERIAL PRIMARY KEY,
  lead_id BIGINT NOT NULL REFERENCES leads(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  window_text TEXT,
  status TEXT NOT NULL DEFAULT 'pending'
);
"""

app = FastAPI(title="SmartLife Engine")
pool: asyncpg.Pool = None
ses = boto3.client("sesv2", region_name=AWS_REGION,
                   aws_access_key_id=ENV.get("AWS_ACCESS_KEY_ID"),
                   aws_secret_access_key=ENV.get("AWS_SECRET_ACCESS_KEY"))


# ---------------------------------------------------------------- helpers
def now():
    return datetime.now(timezone.utc)


def norm_phone(raw):
    if not raw:
        return None
    try:
        p = phonenumbers.parse(str(raw), "US")
        if phonenumbers.is_valid_number(p):
            return phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return None


def in_window(start, end, weekdays_only=False):
    n = datetime.now(PT)
    if weekdays_only and n.weekday() > 4:
        return False
    return start <= n.time() <= end


def check_token(request: Request):
    tok = request.query_params.get("token") or request.headers.get("x-webhook-token", "")
    if WEBHOOK_TOKEN and tok != WEBHOOK_TOKEN:
        raise HTTPException(403, "bad token")


async def log_event(lead_id, etype, payload=None):
    await pool.execute("INSERT INTO events (lead_id, type, payload) VALUES ($1,$2,$3)",
                       lead_id, etype, json.dumps(payload or {}))


async def is_suppressed(phone, email):
    row = await pool.fetchrow(
        "SELECT 1 FROM suppressions WHERE (phone IS NOT NULL AND phone=$1) OR (email IS NOT NULL AND lower(email)=lower($2)) LIMIT 1",
        phone or "", email or "")
    return bool(row)


async def suppress(lead_id, phone, email, reason):
    await pool.execute("INSERT INTO suppressions (phone, email, reason) VALUES ($1,$2,$3)", phone, email, reason)
    if lead_id:
        await pool.execute("UPDATE leads SET status='dnc' WHERE id=$1", lead_id)
        await pool.execute("UPDATE steps SET canceled_at=now() WHERE lead_id=$1 AND done_at IS NULL AND canceled_at IS NULL", lead_id)
        await log_event(lead_id, "suppressed", {"reason": reason})


async def cancel_remaining(lead_id, channels=None):
    if channels:
        await pool.execute(
            "UPDATE steps SET canceled_at=now() WHERE lead_id=$1 AND done_at IS NULL AND canceled_at IS NULL AND channel = ANY($2)",
            lead_id, channels)
    else:
        await pool.execute(
            "UPDATE steps SET canceled_at=now() WHERE lead_id=$1 AND done_at IS NULL AND canceled_at IS NULL", lead_id)


# ---------------------------------------------------------------- senders
async def send_sms(to, body):
    if not SMS_FROM:
        raise RuntimeError("SMS_FROM is not configured - refusing to send SMS (fail-closed)")
    auth = (TWILIO_API_KEY, TWILIO_API_SECRET)
    data = {"To": to, "Body": body}
    if TWILIO_MESSAGING_SERVICE_SID:
        data["MessagingServiceSid"] = TWILIO_MESSAGING_SERVICE_SID
        data["From"] = SMS_FROM
    else:
        data["From"] = SMS_FROM
    async with httpx.AsyncClient() as c:
        r = await c.post(f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
                         auth=auth, data=data, timeout=30)
    r.raise_for_status()
    return r.json().get("sid")


def send_templated_email(to, template, first_name, unsub_token):
    return ses.send_email(
        FromEmailAddress=EMAIL_FROM,
        Destination={"ToAddresses": [to]},
        ConfigurationSetName=SES_CONFIG_SET,
        Content={"Template": {
            "TemplateName": template,
            "TemplateData": json.dumps({
                "first_name": first_name or "there",
                "unsubscribe_url": f"{PUBLIC_BASE_URL}/unsubscribe?t={unsub_token}",
            })}})


async def place_sofia_call(lead):
    """Outbound Sofia call. Gated by VAPI_ENABLED — never fires unless explicitly enabled."""
    payload = {
        "assistantId": VAPI_ASSISTANT_OUTBOUND,
        "phoneNumberId": VAPI_PHONE_ID,
        "customer": {"number": lead["phone"]},
        "assistantOverrides": {"variableValues": {
            "name": (lead["first_name"] or "").strip(),
            "age range": lead["age_band"] or "",
            "state": lead["state"] or "",
            "source": lead["source"] or "",
            "submitted": lead["created_at"].astimezone(PT).strftime("%B %d, %I:%M %p Pacific"),
        }},
    }
    async with httpx.AsyncClient() as c:
        r = await c.post("https://api.vapi.ai/call",
                         headers={"Authorization": f"Bearer {VAPI_API_KEY}"},
                         json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("id")


async def push_calltools(lead_id):
    """Layer 4: mirror the lead into CallTools so agents can work it. Never blocks the sequence."""
    if not CALLTOOLS_KEY:
        return
    try:
        lead = await pool.fetchrow("SELECT * FROM leads WHERE id=$1", lead_id)
        if not lead or not lead["phone"]:
            return
        payload = {
            "first_name": lead["first_name"] or "Unknown",
            "last_name": lead["last_name"] or "",
            "state": lead["state"] or "",
            "phone_number_1": lead["phone"],
            "email": lead["email"] or "",
            "status": f"SmartLife lead via {lead['source']}",
        }
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{CALLTOOLS_BASE}/contacts/",
                             headers={"Authorization": f"Token {CALLTOOLS_KEY}"},
                             json=payload, timeout=30)
        r.raise_for_status()
        ct_id = r.json().get("id")
        # phone doesn't attach on create — PATCH phone_number_1 to create the dialable number
        async with httpx.AsyncClient() as c:
            await c.patch(f"{CALLTOOLS_BASE}/contacts/{ct_id}/",
                          headers={"Authorization": f"Token {CALLTOOLS_KEY}"},
                          json={"phone_number_1": lead["phone"]}, timeout=30)
        await pool.execute("UPDATE leads SET meta = meta || $2::jsonb WHERE id=$1",
                           lead_id, json.dumps({"calltools_id": ct_id}))
        await log_event(lead_id, "calltools_pushed", {"calltools_id": ct_id})
    except Exception as e:
        await log_event(lead_id, "calltools_error", {"error": str(e)[:300]})


async def update_calltools(lead_id, fields):
    """Update the mirrored CallTools contact (callback window, DNC, etc.)."""
    if not CALLTOOLS_KEY:
        return
    try:
        meta = await pool.fetchval("SELECT meta FROM leads WHERE id=$1", lead_id)
        ct_id = (json.loads(meta) if isinstance(meta, str) else (meta or {})).get("calltools_id")
        if not ct_id:
            return
        async with httpx.AsyncClient() as c:
            await c.patch(f"{CALLTOOLS_BASE}/contacts/{ct_id}/",
                          headers={"Authorization": f"Token {CALLTOOLS_KEY}"},
                          json=fields, timeout=30)
        await log_event(lead_id, "calltools_updated", fields)
    except Exception as e:
        await log_event(lead_id, "calltools_error", {"error": str(e)[:300]})


async def alert_team(subject, body, lead_id=None):
    for n in TEAM_ALERT_SMS:
        try:
            await send_sms(n, f"{subject}\n{body}"[:1500])
        except Exception:
            pass
    for e in TEAM_ALERT_EMAIL:
        try:
            ses.send_email(FromEmailAddress=EMAIL_FROM, Destination={"ToAddresses": [e]},
                           Content={"Simple": {"Subject": {"Data": subject},
                                    "Body": {"Text": {"Data": body}}}})
        except Exception:
            pass
    if lead_id:
        await log_event(lead_id, "team_alert", {"subject": subject})


# ---------------------------------------------------------------- lifecycle
@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await pool.execute(SCHEMA)
    asyncio.create_task(scheduler_loop())


@app.get("/health")
async def health():
    return {"ok": True, "vapi_enabled": VAPI_ENABLED, "calltools": bool(CALLTOOLS_KEY),
            "time_pt": datetime.now(PT).isoformat()}


# ---------------------------------------------------------------- lead intake
@app.post("/webhook/lead")
async def webhook_lead(request: Request):
    check_token(request)
    try:
        data = await request.json()
    except Exception:
        form = await request.form()
        data = dict(form)
    first = (data.get("first_name") or data.get("full_name", "").split(" ")[0] or "").strip().title()
    last = (data.get("last_name") or " ".join(data.get("full_name", "").split(" ")[1:]) or "").strip().title()
    phone = norm_phone(data.get("phone") or data.get("phone_number") or data.get("best_mobile"))
    email = (data.get("email") or "").strip().lower() or None
    source = (data.get("source") or "unknown").strip()
    state = (data.get("state") or "").strip() or None
    age_band = (data.get("age_range") or data.get("age_band") or "").strip() or None

    if not phone and not email:
        raise HTTPException(422, "lead has neither phone nor email")

    if await is_suppressed(phone, email):
        lead_id = await pool.fetchval(
            "INSERT INTO leads (source, first_name, last_name, phone, email, state, age_band, status, unsub_token, meta) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,'dnc',$8,$9) RETURNING id",
            source, first, last, phone, email, state, age_band, secrets.token_urlsafe(16), json.dumps(data))
        await log_event(lead_id, "lead_received_suppressed", data)
        return {"ok": True, "lead_id": lead_id, "suppressed": True}

    # dedupe: same phone within 30 days → don't restart the ladder
    dup = await pool.fetchrow(
        "SELECT id FROM leads WHERE phone=$1 AND phone IS NOT NULL AND created_at > now() - interval '30 days' LIMIT 1", phone)
    unsub = secrets.token_urlsafe(16)
    lead_id = await pool.fetchval(
        "INSERT INTO leads (source, first_name, last_name, phone, email, state, age_band, unsub_token, meta) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id",
        source, first, last, phone, email, state, age_band, unsub, json.dumps(data))
    await log_event(lead_id, "lead_received", data)

    if dup:
        await pool.execute("UPDATE leads SET status='duplicate' WHERE id=$1", lead_id)
        await log_event(lead_id, "duplicate_of", {"lead_id": dup["id"]})
        return {"ok": True, "lead_id": lead_id, "duplicate": True}

    base = now()
    for step, channel, delay in SEQUENCE:
        if channel == "sms" and not phone:
            continue
        if channel == "email" and not email:
            continue
        if channel == "call" and not phone:
            continue
        await pool.execute("INSERT INTO steps (lead_id, step, channel, due_at) VALUES ($1,$2,$3,$4)",
                           lead_id, step, channel, base + delay)
    asyncio.create_task(push_calltools(lead_id))
    return {"ok": True, "lead_id": lead_id}


# ---------------------------------------------------------------- inbound SMS (Twilio)
@app.post("/webhook/twilio-inbound")
async def twilio_inbound(request: Request):
    form = await request.form()
    frm = norm_phone(form.get("From"))
    body = (form.get("Body") or "").strip().lower()
    lead = await pool.fetchrow("SELECT * FROM leads WHERE phone=$1 ORDER BY created_at DESC LIMIT 1", frm)
    lead_id = lead["id"] if lead else None
    await log_event(lead_id, "sms_reply", {"from": frm, "body": body})

    if any(w in body for w in STOP_WORDS):
        await suppress(lead_id, frm, lead["email"] if lead else None, "sms_stop")
        if lead_id:
            asyncio.create_task(update_calltools(lead_id, {"do_not_contact": True}))
        return Response(content="<Response></Response>", media_type="application/xml")

    if "yes" in body.split() or body == "yes":
        if lead_id:
            await pool.execute("UPDATE leads SET status='hot_reply' WHERE id=$1", lead_id)
        await alert_team("HOT SmartLife lead replied YES",
                         f"{(lead['first_name'] + ' ' + (lead['last_name'] or '')).strip() if lead else frm} · {frm}\n"
                         f"They want a call NOW. Call from the team line.", lead_id)
    else:
        await alert_team("SmartLife SMS reply",
                         f"From {frm}: {form.get('Body','')[:300]}", lead_id)
    return Response(content="<Response></Response>", media_type="application/xml")


# ---------------------------------------------------------------- Sofia outcomes (Vapi)
@app.post("/webhook/vapi")
async def vapi_webhook(request: Request):
    data = await request.json()
    msg = data.get("message", data)
    if msg.get("type") != "end-of-call-report":
        return {"ok": True}
    call = msg.get("call", {})
    analysis = msg.get("analysis", {})
    sd = analysis.get("structuredData", {}) or {}
    phone = norm_phone((call.get("customer") or {}).get("number"))
    lead = await pool.fetchrow("SELECT * FROM leads WHERE phone=$1 ORDER BY created_at DESC LIMIT 1", phone)
    lead_id = lead["id"] if lead else None
    routing = sd.get("routing", "")
    await log_event(lead_id, "call_outcome", {"routing": routing, "structured": sd,
                                             "summary": analysis.get("summary", "")})
    if not lead_id:
        return {"ok": True}

    if routing == "dnc":
        await suppress(lead_id, phone, lead["email"], "call_dnc")
        asyncio.create_task(update_calltools(lead_id, {"do_not_contact": True}))
    elif routing in ("transferred", "team_handoff_now"):
        await pool.execute("UPDATE leads SET status='transferred' WHERE id=$1", lead_id)
        await cancel_remaining(lead_id)
        await alert_team("SmartLife lead transferred to team",
                         f"{sd.get('first_name','')} {sd.get('last_name','')} · {phone}", lead_id)
    elif routing == "callback_scheduled":
        await pool.execute("UPDATE leads SET status='callback_scheduled' WHERE id=$1", lead_id)
        await pool.execute("INSERT INTO callbacks (lead_id, window_text) VALUES ($1,$2)",
                           lead_id, sd.get("callback_window", ""))
        asyncio.create_task(update_calltools(lead_id, {"besttimeforcallback": sd.get("callback_window", "")}))
        await cancel_remaining(lead_id, ["sms", "call"])  # keep the email nurture
        await alert_team("SmartLife callback booked",
                         f"{sd.get('first_name','')} · {phone}\nWindow: {sd.get('callback_window','?')}", lead_id)
    elif routing in ("not_interested", "wrong_person"):
        await pool.execute("UPDATE leads SET status=$2 WHERE id=$1", lead_id, routing)
        await cancel_remaining(lead_id)
    # voicemail / no answer → sequence continues
    if sd.get("email") and not lead["email"]:
        await pool.execute("UPDATE leads SET email=$2 WHERE id=$1", lead_id, sd["email"].lower())
    if sd.get("age_given") and not lead["age_band"]:
        await pool.execute("UPDATE leads SET age_band=$2 WHERE id=$1", lead_id, sd["age_given"])
    return {"ok": True}


# ---------------------------------------------------------------- SES events (SNS)
@app.post("/webhook/ses")
async def ses_events(request: Request):
    body = json.loads((await request.body()).decode())
    if body.get("Type") == "SubscriptionConfirmation":
        async with httpx.AsyncClient() as c:
            await c.get(body["SubscribeURL"], timeout=30)
        return {"ok": True, "confirmed": True}
    if body.get("Type") == "Notification":
        msg = json.loads(body.get("Message", "{}"))
        etype = msg.get("eventType", "")
        dest = (msg.get("mail", {}).get("destination") or [None])[0]
        if etype in ("Bounce", "Complaint") and dest:
            lead = await pool.fetchrow("SELECT * FROM leads WHERE lower(email)=lower($1) ORDER BY created_at DESC LIMIT 1", dest)
            if etype == "Complaint":
                await suppress(lead["id"] if lead else None, lead["phone"] if lead else None, dest, "email_complaint")
            else:
                await pool.execute("INSERT INTO suppressions (email, reason) VALUES ($1,'email_bounce')", dest)
                if lead:
                    await cancel_remaining(lead["id"], ["email"])
                    await log_event(lead["id"], "email_bounce", {"email": dest})
    return {"ok": True}


# ---------------------------------------------------------------- unsubscribe
@app.get("/unsubscribe")
async def unsubscribe(t: str = ""):
    lead = await pool.fetchrow("SELECT * FROM leads WHERE unsub_token=$1", t)
    if lead:
        await suppress(lead["id"], lead["phone"], lead["email"], "unsubscribe_link")
    return Response(content="""<html><body style="font-family:Arial;max-width:480px;margin:80px auto;text-align:center;color:#26324a">
<h2>You're unsubscribed.</h2><p>You won't receive any more messages from SmartLife Insurance Quote.
If this was a mistake, call us at (760) 921-0803.</p></body></html>""", media_type="text/html")


# ---------------------------------------------------------------- portal seed endpoints
@app.get("/stats")
async def stats(request: Request):
    check_token(request)
    rows = await pool.fetch("SELECT status, count(*) c FROM leads GROUP BY status")
    steps = await pool.fetchrow(
        "SELECT count(*) FILTER (WHERE done_at IS NOT NULL) sent, "
        "count(*) FILTER (WHERE done_at IS NULL AND canceled_at IS NULL) pending FROM steps")
    return {"leads_by_status": {r["status"]: r["c"] for r in rows},
            "touches": dict(steps)}


@app.get("/leads")
async def list_leads(request: Request, limit: int = 50):
    check_token(request)
    rows = await pool.fetch(
        "SELECT id, created_at, source, first_name, last_name, phone, email, state, age_band, status "
        "FROM leads ORDER BY created_at DESC LIMIT $1", min(limit, 200))
    return [dict(r) | {"created_at": r["created_at"].isoformat()} for r in rows]


@app.get("/events")
async def list_events(request: Request, limit: int = 100):
    check_token(request)
    rows = await pool.fetch(
        "SELECT lead_id, ts, type, payload FROM events ORDER BY ts DESC LIMIT $1", min(limit, 500))
    return [{"lead_id": r["lead_id"], "ts": r["ts"].isoformat(), "type": r["type"],
             "payload": json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]} for r in rows]


# ---------------------------------------------------------------- scheduler
async def run_step(step_row):
    lead = await pool.fetchrow("SELECT * FROM leads WHERE id=$1", step_row["lead_id"])
    if not lead or lead["status"] not in ("in_sequence", "hot_reply"):
        await pool.execute("UPDATE steps SET canceled_at=now() WHERE id=$1", step_row["id"])
        return
    step, channel = step_row["step"], step_row["channel"]
    try:
        if channel == "sms":
            if not in_window(SMS_START, SMS_END):
                return  # retry next tick inside the window
            sid = await send_sms(lead["phone"], SMS_COPY[step].format(first_name=lead["first_name"] or "there"))
            await log_event(lead["id"], "sms_sent", {"step": step, "sid": sid})
        elif channel == "email":
            send_templated_email(lead["email"], EMAIL_TEMPLATES[step], lead["first_name"], lead["unsub_token"])
            await log_event(lead["id"], "email_sent", {"step": step})
        elif channel == "call":
            if not VAPI_ENABLED:
                await log_event(lead["id"], "call_skipped_disabled", {"step": step})
                await pool.execute("UPDATE steps SET canceled_at=now() WHERE id=$1", step_row["id"])
                return
            if not in_window(CALL_START, CALL_END):
                return
            call_id = await place_sofia_call(lead)
            await log_event(lead["id"], "call_placed", {"step": step, "call_id": call_id})
        await pool.execute("UPDATE steps SET done_at=now() WHERE id=$1", step_row["id"])
    except Exception as e:
        await log_event(lead["id"], "step_error", {"step": step, "error": str(e)[:500]})
        # push the step 30 min out so one failure doesn't hot-loop
        await pool.execute("UPDATE steps SET due_at=now() + interval '30 minutes' WHERE id=$1", step_row["id"])


async def scheduler_loop():
    while True:
        try:
            due = await pool.fetch(
                "SELECT * FROM steps WHERE due_at <= now() AND done_at IS NULL AND canceled_at IS NULL "
                "ORDER BY due_at LIMIT 25")
            for row in due:
                await run_step(row)
        except Exception:
            pass
        await asyncio.sleep(30)
