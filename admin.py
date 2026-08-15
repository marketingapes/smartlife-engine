"""
SmartLife Engine — admin layer.
Wraps core.py's app with token-gated operator endpoints (ad-hoc broadcast email).
Run: uvicorn admin:app
"""
import core
from core import app, send_email_raw, check_token, PUBLIC_BASE_URL
from fastapi import Request


def _render(s, first_name, unsub_url):
    return (s or "").replace("{{first_name}}", first_name).replace("{{unsubscribe_url}}", unsub_url)


@app.post("/admin/broadcast-email")
async def broadcast_email(request: Request):
    """One-off email to leads. Body: {subject, html?, text?, statuses?, lead_ids?, dry_run?}
    Renders {{first_name}} and {{unsubscribe_url}} per lead. Skips suppressed leads always."""
    check_token(request)
    body = await request.json()
    subject = body["subject"]
    html, text = body.get("html"), body.get("text")
    statuses = body.get("statuses", ["in_sequence", "hot_reply"])
    lead_ids = body.get("lead_ids")
    dry_run = bool(body.get("dry_run"))

    rows = await core.pool.fetch(
        "SELECT * FROM leads WHERE email IS NOT NULL AND email <> '' AND status = ANY($1) ORDER BY id", statuses)
    sent, skipped, errors = [], [], []
    for lead in rows:
        if lead_ids and lead["id"] not in lead_ids:
            continue
        if await core.is_suppressed(lead["phone"], lead["email"]):
            skipped.append({"lead_id": lead["id"], "reason": "suppressed"})
            continue
        fn = (lead["first_name"] or "there").strip() or "there"
        unsub = f"{PUBLIC_BASE_URL}/unsubscribe?t={lead['unsub_token']}"
        if dry_run:
            sent.append({"lead_id": lead["id"], "email": lead["email"], "dry_run": True})
            continue
        try:
            send_email_raw(lead["email"], _render(subject, fn, unsub),
                           _render(html, fn, unsub) if html else None,
                           _render(text, fn, unsub) if text else None)
            await core.log_event(lead["id"], "email_sent", {"step": "broadcast", "subject": subject[:100]})
            sent.append({"lead_id": lead["id"], "email": lead["email"]})
        except Exception as e:
            await core.log_event(lead["id"], "step_error", {"step": "broadcast", "error": str(e)[:300]})
            errors.append({"lead_id": lead["id"], "error": str(e)[:200]})
    return {"ok": True, "sent": len(sent), "skipped": len(skipped), "errors": len(errors),
            "detail": {"sent": sent, "skipped": skipped, "errors": errors}}
