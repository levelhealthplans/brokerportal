# HubSpot Card Scaffold (Level Health)

This folder contains a HubSpot app-card scaffold for Ticket records.

## Files

- `src/app/extensions/level-health-ticket-card.tsx`
- `src/app/extensions/level-health-ticket-card-hsmeta.json`
- `src/app/app-hsmeta.json`

## Backend dependency

The card fetches from:

- `POST /api/integrations/hubspot/card-data`

That endpoint is implemented in this repo backend and validates HubSpot request signatures.

## Required setup

1. Set the real backend URL in:
   - `src/app/app-hsmeta.json` (`permittedUrls`)
   - `src/app/extensions/level-health-ticket-card.tsx` (`BACKEND_BASE_URL`)
2. Ensure backend has one of these env vars set:
   - `HUBSPOT_APP_CLIENT_SECRET`
   - `HUBSPOT_CLIENT_SECRET`
3. Deploy backend before deploying/updating the HubSpot app card.

## Notes

- The card expects a ticket property named `level_health_quote_id` when available.
- If that property is absent, backend falls back to matching by `hubspot_ticket_id`.
