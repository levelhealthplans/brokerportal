import {
  Alert,
  Box,
  Button,
  Divider,
  Flex,
  Link,
  Text,
  hubspot,
} from "@hubspot/ui-extensions";
import { useCallback, useEffect, useMemo, useState } from "react";

type CardData = {
  status: "ok" | "not_found";
  resolved_by?: string;
  quote?: {
    id: string;
    company: string;
    status: string;
    effective_date: string;
    primary_network?: string | null;
    secondary_network?: string | null;
    hubspot_ticket_id?: string | null;
    hubspot_ticket_url?: string | null;
    hubspot_last_synced_at?: string | null;
    hubspot_sync_error?: string | null;
    quote_url?: string | null;
  };
  assignment?: {
    recommendation?: string | null;
    confidence?: number | null;
    coverage_percentage?: number | null;
    fallback_used?: boolean | null;
    review_required?: boolean | null;
    coverage_threshold?: number | null;
    default_network?: string | null;
  };
};

const BACKEND_BASE_URL =
  process.env.BACKEND_BASE_URL || "https://YOUR-RENDER-BACKEND.onrender.com";

hubspot.extend(({ context }) => <LevelHealthTicketCard context={context} />);

function LevelHealthTicketCard({ context }: { context: any }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<CardData | null>(null);

  const payload = useMemo(() => {
    const crm = context?.crm || {};
    const properties = crm?.properties || {};
    return {
      objectId: crm?.objectId || null,
      objectTypeId: crm?.objectTypeId || null,
      properties: {
        hs_ticket_id: properties?.hs_ticket_id || null,
        level_health_quote_id: properties?.level_health_quote_id || null,
      },
    };
  }, [context]);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await hubspot.fetch(
        `${BACKEND_BASE_URL}/api/integrations/hubspot/card-data`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }
      );
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed (${response.status})`);
      }
      const body = (await response.json()) as CardData;
      setData(body);
    } catch (err: any) {
      setError(err?.message || "Failed to load Level Health data.");
    } finally {
      setLoading(false);
    }
  }, [payload]);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  if (loading) {
    return <Text>Loading Level Health quote details...</Text>;
  }

  if (error) {
    return (
      <Box>
        <Alert title="Card load failed" variant="error">
          {error}
        </Alert>
        <Box marginTop="small">
          <Button onClick={() => void loadData()}>Retry</Button>
        </Box>
      </Box>
    );
  }

  if (!data || data.status !== "ok" || !data.quote) {
    return (
      <Box>
        <Text>No Level Health quote was linked to this ticket.</Text>
        <Box marginTop="small">
          <Button onClick={() => void loadData()}>Refresh</Button>
        </Box>
      </Box>
    );
  }

  const assignment = data.assignment || {};
  const quote = data.quote;
  const coveragePct =
    typeof assignment.coverage_percentage === "number"
      ? `${Math.round(assignment.coverage_percentage * 100)}%`
      : "-";

  return (
    <Box>
      <Text format={{ fontWeight: "bold" }}>{quote.company || "Quote"}</Text>
      <Text>
        Quote ID:{" "}
        {quote.quote_url ? (
          <Link href={quote.quote_url} openInNewTab>
            {quote.id}
          </Link>
        ) : (
          quote.id
        )}
      </Text>
      <Text>Status: {quote.status || "-"}</Text>
      <Text>Primary Network: {quote.primary_network || "-"}</Text>
      <Text>Coverage: {coveragePct}</Text>
      <Text>
        Fallback Used: {assignment.fallback_used == null ? "-" : assignment.fallback_used ? "Yes" : "No"}
      </Text>
      <Text>
        Manual Review: {assignment.review_required == null ? "-" : assignment.review_required ? "Yes" : "No"}
      </Text>
      {quote.hubspot_sync_error ? (
        <Box marginTop="small">
          <Alert title="Last sync warning" variant="warning">
            {quote.hubspot_sync_error}
          </Alert>
        </Box>
      ) : null}
      <Divider />
      <Flex direction="row" gap="small" align="start" wrap>
        {quote.quote_url ? (
          <Link href={quote.quote_url} openInNewTab>
            Open Quote
          </Link>
        ) : null}
        {quote.hubspot_ticket_url ? (
          <Link href={quote.hubspot_ticket_url} openInNewTab>
            Open Ticket
          </Link>
        ) : null}
        <Button onClick={() => void loadData()}>Refresh</Button>
      </Flex>
    </Box>
  );
}
