import { useEffect, useMemo, useState } from "react";
import { getQuote, getQuotes, Quote, QuoteDetail } from "../api";
import { Link } from "react-router-dom";
import { useAccess } from "../access";

export default function Dashboard() {
  const [quotes, setQuotes] = useState<Quote[]>([]);
  const [needsActionDetails, setNeedsActionDetails] = useState<
    Record<string, QuoteDetail>
  >({});
  const [error, setError] = useState<string | null>(null);
  const { role, email } = useAccess();

  useEffect(() => {
    getQuotes({ role, email })
      .then((quoteData) => {
        setQuotes(quoteData);
      })
      .catch((err) => setError(err.message));
  }, [role, email]);

  useEffect(() => {
    const needs = quotes.filter((quote) => quote.needs_action);
    if (needs.length === 0) {
      setNeedsActionDetails({});
      return;
    }
    Promise.all(needs.map((quote) => getQuote(quote.id)))
      .then((details) => {
        const mapping: Record<string, QuoteDetail> = {};
        details.forEach((detail) => {
          mapping[detail.quote.id] = detail;
        });
        setNeedsActionDetails(mapping);
      })
      .catch(() => {
        setNeedsActionDetails({});
      });
  }, [quotes]);

  const counts = useMemo(() => {
    const needsActionCount = quotes.filter((quote) => quote.needs_action).length;
    const proposalReadyCount = quotes.filter(
      (quote) => quote.status === "Proposal" || quote.status === "Proposal Ready"
    ).length;
    const inPipelineCount = quotes.filter((quote) =>
      ["Draft", "Quote Submitted", "Submitted", "In Review", "Needs Action", "Proposal", "Proposal Ready"].includes(quote.status)
    ).length;
    return { needsActionCount, proposalReadyCount, inPipelineCount };
  }, [quotes]);

  const needsAction = useMemo(
    () => quotes.filter((quote) => quote.needs_action).slice(0, 6),
    [quotes]
  );

  const activeQuotes = useMemo(
    () =>
      quotes.filter((quote) =>
        ["Draft", "Quote Submitted", "Submitted", "In Review", "Needs Action", "Proposal", "Proposal Ready"].includes(quote.status)
      ),
    [quotes]
  );

  const getNeedsActionReason = (detail?: QuoteDetail) => {
    if (!detail) return "Needs review";
    const hasCensus = detail.uploads.some((upload) => upload.type === "census");
    if (!hasCensus) return "Missing census upload";
    const latestStandardization = detail.standardizations[0];
    if (latestStandardization && latestStandardization.issue_count > 0) {
      return "Census issues";
    }
    if (detail.assignments.length === 0) return "Missing network assignment";
    return "Needs review";
  };

  return (
    <div>
      <section className="section">
        <h2>Snapshot</h2>
        {error && <div className="notice">{error}</div>}
        <div className="grid grid-3">
          <div className="section">
            <h2>Needs Action</h2>
            <div style={{ fontSize: 28, fontWeight: 700 }}>
              {counts.needsActionCount}
            </div>
          </div>
          <div className="section">
            <h2>Proposal</h2>
            <div style={{ fontSize: 28, fontWeight: 700 }}>
              {counts.proposalReadyCount}
            </div>
          </div>
          <div className="section">
            <h2>In Pipeline</h2>
            <div style={{ fontSize: 28, fontWeight: 700 }}>
              {counts.inPipelineCount}
            </div>
          </div>
        </div>
      </section>

      <section className="section">
        <h2>Needs Action</h2>
        {needsAction.length === 0 && (
          <div className="helper">No items need action right now.</div>
        )}
        {needsAction.map((quote) => (
          <div key={quote.id} className="card-row">
            <div>
              <strong>{quote.company}</strong>
              <div className="helper">
                {getNeedsActionReason(needsActionDetails[quote.id])}
              </div>
            </div>
            <Link className="button secondary" to={`/quotes/${quote.id}`}>
              Open
            </Link>
          </div>
        ))}
      </section>

      <section className="section">
        <h2>Active Quotes</h2>
        {activeQuotes.length === 0 && (
          <div className="helper">No active quotes yet.</div>
        )}
        {activeQuotes.slice(0, 6).map((quote) => (
          <div key={quote.id} className="card-row">
            <div>
              <strong>{quote.company}</strong>
              <div className="helper">
                {quote.state} · {quote.effective_date} · {quote.status}
              </div>
            </div>
            <Link className="button secondary" to={`/quotes/${quote.id}`}>
              View
            </Link>
          </div>
        ))}
      </section>
    </div>
  );
}
