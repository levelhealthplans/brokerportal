import { Fragment, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  getNetworkOptions,
  getQuote,
  getQuotes,
  Quote,
  QuoteDetail,
  updateQuote,
} from "../api";
import { useAccess } from "../access";
import { formatNetworkLabel } from "../utils/formatNetwork";

type DetailState = {
  loading: boolean;
  error?: string;
  detail?: QuoteDetail;
};

export default function NetworkAssignments() {
  const { role, email } = useAccess();
  const [quotes, setQuotes] = useState<Quote[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [detailByQuote, setDetailByQuote] = useState<Record<string, DetailState>>({});
  const [networkOptions, setNetworkOptions] = useState<string[]>([]);
  const [manualDraftByQuote, setManualDraftByQuote] = useState<Record<string, string>>({});
  const [savingManualByQuote, setSavingManualByQuote] = useState<Record<string, boolean>>({});

  useEffect(() => {
    getQuotes({ role, email })
      .then(setQuotes)
      .catch((err) => setError(err.message));
  }, [role, email]);

  useEffect(() => {
    getNetworkOptions()
      .then(setNetworkOptions)
      .catch(() => setNetworkOptions(["Cigna_PPO"]));
  }, []);

  const toggleExpand = async (quoteId: string) => {
    setExpanded((prev) => ({ ...prev, [quoteId]: !prev[quoteId] }));
    if (detailByQuote[quoteId]?.detail || detailByQuote[quoteId]?.loading) return;
    setDetailByQuote((prev) => ({ ...prev, [quoteId]: { loading: true } }));
    try {
      const detail = await getQuote(quoteId);
      setDetailByQuote((prev) => ({ ...prev, [quoteId]: { loading: false, detail } }));
    } catch (err: any) {
      setDetailByQuote((prev) => ({
        ...prev,
        [quoteId]: { loading: false, error: err.message || "Failed to load assignment details." },
      }));
    }
  };

  const saveManualNetwork = async (quote: Quote) => {
    const nextValue = (manualDraftByQuote[quote.id] ?? quote.manual_network ?? "").trim();
    setSavingManualByQuote((prev) => ({ ...prev, [quote.id]: true }));
    setError(null);
    try {
      await updateQuote(quote.id, { manual_network: nextValue || null });
      const updatedQuotes = await getQuotes({ role, email });
      setQuotes(updatedQuotes);
      if (expanded[quote.id]) {
        const detail = await getQuote(quote.id);
        setDetailByQuote((prev) => ({ ...prev, [quote.id]: { loading: false, detail } }));
      }
      setManualDraftByQuote((prev) => ({ ...prev, [quote.id]: nextValue }));
    } catch (err: any) {
      setError(err.message || "Failed to save manual network.");
    } finally {
      setSavingManualByQuote((prev) => ({ ...prev, [quote.id]: false }));
    }
  };

  return (
    <section className="section">
      <h2>Network Assignment</h2>
      {error && <div className="notice">{error}</div>}
      <table className="table">
        <thead>
          <tr>
            <th style={{ width: 44 }}></th>
            <th>Quote ID</th>
            <th>Group</th>
            <th>Effective Date</th>
            <th>Primary Network</th>
            <th>Match Rate</th>
          </tr>
        </thead>
        <tbody>
          {quotes.map((quote) => {
            const isOpen = Boolean(expanded[quote.id]);
            const state = detailByQuote[quote.id];
            const latest = state?.detail?.assignments?.[0];
            const groupSummary = latest?.result_json?.group_summary;
            const primaryNetwork = groupSummary
              ? quote.manual_network
                ? formatNetworkLabel(quote.manual_network)
                : formatNetworkLabel(groupSummary.primary_network)
              : quote.manual_network
              ? formatNetworkLabel(quote.manual_network)
              : quote.latest_assignment
              ? formatNetworkLabel(quote.latest_assignment.recommendation)
              : "No network assigned";
            const computedMatchRate = groupSummary
              ? `${Math.round(groupSummary.coverage_percentage * 100)}%`
              : quote.latest_assignment
              ? `${Math.round(quote.latest_assignment.confidence * 100)}%`
              : "—";

            return (
              <Fragment key={quote.id}>
                <tr key={quote.id}>
                  <td>
                    <button
                      type="button"
                      className="expand-toggle"
                      onClick={() => toggleExpand(quote.id)}
                      aria-label={isOpen ? "Collapse assignment details" : "Expand assignment details"}
                    >
                      {isOpen ? "▾" : "▸"}
                    </button>
                  </td>
                  <td>{quote.id.slice(0, 8)}</td>
                  <td>
                    <Link className="table-link" to={`/quotes/${quote.id}`}>
                      {quote.company}
                    </Link>
                  </td>
                  <td>{quote.effective_date || "—"}</td>
                  <td>{primaryNetwork}</td>
                  <td>{quote.manual_network ? "Manual" : computedMatchRate}</td>
                </tr>
                {isOpen && (
                  <tr>
                    <td colSpan={6} style={{ paddingTop: 0, paddingBottom: 16 }}>
                      {role === "admin" && (
                        <div className="inline-actions" style={{ marginBottom: 10 }}>
                          <label style={{ minWidth: 320 }}>
                            Manual Network Override
                            <select
                              value={manualDraftByQuote[quote.id] ?? quote.manual_network ?? ""}
                              onChange={(e) =>
                                setManualDraftByQuote((prev) => ({
                                  ...prev,
                                  [quote.id]: e.target.value,
                                }))
                              }
                            >
                              <option value="">None</option>
                              {(
                                quote.manual_network &&
                                !networkOptions.includes(quote.manual_network)
                                  ? [quote.manual_network, ...networkOptions]
                                  : networkOptions
                              ).map((network) => (
                                <option key={network} value={network}>
                                  {formatNetworkLabel(network)}
                                </option>
                              ))}
                            </select>
                          </label>
                          <button
                            className="button secondary"
                            type="button"
                            onClick={() => saveManualNetwork(quote)}
                            disabled={Boolean(savingManualByQuote[quote.id])}
                          >
                            Save Override
                          </button>
                        </div>
                      )}
                      {state?.loading && <div className="helper">Loading assignment details…</div>}
                      {state?.error && <div className="notice">{state.error}</div>}
                      {state?.detail && (
                        <AssignmentDetails detail={state.detail} manualNetwork={quote.manual_network} />
                      )}
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
          {quotes.length === 0 && (
            <tr>
              <td colSpan={6} className="helper">
                No quote assignments available yet.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}

function AssignmentDetails({
  detail,
  manualNetwork,
}: {
  detail: QuoteDetail;
  manualNetwork?: string | null;
}) {
  const latest = detail.assignments?.[0];
  if (!latest) {
    if (manualNetwork) {
      return (
        <div className="helper">
          Manual override active: {formatNetworkLabel(manualNetwork)}. Re-run assignment or
          upload a new census to replace it.
        </div>
      );
    }
    return (
      <div className="helper">A network will be automatically assigned based on the census.</div>
    );
  }

  const result = latest.result_json || ({} as any);
  const coverageByNetwork = result.coverage_by_network || {};
  const memberAssignments = result.member_assignments || [];
  const groupSummary = result.group_summary;
  const rankedContracts = result.ranked_contracts || [];

  return (
    <div className="assignment-expand">
      {groupSummary && (
        <div className="kv" style={{ marginBottom: 8 }}>
          <strong>Primary Network</strong>
          <span>
            {manualNetwork
              ? `${formatNetworkLabel(manualNetwork)} (manual override)`
              : formatNetworkLabel(groupSummary.primary_network)}
          </span>
          <strong>Coverage Rate</strong>
          <span>{Math.round(groupSummary.coverage_percentage * 100)}%</span>
          <strong>Members Counted</strong>
          <span>{groupSummary.total_members}</span>
        </div>
      )}

      {Object.keys(coverageByNetwork).length > 0 && (
        <>
          <h3 style={{ marginTop: 8 }}>Network Match Rates</h3>
          <table className="table slim">
            <thead>
              <tr>
                <th>Network</th>
                <th>Match Rate</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(coverageByNetwork)
                .sort(([, a], [, b]) => Number(b) - Number(a))
                .map(([network, coverage]) => (
                  <tr key={network}>
                    <td>{formatNetworkLabel(network)}</td>
                    <td>{Math.round(Number(coverage) * 100)}%</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </>
      )}

      {memberAssignments.length > 0 && (
        <>
          <h3 style={{ marginTop: 10 }}>ZIP-Level Assignment Results</h3>
          <div className="assignment-zip-scroll">
            <table className="table slim">
              <thead>
                <tr>
                  <th>Row</th>
                  <th>ZIP</th>
                  <th>Assigned Network</th>
                  <th>Matched</th>
                </tr>
              </thead>
              <tbody>
                {memberAssignments.map((item: any) => (
                  <tr key={`${item.row}-${item.zip}`}>
                    <td>{item.row}</td>
                    <td>{item.zip}</td>
                    <td>{formatNetworkLabel(item.assigned_network)}</td>
                    <td>{item.matched ? "Yes" : "No"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {rankedContracts.length > 0 && (
        <>
          <h3 style={{ marginTop: 10 }}>Network Match Rates</h3>
          <table className="table slim">
            <thead>
              <tr>
                <th>Network</th>
                <th>Match Rate</th>
                <th>Fit</th>
              </tr>
            </thead>
            <tbody>
              {rankedContracts.map((contract: any) => (
                <tr key={contract.name}>
                  <td>{formatNetworkLabel(contract.name)}</td>
                  <td>{contract.score}%</td>
                  <td>{contract.fit || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
