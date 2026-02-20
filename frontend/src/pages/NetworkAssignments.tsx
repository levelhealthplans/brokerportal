import { Fragment, useEffect, useMemo, useState } from "react";
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
import { paginateItems, TablePagination } from "../components/TablePagination";
import { formatNetworkLabel } from "../utils/formatNetwork";

type DetailState = {
  loading: boolean;
  error?: string;
  detail?: QuoteDetail;
};

type AssignmentGroupSummary = NonNullable<
  QuoteDetail["assignments"][number]["result_json"]["group_summary"]
>;

function formatEffectiveCoverageRate(summary?: AssignmentGroupSummary | null): string {
  if (!summary) return "—";
  if (summary.fallback_used) return "100% (fallback)";
  return `${Math.round(summary.coverage_percentage * 100)}%`;
}

function formatDirectCoverageRate(summary?: AssignmentGroupSummary | null): string {
  if (!summary) return "—";
  return `${Math.round(summary.coverage_percentage * 100)}%`;
}

export default function NetworkAssignments() {
  const { role, email } = useAccess();
  const [quotes, setQuotes] = useState<Quote[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [detailByQuote, setDetailByQuote] = useState<
    Record<string, DetailState>
  >({});
  const [networkOptions, setNetworkOptions] = useState<string[]>([]);
  const [manualDraftByQuote, setManualDraftByQuote] = useState<
    Record<string, string>
  >({});
  const [savingManualByQuote, setSavingManualByQuote] = useState<
    Record<string, boolean>
  >({});
  const [quotePage, setQuotePage] = useState(1);

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
    if (detailByQuote[quoteId]?.detail || detailByQuote[quoteId]?.loading)
      return;
    setDetailByQuote((prev) => ({ ...prev, [quoteId]: { loading: true } }));
    try {
      const detail = await getQuote(quoteId);
      setDetailByQuote((prev) => ({
        ...prev,
        [quoteId]: { loading: false, detail },
      }));
    } catch (err: any) {
      setDetailByQuote((prev) => ({
        ...prev,
        [quoteId]: {
          loading: false,
          error: err.message || "Failed to load assignment details.",
        },
      }));
    }
  };

  const saveManualNetwork = async (quote: Quote) => {
    const nextValue = (
      manualDraftByQuote[quote.id] ??
      quote.manual_network ??
      ""
    ).trim();
    setSavingManualByQuote((prev) => ({ ...prev, [quote.id]: true }));
    setError(null);
    try {
      await updateQuote(quote.id, { manual_network: nextValue || null });
      const updatedQuotes = await getQuotes({ role, email });
      setQuotes(updatedQuotes);
      if (expanded[quote.id]) {
        const detail = await getQuote(quote.id);
        setDetailByQuote((prev) => ({
          ...prev,
          [quote.id]: { loading: false, detail },
        }));
      }
      setManualDraftByQuote((prev) => ({ ...prev, [quote.id]: nextValue }));
    } catch (err: any) {
      setError(err.message || "Failed to save manual network.");
    } finally {
      setSavingManualByQuote((prev) => ({ ...prev, [quote.id]: false }));
    }
  };

  const quotePagination = useMemo(
    () => paginateItems(quotes, quotePage),
    [quotes, quotePage],
  );

  useEffect(() => {
    if (quotePage !== quotePagination.currentPage) {
      setQuotePage(quotePagination.currentPage);
    }
  }, [quotePage, quotePagination.currentPage]);

  return (
    <section className="section">
      <h2>Network Assignment</h2>
      {error && <div className="notice notice-error">{error}</div>}
      <div className="table-scroll">
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
            {quotePagination.pageItems.map((quote) => {
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
                ? formatEffectiveCoverageRate(groupSummary)
                : quote.latest_assignment
                  ? quote.latest_assignment.recommendation === "Cigna_PPO" &&
                    quote.latest_assignment.confidence < 0.9
                    ? "100% (fallback)"
                    : `${Math.round(quote.latest_assignment.confidence * 100)}%`
                  : "—";

              return (
                <Fragment key={quote.id}>
                  <tr key={quote.id}>
                    <td>
                      <button
                        type="button"
                        className="expand-toggle"
                        onClick={() => toggleExpand(quote.id)}
                        aria-label={
                          isOpen
                            ? "Collapse assignment details"
                            : "Expand assignment details"
                        }
                      >
                        {isOpen ? "▾" : "▸"}
                      </button>
                    </td>
                    <td>{(quote.hubspot_ticket_id || "").trim() || quote.id}</td>
                    <td>
                      <Link className="table-link" to={`/quotes/${quote.id}`}>
                        {quote.company}
                      </Link>
                    </td>
                    <td>{quote.effective_date || "—"}</td>
                    <td>{primaryNetwork}</td>
                    <td>
                      {quote.manual_network ? "Manual" : computedMatchRate}
                    </td>
                  </tr>
                  {isOpen && (
                    <tr>
                      <td
                        colSpan={6}
                        style={{ paddingTop: 0, paddingBottom: 16 }}
                      >
                        {role === "admin" && (
                          <div
                            className="inline-actions"
                            style={{ marginBottom: 10 }}
                          >
                            <label style={{ minWidth: 320 }}>
                              Manual Network Override
                              <select
                                value={
                                  manualDraftByQuote[quote.id] ??
                                  quote.manual_network ??
                                  ""
                                }
                                onChange={(e) =>
                                  setManualDraftByQuote((prev) => ({
                                    ...prev,
                                    [quote.id]: e.target.value,
                                  }))
                                }
                              >
                                <option value="">None</option>
                                {(quote.manual_network &&
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
                        {state?.loading && (
                          <div className="helper">
                            Loading assignment details…
                          </div>
                        )}
                        {state?.error && (
                          <div className="notice notice-error">{state.error}</div>
                        )}
                        {state?.detail && (
                          <AssignmentDetails
                            detail={state.detail}
                            manualNetwork={quote.manual_network}
                          />
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
      </div>
      <TablePagination
        page={quotePagination.currentPage}
        totalItems={quotes.length}
        onPageChange={setQuotePage}
      />
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
  const result = latest?.result_json || {};
  const coverageByNetwork = result.coverage_by_network || {};
  const memberAssignments = result.member_assignments || [];
  const groupSummary = result.group_summary;
  const rankedContracts = result.ranked_contracts || [];
  const [coveragePage, setCoveragePage] = useState(1);
  const [memberPage, setMemberPage] = useState(1);
  const [rankedPage, setRankedPage] = useState(1);

  const coverageRows = useMemo(
    () =>
      Object.entries(coverageByNetwork).sort(
        ([, a], [, b]) => Number(b) - Number(a),
      ),
    [coverageByNetwork],
  );

  const coveragePagination = useMemo(
    () => paginateItems(coverageRows, coveragePage),
    [coverageRows, coveragePage],
  );
  const memberPagination = useMemo(
    () => paginateItems(memberAssignments, memberPage),
    [memberAssignments, memberPage],
  );
  const rankedPagination = useMemo(
    () => paginateItems(rankedContracts, rankedPage),
    [rankedContracts, rankedPage],
  );

  useEffect(() => {
    if (coveragePage !== coveragePagination.currentPage) {
      setCoveragePage(coveragePagination.currentPage);
    }
  }, [coveragePage, coveragePagination.currentPage]);

  useEffect(() => {
    if (memberPage !== memberPagination.currentPage) {
      setMemberPage(memberPagination.currentPage);
    }
  }, [memberPage, memberPagination.currentPage]);

  useEffect(() => {
    if (rankedPage !== rankedPagination.currentPage) {
      setRankedPage(rankedPagination.currentPage);
    }
  }, [rankedPage, rankedPagination.currentPage]);

  if (!latest) {
    if (manualNetwork) {
      return (
        <div className="helper">
          Manual override active: {formatNetworkLabel(manualNetwork)}. Re-run
          assignment or upload a new census to replace it.
        </div>
      );
    }
    return (
      <div className="helper">
        A network will be automatically assigned based on the census.
      </div>
    );
  }

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
          <span>{formatEffectiveCoverageRate(groupSummary)}</span>
          {groupSummary.fallback_used && (
            <>
              <strong>Direct Contract Match</strong>
              <span>{formatDirectCoverageRate(groupSummary)}</span>
            </>
          )}
          <strong>Members Counted</strong>
          <span>{groupSummary.total_members}</span>
        </div>
      )}

      {Object.keys(coverageByNetwork).length > 0 && (
        <>
          <h3 style={{ marginTop: 8 }}>
            {groupSummary?.fallback_used
              ? "Direct Contract Match Rates"
              : "Network Match Rates"}
          </h3>
          <div className="table-scroll">
            <table className="table slim">
              <thead>
                <tr>
                  <th>Network</th>
                  <th>{groupSummary?.fallback_used ? "Direct Match Rate" : "Match Rate"}</th>
                </tr>
              </thead>
              <tbody>
                {coveragePagination.pageItems.map(([network, coverage]) => (
                  <tr key={network}>
                    <td>{formatNetworkLabel(network)}</td>
                    <td>{Math.round(Number(coverage) * 100)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <TablePagination
            page={coveragePagination.currentPage}
            totalItems={coverageRows.length}
            onPageChange={setCoveragePage}
          />
        </>
      )}

      {memberAssignments.length > 0 && (
        <>
          <h3 style={{ marginTop: 10 }}>ZIP-Level Assignment Results</h3>
          <div className="assignment-zip-scroll">
            <div className="table-scroll">
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
                  {memberPagination.pageItems.map((item: any) => (
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
          </div>
          <TablePagination
            page={memberPagination.currentPage}
            totalItems={memberAssignments.length}
            onPageChange={setMemberPage}
          />
        </>
      )}

      {rankedContracts.length > 0 && (
        <>
          <h3 style={{ marginTop: 10 }}>Network Match Rates</h3>
          <div className="table-scroll">
            <table className="table slim">
              <thead>
                <tr>
                  <th>Network</th>
                  <th>Match Rate</th>
                  <th>Fit</th>
                </tr>
              </thead>
              <tbody>
                {rankedPagination.pageItems.map((contract: any) => (
                  <tr key={contract.name}>
                    <td>{formatNetworkLabel(contract.name)}</td>
                    <td>{contract.score}%</td>
                    <td>{contract.fit || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <TablePagination
            page={rankedPagination.currentPage}
            totalItems={rankedContracts.length}
            onPageChange={setRankedPage}
          />
        </>
      )}
    </div>
  );
}
