import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { deleteQuote, getQuotes, getUsers, Quote } from "../api";
import { useAccess } from "../access";
import { paginateItems, TablePagination } from "../components/TablePagination";
import { formatNetworkLabel } from "../utils/formatNetwork";
import { getQuoteStageClass, getQuoteStageLabel } from "../utils/quoteStatus";

const NO_NETWORK_LABEL = "No network assigned";
const MANUAL_SUFFIX = "Manual";

type MultiSelectDropdownProps = {
  label: string;
  options: string[];
  selected: string[];
  onChange: (values: string[]) => void;
};

function MultiSelectDropdown({
  label,
  options,
  selected,
  onChange,
}: MultiSelectDropdownProps) {
  const toggle = (value: string) => {
    if (selected.includes(value)) {
      onChange(selected.filter((item) => item !== value));
      return;
    }
    onChange([...selected, value]);
  };

  const summary =
    selected.length === 0
      ? "All"
      : selected.length === 1
        ? selected[0]
        : `${selected.length} selected`;

  return (
    <div className="multi-dropdown-field">
      <label>{label}</label>
      <details className="multi-dropdown">
        <summary className="multi-dropdown-trigger">{summary}</summary>
        <div className="multi-dropdown-menu">
          {options.map((option) => (
            <button
              key={option}
              type="button"
              className={`multi-dropdown-option ${selected.includes(option) ? "selected" : ""}`}
              onClick={() => toggle(option)}
            >
              <span className="multi-dropdown-check">
                {selected.includes(option) ? "✓" : ""}
              </span>
              <span>{option}</span>
            </button>
          ))}
        </div>
      </details>
    </div>
  );
}

export default function QuotesList() {
  const [quotes, setQuotes] = useState<Quote[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [deletingQuoteId, setDeletingQuoteId] = useState<string | null>(null);
  const [userNameById, setUserNameById] = useState<Record<string, string>>({});
  const [statusFilters, setStatusFilters] = useState<string[]>([]);
  const [networkFilters, setNetworkFilters] = useState<string[]>([]);
  const [effectiveFrom, setEffectiveFrom] = useState("");
  const [effectiveTo, setEffectiveTo] = useState("");
  const [sortBy, setSortBy] = useState<"effective_date" | "status" | "network">(
    "effective_date",
  );
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);
  const filtersRef = useRef<HTMLDivElement | null>(null);
  const { role, email } = useAccess();
  const isAdmin = role === "admin";

  const closeOpenFilterDropdowns = () => {
    if (!filtersRef.current) return;
    filtersRef.current
      .querySelectorAll<HTMLDetailsElement>(".multi-dropdown[open]")
      .forEach((el) => el.removeAttribute("open"));
  };

  useEffect(() => {
    getQuotes({ role, email })
      .then(setQuotes)
      .catch((err) => setError(err.message));
    getUsers()
      .then((users) =>
        setUserNameById(
          Object.fromEntries(
            users.map((user) => [
              user.id,
              `${user.first_name} ${user.last_name}`.trim(),
            ]),
          ),
        ),
      )
      .catch(() => setUserNameById({}));
  }, [role, email]);

  const handleDeleteQuote = async (quote: Quote) => {
    const confirmed = window.confirm(
      `Delete quote "${quote.company}"? This removes related uploads, assignment history, and implementation data.`,
    );
    if (!confirmed) return;
    setDeletingQuoteId(quote.id);
    setError(null);
    setStatusMessage(null);
    try {
      await deleteQuote(quote.id);
      setQuotes((prev) => prev.filter((item) => item.id !== quote.id));
      setStatusMessage("Quote deleted.");
    } catch (err: any) {
      setError(err.message);
    } finally {
      setDeletingQuoteId(null);
    }
  };

  useEffect(() => {
    const handleOutsideClick = (event: MouseEvent) => {
      const target = event.target as Node | null;
      if (
        !filtersRef.current ||
        (target && filtersRef.current.contains(target))
      ) {
        return;
      }
      closeOpenFilterDropdowns();
    };
    document.addEventListener("mousedown", handleOutsideClick);
    return () => document.removeEventListener("mousedown", handleOutsideClick);
  }, []);

  const statusOptions = useMemo(
    () =>
      Array.from(
        new Set(quotes.map((quote) => getQuoteStageLabel(quote.status))),
      ).sort((a, b) => a.localeCompare(b)),
    [quotes],
  );

  const networkOptions = useMemo(
    () =>
      Array.from(
        new Set(
          quotes.map((quote) => {
            if (quote.manual_network)
              return formatNetworkLabel(quote.manual_network);
            if (quote.latest_assignment) {
              return formatNetworkLabel(quote.latest_assignment.recommendation);
            }
            return NO_NETWORK_LABEL;
          }),
        ),
      ).sort((a, b) => a.localeCompare(b)),
    [quotes],
  );

  const visibleQuotes = useMemo(() => {
    const filtered = quotes.filter((quote) => {
      const quoteStatus = getQuoteStageLabel(quote.status);
      const quoteNetwork = quote.manual_network
        ? formatNetworkLabel(quote.manual_network)
        : quote.latest_assignment
          ? formatNetworkLabel(quote.latest_assignment.recommendation)
          : NO_NETWORK_LABEL;
      const quoteEffective = quote.effective_date || "";
      const matchesStatus =
        statusFilters.length === 0 || statusFilters.includes(quoteStatus);
      const matchesNetwork =
        networkFilters.length === 0 || networkFilters.includes(quoteNetwork);
      const matchesFrom =
        !effectiveFrom || (quoteEffective && quoteEffective >= effectiveFrom);
      const matchesTo =
        !effectiveTo || (quoteEffective && quoteEffective <= effectiveTo);
      return matchesStatus && matchesNetwork && matchesFrom && matchesTo;
    });

    const sorted = [...filtered].sort((a, b) => {
      if (sortBy === "effective_date") {
        const left = a.effective_date || "";
        const right = b.effective_date || "";
        return left.localeCompare(right);
      }
      if (sortBy === "status") {
        const left = getQuoteStageLabel(a.status);
        const right = getQuoteStageLabel(b.status);
        return left.localeCompare(right);
      }
      const left = a.manual_network
        ? formatNetworkLabel(a.manual_network)
        : a.latest_assignment
          ? formatNetworkLabel(a.latest_assignment.recommendation)
          : NO_NETWORK_LABEL;
      const right = b.manual_network
        ? formatNetworkLabel(b.manual_network)
        : b.latest_assignment
          ? formatNetworkLabel(b.latest_assignment.recommendation)
          : NO_NETWORK_LABEL;
      return left.localeCompare(right);
    });

    return sortDirection === "asc" ? sorted : sorted.reverse();
  }, [
    quotes,
    statusFilters,
    networkFilters,
    effectiveFrom,
    effectiveTo,
    sortBy,
    sortDirection,
  ]);

  const pagination = useMemo(
    () => paginateItems(visibleQuotes, page),
    [visibleQuotes, page],
  );

  useEffect(() => {
    if (page !== pagination.currentPage) {
      setPage(pagination.currentPage);
    }
  }, [page, pagination.currentPage]);

  return (
    <section className="section">
      <div
        className="inline-actions"
        style={{ justifyContent: "space-between", marginBottom: 12 }}
      >
        <h2 style={{ margin: 0 }}>Quotes</h2>
        <Link className="button" to="/quotes/new">
          New Quote
        </Link>
      </div>
      {statusMessage && (
        <div className="notice notice-success">{statusMessage}</div>
      )}
      {error && <div className="notice notice-error">{error}</div>}
      <div
        ref={filtersRef}
        className="inline-actions"
        style={{ marginBottom: 12 }}
      >
        <MultiSelectDropdown
          label="Status"
          options={statusOptions}
          selected={statusFilters}
          onChange={setStatusFilters}
        />
        <MultiSelectDropdown
          label="Network"
          options={networkOptions}
          selected={networkFilters}
          onChange={setNetworkFilters}
        />
        <label>
          Effective Date From
          <input
            type="date"
            value={effectiveFrom}
            onChange={(e) => setEffectiveFrom(e.target.value)}
          />
        </label>
        <label>
          Effective Date To
          <input
            type="date"
            value={effectiveTo}
            onChange={(e) => setEffectiveTo(e.target.value)}
          />
        </label>
        <label>
          Sort By
          <select
            value={sortBy}
            onChange={(e) =>
              setSortBy(
                e.target.value as "effective_date" | "status" | "network",
              )
            }
          >
            <option value="effective_date">Effective Date</option>
            <option value="status">Status</option>
            <option value="network">Network</option>
          </select>
        </label>
        <label>
          Direction
          <select
            value={sortDirection}
            onChange={(e) => setSortDirection(e.target.value as "asc" | "desc")}
          >
            <option value="desc">Descending</option>
            <option value="asc">Ascending</option>
          </select>
        </label>
        <button
          type="button"
          className="button subtle"
          onClick={() => {
            setStatusFilters([]);
            setNetworkFilters([]);
            setEffectiveFrom("");
            setEffectiveTo("");
            closeOpenFilterDropdowns();
          }}
        >
          Clear filters
        </button>
      </div>
      <div className="table-scroll">
        <table className="table">
          <thead>
            <tr>
              <th>Quote ID</th>
              <th>Group</th>
              <th>Effective Date</th>
              <th>Assigned To</th>
              <th>Status</th>
              <th>Network</th>
              {isAdmin && <th>Actions</th>}
            </tr>
          </thead>
          <tbody>
            {pagination.pageItems.map((quote) => (
              <tr key={quote.id}>
                <td>{(quote.hubspot_ticket_id || "").trim() || quote.id}</td>
                <td>
                  <Link className="table-link" to={`/quotes/${quote.id}`}>
                    {quote.company}
                  </Link>
                </td>
                <td>{quote.effective_date || "—"}</td>
                <td>
                  {(quote.assigned_user_id &&
                    userNameById[quote.assigned_user_id]) ||
                    "—"}
                </td>
                <td>
                  <span className={`badge ${getQuoteStageClass(quote.status)}`}>
                    {getQuoteStageLabel(quote.status)}
                  </span>
                </td>
                <td>
                  {quote.manual_network ? (
                    <span className="badge primary">
                      {formatNetworkLabel(quote.manual_network)} ·{" "}
                      {MANUAL_SUFFIX}
                    </span>
                  ) : quote.latest_assignment ? (
                    <span className="badge primary">
                      {formatNetworkLabel(
                        quote.latest_assignment.recommendation,
                      )}{" "}
                      · {Math.round(quote.latest_assignment.confidence * 100)}%
                    </span>
                  ) : (
                    <span className="helper">{NO_NETWORK_LABEL}</span>
                  )}
                </td>
                {isAdmin && (
                  <td>
                    <button
                      className="button"
                      type="button"
                      onClick={() => handleDeleteQuote(quote)}
                      disabled={deletingQuoteId === quote.id}
                    >
                      Delete
                    </button>
                  </td>
                )}
              </tr>
            ))}
            {visibleQuotes.length === 0 && (
              <tr>
                <td colSpan={isAdmin ? 7 : 6} className="helper">
                  No quotes match current filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <TablePagination
        page={pagination.currentPage}
        totalItems={visibleQuotes.length}
        onPageChange={setPage}
      />
    </section>
  );
}
