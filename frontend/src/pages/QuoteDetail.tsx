import { useEffect, useMemo, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import {
  assignNetwork,
  syncQuoteFromHubSpot,
  getQuote,
  getNetworkOptions,
  resolveStandardization,
  standardizeQuote,
  updateQuote,
  uploadFile,
  deleteUpload,
  convertToInstallation,
  QuoteDetail as QuoteDetailType,
} from "../api";
import { useAccess } from "../access";
import { paginateItems, TablePagination } from "../components/TablePagination";
import { formatNetworkLabel } from "../utils/formatNetwork";
import { getQuoteStageClass, getQuoteStageLabel } from "../utils/quoteStatus";
import { useAutoDismissMessage } from "../hooks/useAutoDismissMessage";

export default function QuoteDetail() {
  const { id } = useParams();
  const quoteId = id || "";
  const location = useLocation();
  const { role, email } = useAccess();
  const [data, setData] = useState<QuoteDetailType | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [wizardOpen, setWizardOpen] = useState(false);
  const [wizardIssues, setWizardIssues] = useState<
    {
      row: number;
      field: string;
      issue: string;
      value?: string;
      mapped_value?: string;
    }[]
  >([]);
  const [wizardStatus, setWizardStatus] = useState<string | null>(null);
  const [autoOpened, setAutoOpened] = useState(false);
  const [autoSubmit, setAutoSubmit] = useState(false);
  const [sampleData, setSampleData] = useState<Record<string, string[]>>({});
  const [sampleRows, setSampleRows] = useState<Record<string, any>[]>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [issueRows, setIssueRows] = useState(0);
  const [detectedHeaders, setDetectedHeaders] = useState<string[]>([]);
  const [headerMappings, setHeaderMappings] = useState<Record<string, string>>(
    {},
  );
  const [editGroupOpen, setEditGroupOpen] = useState(false);
  const [groupDraft, setGroupDraft] = useState({
    company: "",
    employer_street: "",
    employer_city: "",
    state: "",
    employer_zip: "",
    employer_domain: "",
    quote_deadline: "",
    employer_sic: "",
    effective_date: "",
    current_enrolled: "",
    current_eligible: "",
    current_insurance_type: "",
  });
  const [stageDraft, setStageDraft] = useState("Draft");
  const [manualNetworkDraft, setManualNetworkDraft] = useState("");
  const [networkOptions, setNetworkOptions] = useState<string[]>([]);
  const [proposalUrlDraft, setProposalUrlDraft] = useState("");
  const [coveragePage, setCoveragePage] = useState(1);
  const [wizardIssuesPage, setWizardIssuesPage] = useState(1);
  const statusMessageFading = useAutoDismissMessage(
    statusMessage,
    setStatusMessage,
    5000,
    500,
  );

  const stageOptions = [
    "Draft",
    "Quote Submitted",
    "In Review",
    "Needs Action",
    "Proposal",
    "Sold",
    "Lost",
  ];

  const jumpToAssignment = () => {
    const element = document.getElementById("assignment-output");
    if (element) {
      element.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  const requiredHeaderLabels: Record<string, string> = {
    first_name: "First Name",
    last_name: "Last Name",
    dob: "DOB",
    zip: "Zip",
    gender: "Gender",
    relationship: "Relationship",
    enrollment_tier: "Enrollment Tier",
  };

  const headerAliases: Record<string, string[]> = {
    first_name: ["first name", "firstname", "first_name", "fname", "given"],
    last_name: [
      "last name",
      "lastname",
      "last_name",
      "lname",
      "surname",
      "family",
    ],
    dob: ["dob", "date of birth", "birthdate", "birth date"],
    zip: ["zip", "zip code", "zipcode", "postal", "postal code"],
    gender: ["gender", "sex"],
    relationship: ["relationship", "rel", "relation"],
    enrollment_tier: [
      "enrollment tier",
      "tier",
      "coverage tier",
      "enrollment",
      "coverage",
    ],
  };

  const normalizeHeader = (value: string) =>
    value.toLowerCase().replace(/[^a-z0-9]/g, "");

  const applyDetectedHeaders = (
    headers: string[],
    samples: Record<string, string[]>,
    rows: Record<string, any>[],
    total: number,
    issues: number,
  ) => {
    setDetectedHeaders(headers);
    setSampleData(samples || {});
    setSampleRows(rows || []);
    setTotalRows(total || 0);
    setIssueRows(issues || 0);
    if (!headers.length) return;
    setHeaderMappings((prev) => {
      const next = { ...prev };
      Object.keys(requiredHeaderLabels).forEach((key) => {
        if (!next[key]) {
          const normalizedHeaders = headers.map((h) => ({
            raw: h,
            norm: normalizeHeader(h),
          }));

          const aliases = headerAliases[key] || [];
          const normalizedAliases = aliases.map(normalizeHeader);

          let match = normalizedHeaders.find((h) =>
            normalizedAliases.includes(h.norm),
          )?.raw;

          if (!match) {
            match = normalizedHeaders.find((h) =>
              normalizedAliases.some((alias) => h.norm.includes(alias)),
            )?.raw;
          }

          if (match) next[key] = match;
        }
      });
      return next;
    });
  };

  const refresh = () => {
    if (!quoteId) return;
    getQuote(quoteId)
      .then(setData)
      .catch((err) => setError(err.message));
  };

  const isHubSpotSyncPending = (quote?: QuoteDetailType["quote"] | null) => {
    if (!quote) return false;
    const updatedAt = Date.parse(quote.updated_at || "");
    if (!Number.isFinite(updatedAt)) return false;
    const syncedAt = Date.parse(quote.hubspot_last_synced_at || "");
    if (!Number.isFinite(syncedAt)) return true;
    return syncedAt < updatedAt;
  };

  useEffect(() => {
    refresh();
  }, [quoteId]);

  useEffect(() => {
    if (!quoteId || !data?.quote) return;
    if (!isHubSpotSyncPending(data.quote)) return;

    let cancelled = false;
    let attempts = 0;
    const maxAttempts = 12;
    const intervalId = window.setInterval(() => {
      attempts += 1;
      getQuote(quoteId)
        .then((next) => {
          if (cancelled) return;
          setData(next);
          if (!isHubSpotSyncPending(next.quote) || attempts >= maxAttempts) {
            window.clearInterval(intervalId);
          }
        })
        .catch(() => {
          if (attempts >= maxAttempts) {
            window.clearInterval(intervalId);
          }
        });
    }, 2000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [quoteId, data?.quote?.updated_at, data?.quote?.hubspot_last_synced_at]);

  useEffect(() => {
    getNetworkOptions()
      .then(setNetworkOptions)
      .catch(() => setNetworkOptions(["Cigna_PPO"]));
  }, []);

  const resetGroupDraft = (quoteData?: QuoteDetailType["quote"]) => {
    const quote = quoteData || data?.quote;
    if (!quote) return;
    setGroupDraft({
      company: quote.company || "",
      employer_street: quote.employer_street || "",
      employer_city: quote.employer_city || "",
      state: quote.state || "",
      employer_zip: quote.employer_zip || "",
      employer_domain: quote.employer_domain || "",
      quote_deadline: quote.quote_deadline || "",
      employer_sic: quote.employer_sic || "",
      effective_date: quote.effective_date || "",
      current_enrolled: String(quote.current_enrolled ?? ""),
      current_eligible: String(quote.current_eligible ?? ""),
      current_insurance_type: quote.current_insurance_type || "",
    });
  };

  useEffect(() => {
    resetGroupDraft(data?.quote);
    if (data?.quote?.status) {
      setStageDraft(
        data.quote.status === "Submitted"
          ? "Quote Submitted"
          : data.quote.status,
      );
    }
    setManualNetworkDraft(data?.quote?.manual_network || "");
    setProposalUrlDraft(data?.quote?.proposal_url || "");
  }, [data]);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const shouldOpen = params.get("wizard") === "standardize";
    const shouldSubmit = params.get("submit") === "1";
    if (!shouldOpen || autoOpened || !quoteId) return;
    setAutoOpened(true);
    setAutoSubmit(shouldSubmit);
    setWizardOpen(true);
    standardizeQuote(quoteId)
      .then((result) => {
        setWizardIssues(result.issues_json);
        setWizardStatus(result.status);
        applyDetectedHeaders(
          result.detected_headers || [],
          result.sample_data || {},
          result.sample_rows || [],
          result.total_rows || 0,
          result.issue_rows || 0,
        );
        refresh();
        if (shouldSubmit && result.issue_count === 0) {
          return updateQuote(quoteId, { status: "Quote Submitted" })
            .then(() => assignNetwork(quoteId))
            .then(() =>
              setStatusMessage(
                "Submission complete. Network assignment started.",
              ),
            )
            .then(() => setWizardOpen(false))
            .then(() => refresh())
            .then(() => setTimeout(jumpToAssignment, 200));
        }
      })
      .catch(() => {
        // fallback to just opening the wizard
      });
  }, [location.search, autoOpened, quoteId]);

  const latestAssignment = useMemo(() => {
    if (!data?.assignments.length) return null;
    return data.assignments[0];
  }, [data]);

  const latestStandardization = useMemo(() => {
    if (!data?.standardizations.length) return null;
    return data.standardizations[0];
  }, [data]);

  const coverageRows = useMemo(
    () =>
      Object.entries(latestAssignment?.result_json.coverage_by_network || {}),
    [latestAssignment],
  );

  const coveragePagination = useMemo(
    () => paginateItems(coverageRows, coveragePage),
    [coverageRows, coveragePage],
  );

  const wizardIssuesPagination = useMemo(
    () => paginateItems(wizardIssues, wizardIssuesPage),
    [wizardIssues, wizardIssuesPage],
  );

  useEffect(() => {
    if (coveragePage !== coveragePagination.currentPage) {
      setCoveragePage(coveragePagination.currentPage);
    }
  }, [coveragePage, coveragePagination.currentPage]);

  useEffect(() => {
    if (wizardIssuesPage !== wizardIssuesPagination.currentPage) {
      setWizardIssuesPage(wizardIssuesPagination.currentPage);
    }
  }, [wizardIssuesPage, wizardIssuesPagination.currentPage]);

  const handleStageChange = async (nextStage: string) => {
    setStageDraft(nextStage);
    if (nextStage === stageDraft) return;
    setBusy(true);
    setError(null);
    try {
      await updateQuote(quoteId, { status: nextStage });
      setStatusMessage("Stage updated.");
      refresh();
    } catch (err: any) {
      setError(err.message);
      setStageDraft(
        data?.quote?.status === "Submitted"
          ? "Quote Submitted"
          : data?.quote?.status || "Draft",
      );
    } finally {
      setBusy(false);
    }
  };

  const handleStandardize = async () => {
    setBusy(true);
    setError(null);
    try {
      const result = await standardizeQuote(quoteId);
      if (result.issue_count === 0) {
        setStatusMessage("Census checks passed.");
      } else {
        setStatusMessage(`Found ${result.issue_count} census issues.`);
      }
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const openWizard = () => {
    setWizardOpen(true);
    if (latestStandardization) {
      setWizardIssues(latestStandardization.issues_json);
      setWizardStatus(latestStandardization.status);
    } else {
      setWizardIssues([]);
      setWizardStatus(null);
    }
  };

  const buildMappings = () => ({
    gender_map: {},
    relationship_map: {},
    tier_map: {},
  });

  const handleWizardStandardize = async () => {
    setBusy(true);
    setError(null);
    try {
      const result = await standardizeQuote(quoteId, {
        ...buildMappings(),
        header_map: headerMappings,
      });
      setWizardIssues(result.issues_json);
      setWizardStatus(result.status);
      applyDetectedHeaders(
        result.detected_headers || [],
        result.sample_data || {},
        result.sample_rows || [],
        result.total_rows || 0,
        result.issue_rows || 0,
      );
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleWizardResolve = async () => {
    setBusy(true);
    setError(null);
    try {
      const result = await resolveStandardization(quoteId, wizardIssues);
      setWizardIssues(result.issues_json);
      setWizardStatus(result.status);
      if (autoSubmit) {
        await updateQuote(quoteId, { status: "Quote Submitted" });
        await assignNetwork(quoteId);
        setStatusMessage("Submission complete. Network assignment started.");
        setWizardOpen(false);
        setTimeout(jumpToAssignment, 200);
      }
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleWizardSubmit = async () => {
    setBusy(true);
    setError(null);
    try {
      if (!data?.uploads?.some((upload) => upload.type === "census")) {
        setError("Upload a census before submitting.");
        return;
      }

      let remainingIssues = wizardIssues;
      if (wizardIssues.length > 0) {
        const resolved = await resolveStandardization(quoteId, wizardIssues);
        setWizardIssues(resolved.issues_json);
        setWizardStatus(resolved.status);
        remainingIssues = resolved.issues_json;
      }

      if (remainingIssues.length > 0) {
        setError("Please resolve all census issues before submitting.");
        return;
      }

      await updateQuote(quoteId, { status: "Quote Submitted" });
      await assignNetwork(quoteId);
      setStatusMessage("Submission complete. Network assignment started.");
      setWizardOpen(false);
      refresh();
      setTimeout(jumpToAssignment, 200);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleAssign = async () => {
    setBusy(true);
    setError(null);
    try {
      await assignNetwork(quoteId);
      setStatusMessage("Assignment run completed.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleConvert = async () => {
    setBusy(true);
    setError(null);
    try {
      const installation = await convertToInstallation(quoteId, {
        role,
        email,
      });
      setStatusMessage("Case marked sold and moved to implementation.");
      refresh();
      window.location.href = `/implementations/${installation.id}`;
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleUpload = async (
    event: React.ChangeEvent<HTMLInputElement>,
    uploadType: string,
  ) => {
    const files = Array.from(event.target.files || []);
    if (!files.length) return;
    setBusy(true);
    setError(null);
    try {
      await Promise.all(
        files.map((file) => uploadFile(quoteId, file, uploadType)),
      );
      setStatusMessage("Upload complete.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleWizardCensusUpload = async (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const files = Array.from(event.target.files || []);
    if (!files.length) return;
    setBusy(true);
    setError(null);
    try {
      await Promise.all(
        files.map((file) => uploadFile(quoteId, file, "census")),
      );
      const result = await standardizeQuote(quoteId, {
        ...buildMappings(),
        header_map: headerMappings,
      });
      setWizardIssues(result.issues_json);
      setWizardStatus(result.status);
      applyDetectedHeaders(
        result.detected_headers || [],
        result.sample_data || {},
        result.sample_rows || [],
        result.total_rows || 0,
        result.issue_rows || 0,
      );
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleDeleteUpload = async (uploadId: string) => {
    if (!quoteId) return;
    const confirmed = window.confirm(
      "Delete this file? This cannot be undone.",
    );
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      await deleteUpload(quoteId, uploadId);
      setStatusMessage("Upload deleted.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleClearCensus = async () => {
    if (!quoteId) return;
    const censusUploads = (data?.uploads || []).filter(
      (upload) => upload.type === "census",
    );
    if (censusUploads.length === 0) {
      setStatusMessage("No census file to remove.");
      return;
    }
    const confirmed = window.confirm("Remove the current census file?");
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      await Promise.all(
        censusUploads.map((upload) => deleteUpload(quoteId, upload.id)),
      );
      setWizardIssues([]);
      setWizardStatus(null);
      setDetectedHeaders([]);
      setHeaderMappings({});
      setSampleData({});
      setSampleRows([]);
      setTotalRows(0);
      setIssueRows(0);
      setStatusMessage("Census removed.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleGroupDraftChange = (field: string, value: string) => {
    setGroupDraft((prev) => ({ ...prev, [field]: value }));
  };

  const toNumberOrUndefined = (value: string) => {
    if (value === "") return undefined;
    const parsed = Number(value);
    if (Number.isNaN(parsed)) return undefined;
    return parsed;
  };

  const handleGroupSave = async () => {
    setBusy(true);
    setError(null);
    try {
      await updateQuote(quoteId, {
        company: groupDraft.company,
        employer_street: groupDraft.employer_street || null,
        employer_city: groupDraft.employer_city || null,
        state: groupDraft.state,
        employer_zip: groupDraft.employer_zip || null,
        employer_domain: groupDraft.employer_domain || null,
        quote_deadline: groupDraft.quote_deadline || null,
        employer_sic: groupDraft.employer_sic || null,
        effective_date: groupDraft.effective_date,
        current_enrolled: toNumberOrUndefined(groupDraft.current_enrolled),
        current_eligible: toNumberOrUndefined(groupDraft.current_eligible),
        current_insurance_type: groupDraft.current_insurance_type,
      });
      setStatusMessage("Group info updated.");
      setEditGroupOpen(false);
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleProposalUrlSave = async () => {
    setBusy(true);
    setError(null);
    try {
      await updateQuote(quoteId, {
        proposal_url: proposalUrlDraft.trim() || null,
      });
      setStatusMessage("Proposal link updated.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleManualNetworkSave = async () => {
    setBusy(true);
    setError(null);
    try {
      await updateQuote(quoteId, {
        manual_network: manualNetworkDraft.trim() || null,
      });
      setStatusMessage("Manual network override saved.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleSyncFromHubSpot = async () => {
    setBusy(true);
    setError(null);
    try {
      const result = await syncQuoteFromHubSpot(quoteId);
      setStatusMessage(
        `Synced from HubSpot. Ticket stage: ${result.ticket_stage || "n/a"} · Quote status: ${result.quote_status}`,
      );
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  if (!data) {
    return <div className="section">Loading quote...</div>;
  }

  const { quote, uploads, proposals } = data;
  const hubSpotSyncPending = isHubSpotSyncPending(quote);
  const manualOptions =
    quote.manual_network && !networkOptions.includes(quote.manual_network)
      ? [quote.manual_network, ...networkOptions]
      : networkOptions;
  const uploadUrl = (path: string) => {
    const basename = path.split("/").pop() || path;
    return `/uploads/${quote.id}/${basename}`;
  };

  return (
    <div className="section">
      <h2>Quote Detail</h2>
      {error && <div className="notice">{error}</div>}
      {statusMessage && (
        <div
          className={`notice notice-success ${statusMessageFading ? "fade-out" : ""}`}
        >
          {statusMessage}
        </div>
      )}

      <div className="kv" style={{ marginTop: 12 }}>
        <strong>Status</strong>
        <span className={`badge ${getQuoteStageClass(quote.status)}`}>
          {getQuoteStageLabel(quote.status)}
        </span>
        <strong>Version</strong>
        <span>v{quote.version}</span>
        <strong>HubSpot Ticket</strong>
        <span>
          {quote.hubspot_ticket_url ? (
            <a href={quote.hubspot_ticket_url} target="_blank" rel="noreferrer">
              {quote.hubspot_ticket_id || "Open Ticket"}
            </a>
          ) : (
            quote.hubspot_ticket_id || "Not linked"
          )}
        </span>
        <strong>HubSpot Last Sync</strong>
        <span>
          {quote.hubspot_last_synced_at
            ? new Date(quote.hubspot_last_synced_at).toLocaleString()
            : "Never"}
          {hubSpotSyncPending ? " (syncing...)" : ""}
        </span>
        <strong>HubSpot Sync Error</strong>
        <span>{quote.hubspot_sync_error || "—"}</span>
      </div>

      <div className="section-header">
        <h3>Employer Information</h3>
        <div className="inline-actions">
          {role === "admin" && (
            <>
              <label style={{ minWidth: 220 }}>
                Stage
                <select
                  value={stageDraft}
                  onChange={(e) => handleStageChange(e.target.value)}
                  disabled={busy}
                >
                  {stageOptions.map((stage) => (
                    <option key={stage} value={stage}>
                      {stage}
                    </option>
                  ))}
                </select>
              </label>
              {quote.hubspot_ticket_id && (
                <button
                  className="button ghost"
                  type="button"
                  onClick={handleSyncFromHubSpot}
                  disabled={busy}
                >
                  Sync from HubSpot
                </button>
              )}
            </>
          )}
          <button
            className="button ghost"
            type="button"
            onClick={() => {
              setEditGroupOpen((prev) => {
                const next = !prev;
                if (!next) resetGroupDraft();
                return next;
              });
            }}
          >
            {editGroupOpen ? "Cancel" : "Edit Group Info"}
          </button>
        </div>
      </div>
      {editGroupOpen ? (
        <div className="form-grid">
          <label>
            Employer Name
            <input
              value={groupDraft.company}
              onChange={(e) =>
                handleGroupDraftChange("company", e.target.value)
              }
            />
          </label>
          <label>
            Employer Street Address
            <input
              value={groupDraft.employer_street}
              onChange={(e) =>
                handleGroupDraftChange("employer_street", e.target.value)
              }
            />
          </label>
          <label>
            Employer City
            <input
              value={groupDraft.employer_city}
              onChange={(e) =>
                handleGroupDraftChange("employer_city", e.target.value)
              }
            />
          </label>
          <label>
            Employer State
            <input
              value={groupDraft.state}
              onChange={(e) => handleGroupDraftChange("state", e.target.value)}
            />
          </label>
          <label>
            Employer Zip Code
            <input
              value={groupDraft.employer_zip}
              onChange={(e) =>
                handleGroupDraftChange("employer_zip", e.target.value)
              }
            />
          </label>
          <label>
            Employer Domain
            <input
              value={groupDraft.employer_domain}
              onChange={(e) =>
                handleGroupDraftChange("employer_domain", e.target.value)
              }
              placeholder="company.com"
            />
          </label>
          <label>
            Quote Deadline
            <input
              type="date"
              value={groupDraft.quote_deadline}
              onChange={(e) =>
                handleGroupDraftChange("quote_deadline", e.target.value)
              }
            />
          </label>
          <label>
            Employer SIC
            <input
              value={groupDraft.employer_sic}
              onChange={(e) =>
                handleGroupDraftChange("employer_sic", e.target.value)
              }
            />
          </label>
          <label>
            Employer Effective Date
            <input
              type="date"
              value={groupDraft.effective_date}
              onChange={(e) =>
                handleGroupDraftChange("effective_date", e.target.value)
              }
            />
          </label>
          <label>
            Current Number Enrolled
            <input
              type="number"
              value={groupDraft.current_enrolled}
              onChange={(e) =>
                handleGroupDraftChange("current_enrolled", e.target.value)
              }
            />
          </label>
          <label>
            Current Number Eligible
            <input
              type="number"
              value={groupDraft.current_eligible}
              onChange={(e) =>
                handleGroupDraftChange("current_eligible", e.target.value)
              }
            />
          </label>
          <label>
            Employer Current Insurance Type
            <input
              value={groupDraft.current_insurance_type}
              onChange={(e) =>
                handleGroupDraftChange("current_insurance_type", e.target.value)
              }
            />
          </label>
          <div className="inline-actions">
            <button
              className="button"
              onClick={handleGroupSave}
              disabled={busy}
            >
              Save Changes
            </button>
            <button
              className="button ghost"
              type="button"
              onClick={() => {
                resetGroupDraft();
                setEditGroupOpen(false);
              }}
            >
              Cancel
            </button>
          </div>
        </div>
      ) : (
        <div className="kv">
          <strong>Employer Name</strong>
          <span>{quote.company}</span>
          <strong>Address</strong>
          <span>
            {quote.employer_street || "—"} · {quote.employer_city || "—"} ·{" "}
            {quote.state} {quote.employer_zip || ""}
          </span>
          <strong>Employer Domain</strong>
          <span>{quote.employer_domain || "—"}</span>
          <strong>Quote Deadline</strong>
          <span>{quote.quote_deadline || "—"}</span>
          <strong>Employer SIC</strong>
          <span>{quote.employer_sic || "—"}</span>
          <strong>Effective Date</strong>
          <span>{quote.effective_date}</span>
          <strong>Current Enrolled</strong>
          <span>{quote.current_enrolled}</span>
          <strong>Current Eligible</strong>
          <span>{quote.current_eligible}</span>
          <strong>Insurance Type</strong>
          <span>{quote.current_insurance_type}</span>
        </div>
      )}

      <h3 style={{ marginTop: 20 }}>Broker Information</h3>
      <div className="kv">
        <strong>Broker</strong>
        <span>
          {quote.broker_first_name || "—"} {quote.broker_last_name || ""}
        </span>
        <strong>Email</strong>
        <span>{quote.broker_email}</span>
        <strong>Phone</strong>
        <span>{quote.broker_phone}</span>
        <strong>Broker Fee</strong>
        <span>${quote.broker_fee_pepm.toFixed(2)} PEPM</span>
        <strong>Agent of Record</strong>
        <span>{quote.agent_of_record ? "Yes" : "No"}</span>
      </div>

      <h3 style={{ marginTop: 20 }}>High Cost Claimants</h3>
      <div className="notice">
        {quote.high_cost_info || "No details provided."}
      </div>

      <section className="section" style={{ marginTop: 24 }}>
        <h2>Census</h2>
        <div className="card-row">
          <div>
            <strong>
              {latestStandardization
                ? latestStandardization.status
                : "No Census Uploaded"}
            </strong>
            <div className="helper">
              {latestStandardization
                ? `${latestStandardization.issue_count} issue(s) · ${new Date(
                    latestStandardization.created_at,
                  ).toLocaleString()}`
                : "Upload a census to get started."}
            </div>
          </div>
          <button className="button secondary" onClick={openWizard}>
            Upload Census
          </button>
        </div>
      </section>

      <section className="section" id="assignment-output">
        <h2>Network</h2>
        <details className="config-collapse" open>
          <summary>Network Assignment</summary>
          <div className="config-collapse-body">
            {role === "admin" && (
              <div className="inline-actions" style={{ marginBottom: 12 }}>
                <label style={{ minWidth: 320 }}>
                  Manual Network Override
                  <select
                    value={manualNetworkDraft}
                    onChange={(e) => setManualNetworkDraft(e.target.value)}
                  >
                    <option value="">None</option>
                    {manualOptions.map((network) => (
                      <option key={network} value={network}>
                        {formatNetworkLabel(network)}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  className="button secondary"
                  type="button"
                  onClick={handleManualNetworkSave}
                  disabled={busy}
                >
                  Save Override
                </button>
              </div>
            )}
            {latestAssignment || quote.manual_network ? (
              <div>
                <div className="kv">
                  <strong>Direct Contract / Primary Network</strong>
                  <span>
                    {quote.manual_network
                      ? formatNetworkLabel(quote.manual_network)
                      : latestAssignment?.result_json.group_summary
                        ? formatNetworkLabel(
                            latestAssignment.result_json.group_summary
                              .primary_network,
                          )
                        : formatNetworkLabel(latestAssignment?.recommendation)}
                  </span>
                  <strong>
                    {latestAssignment?.result_json.group_summary
                      ? "Coverage Rate"
                      : "Match Rate"}
                  </strong>
                  <span>
                    {latestAssignment?.result_json.group_summary
                      ? `${Math.round(
                          latestAssignment.result_json.group_summary
                            .coverage_percentage * 100,
                        )}%`
                      : latestAssignment
                        ? `${Math.round(latestAssignment.confidence * 100)}%`
                        : "—"}
                  </span>
                  <strong>Confidence</strong>
                  <span>
                    {latestAssignment
                      ? `${Math.round(latestAssignment.confidence * 100)}%`
                      : "—"}
                  </span>
                  <strong>Rationale</strong>
                  <span>
                    {quote.manual_network && !latestAssignment
                      ? "Manual override set by admin."
                      : latestAssignment?.rationale || "—"}
                  </span>
                </div>
                <div className="inline-actions" style={{ marginBottom: 12 }}>
                  <button
                    className="button secondary"
                    onClick={handleAssign}
                    disabled={busy}
                  >
                    Refresh Network Assignment
                  </button>
                </div>
                {latestAssignment?.result_json.group_summary ? (
                  <>
                    <div className="card-row">
                      <div>
                        <strong>Primary Network</strong>
                        <div className="helper">
                          {quote.manual_network
                            ? `${formatNetworkLabel(quote.manual_network)} (manual override)`
                            : formatNetworkLabel(
                                latestAssignment.result_json.group_summary
                                  .primary_network,
                              )}
                        </div>
                      </div>
                      <div className="helper">
                        Coverage:{" "}
                        {Math.round(
                          latestAssignment.result_json.group_summary
                            .coverage_percentage * 100,
                        )}
                        % · Members counted:{" "}
                        {latestAssignment.result_json.group_summary
                          .total_members}
                      </div>
                    </div>
                    {latestAssignment && (
                      <div className="table-scroll">
                        <table className="table">
                          <thead>
                            <tr>
                              <th>Network</th>
                              <th>Coverage</th>
                            </tr>
                          </thead>
                          <tbody>
                            {coveragePagination.pageItems.map(
                              ([network, coverage]) => (
                                <tr key={network}>
                                  <td>{formatNetworkLabel(network)}</td>
                                  <td>{Math.round(Number(coverage) * 100)}%</td>
                                </tr>
                              ),
                            )}
                          </tbody>
                        </table>
                      </div>
                    )}
                    {latestAssignment && (
                      <TablePagination
                        page={coveragePagination.currentPage}
                        totalItems={coverageRows.length}
                        onPageChange={setCoveragePage}
                      />
                    )}
                    {latestAssignment.result_json.group_summary.invalid_rows
                      ?.length > 0 && (
                      <div className="notice" style={{ marginTop: 12 }}>
                        {
                          latestAssignment.result_json.group_summary.invalid_rows
                            .length
                        }{" "}
                        row(s) had invalid ZIPs and were excluded from coverage.
                      </div>
                    )}
                  </>
                ) : (
                  <div className="notice" style={{ marginTop: 12 }}>
                    {quote.manual_network
                      ? "Manual network override is active. Re-run assignment or upload a new census to replace it."
                      : "This quote has an older assignment format. Re-run assignment to see the latest network summary."}
                  </div>
                )}
              </div>
            ) : (
              <div className="helper">
                A network will be automatically assigned based on the census.
              </div>
            )}
          </div>
        </details>
      </section>

      <section className="section">
        <h2>Uploads</h2>
        <div className="form-grid">
          <label>
            Add Census
            <input
              type="file"
              multiple
              onChange={(e) => handleUpload(e, "census")}
            />
          </label>
          <label>
            Renewal
            <input
              type="file"
              multiple
              onChange={(e) => handleUpload(e, "renewal")}
            />
          </label>
          <label>
            SBC
            <input
              type="file"
              multiple
              onChange={(e) => handleUpload(e, "sbc")}
            />
          </label>
          <label>
            Claims
            <input
              type="file"
              multiple
              onChange={(e) => handleUpload(e, "claims")}
            />
          </label>
        </div>
        {uploads.length === 0 && <div className="helper">No uploads yet.</div>}
        {uploads.map((upload) => (
          <div key={upload.id} className="card-row">
            <div>
              <strong>{upload.filename}</strong>
              <div className="helper">
                {upload.type} · {new Date(upload.created_at).toLocaleString()}
              </div>
            </div>
            <div className="inline-actions">
              <a className="button secondary" href={uploadUrl(upload.path)}>
                View
              </a>
              <button
                className="button ghost"
                type="button"
                onClick={() => handleDeleteUpload(upload.id)}
              >
                Delete
              </button>
            </div>
          </div>
        ))}
      </section>

      <section className="section">
        <h2>Proposals</h2>
        {(role === "admin" || role === "broker") && (
          <div className="inline-actions" style={{ marginBottom: 12 }}>
            {quote.proposal_url ? (
              <a
                className="button secondary proposal-action"
                href={quote.proposal_url}
                target="_blank"
                rel="noreferrer"
              >
                View Proposal
              </a>
            ) : (
              <button
                className="button proposal-disabled proposal-action"
                type="button"
                disabled
              >
                View Proposal
              </button>
            )}
            <button
              className="button secondary proposal-action"
              onClick={handleConvert}
              disabled={busy}
            >
              Mark Sold
            </button>
          </div>
        )}
        {role === "admin" && (
          <div className="inline-actions" style={{ marginBottom: 12 }}>
            <label style={{ minWidth: 380 }}>
              Proposal URL (PandaDoc)
              <input
                type="url"
                placeholder="https://app.pandadoc.com/..."
                value={proposalUrlDraft}
                onChange={(e) => setProposalUrlDraft(e.target.value)}
              />
            </label>
            <button
              className="button secondary"
              type="button"
              onClick={handleProposalUrlSave}
              disabled={busy}
            >
              Save Link
            </button>
          </div>
        )}
        {proposals.length === 0 && (
          <div className="helper">No proposals yet.</div>
        )}
        {proposals.map((proposal) => (
          <div key={proposal.id} className="card-row">
            <div>
              <strong>{proposal.filename}</strong>
              <div className="helper">{proposal.status}</div>
            </div>
            <a className="button secondary" href={uploadUrl(proposal.path)}>
              Download
            </a>
          </div>
        ))}
      </section>

      <div className="inline-actions">
        <Link className="button ghost" to="/quotes">
          Back to Quotes
        </Link>
      </div>

      {wizardOpen && (
        <div className="modal-backdrop" onClick={() => setWizardOpen(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Census</h2>
              <button
                className="button ghost"
                onClick={() => setWizardOpen(false)}
              >
                Close
              </button>
            </div>
            {wizardStatus && (
              <div className="notice" style={{ marginBottom: 12 }}>
                Latest status: {wizardStatus}
              </div>
            )}
            <label>
              Upload Census
              <input
                type="file"
                accept=".csv,.xls,.xlsx"
                onChange={handleWizardCensusUpload}
              />
              <span className="helper">
                We’ll automatically standardize it and surface anything that
                needs attention.
              </span>
            </label>
            <div className="inline-actions" style={{ marginTop: 10 }}>
              <button
                className="button ghost"
                type="button"
                onClick={handleClearCensus}
                disabled={busy}
              >
                Remove Census
              </button>
            </div>
            <section style={{ marginTop: 12 }}>
              <h3>Map Columns</h3>
              <div className="wizard-layout">
                <aside className="wizard-sidebar">
                  <div className="wizard-note">
                    Please ensure your census file can be converted to Level
                    Health's census format. Map each uploaded column to the
                    expected field. Supported files: .csv, .xls, .xlsx.
                  </div>
                  <h4>Required Columns</h4>
                  {Object.entries(requiredHeaderLabels).map(([key, label]) => {
                    const mapped = Boolean(headerMappings[key]);
                    return (
                      <div key={key} className="required-item">
                        <span>{label}</span>
                        <span
                          className={`wizard-check ${mapped ? "ok" : "warn"}`}
                        >
                          {mapped ? "✓" : "!"}
                        </span>
                      </div>
                    );
                  })}
                </aside>
                <div className="wizard-table">
                  <header>
                    <div>Your (Uploaded) Columns</div>
                    <div>Sample Data</div>
                    <div>Expected Columns</div>
                  </header>
                  {detectedHeaders.length === 0 && (
                    <div className="wizard-row">
                      <div className="helper">
                        Upload a census to see columns.
                      </div>
                      <div />
                      <div />
                    </div>
                  )}
                  {detectedHeaders.map((header) => {
                    const mappedField = Object.entries(headerMappings).find(
                      ([, value]) => value === header,
                    )?.[0];
                    return (
                      <div key={header} className="wizard-row">
                        <div>{header}</div>
                        <div className="wizard-sample">
                          {(sampleData[header] || []).map((value, idx) => (
                            <span key={`${header}-${idx}`}>{value}</span>
                          ))}
                          {(sampleData[header] || []).length === 0 && (
                            <span className="helper">No sample data</span>
                          )}
                        </div>
                        <div className="wizard-select">
                          <select
                            value={mappedField || ""}
                            onChange={(e) => {
                              const nextField = e.target.value;
                              setHeaderMappings((prev) => {
                                const next = { ...prev };
                                if (!nextField) {
                                  Object.keys(next).forEach((key) => {
                                    if (next[key] === header) delete next[key];
                                  });
                                  return next;
                                }
                                Object.keys(next).forEach((key) => {
                                  if (next[key] === header) delete next[key];
                                });
                                next[nextField] = header;
                                return next;
                              });
                            }}
                          >
                            <option value="">Unmapped</option>
                            {Object.entries(requiredHeaderLabels).map(
                              ([key, label]) => (
                                <option key={key} value={key}>
                                  {label} (required)
                                </option>
                              ),
                            )}
                          </select>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              <div className="inline-actions" style={{ marginTop: 12 }}>
                <button
                  className="button"
                  onClick={handleWizardStandardize}
                  disabled={busy}
                >
                  Apply Mappings
                </button>
                <button
                  className="button secondary"
                  onClick={handleWizardResolve}
                  disabled={busy || wizardIssues.length === 0}
                >
                  Save Corrections
                </button>
                <button
                  className="button"
                  onClick={handleWizardSubmit}
                  disabled={busy}
                >
                  Submit
                </button>
              </div>
            </section>

            <section style={{ marginTop: 16 }}>
              <h3>Issues</h3>
              {wizardStatus && (
                <div className="helper" style={{ marginBottom: 8 }}>
                  {wizardStatus === "Complete"
                    ? "All required fields are valid."
                    : "Issues found. Edit values or mappings and re-check."}
                </div>
              )}
              {wizardStatus === "Complete" && wizardIssues.length === 0 && (
                <div style={{ marginBottom: 8 }}>
                  <span className="badge success">Census Complete</span>
                </div>
              )}
              {wizardStatus && (
                <div className="helper" style={{ marginBottom: 8 }}>
                  {wizardStatus === "Complete"
                    ? "Download the standardized file for your records."
                    : "Standardized file includes your current mappings."}
                </div>
              )}
              {wizardStatus && latestStandardization?.standardized_filename && (
                <div style={{ marginBottom: 12 }}>
                  <a
                    className="button secondary"
                    href={`/uploads/${quote.id}/${latestStandardization.standardized_filename}`}
                  >
                    Download Standardized Census
                  </a>
                </div>
              )}
              {wizardIssues.length === 0 && (
                <div className="helper">
                  No issues found. You're all set to continue.
                </div>
              )}
              {wizardIssues.length > 0 && (
                <>
                  <div className="table-scroll">
                    <table className="table elegant slim">
                      <thead>
                        <tr>
                          <th>Row</th>
                          <th>Field</th>
                          <th>Value</th>
                          <th>Mapped Value</th>
                          <th>Issue</th>
                        </tr>
                      </thead>
                      <tbody>
                        {wizardIssuesPagination.pageItems.map(
                          (issue, index) => {
                            const issueIndex =
                              wizardIssuesPagination.startItem + index - 1;
                            return (
                              <tr
                                key={`${issue.row}-${issue.field}-${issueIndex}`}
                              >
                                <td>
                                  <input
                                    type="number"
                                    value={issue.row}
                                    onChange={(e) => {
                                      const next = [...wizardIssues];
                                      next[issueIndex] = {
                                        ...issue,
                                        row: Number(e.target.value),
                                      };
                                      setWizardIssues(next);
                                    }}
                                  />
                                </td>
                                <td>
                                  <input
                                    value={issue.field}
                                    onChange={(e) => {
                                      const next = [...wizardIssues];
                                      next[issueIndex] = {
                                        ...issue,
                                        field: e.target.value,
                                      };
                                      setWizardIssues(next);
                                    }}
                                  />
                                </td>
                                <td>
                                  <input
                                    value={issue.value || ""}
                                    onChange={(e) => {
                                      const next = [...wizardIssues];
                                      next[issueIndex] = {
                                        ...issue,
                                        value: e.target.value,
                                      };
                                      setWizardIssues(next);
                                    }}
                                  />
                                </td>
                                <td>
                                  <input
                                    value={issue.mapped_value || ""}
                                    onChange={(e) => {
                                      const next = [...wizardIssues];
                                      next[issueIndex] = {
                                        ...issue,
                                        mapped_value: e.target.value,
                                      };
                                      setWizardIssues(next);
                                    }}
                                  />
                                </td>
                                <td>
                                  <input
                                    value={issue.issue}
                                    onChange={(e) => {
                                      const next = [...wizardIssues];
                                      next[issueIndex] = {
                                        ...issue,
                                        issue: e.target.value,
                                      };
                                      setWizardIssues(next);
                                    }}
                                  />
                                </td>
                              </tr>
                            );
                          },
                        )}
                      </tbody>
                    </table>
                  </div>
                  <TablePagination
                    page={wizardIssuesPagination.currentPage}
                    totalItems={wizardIssues.length}
                    onPageChange={setWizardIssuesPage}
                  />
                </>
              )}
            </section>
          </div>
        </div>
      )}
    </div>
  );
}
