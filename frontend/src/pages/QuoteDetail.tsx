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
import {
  TABLE_PAGE_SIZE,
  paginateItems,
  TablePagination,
} from "../components/TablePagination";
import { formatNetworkLabel } from "../utils/formatNetwork";
import { getQuoteStageClass, getQuoteStageLabel } from "../utils/quoteStatus";
import { useAutoDismissMessage } from "../hooks/useAutoDismissMessage";

type AssignmentGroupSummary = NonNullable<
  QuoteDetailType["assignments"][number]["result_json"]["group_summary"]
>;

type CensusValueMappings = {
  gender_map: Record<string, string>;
  relationship_map: Record<string, string>;
  tier_map: Record<string, string>;
};

type BulkFixField = "gender" | "relationship" | "enrollment_tier";

const BULK_FIX_FIELDS: Array<{ value: BulkFixField; label: string }> = [
  { value: "gender", label: "Gender" },
  { value: "relationship", label: "Relationship" },
  { value: "enrollment_tier", label: "Enrollment Tier" },
];

const BULK_FIX_TARGET_OPTIONS: Record<BulkFixField, string[]> = {
  gender: ["M", "F"],
  relationship: ["E", "S", "C"],
  enrollment_tier: ["EE", "ES", "EC", "EF", "W"],
};

const BULK_FIX_MAPPING_KEY_BY_FIELD: Record<
  BulkFixField,
  keyof CensusValueMappings
> = {
  gender: "gender_map",
  relationship: "relationship_map",
  enrollment_tier: "tier_map",
};

function formatEffectiveCoverageRate(summary?: AssignmentGroupSummary | null): string {
  if (!summary) return "—";
  if (summary.fallback_used) return "100% (fallback)";
  return `${Math.round(summary.coverage_percentage * 100)}%`;
}

function formatDirectCoverageRate(summary?: AssignmentGroupSummary | null): string {
  if (!summary) return "—";
  return `${Math.round(summary.coverage_percentage * 100)}%`;
}

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
  const [editBrokerOpen, setEditBrokerOpen] = useState(false);
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
    primary_network: "",
    secondary_network: "",
    tpa: "",
    stoploss: "",
    current_carrier: "",
    renewal_comparison: "",
  });
  const [brokerDraft, setBrokerDraft] = useState({
    broker_first_name: "",
    broker_last_name: "",
    broker_email: "",
    broker_phone: "",
    broker_org: "",
    broker_fee_pepm: "",
    agent_of_record: false,
  });
  const [stageDraft, setStageDraft] = useState("Draft");
  const [manualNetworkDraft, setManualNetworkDraft] = useState("");
  const [networkOptions, setNetworkOptions] = useState<string[]>([]);
  const [proposalUrlDraft, setProposalUrlDraft] = useState("");
  const [coveragePage, setCoveragePage] = useState(1);
  const [memberPage, setMemberPage] = useState(1);
  const [rankedPage, setRankedPage] = useState(1);
  const [wizardIssuesPage, setWizardIssuesPage] = useState(1);
  const [wizardView, setWizardView] = useState<"fix_queue" | "advanced">(
    "fix_queue",
  );
  const [activeIssueIndex, setActiveIssueIndex] = useState(0);
  const [showIssueRowsOnly, setShowIssueRowsOnly] = useState(false);
  const [valueMappings, setValueMappings] = useState<CensusValueMappings>({
    gender_map: {},
    relationship_map: {},
    tier_map: {},
  });
  const [bulkFixField, setBulkFixField] = useState<BulkFixField>("gender");
  const [bulkFixSourceValue, setBulkFixSourceValue] = useState("");
  const [bulkFixTargetValue, setBulkFixTargetValue] = useState("");
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
      primary_network: quote.primary_network || "",
      secondary_network: quote.secondary_network || "",
      tpa: quote.tpa || "",
      stoploss: quote.stoploss || "",
      current_carrier: quote.current_carrier || "",
      renewal_comparison: quote.renewal_comparison || "",
    });
  };

  const resetBrokerDraft = (quoteData?: QuoteDetailType["quote"]) => {
    const quote = quoteData || data?.quote;
    if (!quote) return;
    setBrokerDraft({
      broker_first_name: quote.broker_first_name || "",
      broker_last_name: quote.broker_last_name || "",
      broker_email: quote.broker_email || "",
      broker_phone: quote.broker_phone || "",
      broker_org: quote.broker_org || "",
      broker_fee_pepm:
        quote.broker_fee_pepm !== undefined && quote.broker_fee_pepm !== null
          ? String(quote.broker_fee_pepm)
          : "",
      agent_of_record: Boolean(quote.agent_of_record),
    });
  };

  useEffect(() => {
    resetGroupDraft(data?.quote);
    resetBrokerDraft(data?.quote);
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
  const latestGroupSummary = latestAssignment?.result_json.group_summary || null;
  const latestConfidencePercent = latestAssignment
    ? Math.round(latestAssignment.confidence * 100)
    : null;
  const directCoveragePercent = latestGroupSummary
    ? Math.round(latestGroupSummary.coverage_percentage * 100)
    : null;
  const hideConfidenceAsDuplicate =
    latestConfidencePercent !== null &&
    directCoveragePercent !== null &&
    latestConfidencePercent === directCoveragePercent;

  const coverageRows = useMemo(
    () =>
      Object.entries(latestAssignment?.result_json.coverage_by_network || {}).sort(
        ([, a], [, b]) => Number(b) - Number(a),
      ),
    [latestAssignment],
  );
  const memberAssignments = useMemo(
    () => latestAssignment?.result_json.member_assignments || [],
    [latestAssignment],
  );
  const rankedContracts = useMemo(
    () => latestAssignment?.result_json.ranked_contracts || [],
    [latestAssignment],
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

  const wizardIssuesPagination = useMemo(
    () => paginateItems(wizardIssues, wizardIssuesPage),
    [wizardIssues, wizardIssuesPage],
  );
  const issueRowNumbers = useMemo(
    () =>
      new Set(
        wizardIssues
          .map((issue) => Number(issue.row))
          .filter((row) => Number.isFinite(row)),
      ),
    [wizardIssues],
  );
  const samplePreviewRows = useMemo(
    () =>
      showIssueRowsOnly
        ? sampleRows.filter((row) => issueRowNumbers.has(Number(row.row)))
        : sampleRows,
    [sampleRows, issueRowNumbers, showIssueRowsOnly],
  );
  const issueSummaryRows = useMemo(() => {
    const fieldLabelMap: Record<string, string> = {
      first_name: "First Name",
      last_name: "Last Name",
      dob: "DOB",
      zip: "ZIP",
      gender: "Gender",
      relationship: "Relationship",
      enrollment_tier: "Enrollment Tier",
    };
    const bucketMap = new Map<
      string,
      { key: string; label: string; count: number; firstIndex: number }
    >();
    wizardIssues.forEach((issue, index) => {
      const labelPrefix = fieldLabelMap[issue.field] || issue.field || "General";
      const label = `${labelPrefix}: ${issue.issue}`;
      const key = `${issue.field || "_"}:${issue.issue}`;
      const existing = bucketMap.get(key);
      if (existing) {
        existing.count += 1;
        return;
      }
      bucketMap.set(key, { key, label, count: 1, firstIndex: index });
    });
    return Array.from(bucketMap.values()).sort((a, b) => b.count - a.count);
  }, [wizardIssues]);
  const activeIssue = wizardIssues[activeIssueIndex] || null;
  const activeIssueRowContext = useMemo(() => {
    if (!activeIssue) return null;
    return (
      sampleRows.find((row) => Number(row.row) === Number(activeIssue.row)) ||
      null
    );
  }, [activeIssue, sampleRows]);
  const activeIssueRowContextText = useMemo(() => {
    if (!activeIssueRowContext) return "";
    const rowFieldLabels: Record<string, string> = {
      first_name: "First Name",
      last_name: "Last Name",
      dob: "DOB",
      zip: "ZIP",
      gender: "Gender",
      relationship: "Relationship",
      enrollment_tier: "Enrollment Tier",
    };
    return Object.keys(rowFieldLabels)
      .map((key) => `${rowFieldLabels[key]}: ${activeIssueRowContext[key] || "—"}`)
      .join(" · ");
  }, [activeIssueRowContext]);

  const normalizeMappingValue = (value: string) => value.trim().toLowerCase();

  const cleanMappingRecord = (mapping: Record<string, string>) =>
    Object.fromEntries(
      Object.entries(mapping)
        .map(([from, to]) => [from.trim(), to.trim()] as const)
        .filter(([from, to]) => Boolean(from) && Boolean(to)),
    );

  const buildMappings = (mappings: CensusValueMappings = valueMappings) => ({
    gender_map: cleanMappingRecord(mappings.gender_map),
    relationship_map: cleanMappingRecord(mappings.relationship_map),
    tier_map: cleanMappingRecord(mappings.tier_map),
  });

  const bulkFixSourceOptions = useMemo(() => {
    const uniqueValues = new Set<string>();
    wizardIssues.forEach((issue) => {
      if (issue.field !== bulkFixField) return;
      const raw = String(issue.value || "").trim();
      if (raw) uniqueValues.add(raw);
    });
    Object.keys(valueMappings[BULK_FIX_MAPPING_KEY_BY_FIELD[bulkFixField]]).forEach(
      (raw) => {
        if (raw.trim()) uniqueValues.add(raw.trim());
      },
    );
    return Array.from(uniqueValues).sort((a, b) => a.localeCompare(b));
  }, [wizardIssues, bulkFixField, valueMappings]);

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

  useEffect(() => {
    if (wizardIssuesPage !== wizardIssuesPagination.currentPage) {
      setWizardIssuesPage(wizardIssuesPagination.currentPage);
    }
  }, [wizardIssuesPage, wizardIssuesPagination.currentPage]);

  useEffect(() => {
    if (wizardIssues.length === 0) {
      setActiveIssueIndex(0);
      return;
    }
    if (activeIssueIndex >= wizardIssues.length) {
      setActiveIssueIndex(wizardIssues.length - 1);
    }
  }, [wizardIssues, activeIssueIndex]);

  useEffect(() => {
    if (
      bulkFixSourceValue &&
      bulkFixSourceOptions.some((value) => value === bulkFixSourceValue)
    ) {
      return;
    }
    setBulkFixSourceValue(bulkFixSourceOptions[0] || "");
  }, [bulkFixSourceOptions, bulkFixSourceValue]);

  useEffect(() => {
    if (
      bulkFixTargetValue &&
      BULK_FIX_TARGET_OPTIONS[bulkFixField].includes(bulkFixTargetValue.toUpperCase())
    ) {
      return;
    }
    setBulkFixTargetValue(BULK_FIX_TARGET_OPTIONS[bulkFixField][0] || "");
  }, [bulkFixField, bulkFixTargetValue]);

  useEffect(() => {
    setValueMappings({
      gender_map: {},
      relationship_map: {},
      tier_map: {},
    });
    setBulkFixField("gender");
    setBulkFixSourceValue("");
    setBulkFixTargetValue("");
  }, [quoteId]);

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
    setWizardView("fix_queue");
    setShowIssueRowsOnly(false);
    setActiveIssueIndex(0);
    setWizardIssuesPage(1);
    if (latestStandardization) {
      setWizardIssues(latestStandardization.issues_json);
      setWizardStatus(latestStandardization.status);
    } else {
      setWizardIssues([]);
      setWizardStatus(null);
    }
  };

  const runWizardStandardize = async (mappings: CensusValueMappings) => {
    const result = await standardizeQuote(quoteId, {
      ...buildMappings(mappings),
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
  };

  const applyBulkMapping = (
    field: BulkFixField,
    sourceValue: string,
    targetValue: string,
  ) => {
    const rawSource = sourceValue.trim();
    const rawTarget = targetValue.trim().toUpperCase();
    if (!rawSource || !rawTarget) {
      setError("Provide both a source value and corrected value to apply a bulk fix.");
      return null;
    }
    const mappingKey = BULK_FIX_MAPPING_KEY_BY_FIELD[field];
    const sourceNormalized = normalizeMappingValue(rawSource);
    let affected = 0;
    setWizardIssues((prev) =>
      prev.map((issue) => {
        if (issue.field !== field) return issue;
        const issueValue = String(issue.value || "");
        if (normalizeMappingValue(issueValue) !== sourceNormalized) return issue;
        affected += 1;
        return {
          ...issue,
          mapped_value: rawTarget,
        };
      }),
    );
    let nextMappings: CensusValueMappings | null = null;
    setValueMappings((prev) => {
      const nextMap = {
        ...prev[mappingKey],
        [rawSource]: rawTarget,
      };
      nextMappings = {
        ...prev,
        [mappingKey]: nextMap,
      };
      return nextMappings;
    });
    setStatusMessage(
      `Applied bulk mapping ${field}: "${rawSource}" -> "${rawTarget}" (${affected} issue(s) updated).`,
    );
    return nextMappings;
  };

  const handleWizardStandardize = async () => {
    setBusy(true);
    setError(null);
    try {
      await runWizardStandardize(valueMappings);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleApplyBulkFix = async (reRunCheck: boolean) => {
    setError(null);
    const nextMappings = applyBulkMapping(
      bulkFixField,
      bulkFixSourceValue,
      bulkFixTargetValue,
    );
    if (!nextMappings || !reRunCheck) return;
    setBusy(true);
    try {
      await runWizardStandardize(nextMappings);
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

  const jumpToIssueIndex = (index: number) => {
    if (index < 0 || index >= wizardIssues.length) return;
    setWizardView("fix_queue");
    setActiveIssueIndex(index);
    setWizardIssuesPage(Math.floor(index / TABLE_PAGE_SIZE) + 1);
  };

  const updateWizardIssue = (
    index: number,
    patch: Partial<{
      row: number;
      field: string;
      value?: string;
      mapped_value?: string;
      issue: string;
    }>,
  ) => {
    setWizardIssues((prev) => {
      const next = [...prev];
      if (!next[index]) return prev;
      next[index] = { ...next[index], ...patch };
      return next;
    });
  };

  const getIssueGuidance = (field: string, issue: string) => {
    const normalizedField = (field || "").toLowerCase();
    const normalizedIssue = (issue || "").toLowerCase();
    if (normalizedField === "dob") {
      return "Use MM/DD/YYYY or YYYY-MM-DD. Example: 01/26/1968.";
    }
    if (normalizedField === "zip") {
      return "ZIP must be 5 digits. Example: 63101.";
    }
    if (normalizedField === "gender") {
      return "Use M or F.";
    }
    if (normalizedField === "relationship") {
      return "Use E, S, or C.";
    }
    if (normalizedField === "enrollment_tier") {
      return "Use EE, ES, EC, EF, or W.";
    }
    if (normalizedIssue.includes("missing required column")) {
      return "Map this field to the matching uploaded column in Step 1.";
    }
    if (normalizedIssue.includes("missing value")) {
      return "This row is missing data for a required field.";
    }
    return "Review this value and mapping, then run check again.";
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
      await runWizardStandardize(valueMappings);
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

  const handleBrokerDraftChange = (field: string, value: string | boolean) => {
    setBrokerDraft((prev) => ({ ...prev, [field]: value }));
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
      const payload: Record<string, string | number | null | undefined> = {
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
      };
      if (role === "admin") {
        payload.primary_network = groupDraft.primary_network || null;
        payload.secondary_network = groupDraft.secondary_network || null;
        payload.tpa = groupDraft.tpa || null;
        payload.stoploss = groupDraft.stoploss || null;
        payload.current_carrier = groupDraft.current_carrier || null;
        payload.renewal_comparison = groupDraft.renewal_comparison || null;
      }
      await updateQuote(quoteId, payload);
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

  const handleBrokerSave = async () => {
    setBusy(true);
    setError(null);
    try {
      const fee = toNumberOrUndefined(brokerDraft.broker_fee_pepm);
      if (fee === undefined) {
        setError("Broker fee must be a valid number.");
        return;
      }
      await updateQuote(quoteId, {
        broker_first_name: brokerDraft.broker_first_name.trim() || null,
        broker_last_name: brokerDraft.broker_last_name.trim() || null,
        broker_email: brokerDraft.broker_email.trim() || null,
        broker_phone: brokerDraft.broker_phone.trim() || null,
        broker_org: brokerDraft.broker_org.trim() || null,
        broker_fee_pepm: fee,
        agent_of_record: brokerDraft.agent_of_record,
      });
      setStatusMessage("Broker info updated.");
      setEditBrokerOpen(false);
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
          {role === "admin" && (
            <>
              <label>
                Primary Network
                <input
                  value={groupDraft.primary_network}
                  onChange={(e) =>
                    handleGroupDraftChange("primary_network", e.target.value)
                  }
                />
              </label>
              <label>
                Secondary Network
                <input
                  value={groupDraft.secondary_network}
                  onChange={(e) =>
                    handleGroupDraftChange("secondary_network", e.target.value)
                  }
                />
              </label>
              <label>
                TPA
                <input
                  value={groupDraft.tpa}
                  onChange={(e) => handleGroupDraftChange("tpa", e.target.value)}
                />
              </label>
              <label>
                Stoploss
                <input
                  value={groupDraft.stoploss}
                  onChange={(e) =>
                    handleGroupDraftChange("stoploss", e.target.value)
                  }
                />
              </label>
              <label>
                Current Carrier
                <input
                  value={groupDraft.current_carrier}
                  onChange={(e) =>
                    handleGroupDraftChange("current_carrier", e.target.value)
                  }
                />
              </label>
              <label>
                Renewal Comparison
                <input
                  value={groupDraft.renewal_comparison}
                  onChange={(e) =>
                    handleGroupDraftChange("renewal_comparison", e.target.value)
                  }
                />
              </label>
            </>
          )}
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
          <strong>Primary Network</strong>
          <span>{quote.primary_network || "—"}</span>
          <strong>Secondary Network</strong>
          <span>{quote.secondary_network || "—"}</span>
          <strong>TPA</strong>
          <span>{quote.tpa || "—"}</span>
          <strong>Stoploss</strong>
          <span>{quote.stoploss || "—"}</span>
          <strong>Current Carrier</strong>
          <span>{quote.current_carrier || "—"}</span>
          <strong>Renewal Comparison</strong>
          <span>{quote.renewal_comparison || "—"}</span>
        </div>
      )}

      <h3 style={{ marginTop: 20 }}>Broker Information</h3>
      {role === "admin" && (
        <div className="inline-actions" style={{ marginBottom: 10 }}>
          <button
            className="button ghost"
            type="button"
            onClick={() => {
              setEditBrokerOpen((prev) => {
                const next = !prev;
                if (!next) resetBrokerDraft();
                return next;
              });
            }}
            disabled={busy}
          >
            {editBrokerOpen ? "Cancel" : "Edit Broker Info"}
          </button>
        </div>
      )}
      {editBrokerOpen ? (
        <div className="edit-grid">
          <label>
            Broker First Name
            <input
              value={brokerDraft.broker_first_name}
              onChange={(e) =>
                handleBrokerDraftChange("broker_first_name", e.target.value)
              }
            />
          </label>
          <label>
            Broker Last Name
            <input
              value={brokerDraft.broker_last_name}
              onChange={(e) =>
                handleBrokerDraftChange("broker_last_name", e.target.value)
              }
            />
          </label>
          <label>
            Broker Email
            <input
              type="email"
              value={brokerDraft.broker_email}
              onChange={(e) =>
                handleBrokerDraftChange("broker_email", e.target.value)
              }
            />
          </label>
          <label>
            Broker Phone
            <input
              value={brokerDraft.broker_phone}
              onChange={(e) =>
                handleBrokerDraftChange("broker_phone", e.target.value)
              }
            />
          </label>
          <label>
            Broker Organization
            <input
              value={brokerDraft.broker_org}
              onChange={(e) =>
                handleBrokerDraftChange("broker_org", e.target.value)
              }
            />
          </label>
          <label>
            Broker Fee (PEPM)
            <input
              type="number"
              step="0.01"
              value={brokerDraft.broker_fee_pepm}
              onChange={(e) =>
                handleBrokerDraftChange("broker_fee_pepm", e.target.value)
              }
            />
          </label>
          <label>
            Agent of Record
            <input
              type="checkbox"
              checked={brokerDraft.agent_of_record}
              onChange={(e) =>
                handleBrokerDraftChange("agent_of_record", e.target.checked)
              }
            />
          </label>
          <div className="inline-actions">
            <button
              className="button"
              onClick={handleBrokerSave}
              disabled={busy}
            >
              Save Broker Info
            </button>
            <button
              className="button ghost"
              type="button"
              onClick={() => {
                resetBrokerDraft();
                setEditBrokerOpen(false);
              }}
            >
              Cancel
            </button>
          </div>
        </div>
      ) : (
        <div className="kv">
          <strong>Broker</strong>
          <span>
            {quote.broker_first_name || "—"} {quote.broker_last_name || ""}
          </span>
          <strong>Email</strong>
          <span>{quote.broker_email || "—"}</span>
          <strong>Phone</strong>
          <span>{quote.broker_phone || "—"}</span>
          <strong>Organization</strong>
          <span>{quote.broker_org || "—"}</span>
          <strong>Broker Fee</strong>
          <span>${quote.broker_fee_pepm.toFixed(2)} PEPM</span>
          <strong>Agent of Record</strong>
          <span>{quote.agent_of_record ? "Yes" : "No"}</span>
        </div>
      )}

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
                {role === "admin" && (
                  <>
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
                        {latestGroupSummary
                          ? formatEffectiveCoverageRate(latestGroupSummary)
                          : latestAssignment
                            ? `${Math.round(latestAssignment.confidence * 100)}%`
                            : "—"}
                      </span>
                      {latestGroupSummary?.fallback_used && (
                        <>
                          <strong>Direct Contract Match</strong>
                          <span>{formatDirectCoverageRate(latestGroupSummary)}</span>
                        </>
                      )}
                      {!hideConfidenceAsDuplicate && (
                        <>
                          <strong>Confidence</strong>
                          <span>
                            {latestAssignment
                              ? `${Math.round(latestAssignment.confidence * 100)}%`
                              : "—"}
                          </span>
                        </>
                      )}
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
                  </>
                )}
                {latestGroupSummary ? (
                  <>
                    <div className="card-row">
                      <div>
                        <strong>Primary Network</strong>
                        <div className="helper">
                          {quote.manual_network
                            ? `${formatNetworkLabel(quote.manual_network)} (manual override)`
                            : formatNetworkLabel(latestGroupSummary.primary_network)}
                        </div>
                      </div>
                      <div className="helper">
                        Coverage: {formatEffectiveCoverageRate(latestGroupSummary)}
                        {latestGroupSummary.fallback_used
                          ? ` · Direct contract match: ${formatDirectCoverageRate(latestGroupSummary)}`
                          : ""}
                        {" · Members counted: "}
                        {latestGroupSummary.total_members}
                      </div>
                    </div>
                    {latestAssignment && (
                      <div className="table-scroll">
                        <table className="table">
                          <thead>
                            <tr>
                              <th>Network</th>
                              <th>
                                {latestGroupSummary.fallback_used
                                  ? "Direct Match Rate"
                                  : "Coverage"}
                              </th>
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
                    {latestGroupSummary.invalid_rows?.length > 0 && (
                      <div className="notice" style={{ marginTop: 12 }}>
                        {latestGroupSummary.invalid_rows.length}{" "}
                        row(s) had invalid ZIPs and were excluded from coverage.
                      </div>
                    )}
                    {memberAssignments.length > 0 && (
                      <details className="config-collapse" style={{ marginTop: 12 }}>
                        <summary>
                          ZIP &amp; Member Assignment Results ({memberAssignments.length})
                        </summary>
                        <div className="config-collapse-body">
                          <div className="helper" style={{ marginBottom: 8 }}>
                            Member-level ZIP mapping details used to calculate
                            network coverage.
                          </div>
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
                                      <td>
                                        {formatNetworkLabel(item.assigned_network)}
                                      </td>
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
                        </div>
                      </details>
                    )}
                    {rankedContracts.length > 0 && (
                      <details className="config-collapse" style={{ marginTop: 12 }}>
                        <summary>
                          Ranked Networks ({rankedContracts.length})
                        </summary>
                        <div className="config-collapse-body">
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
                        </div>
                      </details>
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

      <section className="section">
        <details className="config-collapse">
          <summary>HubSpot Sync Details</summary>
          <div className="config-collapse-body">
            <div className="kv">
              <strong>Version</strong>
              <span>v{quote.version}</span>
              <strong>HubSpot Ticket</strong>
              <span>
                {quote.hubspot_ticket_url ? (
                  <a
                    href={quote.hubspot_ticket_url}
                    target="_blank"
                    rel="noreferrer"
                  >
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
          </div>
        </details>
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
            <div className="notice" style={{ marginTop: 12 }}>
              <strong>Step-by-Step Guide</strong>
              <ol style={{ margin: "8px 0 0 18px" }}>
                <li>Upload or replace the census file.</li>
                <li>Map uploaded columns to required Level Health fields.</li>
                <li>Use Bulk Value Fixes for recurring values (example: female to F).</li>
                <li>Run Check to validate and refresh issue counts.</li>
                <li>Fix remaining one-off issues, then submit.</li>
              </ol>
            </div>
            <section style={{ marginTop: 12 }}>
              <h3>Step 1: Map Columns</h3>
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
              <div style={{ marginTop: 12 }}>
                <div
                  className="card-row"
                  style={{ marginBottom: 8, alignItems: "center" }}
                >
                  <div>
                    <strong>Sample Rows</strong>
                    <div className="helper">
                      Showing up to {sampleRows.length} rows from this census.
                      Total rows scanned: {totalRows}. Rows with issues:{" "}
                      {issueRows}.
                    </div>
                  </div>
                  <label
                    style={{
                      display: "inline-flex",
                      alignItems: "center",
                      gap: 8,
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={showIssueRowsOnly}
                      onChange={(e) => setShowIssueRowsOnly(e.target.checked)}
                    />
                    Show only rows with issues
                  </label>
                </div>
                {samplePreviewRows.length === 0 ? (
                  <div className="helper">
                    No sample rows available for this filter.
                  </div>
                ) : (
                  <div className="table-scroll">
                    <table className="table slim">
                      <thead>
                        <tr>
                          <th>Row</th>
                          {Object.entries(requiredHeaderLabels).map(
                            ([key, label]) => (
                              <th key={`sample-head-${key}`}>{label}</th>
                            ),
                          )}
                        </tr>
                      </thead>
                      <tbody>
                        {samplePreviewRows.map((row, index) => (
                          <tr key={`sample-row-${row.row || index}`}>
                            <td>{row.row || "—"}</td>
                            {Object.keys(requiredHeaderLabels).map((key) => (
                              <td key={`sample-cell-${row.row}-${key}`}>
                                {row[key] || "—"}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              <div className="inline-actions" style={{ marginTop: 12 }}>
                <button
                  className="button"
                  onClick={handleWizardStandardize}
                  disabled={busy}
                >
                  Run Check
                </button>
                <button
                  className="button secondary"
                  onClick={handleWizardResolve}
                  disabled={busy || wizardIssues.length === 0}
                >
                  Save Issue Edits
                </button>
                <button
                  className="button"
                  onClick={handleWizardSubmit}
                  disabled={busy}
                >
                  Fix &amp; Submit
                </button>
              </div>
            </section>

            <section style={{ marginTop: 16 }}>
              <h3>Step 2: Bulk Value Fixes</h3>
              <div className="helper" style={{ marginBottom: 8 }}>
                Correct repeated values in one action and optionally re-run checks.
              </div>
              <div className="form-grid">
                <label>
                  Field
                  <select
                    value={bulkFixField}
                    onChange={(e) => setBulkFixField(e.target.value as BulkFixField)}
                  >
                    {BULK_FIX_FIELDS.map((fieldOption) => (
                      <option key={fieldOption.value} value={fieldOption.value}>
                        {fieldOption.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Current Value (from census)
                  <input
                    list={`bulk-source-values-${bulkFixField}`}
                    value={bulkFixSourceValue}
                    onChange={(e) => setBulkFixSourceValue(e.target.value)}
                    placeholder="ex: female"
                  />
                  <datalist id={`bulk-source-values-${bulkFixField}`}>
                    {bulkFixSourceOptions.map((value) => (
                      <option key={`bulk-source-${bulkFixField}-${value}`} value={value} />
                    ))}
                  </datalist>
                </label>
                <label>
                  Correct To
                  <input
                    list={`bulk-target-values-${bulkFixField}`}
                    value={bulkFixTargetValue}
                    onChange={(e) => setBulkFixTargetValue(e.target.value.toUpperCase())}
                    placeholder="ex: F"
                  />
                  <datalist id={`bulk-target-values-${bulkFixField}`}>
                    {BULK_FIX_TARGET_OPTIONS[bulkFixField].map((value) => (
                      <option key={`bulk-target-${bulkFixField}-${value}`} value={value} />
                    ))}
                  </datalist>
                </label>
              </div>
              <div className="inline-actions" style={{ marginTop: 10 }}>
                <button
                  className="button ghost"
                  type="button"
                  onClick={() => void handleApplyBulkFix(false)}
                  disabled={busy}
                >
                  Apply Bulk Fix
                </button>
                <button
                  className="button secondary"
                  type="button"
                  onClick={() => void handleApplyBulkFix(true)}
                  disabled={busy}
                >
                  Apply &amp; Run Check
                </button>
              </div>
            </section>

            <section style={{ marginTop: 16 }}>
              <h3>Step 3: Needs Action</h3>
              {wizardStatus && (
                <div className="helper" style={{ marginBottom: 8 }}>
                  {wizardStatus === "Complete"
                    ? "All required fields are valid."
                    : "Fix issues below, then run check again."}
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
                  <div className="notice" style={{ marginBottom: 10 }}>
                    <strong>Issue Summary</strong>
                    <div className="helper" style={{ marginTop: 4 }}>
                      Click a row to jump to the first matching issue.
                    </div>
                    <div style={{ marginTop: 8 }}>
                      {issueSummaryRows.map((summary) => (
                        <div
                          key={summary.key}
                          className="card-row"
                          style={{ marginBottom: 8, padding: "8px 10px" }}
                        >
                          <div>
                            <strong>{summary.label}</strong>
                            <div className="helper">{summary.count} issue(s)</div>
                          </div>
                          <button
                            className="button ghost"
                            type="button"
                            onClick={() => jumpToIssueIndex(summary.firstIndex)}
                          >
                            Jump to first
                          </button>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div
                    className="inline-actions"
                    style={{ marginBottom: 10, justifyContent: "space-between" }}
                  >
                    <div className="helper">View mode</div>
                    <div className="inline-actions">
                      <button
                        className={`button subtle ${wizardView === "fix_queue" ? "active-chip" : ""}`}
                        type="button"
                        onClick={() => setWizardView("fix_queue")}
                      >
                        Fix Queue
                      </button>
                      <button
                        className={`button subtle ${wizardView === "advanced" ? "active-chip" : ""}`}
                        type="button"
                        onClick={() => setWizardView("advanced")}
                      >
                        Advanced Table
                      </button>
                    </div>
                  </div>
                  {wizardView === "fix_queue" && activeIssue && (
                    <div className="section" style={{ marginTop: 0 }}>
                      <div
                        className="card-row"
                        style={{ marginBottom: 10, alignItems: "center" }}
                      >
                        <div>
                          <strong>
                            Issue {activeIssueIndex + 1} of {wizardIssues.length}
                          </strong>
                          <div className="helper">
                            Row {activeIssue.row} · {activeIssue.field}
                          </div>
                        </div>
                        <div className="inline-actions">
                          <button
                            className="button ghost"
                            type="button"
                            onClick={() =>
                              jumpToIssueIndex(Math.max(0, activeIssueIndex - 1))
                            }
                            disabled={activeIssueIndex <= 0}
                          >
                            Previous
                          </button>
                          <button
                            className="button ghost"
                            type="button"
                            onClick={() =>
                              jumpToIssueIndex(
                                Math.min(wizardIssues.length - 1, activeIssueIndex + 1),
                              )
                            }
                            disabled={activeIssueIndex >= wizardIssues.length - 1}
                          >
                            Next
                          </button>
                        </div>
                      </div>
                      <div className="form-grid">
                        <label>
                          Row
                          <input
                            type="number"
                            value={activeIssue.row}
                            onChange={(e) =>
                              updateWizardIssue(activeIssueIndex, {
                                row: Number(e.target.value),
                              })
                            }
                          />
                        </label>
                        <label>
                          Field
                          <input
                            value={activeIssue.field}
                            onChange={(e) =>
                              updateWizardIssue(activeIssueIndex, {
                                field: e.target.value,
                              })
                            }
                          />
                        </label>
                        <label>
                          Value
                          <input
                            value={activeIssue.value || ""}
                            onChange={(e) =>
                              updateWizardIssue(activeIssueIndex, {
                                value: e.target.value,
                              })
                            }
                          />
                        </label>
                        <label>
                          Mapped Value
                          <input
                            value={activeIssue.mapped_value || ""}
                            onChange={(e) =>
                              updateWizardIssue(activeIssueIndex, {
                                mapped_value: e.target.value,
                              })
                            }
                          />
                          {(
                            activeIssue.field === "gender" ||
                            activeIssue.field === "relationship" ||
                            activeIssue.field === "enrollment_tier"
                          ) && (
                            <>
                              <span className="helper">
                                Tip: Apply this correction to all matching values below.
                              </span>
                              <button
                                className="button ghost"
                                type="button"
                                style={{ marginTop: 6 }}
                                onClick={() => {
                                  setBulkFixField(activeIssue.field as BulkFixField);
                                  setBulkFixSourceValue(String(activeIssue.value || ""));
                                  setBulkFixTargetValue(
                                    String(activeIssue.mapped_value || ""),
                                  );
                                  void handleApplyBulkFix(false);
                                }}
                                disabled={
                                  busy ||
                                  !String(activeIssue.value || "").trim() ||
                                  !String(activeIssue.mapped_value || "").trim()
                                }
                              >
                                Apply To All Matching Values
                              </button>
                            </>
                          )}
                        </label>
                        <label>
                          Issue
                          <input
                            value={activeIssue.issue}
                            onChange={(e) =>
                              updateWizardIssue(activeIssueIndex, {
                                issue: e.target.value,
                              })
                            }
                          />
                        </label>
                      </div>
                      <div className="notice" style={{ marginTop: 10 }}>
                        <strong>Guidance</strong>
                        <div className="helper">
                          {getIssueGuidance(activeIssue.field, activeIssue.issue)}
                        </div>
                        {activeIssueRowContext && (
                          <div className="helper" style={{ marginTop: 6 }}>
                            Row context: {activeIssueRowContextText}
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                  {wizardView === "advanced" && (
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
                            {wizardIssuesPagination.pageItems.map((issue, index) => {
                              const issueIndex =
                                wizardIssuesPagination.startItem + index - 1;
                              return (
                                <tr key={`${issue.row}-${issue.field}-${issueIndex}`}>
                                  <td>
                                    <input
                                      type="number"
                                      value={issue.row}
                                      onChange={(e) =>
                                        updateWizardIssue(issueIndex, {
                                          row: Number(e.target.value),
                                        })
                                      }
                                    />
                                  </td>
                                  <td>
                                    <input
                                      value={issue.field}
                                      onChange={(e) =>
                                        updateWizardIssue(issueIndex, {
                                          field: e.target.value,
                                        })
                                      }
                                    />
                                  </td>
                                  <td>
                                    <input
                                      value={issue.value || ""}
                                      onChange={(e) =>
                                        updateWizardIssue(issueIndex, {
                                          value: e.target.value,
                                        })
                                      }
                                    />
                                  </td>
                                  <td>
                                    <input
                                      value={issue.mapped_value || ""}
                                      onChange={(e) =>
                                        updateWizardIssue(issueIndex, {
                                          mapped_value: e.target.value,
                                        })
                                      }
                                    />
                                  </td>
                                  <td>
                                    <input
                                      value={issue.issue}
                                      onChange={(e) =>
                                        updateWizardIssue(issueIndex, {
                                          issue: e.target.value,
                                        })
                                      }
                                    />
                                  </td>
                                </tr>
                              );
                            })}
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
                </>
              )}
            </section>
          </div>
        </div>
      )}
    </div>
  );
}
