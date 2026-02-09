import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { getInstallations, getQuote, getQuotes, getTasks, Installation, Quote, QuoteDetail, Task } from "../api";
import { useAccess } from "../access";
import { getQuoteStageLabel } from "../utils/quoteStatus";

type QueueFilter = "all" | "needs_action" | "due_week" | "submitted";

type DashboardAction = {
  id: string;
  kind: "quote" | "task";
  title: string;
  subtitle: string;
  href: string;
  actionLabel: string;
  dueDate: string | null;
  priority: number;
  tags: QueueFilter[];
};

type TimelineItem = {
  id: string;
  label: string;
  href: string;
  date: string;
  kind: "task" | "quote-deadline" | "effective-date";
};

const ACTIVE_QUOTE_STATUSES = new Set([
  "Draft",
  "Quote Submitted",
  "Submitted",
  "In Review",
  "Needs Action",
  "Proposal",
  "Proposal Ready",
]);
const COMPLETED_TASK_STATES = new Set(["Done", "Complete"]);
const FILTER_STORAGE_KEY = "dashboard_queue_filter";

function parseDateOnly(value?: string | null): Date | null {
  if (!value) return null;
  const parsed = new Date(`${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function startOfToday(): Date {
  const date = new Date();
  date.setHours(0, 0, 0, 0);
  return date;
}

function diffInDays(targetDate: Date, fromDate: Date): number {
  const msPerDay = 24 * 60 * 60 * 1000;
  return Math.floor((targetDate.getTime() - fromDate.getTime()) / msPerDay);
}

function formatDate(value?: string | null): string {
  if (!value) return "No date";
  const parsed = parseDateOnly(value);
  if (!parsed) return value;
  return parsed.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function dueLabel(value?: string | null): string {
  const target = parseDateOnly(value);
  if (!target) return "No due date";
  const days = diffInDays(target, startOfToday());
  if (days < 0) return `${Math.abs(days)}d overdue`;
  if (days === 0) return "Due today";
  if (days === 1) return "Due tomorrow";
  return `Due in ${days}d`;
}

function sortByDueAndPriority(items: DashboardAction[]): DashboardAction[] {
  return [...items].sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    const aDate = parseDateOnly(a.dueDate);
    const bDate = parseDateOnly(b.dueDate);
    if (aDate && bDate) return aDate.getTime() - bDate.getTime();
    if (aDate && !bDate) return -1;
    if (!aDate && bDate) return 1;
    return a.title.localeCompare(b.title);
  });
}

export default function Dashboard() {
  const [quotes, setQuotes] = useState<Quote[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [installations, setInstallations] = useState<Installation[]>([]);
  const [needsActionDetails, setNeedsActionDetails] = useState<Record<string, QuoteDetail>>({});
  const [queueFilter, setQueueFilter] = useState<QueueFilter>(() => {
    const stored = window.localStorage.getItem(FILTER_STORAGE_KEY);
    if (stored === "all" || stored === "needs_action" || stored === "due_week" || stored === "submitted") {
      return stored;
    }
    return "needs_action";
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { role, email } = useAccess();

  useEffect(() => {
    window.localStorage.setItem(FILTER_STORAGE_KEY, queueFilter);
  }, [queueFilter]);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    Promise.all([
      getQuotes({ role, email }),
      getTasks({ role, email }),
      getInstallations({ role, email }),
    ])
      .then(([quoteData, taskData, installationData]) => {
        if (!active) return;
        setQuotes(quoteData);
        setTasks(taskData);
        setInstallations(installationData);
      })
      .catch((err: any) => {
        if (!active) return;
        setError(err.message || "Failed to load dashboard data.");
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [role, email]);

  useEffect(() => {
    const needs = quotes.filter((quote) => quote.needs_action);
    if (needs.length === 0) {
      setNeedsActionDetails({});
      return;
    }
    Promise.all(
      needs.map(async (quote) => {
        try {
          return await getQuote(quote.id);
        } catch {
          return null;
        }
      })
    ).then((details) => {
      const mapping: Record<string, QuoteDetail> = {};
      details.forEach((detail) => {
        if (!detail) return;
        mapping[detail.quote.id] = detail;
      });
      setNeedsActionDetails(mapping);
    });
  }, [quotes]);

  const installationById = useMemo(
    () => Object.fromEntries(installations.map((item) => [item.id, item])),
    [installations]
  );

  const getNeedsActionReason = (quote: Quote) => {
    const detail = needsActionDetails[quote.id];
    if (!detail) return "Needs review";
    const hasCensus = detail.uploads.some((upload) => upload.type === "census");
    if (!hasCensus) return "Missing census upload";
    const latestStandardization = detail.standardizations[0];
    if (latestStandardization && latestStandardization.issue_count > 0) return "Census issues";
    if (detail.assignments.length === 0) return "Missing network assignment";
    return "Needs review";
  };

  const actionItems = useMemo(() => {
    const items: DashboardAction[] = [];
    const today = startOfToday();

    quotes.forEach((quote) => {
      const stage = getQuoteStageLabel(quote.status);
      const daysToDeadline = parseDateOnly(quote.quote_deadline)
        ? diffInDays(parseDateOnly(quote.quote_deadline) as Date, today)
        : null;
      const dueSoon = daysToDeadline !== null && daysToDeadline <= 7;

      if (quote.needs_action) {
        const reason = getNeedsActionReason(quote);
        let actionLabel = "Open Quote";
        if (reason === "Missing census upload") actionLabel = "Upload Census";
        if (reason === "Missing network assignment") actionLabel = "Run Assignment";
        items.push({
          id: `quote-${quote.id}`,
          kind: "quote",
          title: quote.company,
          subtitle: reason,
          href: `/quotes/${quote.id}`,
          actionLabel,
          dueDate: quote.quote_deadline || null,
          priority: reason === "Missing census upload" ? 1 : 2,
          tags: [
            "all",
            "needs_action",
            dueSoon ? "due_week" : "all",
            stage === "Quote Submitted" || stage === "In Review" ? "submitted" : "all",
          ],
        });
        return;
      }

      if (stage === "Draft") {
        items.push({
          id: `quote-draft-${quote.id}`,
          kind: "quote",
          title: quote.company,
          subtitle: "Complete intake and upload census",
          href: `/quotes/${quote.id}`,
          actionLabel: "Continue Quote",
          dueDate: quote.quote_deadline || null,
          priority: 4,
          tags: ["all", dueSoon ? "due_week" : "all"],
        });
      } else if ((stage === "Quote Submitted" || stage === "In Review") && !quote.latest_assignment) {
        items.push({
          id: `quote-submitted-${quote.id}`,
          kind: "quote",
          title: quote.company,
          subtitle: "Run network assignment",
          href: `/quotes/${quote.id}`,
          actionLabel: "Open Quote",
          dueDate: quote.quote_deadline || null,
          priority: 3,
          tags: ["all", "submitted", dueSoon ? "due_week" : "all"],
        });
      }
    });

    tasks
      .filter((task) => !COMPLETED_TASK_STATES.has(task.state))
      .forEach((task) => {
        const due = parseDateOnly(task.due_date);
        const daysToDue = due ? diffInDays(due, today) : null;
        const priority = daysToDue === null ? 6 : daysToDue < 0 ? 1 : daysToDue <= 2 ? 2 : daysToDue <= 7 ? 3 : 5;
        const taskCompany =
          task.installation_company || installationById[task.installation_id]?.company || "Implementation Task";
        items.push({
          id: `task-${task.id}`,
          kind: "task",
          title: `${taskCompany}`,
          subtitle: `${task.title} · ${task.state} · ${dueLabel(task.due_date)}`,
          href: `/implementations/${task.installation_id}`,
          actionLabel: "Open Task",
          dueDate: task.due_date,
          priority,
          tags: [
            "all",
            daysToDue !== null && daysToDue <= 7 ? "due_week" : "all",
            daysToDue !== null && daysToDue < 0 ? "needs_action" : "all",
          ],
        });
      });

    return sortByDueAndPriority(items);
  }, [quotes, tasks, installationById, needsActionDetails]);

  const filteredActionItems = useMemo(() => {
    if (queueFilter === "all") return actionItems.slice(0, 12);
    return actionItems.filter((item) => item.tags.includes(queueFilter)).slice(0, 12);
  }, [actionItems, queueFilter]);

  const activeQuotes = useMemo(
    () => quotes.filter((quote) => ACTIVE_QUOTE_STATUSES.has(getQuoteStageLabel(quote.status))),
    [quotes]
  );

  const latestQuote = useMemo(() => {
    if (activeQuotes.length === 0) return null;
    return [...activeQuotes].sort((a, b) => b.updated_at.localeCompare(a.updated_at))[0];
  }, [activeQuotes]);

  const censusPriorityQuote = useMemo(() => {
    return quotes.find((quote) => quote.needs_action && getNeedsActionReason(quote) === "Missing census upload") || null;
  }, [quotes, needsActionDetails]);

  const assignmentPriorityQuote = useMemo(() => {
    return (
      quotes.find(
        (quote) =>
          quote.needs_action && getNeedsActionReason(quote) === "Missing network assignment"
      ) || null
    );
  }, [quotes, needsActionDetails]);

  const timelineItems = useMemo(() => {
    const today = startOfToday();
    const timeline: TimelineItem[] = [];

    tasks.forEach((task) => {
      if (COMPLETED_TASK_STATES.has(task.state) || !task.due_date) return;
      const due = parseDateOnly(task.due_date);
      if (!due) return;
      const delta = diffInDays(due, today);
      if (delta < -7 || delta > 14) return;
      const taskCompany =
        task.installation_company || installationById[task.installation_id]?.company || "Implementation";
      timeline.push({
        id: `task-${task.id}`,
        label: `${taskCompany}: ${task.title}`,
        href: `/implementations/${task.installation_id}`,
        date: task.due_date,
        kind: "task",
      });
    });

    quotes.forEach((quote) => {
      if (quote.quote_deadline) {
        const deadline = parseDateOnly(quote.quote_deadline);
        if (deadline) {
          const delta = diffInDays(deadline, today);
          if (delta >= -7 && delta <= 14) {
            timeline.push({
              id: `quote-deadline-${quote.id}`,
              label: `${quote.company}: Quote deadline`,
              href: `/quotes/${quote.id}`,
              date: quote.quote_deadline,
              kind: "quote-deadline",
            });
          }
        }
      }
      if (quote.effective_date) {
        const effective = parseDateOnly(quote.effective_date);
        if (effective) {
          const delta = diffInDays(effective, today);
          if (delta >= -7 && delta <= 14) {
            timeline.push({
              id: `quote-effective-${quote.id}`,
              label: `${quote.company}: Effective date`,
              href: `/quotes/${quote.id}`,
              date: quote.effective_date,
              kind: "effective-date",
            });
          }
        }
      }
    });

    return timeline
      .sort((a, b) => {
        const aDate = parseDateOnly(a.date) as Date;
        const bDate = parseDateOnly(b.date) as Date;
        return aDate.getTime() - bDate.getTime();
      })
      .slice(0, 12);
  }, [tasks, quotes, installationById]);

  const recentActivity = useMemo(() => {
    const quoteActivity = quotes.map((quote) => ({
      id: `quote-${quote.id}`,
      timestamp: quote.updated_at,
      href: `/quotes/${quote.id}`,
      text: `${quote.company} updated · ${getQuoteStageLabel(quote.status)}`,
    }));
    const installationActivity = installations.map((installation) => ({
      id: `install-${installation.id}`,
      timestamp: installation.updated_at,
      href: `/implementations/${installation.id}`,
      text: `${installation.company} implementation · ${installation.status}`,
    }));
    return [...quoteActivity, ...installationActivity]
      .sort((a, b) => b.timestamp.localeCompare(a.timestamp))
      .slice(0, 8);
  }, [quotes, installations]);

  const summary = useMemo(() => {
    const submittedQuotes = quotes.filter((quote) => {
      const stage = getQuoteStageLabel(quote.status);
      return stage === "Quote Submitted" || stage === "In Review";
    }).length;
    const openTasks = tasks.filter((task) => !COMPLETED_TASK_STATES.has(task.state)).length;
    const needsAction = quotes.filter((quote) => quote.needs_action).length;
    const deadlines14 = timelineItems.length;
    return { submittedQuotes, openTasks, needsAction, deadlines14 };
  }, [quotes, tasks, timelineItems]);

  return (
    <div>
      <section className="section dashboard-hero">
        <div>
          <h2>Home</h2>
          <div className="helper">Focus on your highest-priority quotes and implementation tasks first.</div>
        </div>
        <div className="dashboard-quick-actions">
          <Link className="button" to="/quotes/new">
            New Quote
          </Link>
          <Link className={`button secondary ${!latestQuote ? "disabled-link" : ""}`} to={latestQuote ? `/quotes/${latestQuote.id}` : "#"}>
            Continue Last Quote
          </Link>
          <Link
            className={`button ghost ${!censusPriorityQuote ? "disabled-link" : ""}`}
            to={censusPriorityQuote ? `/quotes/${censusPriorityQuote.id}` : "#"}
          >
            Upload Census
          </Link>
          <Link
            className={`button ghost ${!assignmentPriorityQuote ? "disabled-link" : ""}`}
            to={assignmentPriorityQuote ? `/quotes/${assignmentPriorityQuote.id}` : "#"}
          >
            Run Assignment
          </Link>
        </div>
      </section>

      {error && <div className="notice">{error}</div>}
      {loading && <div className="section">Loading home page...</div>}

      {!loading && (
        <>
          <section className="section">
            <div className="dashboard-stats">
              <div className="dashboard-stat-card">
                <span className="helper">Needs Action</span>
                <strong>{summary.needsAction}</strong>
              </div>
              <div className="dashboard-stat-card">
                <span className="helper">Open Tasks</span>
                <strong>{summary.openTasks}</strong>
              </div>
              <div className="dashboard-stat-card">
                <span className="helper">Submitted Quotes</span>
                <strong>{summary.submittedQuotes}</strong>
              </div>
              <div className="dashboard-stat-card">
                <span className="helper">Deadlines (14d)</span>
                <strong>{summary.deadlines14}</strong>
              </div>
            </div>
          </section>

          <section className="section">
            <div className="section-header">
              <h3>What Needs Action Today</h3>
              <div className="dashboard-filter-row">
                <button
                  className={`button subtle ${queueFilter === "needs_action" ? "active-chip" : ""}`}
                  type="button"
                  onClick={() => setQueueFilter("needs_action")}
                >
                  Needs Action
                </button>
                <button
                  className={`button subtle ${queueFilter === "due_week" ? "active-chip" : ""}`}
                  type="button"
                  onClick={() => setQueueFilter("due_week")}
                >
                  Due This Week
                </button>
                <button
                  className={`button subtle ${queueFilter === "submitted" ? "active-chip" : ""}`}
                  type="button"
                  onClick={() => setQueueFilter("submitted")}
                >
                  Submitted
                </button>
                <button
                  className={`button subtle ${queueFilter === "all" ? "active-chip" : ""}`}
                  type="button"
                  onClick={() => setQueueFilter("all")}
                >
                  All
                </button>
              </div>
            </div>
            {filteredActionItems.length === 0 && (
              <div className="dashboard-empty">
                <strong>No action items for this filter.</strong>
                <span className="helper">Try a different filter or start a new quote.</span>
              </div>
            )}
            {filteredActionItems.map((item) => (
              <div key={item.id} className="dashboard-action-row">
                <div className="dashboard-action-content">
                  <div className="dashboard-action-title">{item.title}</div>
                  <div className="helper">{item.subtitle}</div>
                </div>
                <div className="dashboard-action-meta">
                  <span className={`dashboard-due-pill ${item.dueDate ? "" : "muted"}`}>
                    {item.dueDate ? dueLabel(item.dueDate) : "No due date"}
                  </span>
                  <Link className="button secondary" to={item.href}>
                    {item.actionLabel}
                  </Link>
                </div>
              </div>
            ))}
          </section>

          <section className="section">
            <h3>Deadlines Timeline (14 Days)</h3>
            {timelineItems.length === 0 && (
              <div className="helper">No deadlines in the next 14 days.</div>
            )}
            {timelineItems.map((item) => (
              <div key={item.id} className="dashboard-timeline-row">
                <div className="dashboard-timeline-date">{formatDate(item.date)}</div>
                <div className="dashboard-timeline-content">
                  <div>{item.label}</div>
                  <div className="helper">
                    {item.kind === "task" ? "Task due" : item.kind === "quote-deadline" ? "Quote deadline" : "Effective date"}
                  </div>
                </div>
                <Link className="button ghost" to={item.href}>
                  Open
                </Link>
              </div>
            ))}
          </section>

          <section className="section">
            <h3>Recent Activity</h3>
            {recentActivity.length === 0 && <div className="helper">No recent updates yet.</div>}
            {recentActivity.map((item) => (
              <div key={item.id} className="dashboard-activity-row">
                <div className="helper">{formatDate(item.timestamp.slice(0, 10))}</div>
                <Link className="table-link" to={item.href}>
                  {item.text}
                </Link>
              </div>
            ))}
          </section>
        </>
      )}
    </div>
  );
}
