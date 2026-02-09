import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { getTasks, getUsers, Task, User, updateTask } from "../api";
import { useAccess } from "../access";
import { paginateItems, TablePagination } from "../components/TablePagination";

type MultiSelectDropdownProps = {
  label: string;
  options: string[];
  selected: string[];
  onChange: (values: string[]) => void;
};

function MultiSelectDropdown({ label, options, selected, onChange }: MultiSelectDropdownProps) {
  const toggle = (value: string) => {
    if (selected.includes(value)) {
      onChange(selected.filter((item) => item !== value));
      return;
    }
    onChange([...selected, value]);
  };

  const summary =
    selected.length === 0 ? "All" : selected.length === 1 ? selected[0] : `${selected.length} selected`;

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
              <span className="multi-dropdown-check">{selected.includes(option) ? "✓" : ""}</span>
              <span>{option}</span>
            </button>
          ))}
        </div>
      </details>
    </div>
  );
}

function parseDateOnly(value: string | null): Date | null {
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

function formatDueDate(value: string | null): string {
  if (!value) return "No due date";
  const parsed = parseDateOnly(value);
  if (!parsed) return value;
  return parsed.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function dueDescriptor(value: string | null): string {
  const parsed = parseDateOnly(value);
  if (!parsed) return "No due date";
  const msPerDay = 24 * 60 * 60 * 1000;
  const days = Math.floor((parsed.getTime() - startOfToday().getTime()) / msPerDay);
  if (days < 0) return `${Math.abs(days)}d overdue`;
  if (days === 0) return "Due today";
  if (days === 1) return "Due tomorrow";
  return `Due in ${days}d`;
}

export default function Tasks() {
  const { role, email } = useAccess();
  const isAdmin = role === "admin";
  const [tasks, setTasks] = useState<Task[]>([]);
  const [users, setUsers] = useState<User[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [savingTaskId, setSavingTaskId] = useState<string | null>(null);
  const [dueDateDrafts, setDueDateDrafts] = useState<Record<string, string>>({});
  const [assignedUserDrafts, setAssignedUserDrafts] = useState<Record<string, string>>({});
  const [stateFilters, setStateFilters] = useState<string[]>([]);
  const [ownerFilters, setOwnerFilters] = useState<string[]>([]);
  const [dueFilter, setDueFilter] = useState<"all" | "overdue" | "today" | "week" | "no_due">("all");
  const [companySearch, setCompanySearch] = useState("");
  const [sortBy, setSortBy] = useState<"due_date" | "state" | "owner" | "company">("due_date");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [page, setPage] = useState(1);
  const filtersRef = useRef<HTMLDivElement | null>(null);

  const loadTasks = async () => {
    setError(null);
    const rows = await getTasks({ role, email });
    setTasks(rows);
  };

  useEffect(() => {
    loadTasks().catch((err) => setError(err.message));
  }, [role, email]);

  useEffect(() => {
    if (!isAdmin) {
      setUsers([]);
      return;
    }
    getUsers()
      .then(setUsers)
      .catch((err) => setError(err.message));
  }, [isAdmin]);

  useEffect(() => {
    const handleOutsideClick = (event: MouseEvent) => {
      const target = event.target as Node | null;
      if (!filtersRef.current || (target && filtersRef.current.contains(target))) return;
      document
        .querySelectorAll<HTMLDetailsElement>(".multi-dropdown[open]")
        .forEach((el) => el.removeAttribute("open"));
    };
    document.addEventListener("mousedown", handleOutsideClick);
    return () => document.removeEventListener("mousedown", handleOutsideClick);
  }, []);

  useEffect(() => {
    setDueDateDrafts(
      Object.fromEntries(tasks.map((task) => [task.id, task.due_date || ""]))
    );
    setAssignedUserDrafts(
      Object.fromEntries(tasks.map((task) => [task.id, task.assigned_user_id || ""]))
    );
  }, [tasks]);

  const userNameById = useMemo(
    () =>
      Object.fromEntries(
        users.map((user) => [user.id, `${user.first_name} ${user.last_name}`.trim()])
      ),
    [users]
  );

  const handleSaveTaskMeta = async (task: Task) => {
    setSavingTaskId(task.id);
    setError(null);
    setStatusMessage(null);
    try {
      const dueDate = (dueDateDrafts[task.id] ?? task.due_date ?? "").trim();
      const assignedUserId = (assignedUserDrafts[task.id] ?? task.assigned_user_id ?? "").trim();
      await updateTask(
        task.installation_id,
        task.id,
        {
          due_date: dueDate || null,
          assigned_user_id: assignedUserId || null,
        },
        { role, email }
      );
      setStatusMessage("Task assignment and due date updated.");
      await loadTasks();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setSavingTaskId(null);
    }
  };

  const stateOptions = useMemo(
    () => Array.from(new Set(tasks.map((task) => task.state))).sort((a, b) => a.localeCompare(b)),
    [tasks]
  );

  const ownerOptions = useMemo(
    () => Array.from(new Set(tasks.map((task) => task.owner))).sort((a, b) => a.localeCompare(b)),
    [tasks]
  );

  const visibleTasks = useMemo(() => {
    const today = startOfToday();

    const filtered = tasks.filter((task) => {
      const taskCompany = task.installation_company || "";
      const dueDate = parseDateOnly(task.due_date);
      const dueDelta = dueDate
        ? Math.floor((dueDate.getTime() - today.getTime()) / (24 * 60 * 60 * 1000))
        : null;

      const matchesState = stateFilters.length === 0 || stateFilters.includes(task.state);
      const matchesOwner = ownerFilters.length === 0 || ownerFilters.includes(task.owner);
      const matchesSearch =
        !companySearch.trim() ||
        taskCompany.toLowerCase().includes(companySearch.trim().toLowerCase()) ||
        task.title.toLowerCase().includes(companySearch.trim().toLowerCase());

      const matchesDue =
        dueFilter === "all"
          ? true
          : dueFilter === "overdue"
          ? dueDelta !== null && dueDelta < 0
          : dueFilter === "today"
          ? dueDelta !== null && dueDelta === 0
          : dueFilter === "week"
          ? dueDelta !== null && dueDelta >= 0 && dueDelta <= 7
          : task.due_date === null;

      return matchesState && matchesOwner && matchesSearch && matchesDue;
    });

    const sorted = [...filtered].sort((a, b) => {
      if (sortBy === "due_date") {
        const left = parseDateOnly(a.due_date);
        const right = parseDateOnly(b.due_date);
        if (left && right) return left.getTime() - right.getTime();
        if (left && !right) return -1;
        if (!left && right) return 1;
        return a.title.localeCompare(b.title);
      }
      if (sortBy === "state") return a.state.localeCompare(b.state);
      if (sortBy === "owner") return a.owner.localeCompare(b.owner);
      const leftCompany = a.installation_company || "";
      const rightCompany = b.installation_company || "";
      return leftCompany.localeCompare(rightCompany);
    });

    return sortDirection === "asc" ? sorted : sorted.reverse();
  }, [tasks, stateFilters, ownerFilters, companySearch, dueFilter, sortBy, sortDirection]);

  const openCount = useMemo(
    () => tasks.filter((task) => !["Done", "Complete"].includes(task.state)).length,
    [tasks]
  );

  const overdueCount = useMemo(() => {
    const today = startOfToday();
    return tasks.filter((task) => {
      const dueDate = parseDateOnly(task.due_date);
      if (!dueDate) return false;
      return dueDate.getTime() < today.getTime() && !["Done", "Complete"].includes(task.state);
    }).length;
  }, [tasks]);

  const pagination = useMemo(
    () => paginateItems(visibleTasks, page),
    [visibleTasks, page]
  );

  useEffect(() => {
    if (page !== pagination.currentPage) {
      setPage(pagination.currentPage);
    }
  }, [page, pagination.currentPage]);

  return (
    <section className="section">
      <h2>Tasks</h2>
      {statusMessage && <div className="notice notice-success">{statusMessage}</div>}
      {error && <div className="notice">{error}</div>}
      <div className="grid grid-3" style={{ marginBottom: 12 }}>
        <div className="section" style={{ marginBottom: 0 }}>
          <div className="helper">Total Tasks</div>
          <div style={{ fontSize: 28, fontWeight: 700 }}>{tasks.length}</div>
        </div>
        <div className="section" style={{ marginBottom: 0 }}>
          <div className="helper">Open Tasks</div>
          <div style={{ fontSize: 28, fontWeight: 700 }}>{openCount}</div>
        </div>
        <div className="section" style={{ marginBottom: 0 }}>
          <div className="helper">Overdue Tasks</div>
          <div style={{ fontSize: 28, fontWeight: 700 }}>{overdueCount}</div>
        </div>
      </div>

      <div ref={filtersRef} className="inline-actions" style={{ marginBottom: 12 }}>
        <MultiSelectDropdown
          label="State"
          options={stateOptions}
          selected={stateFilters}
          onChange={setStateFilters}
        />
        <MultiSelectDropdown
          label="Owner"
          options={ownerOptions}
          selected={ownerFilters}
          onChange={setOwnerFilters}
        />
        <label>
          Due Window
          <select
            value={dueFilter}
            onChange={(event) =>
              setDueFilter(event.target.value as "all" | "overdue" | "today" | "week" | "no_due")
            }
          >
            <option value="all">All</option>
            <option value="overdue">Overdue</option>
            <option value="today">Due Today</option>
            <option value="week">Due This Week</option>
            <option value="no_due">No Due Date</option>
          </select>
        </label>
        <label>
          Group / Task Search
          <input
            value={companySearch}
            onChange={(event) => setCompanySearch(event.target.value)}
            placeholder="Search group or task"
          />
        </label>
        <label>
          Sort By
          <select
            value={sortBy}
            onChange={(event) =>
              setSortBy(event.target.value as "due_date" | "state" | "owner" | "company")
            }
          >
            <option value="due_date">Due Date</option>
            <option value="state">State</option>
            <option value="owner">Owner</option>
            <option value="company">Group</option>
          </select>
        </label>
        <label>
          Direction
          <select
            value={sortDirection}
            onChange={(event) => setSortDirection(event.target.value as "asc" | "desc")}
          >
            <option value="asc">Ascending</option>
            <option value="desc">Descending</option>
          </select>
        </label>
        <button
          type="button"
          className="button subtle"
          onClick={() => {
            setStateFilters([]);
            setOwnerFilters([]);
            setDueFilter("all");
            setCompanySearch("");
            setSortBy("due_date");
            setSortDirection("asc");
            document
              .querySelectorAll<HTMLDetailsElement>(".multi-dropdown[open]")
              .forEach((el) => el.removeAttribute("open"));
          }}
        >
          Clear Filters
        </button>
      </div>

      <table className="table">
        <thead>
          <tr>
            <th>Task</th>
            <th>Group</th>
            <th>Owner</th>
            <th>Assigned To</th>
            <th>Due Date</th>
            <th>State</th>
            {isAdmin && <th>Admin</th>}
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {pagination.pageItems.map((task) => (
            <tr key={task.id}>
              <td>{task.title}</td>
              <td>{task.installation_company || "—"}</td>
              <td>{task.owner}</td>
              <td>
                {task.assigned_user_id
                  ? userNameById[task.assigned_user_id] || task.assigned_user_id
                  : "—"}
              </td>
              <td>
                <div>{formatDueDate(task.due_date)}</div>
                <div className="helper">{dueDescriptor(task.due_date)}</div>
              </td>
              <td>
                <span className="badge primary">{task.state}</span>
              </td>
              {isAdmin && (
                <td>
                  <div className="task-actions" style={{ justifyContent: "flex-start" }}>
                    <select
                      value={assignedUserDrafts[task.id] ?? ""}
                      onChange={(event) =>
                        setAssignedUserDrafts((prev) => ({
                          ...prev,
                          [task.id]: event.target.value,
                        }))
                      }
                      disabled={savingTaskId === task.id}
                    >
                      <option value="">Unassigned</option>
                      {users.map((user) => (
                        <option key={user.id} value={user.id}>
                          {`${user.first_name} ${user.last_name}`.trim()}
                        </option>
                      ))}
                    </select>
                    <input
                      type="date"
                      value={dueDateDrafts[task.id] ?? ""}
                      onChange={(event) =>
                        setDueDateDrafts((prev) => ({
                          ...prev,
                          [task.id]: event.target.value,
                        }))
                      }
                      disabled={savingTaskId === task.id}
                    />
                    <button
                      className="button ghost"
                      type="button"
                      onClick={() => handleSaveTaskMeta(task)}
                      disabled={savingTaskId === task.id}
                    >
                      Save
                    </button>
                  </div>
                </td>
              )}
              <td>
                <Link className="table-link" to={`/implementations/${task.installation_id}`}>
                  Open
                </Link>
              </td>
            </tr>
          ))}
          {visibleTasks.length === 0 && (
            <tr>
              <td colSpan={isAdmin ? 8 : 7} className="helper">
                No tasks match current filters.
              </td>
            </tr>
          )}
        </tbody>
      </table>
      <TablePagination
        page={pagination.currentPage}
        totalItems={visibleTasks.length}
        onPageChange={setPage}
      />
    </section>
  );
}
