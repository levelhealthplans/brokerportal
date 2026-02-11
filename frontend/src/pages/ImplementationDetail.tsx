import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  deleteImplementation,
  deleteInstallationDocument,
  getInstallation,
  getUsers,
  regressImplementationToQuote,
  uploadInstallationDocument,
  InstallationDetail,
  updateTask,
} from "../api";
import { useAccess } from "../access";
import { useAutoDismissMessage } from "../hooks/useAutoDismissMessage";

export default function ImplementationDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const installationId = id || "";
  const [data, setData] = useState<InstallationDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [userNameById, setUserNameById] = useState<Record<string, string>>({});
  const [taskUrlDrafts, setTaskUrlDrafts] = useState<Record<string, string>>({});
  const statusMessageFading = useAutoDismissMessage(statusMessage, setStatusMessage, 5000, 500);
  const { role, email } = useAccess();
  const isAdmin = role === "admin";

  const refresh = () => {
    if (!installationId) return;
    getInstallation(installationId, { role, email })
      .then(setData)
      .catch((err) => setError(err.message));
  };

  useEffect(() => {
    refresh();
  }, [installationId, role, email]);

  useEffect(() => {
    getUsers()
      .then((users) =>
        setUserNameById(
          Object.fromEntries(
            users.map((user) => [user.id, `${user.first_name} ${user.last_name}`.trim()])
          )
        )
      )
      .catch(() => setUserNameById({}));
  }, []);

  useEffect(() => {
    if (!data) return;
    setTaskUrlDrafts(
      Object.fromEntries(data.tasks.map((task) => [task.id, task.task_url || ""]))
    );
  }, [data]);

  const normalizeState = (state: string) => {
    if (state === "Done") return "Complete";
    return state;
  };

  const handleStateChange = async (taskId: string, state: string) => {
    setBusy(true);
    setError(null);
    try {
      await updateTask(installationId, taskId, { state }, { role, email });
      setStatusMessage("Task status updated.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleSaveTaskUrl = async (taskId: string) => {
    setBusy(true);
    setError(null);
    try {
      await updateTask(
        installationId,
        taskId,
        { task_url: taskUrlDrafts[taskId] || "" },
        { role, email }
      );
      setStatusMessage("Task link updated.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.target.files || []);
    if (!files.length) return;
    setBusy(true);
    setError(null);
    try {
      await Promise.all(
        files.map((file) => uploadInstallationDocument(installationId, file))
      );
      setStatusMessage("Document uploaded.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleDeleteDocument = async (documentId: string) => {
    const confirmed = window.confirm("Delete this document? This cannot be undone.");
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      await deleteInstallationDocument(installationId, documentId);
      setStatusMessage("Document deleted.");
      refresh();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleDeleteImplementation = async () => {
    const confirmed = window.confirm(
      "Delete this implementation and all related tasks/documents? This cannot be undone."
    );
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      await deleteImplementation(installationId);
      setStatusMessage("Implementation deleted.");
      navigate("/implementations");
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleRegressToQuote = async () => {
    const confirmed = window.confirm(
      "Regress this implementation back to quote stage? This removes implementation tasks/documents."
    );
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      const result = await regressImplementationToQuote(installationId);
      setStatusMessage("Implementation regressed back to quote.");
      navigate(`/quotes/${result.quote_id}`);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  if (!data) {
    return <div className="section">Loading implementation...</div>;
  }

  const { installation, tasks, documents } = data;
  const completedTaskCount = tasks.filter(
    (task) => task.state === "Complete" || task.state === "Done"
  ).length;
  const completionPercent =
    tasks.length > 0 ? Math.round((completedTaskCount / tasks.length) * 100) : 0;
  const documentUrl = (path: string) => {
    const basename = path.split("/").pop() || path;
    return `/uploads/installation-${installation.id}/${basename}`;
  };

  return (
    <div className="section">
      <h2>Implementation Detail</h2>
      {error && <div className="notice">{error}</div>}
      {statusMessage && (
        <div className={`notice notice-success ${statusMessageFading ? "fade-out" : ""}`}>
          {statusMessage}
        </div>
      )}
      <div className="progress-card">
        <div className="progress-header">
          <strong>Total Completion</strong>
          <span>{completionPercent}%</span>
        </div>
        <div className="progress-track" aria-label="Implementation completion progress">
          <div className="progress-fill" style={{ width: `${completionPercent}%` }} />
        </div>
      </div>

      <div className="kv">
        <strong>Group</strong>
        <span>{installation.company}</span>
        <strong>Status</strong>
        <span>{installation.status}</span>
        <strong>Effective Date</strong>
        <span>{installation.effective_date}</span>
        <strong>Case ID</strong>
        <span>{installation.id}</span>
      </div>

      <section className="section" style={{ marginTop: 24 }}>
        <h2>Tasks</h2>
        {tasks.length === 0 && <div className="helper">No tasks yet.</div>}
        {tasks.map((task) => (
          <div key={task.id} className="card-row">
            <div>
              <strong>{task.title}</strong>
              <div className="helper">
                Owner: {task.owner} · Assigned:{" "}
                {(task.assigned_user_id && userNameById[task.assigned_user_id]) || "—"} · Due:{" "}
                {task.due_date || "TBD"}
              </div>
            </div>
            <div className="task-actions">
              {task.task_url ? (
                <a
                  className="button secondary"
                  href={task.task_url}
                  target="_blank"
                  rel="noreferrer"
                >
                  Open Link
                </a>
              ) : (
                <button className="button ghost" disabled>
                  No Link
                </button>
              )}
              {isAdmin && (
                <>
                  <select
                    value={normalizeState(task.state)}
                    onChange={(event) => handleStateChange(task.id, event.target.value)}
                    disabled={busy}
                  >
                    <option value="Not Started">Not Started</option>
                    <option value="In Progress">In Progress</option>
                    <option value="Complete">Complete</option>
                  </select>
                  <input
                    type="url"
                    value={taskUrlDrafts[task.id] || ""}
                    placeholder="https://..."
                    onChange={(event) =>
                      setTaskUrlDrafts((prev) => ({ ...prev, [task.id]: event.target.value }))
                    }
                    disabled={busy}
                  />
                  <button
                    className="button ghost"
                    type="button"
                    onClick={() => handleSaveTaskUrl(task.id)}
                    disabled={busy}
                  >
                    Save Link
                  </button>
                </>
              )}
            </div>
          </div>
        ))}
      </section>

      <section className="section">
        <h2>Documents</h2>
        <label>
          Upload Documents
          <input type="file" multiple onChange={handleUpload} />
        </label>
        {documents.length === 0 && <div className="helper">No documents yet.</div>}
        {documents.map((doc) => (
          <div key={doc.id} className="card-row">
            <div>
              <strong>{doc.filename}</strong>
              <div className="helper">
                {new Date(doc.created_at).toLocaleString()}
              </div>
            </div>
            <div className="inline-actions">
              <a className="button secondary" href={documentUrl(doc.path)}>
                View
              </a>
              <button
                className="button ghost"
                type="button"
                onClick={() => handleDeleteDocument(doc.id)}
              >
                Delete
              </button>
            </div>
          </div>
        ))}
      </section>

      <div className="inline-actions">
        {isAdmin && (
          <>
            <button
              className="button secondary"
              type="button"
              onClick={handleRegressToQuote}
              disabled={busy}
            >
              Regress to Quote
            </button>
            <button
              className="button"
              type="button"
              onClick={handleDeleteImplementation}
              disabled={busy}
            >
              Delete Implementation
            </button>
          </>
        )}
        <Link className="button ghost" to="/implementations">
          Back to Implementations
        </Link>
      </div>
    </div>
  );
}
