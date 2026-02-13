import { useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  completeImplementationFormTask,
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

type HubspotFormConfig = {
  portalId: string;
  formId: string;
  region: string;
};

type HubspotTaskFormConfig = HubspotFormConfig & {
  taskId: string;
};

type PandadocDropdownConfig = {
  options: Array<{
    label: string;
    url: string;
  }>;
};

function parseHubspotFormTaskUrl(taskTitle: string, taskUrl: string | null | undefined): HubspotFormConfig | null {
  if ((taskTitle || "").trim().toLowerCase() !== "implementation forms") {
    return null;
  }
  const raw = (taskUrl || "").trim();
  if (!raw || !raw.toLowerCase().startsWith("hubspot-form://")) {
    return null;
  }
  try {
    const parsed = new URL(raw);
    const portalId = (parsed.searchParams.get("portal_id") || "").trim();
    const formId = (parsed.searchParams.get("form_id") || "").trim();
    const region = (parsed.searchParams.get("region") || "na1").trim() || "na1";
    if (!portalId || !formId) {
      return null;
    }
    return { portalId, formId, region };
  } catch {
    return null;
  }
}

function parsePandadocDropdownTaskUrl(
  taskTitle: string,
  taskUrl: string | null | undefined
): PandadocDropdownConfig | null {
  if ((taskTitle || "").trim().toLowerCase() !== "stoploss disclosure") {
    return null;
  }
  const raw = (taskUrl || "").trim();
  if (!raw || !raw.toLowerCase().startsWith("pandadoc-dropdown://")) {
    return null;
  }
  try {
    const parsed = new URL(raw);
    const urls = parsed.searchParams
      .getAll("url")
      .map((value) => value.trim())
      .filter((value, index, arr) => value.length > 0 && arr.indexOf(value) === index);
    if (!urls.length) {
      return null;
    }
    const labels = parsed.searchParams.getAll("label").map((value) => value.trim());
    const options = urls.map((url, index) => ({
      url,
      label: labels[index] || optionLabelFromUrl(url, index),
    }));
    return { options };
  } catch {
    return null;
  }
}

function optionLabelFromUrl(url: string, index: number): string {
  try {
    const parsed = new URL(url);
    const templateMatch = parsed.hash.match(/\/templates\/([^/?#]+)/i);
    if (templateMatch?.[1]) {
      return `Template ${templateMatch[1]}`;
    }
  } catch {
    // no-op
  }
  return `Option ${index + 1}`;
}

function loadHubspotFormsScript(portalId: string): Promise<void> {
  const scriptId = `hubspot-forms-embed-${portalId}`;
  const scriptUrl = `https://js.hsforms.net/forms/embed/developer/${encodeURIComponent(portalId)}.js`;
  const existing = document.getElementById(scriptId) as HTMLScriptElement | null;
  if (existing) {
    return new Promise((resolve, reject) => {
      const currentWindow = window as Window & {
        hbspt?: { forms?: { create?: (config: Record<string, unknown>) => void } };
      };
      if (currentWindow.hbspt?.forms?.create) {
        resolve();
        return;
      }
      existing.addEventListener("load", () => resolve(), { once: true });
      existing.addEventListener(
        "error",
        () => reject(new Error("Failed to load HubSpot form embed script.")),
        { once: true }
      );
    });
  }

  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.id = scriptId;
    script.src = scriptUrl;
    script.defer = true;
    script.async = true;
    script.addEventListener("load", () => resolve(), { once: true });
    script.addEventListener(
      "error",
      () => reject(new Error("Failed to load HubSpot form embed script.")),
      { once: true }
    );
    document.body.appendChild(script);
  });
}

function renderHubspotDeveloperEmbed(
  container: HTMLElement,
  config: HubspotFormConfig
) {
  const formHost = document.createElement("div");
  formHost.className = "hs-form-html";
  formHost.setAttribute("data-region", config.region);
  formHost.setAttribute("data-form-id", config.formId);
  formHost.setAttribute("data-portal-id", config.portalId);
  container.appendChild(formHost);
}

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
  const [taskLinkSelections, setTaskLinkSelections] = useState<Record<string, string>>({});
  const [activeHubspotForm, setActiveHubspotForm] = useState<HubspotTaskFormConfig | null>(null);
  const [hubspotFormError, setHubspotFormError] = useState<string | null>(null);
  const hubspotFormCompletionFired = useRef(false);
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
    setTaskLinkSelections((prev) => {
      const next: Record<string, string> = {};
      data.tasks.forEach((task) => {
        const dropdown = parsePandadocDropdownTaskUrl(task.title, task.task_url);
        if (!dropdown) return;
        const optionUrls = dropdown.options.map((option) => option.url);
        const existingSelection = prev[task.id];
        next[task.id] = existingSelection && optionUrls.includes(existingSelection)
          ? existingSelection
          : optionUrls[0];
      });
      return next;
    });
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

  useEffect(() => {
    if (!activeHubspotForm) {
      return;
    }
    let cancelled = false;
    hubspotFormCompletionFired.current = false;
    setHubspotFormError(null);

    const markTaskCompleteFromForm = async () => {
      if (hubspotFormCompletionFired.current) {
        return;
      }
      hubspotFormCompletionFired.current = true;
      try {
        const updatedTask = await completeImplementationFormTask(
          installationId,
          activeHubspotForm.taskId,
          { role, email }
        );
        if (cancelled) return;
        setData((prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            tasks: prev.tasks.map((task) =>
              task.id === updatedTask.id ? { ...task, ...updatedTask } : task
            ),
          };
        });
        setTaskUrlDrafts((prev) => ({
          ...prev,
          [updatedTask.id]: updatedTask.task_url || "",
        }));
        setStatusMessage("Implementation Forms task marked complete.");
        setActiveHubspotForm(null);
      } catch (err: any) {
        if (cancelled) return;
        hubspotFormCompletionFired.current = false;
        setHubspotFormError(
          err?.message || "Form submitted, but task completion update failed."
        );
      }
    };

    const handleHubspotFormSuccess = (event: Event) => {
      const custom = event as CustomEvent<Record<string, unknown>>;
      const detail = custom?.detail || {};
      const submittedFormId = String(
        detail["formId"] || detail["formGuid"] || ""
      ).trim();
      if (submittedFormId && submittedFormId !== activeHubspotForm.formId) {
        return;
      }
      void markTaskCompleteFromForm();
    };

    window.addEventListener(
      "hs-form-event:on-submission:success",
      handleHubspotFormSuccess as EventListener
    );

    const renderForm = async () => {
      try {
        await loadHubspotFormsScript(activeHubspotForm.portalId);
        if (cancelled) return;
        const container = document.getElementById("implementation-hubspot-form-container");
        if (!container) return;
        container.innerHTML = "";

        const currentWindow = window as Window & {
          hbspt?: { forms?: { create?: (config: Record<string, unknown>) => void } };
        };
        const createForm = currentWindow.hbspt?.forms?.create;
        if (createForm) {
          createForm({
            region: activeHubspotForm.region,
            portalId: activeHubspotForm.portalId,
            formId: activeHubspotForm.formId,
            target: "#implementation-hubspot-form-container",
            onFormSubmitted: () => {
              void markTaskCompleteFromForm();
            },
          });
          return;
        }
        // Developer embed runtime can auto-render via .hs-form-html nodes.
        renderHubspotDeveloperEmbed(container, activeHubspotForm);
      } catch (err: any) {
        if (cancelled) return;
        setHubspotFormError(err?.message || "Unable to load HubSpot form.");
      }
    };

    void renderForm();
    return () => {
      cancelled = true;
      window.removeEventListener(
        "hs-form-event:on-submission:success",
        handleHubspotFormSuccess as EventListener
      );
      const container = document.getElementById("implementation-hubspot-form-container");
      if (container) {
        container.innerHTML = "";
      }
    };
  }, [activeHubspotForm, email, installationId, role]);

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
              {(() => {
                const hubspotFormConfig = parseHubspotFormTaskUrl(task.title, task.task_url);
                if (hubspotFormConfig) {
                  return (
                    <button
                      className="button secondary"
                      type="button"
                      onClick={() => {
                        setHubspotFormError(null);
                        setActiveHubspotForm({
                          taskId: task.id,
                          ...hubspotFormConfig,
                        });
                      }}
                    >
                      Open Form
                    </button>
                  );
                }
                const pandadocDropdownConfig = parsePandadocDropdownTaskUrl(task.title, task.task_url);
                if (pandadocDropdownConfig) {
                  const optionUrls = pandadocDropdownConfig.options.map((option) => option.url);
                  const selectedUrl =
                    taskLinkSelections[task.id] || optionUrls[0];
                  return (
                    <>
                      <select
                        value={selectedUrl}
                        onChange={(event) =>
                          setTaskLinkSelections((prev) => ({
                            ...prev,
                            [task.id]: event.target.value,
                          }))
                        }
                      >
                        {pandadocDropdownConfig.options.map((option, index) => (
                          <option key={`${task.id}-${option.url}-${index}`} value={option.url}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                      <a
                        className="button secondary"
                        href={selectedUrl}
                        target="_blank"
                        rel="noreferrer"
                      >
                        Open Link
                      </a>
                    </>
                  );
                }
                if (task.task_url) {
                  return (
                    <a
                      className="button secondary"
                      href={task.task_url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      Open Link
                    </a>
                  );
                }
                return (
                  <button className="button ghost" disabled>
                    No Link
                  </button>
                );
              })()}
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

      {activeHubspotForm && (
        <div className="modal-backdrop" onClick={() => setActiveHubspotForm(null)}>
          <div
            className="modal hubspot-form-modal"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="modal-header">
              <h3>Implementation Form</h3>
              <button
                className="button subtle"
                type="button"
                onClick={() => setActiveHubspotForm(null)}
              >
                Close
              </button>
            </div>
            {hubspotFormError && <div className="notice">{hubspotFormError}</div>}
            <div
              id="implementation-hubspot-form-container"
              className="hubspot-form-container"
            />
          </div>
        </div>
      )}

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
