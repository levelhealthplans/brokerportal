import { useEffect, useMemo, useState } from "react";
import {
  cleanupUnassignedRecords,
  createNetworkMapping,
  createNetworkOption,
  deleteNetworkMapping,
  deleteNetworkOption,
  getHubSpotPipelines,
  getHubSpotSettings,
  getNetworkMappings,
  getNetworkOptions,
  getNetworkSettings,
  HubSpotPipeline,
  HubSpotSettings,
  NetworkMapping,
  testHubSpotConnection,
  updateHubSpotSettings,
  updateNetworkMapping,
  updateNetworkOption,
  updateNetworkSettings,
} from "../api";
import {
  paginateItems,
  TABLE_PAGE_SIZE,
  TablePagination,
} from "../components/TablePagination";
import { useAutoDismissMessage } from "../hooks/useAutoDismissMessage";
import { formatNetworkLabel } from "../utils/formatNetwork";

const EMPTY_HUBSPOT_SETTINGS: HubSpotSettings = {
  enabled: false,
  portal_id: "7106327",
  pipeline_id: "98238573",
  default_stage_id: "",
  sync_quote_to_hubspot: true,
  sync_hubspot_to_quote: true,
  ticket_subject_template: "Quote {{company}} ({{quote_id}})",
  ticket_content_template:
    "Company: {{company}}\nQuote ID: {{quote_id}}\nStatus: {{status}}\nEffective Date: {{effective_date}}\nBroker Org: {{broker_org}}",
  property_mappings: {},
  quote_status_to_stage: {},
  stage_to_quote_status: {},
  token_configured: false,
};

function formatMappingLines(mapping: Record<string, string>) {
  return Object.entries(mapping)
    .map(([left, right]) => `${left}=${right}`)
    .join("\n");
}

function parseMappingLines(raw: string) {
  const mapping: Record<string, string> = {};
  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex <= 0 || separatorIndex >= trimmed.length - 1) continue;
    const key = trimmed.slice(0, separatorIndex).trim();
    const value = trimmed.slice(separatorIndex + 1).trim();
    if (!key || !value) continue;
    mapping[key] = value;
  }
  return mapping;
}

export default function Configuration() {
  const [networkOptions, setNetworkOptions] = useState<string[]>([]);
  const [networkMappings, setNetworkMappings] = useState<NetworkMapping[]>([]);
  const [hubspotSettings, setHubspotSettings] = useState<HubSpotSettings>(
    EMPTY_HUBSPOT_SETTINGS
  );
  const [hubspotTokenInput, setHubspotTokenInput] = useState("");
  const [hubspotPropertyMappingsText, setHubspotPropertyMappingsText] = useState("");
  const [hubspotQuoteStatusToStageText, setHubspotQuoteStatusToStageText] = useState("");
  const [hubspotStageToQuoteStatusText, setHubspotStageToQuoteStatusText] = useState("");
  const [hubspotPipelines, setHubspotPipelines] = useState<HubSpotPipeline[]>([]);
  const [hubspotTestMessage, setHubspotTestMessage] = useState<string | null>(null);
  const [settings, setSettings] = useState({
    default_network: "Cigna_PPO",
    coverage_threshold: 0.9,
  });
  const [newOption, setNewOption] = useState("");
  const [editingOption, setEditingOption] = useState<Record<string, string>>({});
  const [newMapping, setNewMapping] = useState({ zip: "", network: "Cigna_PPO" });
  const [editingMapping, setEditingMapping] = useState<
    Record<string, { zip: string; network: string }>
  >({});
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [optionPage, setOptionPage] = useState(1);
  const [mappingPage, setMappingPage] = useState(1);
  const statusMessageFading = useAutoDismissMessage(statusMessage, setStatusMessage, 5000, 500);

  const loadAll = () => {
    Promise.all([
      getNetworkOptions(),
      getNetworkMappings(),
      getNetworkSettings(),
      getHubSpotSettings(),
    ])
      .then(([options, mappings, nextSettings, nextHubspotSettings]) => {
        setNetworkOptions(options);
        setNetworkMappings(mappings);
        setSettings(nextSettings);
        setHubspotSettings(nextHubspotSettings);
        setHubspotPropertyMappingsText(formatMappingLines(nextHubspotSettings.property_mappings));
        setHubspotQuoteStatusToStageText(
          formatMappingLines(nextHubspotSettings.quote_status_to_stage)
        );
        setHubspotStageToQuoteStatusText(
          formatMappingLines(nextHubspotSettings.stage_to_quote_status)
        );
        setHubspotTokenInput("");
        setHubspotPipelines([]);
        setHubspotTestMessage(null);
        setNewMapping((prev) => ({
          ...prev,
          network: prev.network || nextSettings.default_network || "Cigna_PPO",
        }));
      })
      .catch((err) => setError(err.message));
  };

  useEffect(() => {
    loadAll();
  }, []);

  const handleAddOption = async () => {
    const name = newOption.trim();
    if (!name) return;
    setBusy(true);
    setError(null);
    try {
      const next = await createNetworkOption(name);
      setNetworkOptions(next);
      setNewOption("");
      setStatusMessage("Network option added.");
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleSaveOption = async (currentName: string) => {
    const nextName = (editingOption[currentName] || "").trim();
    if (!nextName || nextName === currentName) {
      setEditingOption((prev) => {
        const copy = { ...prev };
        delete copy[currentName];
        return copy;
      });
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const next = await updateNetworkOption(currentName, nextName);
      setNetworkOptions(next);
      setEditingOption((prev) => {
        const copy = { ...prev };
        delete copy[currentName];
        return copy;
      });
      setStatusMessage("Network option updated.");
      loadAll();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleDeleteOption = async (name: string) => {
    const confirmed = window.confirm(`Delete network option "${formatNetworkLabel(name)}"?`);
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      const next = await deleteNetworkOption(name);
      setNetworkOptions(next);
      setStatusMessage("Network option deleted.");
      loadAll();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleAddMapping = async () => {
    setBusy(true);
    setError(null);
    try {
      const next = await createNetworkMapping(newMapping);
      setNetworkMappings(next);
      setNewMapping({ zip: "", network: settings.default_network || "Cigna_PPO" });
      setStatusMessage("ZIP mapping added.");
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleSaveMapping = async (sourceZip: string) => {
    const draft = editingMapping[sourceZip];
    if (!draft) return;
    setBusy(true);
    setError(null);
    try {
      const next = await updateNetworkMapping(sourceZip, draft);
      setNetworkMappings(next);
      setEditingMapping((prev) => {
        const copy = { ...prev };
        delete copy[sourceZip];
        return copy;
      });
      setStatusMessage("ZIP mapping updated.");
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleDeleteMapping = async (zip: string) => {
    const confirmed = window.confirm(`Delete ZIP mapping ${zip}?`);
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      const next = await deleteNetworkMapping(zip);
      setNetworkMappings(next);
      setStatusMessage("ZIP mapping deleted.");
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleSaveSettings = async () => {
    setBusy(true);
    setError(null);
    try {
      const next = await updateNetworkSettings(settings);
      setSettings(next);
      setStatusMessage("Assignment settings updated.");
      loadAll();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleCleanupUnassigned = async () => {
    const confirmed = window.confirm(
      "Delete all quotes and tasks that are not assigned to a valid user? This cannot be undone."
    );
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      const result = await cleanupUnassignedRecords();
      setStatusMessage(
        `Cleanup complete. Deleted ${result.deleted_quote_count} quotes and ${result.deleted_task_count} tasks.`
      );
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleSaveHubSpotSettings = async () => {
    setBusy(true);
    setError(null);
    try {
      const next = await updateHubSpotSettings({
        enabled: hubspotSettings.enabled,
        portal_id: hubspotSettings.portal_id.trim(),
        pipeline_id: hubspotSettings.pipeline_id.trim(),
        default_stage_id: hubspotSettings.default_stage_id.trim(),
        sync_quote_to_hubspot: hubspotSettings.sync_quote_to_hubspot,
        sync_hubspot_to_quote: hubspotSettings.sync_hubspot_to_quote,
        ticket_subject_template: hubspotSettings.ticket_subject_template,
        ticket_content_template: hubspotSettings.ticket_content_template,
        property_mappings: parseMappingLines(hubspotPropertyMappingsText),
        quote_status_to_stage: parseMappingLines(hubspotQuoteStatusToStageText),
        stage_to_quote_status: parseMappingLines(hubspotStageToQuoteStatusText),
        private_app_token: hubspotTokenInput.trim() ? hubspotTokenInput.trim() : undefined,
      });
      setHubspotSettings(next);
      setHubspotTokenInput("");
      setStatusMessage("HubSpot settings saved.");
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const handleTestHubSpot = async () => {
    setBusy(true);
    setError(null);
    try {
      const result = await testHubSpotConnection();
      setHubspotTestMessage(`Connection successful. Pipelines found: ${result.pipelines_found}.`);
      setStatusMessage("HubSpot connection test passed.");
    } catch (err: any) {
      setError(err.message);
      setHubspotTestMessage(null);
    } finally {
      setBusy(false);
    }
  };

  const handleLoadHubSpotPipelines = async () => {
    setBusy(true);
    setError(null);
    try {
      const pipelines = await getHubSpotPipelines();
      setHubspotPipelines(pipelines);
      setStatusMessage(`Loaded ${pipelines.length} HubSpot pipelines.`);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  const optionPagination = useMemo(
    () => paginateItems(networkOptions, optionPage, TABLE_PAGE_SIZE),
    [networkOptions, optionPage]
  );
  const mappingPagination = useMemo(
    () => paginateItems(networkMappings, mappingPage, TABLE_PAGE_SIZE),
    [networkMappings, mappingPage]
  );

  useEffect(() => {
    if (optionPage !== optionPagination.currentPage) {
      setOptionPage(optionPagination.currentPage);
    }
  }, [optionPage, optionPagination.currentPage]);

  useEffect(() => {
    if (mappingPage !== mappingPagination.currentPage) {
      setMappingPage(mappingPagination.currentPage);
    }
  }, [mappingPage, mappingPagination.currentPage]);

  return (
    <section className="section">
      <h2>Configuration</h2>
      <div className="helper" style={{ marginBottom: 8 }}>
        Tables on this page are limited to 25 rows per page.
      </div>
      {error && <div className="notice">{error}</div>}
      {statusMessage && (
        <div className={`notice notice-success ${statusMessageFading ? "fade-out" : ""}`}>
          {statusMessage}
        </div>
      )}

      <section className="section" style={{ marginTop: 12 }}>
        <h3>Assignment Settings</h3>
        <div className="inline-actions">
          <label style={{ minWidth: 260 }}>
            Default Network
            <select
              value={settings.default_network}
              onChange={(e) =>
                setSettings((prev) => ({ ...prev, default_network: e.target.value }))
              }
            >
              {networkOptions.map((option) => (
                <option key={option} value={option}>
                  {formatNetworkLabel(option)}
                </option>
              ))}
            </select>
          </label>
          <label style={{ minWidth: 260 }}>
            Coverage Threshold (%)
            <input
              type="number"
              min={0}
              max={100}
              value={Math.round(settings.coverage_threshold * 100)}
              onChange={(e) =>
                setSettings((prev) => ({
                  ...prev,
                  coverage_threshold: Math.max(
                    0,
                    Math.min(1, Number(e.target.value || 0) / 100)
                  ),
                }))
              }
            />
          </label>
          <button className="button secondary" type="button" onClick={handleSaveSettings} disabled={busy}>
            Save Settings
          </button>
        </div>
      </section>

      <section className="section" style={{ marginTop: 12 }}>
        <h3>HubSpot Integration (Admin)</h3>
        <div className="helper" style={{ marginBottom: 12 }}>
          MVP flow: a new quote creates a HubSpot ticket. Configure API token, pipeline/stage,
          and status mappings here.
        </div>
        <div className="inline-actions">
          <label style={{ minWidth: 220 }}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              <input
                type="checkbox"
                checked={hubspotSettings.enabled}
                onChange={(e) =>
                  setHubspotSettings((prev) => ({
                    ...prev,
                    enabled: e.target.checked,
                  }))
                }
              />
              Enable HubSpot Integration
            </span>
          </label>
          <label style={{ minWidth: 220 }}>
            HubSpot Account ID
            <input
              value={hubspotSettings.portal_id}
              onChange={(e) =>
                setHubspotSettings((prev) => ({ ...prev, portal_id: e.target.value }))
              }
              placeholder="7106327"
            />
          </label>
          <label style={{ minWidth: 280 }}>
            Private App Token
            <input
              type="password"
              value={hubspotTokenInput}
              onChange={(e) => setHubspotTokenInput(e.target.value)}
              placeholder={hubspotSettings.token_configured ? "Token saved (enter to replace)" : "pat-..."}
            />
          </label>
        </div>
        <div className="inline-actions" style={{ marginTop: 8 }}>
          <label style={{ minWidth: 260 }}>
            Ticket Pipeline ID
            <input
              value={hubspotSettings.pipeline_id}
              onChange={(e) =>
                setHubspotSettings((prev) => ({ ...prev, pipeline_id: e.target.value }))
              }
              placeholder="98238573"
            />
          </label>
          <label style={{ minWidth: 260 }}>
            Default Stage ID
            <input
              value={hubspotSettings.default_stage_id}
              onChange={(e) =>
                setHubspotSettings((prev) => ({
                  ...prev,
                  default_stage_id: e.target.value,
                }))
              }
              placeholder="1"
            />
          </label>
        </div>
        <div className="inline-actions" style={{ marginTop: 8 }}>
          <label style={{ minWidth: 220 }}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              <input
                type="checkbox"
                checked={hubspotSettings.sync_quote_to_hubspot}
                onChange={(e) =>
                  setHubspotSettings((prev) => ({
                    ...prev,
                    sync_quote_to_hubspot: e.target.checked,
                  }))
                }
              />
              Quote -&gt; HubSpot Sync
            </span>
          </label>
          <label style={{ minWidth: 220 }}>
            <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              <input
                type="checkbox"
                checked={hubspotSettings.sync_hubspot_to_quote}
                onChange={(e) =>
                  setHubspotSettings((prev) => ({
                    ...prev,
                    sync_hubspot_to_quote: e.target.checked,
                  }))
                }
              />
              HubSpot -&gt; Quote Sync
            </span>
          </label>
        </div>

        <div style={{ marginTop: 10 }}>
          <label style={{ display: "block" }}>
            Ticket Subject Template
            <input
              value={hubspotSettings.ticket_subject_template}
              onChange={(e) =>
                setHubspotSettings((prev) => ({
                  ...prev,
                  ticket_subject_template: e.target.value,
                }))
              }
              placeholder="Quote {{company}} ({{quote_id}})"
            />
          </label>
        </div>

        <div style={{ marginTop: 10 }}>
          <label style={{ display: "block" }}>
            Ticket Content Template
            <textarea
              value={hubspotSettings.ticket_content_template}
              onChange={(e) =>
                setHubspotSettings((prev) => ({
                  ...prev,
                  ticket_content_template: e.target.value,
                }))
              }
              rows={4}
            />
          </label>
        </div>

        <div style={{ marginTop: 10 }}>
          <label style={{ display: "block" }}>
            Property Mappings (`local_field=hubspot_property`)
            <textarea
              value={hubspotPropertyMappingsText}
              onChange={(e) => setHubspotPropertyMappingsText(e.target.value)}
              rows={4}
              placeholder={"id=level_health_quote_id\ncompany=level_health_company\nstatus=level_health_quote_status"}
            />
          </label>
        </div>

        <div style={{ marginTop: 10 }}>
          <label style={{ display: "block" }}>
            Quote Status -&gt; HubSpot Stage (`Quote Status=Stage ID`)
            <textarea
              value={hubspotQuoteStatusToStageText}
              onChange={(e) => setHubspotQuoteStatusToStageText(e.target.value)}
              rows={4}
              placeholder={"Draft=1\nQuote Submitted=2\nSold=3"}
            />
          </label>
        </div>

        <div style={{ marginTop: 10 }}>
          <label style={{ display: "block" }}>
            HubSpot Stage -&gt; Quote Status (`Stage ID=Quote Status`)
            <textarea
              value={hubspotStageToQuoteStatusText}
              onChange={(e) => setHubspotStageToQuoteStatusText(e.target.value)}
              rows={4}
              placeholder={"1=Draft\n2=Quote Submitted\n3=Sold"}
            />
          </label>
        </div>

        <div className="inline-actions" style={{ marginTop: 12 }}>
          <button
            className="button secondary"
            type="button"
            onClick={handleSaveHubSpotSettings}
            disabled={busy}
          >
            Save HubSpot Settings
          </button>
          <button className="button ghost" type="button" onClick={handleTestHubSpot} disabled={busy}>
            Test Connection
          </button>
          <button
            className="button ghost"
            type="button"
            onClick={handleLoadHubSpotPipelines}
            disabled={busy}
          >
            Load Pipelines
          </button>
        </div>

        {hubspotTestMessage && (
          <div className="helper" style={{ marginTop: 8 }}>
            {hubspotTestMessage}
          </div>
        )}

        {hubspotPipelines.length > 0 && (
          <div style={{ marginTop: 12 }}>
            <strong>Available Pipelines</strong>
            <div className="helper" style={{ marginTop: 4 }}>
              Click a stage to auto-fill pipeline and default stage IDs.
            </div>
            <div style={{ marginTop: 8 }}>
              {hubspotPipelines.map((pipeline) => (
                <div key={pipeline.id} style={{ marginBottom: 12 }}>
                  <div>
                    <strong>{pipeline.label}</strong> ({pipeline.id})
                  </div>
                  <div className="inline-actions" style={{ marginTop: 6 }}>
                    {pipeline.stages.map((stage) => (
                      <button
                        key={stage.id}
                        className="button ghost"
                        type="button"
                        onClick={() =>
                          setHubspotSettings((prev) => ({
                            ...prev,
                            pipeline_id: pipeline.id,
                            default_stage_id: stage.id,
                          }))
                        }
                      >
                        {stage.label} ({stage.id})
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </section>

      <section className="section" style={{ marginTop: 12 }}>
        <h3>Data Cleanup</h3>
        <div className="helper" style={{ marginBottom: 12 }}>
          Permanently removes quotes and tasks with no assigned user (or an invalid assigned user).
        </div>
        <button className="button" type="button" onClick={handleCleanupUnassigned} disabled={busy}>
          Delete Unassigned Quotes & Tasks
        </button>
      </section>

      <section className="section" style={{ marginTop: 12 }}>
        <h3>Network Options</h3>
        <div className="inline-actions" style={{ marginBottom: 12 }}>
          <label style={{ minWidth: 320 }}>
            New Network Option
            <input
              value={newOption}
              onChange={(e) => setNewOption(e.target.value)}
              placeholder="Example_Network"
            />
          </label>
          <button className="button secondary" type="button" onClick={handleAddOption} disabled={busy}>
            Add Option
          </button>
        </div>
        <table className="table">
          <thead>
            <tr>
              <th>Value</th>
              <th>Display</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {optionPagination.pageItems.map((option) => {
              const isEditing = Object.prototype.hasOwnProperty.call(editingOption, option);
              return (
                <tr key={option}>
                  <td>
                    {isEditing ? (
                      <input
                        value={editingOption[option]}
                        onChange={(e) =>
                          setEditingOption((prev) => ({ ...prev, [option]: e.target.value }))
                        }
                      />
                    ) : (
                      option
                    )}
                  </td>
                  <td>{formatNetworkLabel(option)}</td>
                  <td>
                    <div className="inline-actions">
                      {isEditing ? (
                        <>
                          <button className="button secondary" type="button" onClick={() => handleSaveOption(option)} disabled={busy}>
                            Save
                          </button>
                          <button
                            className="button ghost"
                            type="button"
                            onClick={() =>
                              setEditingOption((prev) => {
                                const copy = { ...prev };
                                delete copy[option];
                                return copy;
                              })
                            }
                          >
                            Cancel
                          </button>
                        </>
                      ) : (
                        <>
                          <button
                            className="button ghost"
                            type="button"
                            onClick={() =>
                              setEditingOption((prev) => ({ ...prev, [option]: option }))
                            }
                          >
                            Edit
                          </button>
                          <button
                            className="button"
                            type="button"
                            onClick={() => handleDeleteOption(option)}
                            disabled={busy || option === "Cigna_PPO"}
                          >
                            Delete
                          </button>
                        </>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        <TablePagination
          page={optionPagination.currentPage}
          totalItems={networkOptions.length}
          pageSize={TABLE_PAGE_SIZE}
          onPageChange={setOptionPage}
        />
      </section>

      <section className="section" style={{ marginTop: 12 }}>
        <h3>ZIP-to-Network Mappings</h3>
        <div className="inline-actions" style={{ marginBottom: 12 }}>
          <label>
            ZIP
            <input
              value={newMapping.zip}
              onChange={(e) => setNewMapping((prev) => ({ ...prev, zip: e.target.value }))}
              placeholder="63011"
            />
          </label>
          <label style={{ minWidth: 260 }}>
            Network
            <select
              value={newMapping.network}
              onChange={(e) => setNewMapping((prev) => ({ ...prev, network: e.target.value }))}
            >
              {networkOptions.map((option) => (
                <option key={option} value={option}>
                  {formatNetworkLabel(option)}
                </option>
              ))}
            </select>
          </label>
          <button className="button secondary" type="button" onClick={handleAddMapping} disabled={busy}>
            Add Mapping
          </button>
        </div>

        <table className="table">
          <thead>
            <tr>
              <th>ZIP</th>
              <th>Network</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {mappingPagination.pageItems.map((row) => {
              const isEditing = Boolean(editingMapping[row.zip]);
              const draft = editingMapping[row.zip] || row;
              return (
                <tr key={row.zip}>
                  <td>
                    {isEditing ? (
                      <input
                        value={draft.zip}
                        onChange={(e) =>
                          setEditingMapping((prev) => ({
                            ...prev,
                            [row.zip]: { ...draft, zip: e.target.value },
                          }))
                        }
                      />
                    ) : (
                      row.zip
                    )}
                  </td>
                  <td>
                    {isEditing ? (
                      <select
                        value={draft.network}
                        onChange={(e) =>
                          setEditingMapping((prev) => ({
                            ...prev,
                            [row.zip]: { ...draft, network: e.target.value },
                          }))
                        }
                      >
                        {networkOptions.map((option) => (
                          <option key={option} value={option}>
                            {formatNetworkLabel(option)}
                          </option>
                        ))}
                      </select>
                    ) : (
                      formatNetworkLabel(row.network)
                    )}
                  </td>
                  <td>
                    <div className="inline-actions">
                      {isEditing ? (
                        <>
                          <button className="button secondary" type="button" onClick={() => handleSaveMapping(row.zip)} disabled={busy}>
                            Save
                          </button>
                          <button
                            className="button ghost"
                            type="button"
                            onClick={() =>
                              setEditingMapping((prev) => {
                                const copy = { ...prev };
                                delete copy[row.zip];
                                return copy;
                              })
                            }
                          >
                            Cancel
                          </button>
                        </>
                      ) : (
                        <>
                          <button
                            className="button ghost"
                            type="button"
                            onClick={() =>
                              setEditingMapping((prev) => ({ ...prev, [row.zip]: { ...row } }))
                            }
                          >
                            Edit
                          </button>
                          <button className="button" type="button" onClick={() => handleDeleteMapping(row.zip)} disabled={busy}>
                            Delete
                          </button>
                        </>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
            {networkMappings.length === 0 && (
              <tr>
                <td colSpan={3} className="helper">
                  No ZIP mappings configured yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
        <TablePagination
          page={mappingPagination.currentPage}
          totalItems={networkMappings.length}
          pageSize={TABLE_PAGE_SIZE}
          onPageChange={setMappingPage}
        />
      </section>
    </section>
  );
}
