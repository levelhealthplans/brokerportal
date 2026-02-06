import { useEffect, useState } from "react";
import {
  createNetworkMapping,
  createNetworkOption,
  deleteNetworkMapping,
  deleteNetworkOption,
  getNetworkMappings,
  getNetworkOptions,
  getNetworkSettings,
  NetworkMapping,
  updateNetworkMapping,
  updateNetworkOption,
  updateNetworkSettings,
} from "../api";
import { useAutoDismissMessage } from "../hooks/useAutoDismissMessage";
import { formatNetworkLabel } from "../utils/formatNetwork";

export default function Configuration() {
  const [networkOptions, setNetworkOptions] = useState<string[]>([]);
  const [networkMappings, setNetworkMappings] = useState<NetworkMapping[]>([]);
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
  const statusMessageFading = useAutoDismissMessage(statusMessage, setStatusMessage, 5000, 500);

  const loadAll = () => {
    Promise.all([getNetworkOptions(), getNetworkMappings(), getNetworkSettings()])
      .then(([options, mappings, nextSettings]) => {
        setNetworkOptions(options);
        setNetworkMappings(mappings);
        setSettings(nextSettings);
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

  return (
    <section className="section">
      <h2>Configuration</h2>
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
            {networkOptions.map((option) => {
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
            {networkMappings.map((row) => {
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
      </section>
    </section>
  );
}
