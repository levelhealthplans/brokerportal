import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  createOrganization,
  deleteOrganization,
  getOrganizations,
  getQuotes,
  Organization,
  updateOrganization,
  assignOrganizationQuotes,
} from "../api";
import { useAccess } from "../access";
import { paginateItems, TablePagination } from "../components/TablePagination";

export default function Organizations() {
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [quotes, setQuotes] = useState<
    {
      id: string;
      company: string;
      sponsor_domain?: string | null;
      broker_org?: string | null;
    }[]
  >([]);
  const [error, setError] = useState<string | null>(null);
  const [form, setForm] = useState({
    name: "",
    type: "broker",
    domain: "",
  });
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editDraft, setEditDraft] = useState({
    name: "",
    type: "broker",
    domain: "",
  });
  const [assignOrgId, setAssignOrgId] = useState<string | null>(null);
  const [selectedQuoteIds, setSelectedQuoteIds] = useState<string[]>([]);
  const [typeFilter, setTypeFilter] = useState<"all" | "broker" | "sponsor">(
    "all",
  );
  const [alphaSort, setAlphaSort] = useState<"asc" | "desc">("asc");
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [page, setPage] = useState(1);
  const { role, email } = useAccess();

  const load = () => {
    getOrganizations()
      .then(setOrgs)
      .catch((err) => setError(err.message));
    getQuotes({ role: "admin", email })
      .then((items) =>
        setQuotes(
          items.map((q) => ({
            id: q.id,
            company: q.company,
            sponsor_domain: q.sponsor_domain,
            broker_org: q.broker_org,
          })),
        ),
      )
      .catch(() => setQuotes([]));
  };

  useEffect(() => {
    load();
  }, [role, email]);

  const handleChange = (field: string, value: string) => {
    setForm((prev) => ({ ...prev, [field]: value }));
  };

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError(null);
    try {
      await createOrganization({
        name: form.name.trim(),
        type: form.type === "sponsor" ? "sponsor" : "broker",
        domain: form.domain.trim().toLowerCase(),
      });
      setForm({ name: "", type: "broker", domain: "" });
      setCreateModalOpen(false);
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const startEdit = (org: Organization) => {
    setEditingId(org.id);
    setEditDraft({
      name: org.name,
      type: org.type,
      domain: org.domain,
    });
  };

  const handleEditChange = (field: string, value: string) => {
    setEditDraft((prev) => ({ ...prev, [field]: value }));
  };

  const handleEditSave = async (orgId: string) => {
    setError(null);
    try {
      await updateOrganization(orgId, {
        name: editDraft.name.trim(),
        type: editDraft.type === "sponsor" ? "sponsor" : "broker",
        domain: editDraft.domain.trim().toLowerCase(),
      });
      setEditingId(null);
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const handleDelete = async (org: Organization) => {
    const confirmed = window.confirm(
      `Delete ${org.name}? This cannot be undone.`,
    );
    if (!confirmed) return;
    setError(null);
    try {
      await deleteOrganization(org.id);
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const toggleAssign = (org: Organization) => {
    if (assignOrgId === org.id) {
      setAssignOrgId(null);
      setSelectedQuoteIds([]);
      return;
    }
    setAssignOrgId(org.id);
    const preselected = quotes
      .filter((q) =>
        org.type === "broker"
          ? q.broker_org === org.name
          : q.sponsor_domain === org.domain,
      )
      .map((q) => q.id);
    setSelectedQuoteIds(preselected);
  };

  const handleAssignSave = async (org: Organization) => {
    setError(null);
    try {
      await assignOrganizationQuotes(org.id, selectedQuoteIds);
      setAssignOrgId(null);
      setSelectedQuoteIds([]);
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const visibleOrgs = useMemo(() => {
    const filtered =
      typeFilter === "all"
        ? orgs
        : orgs.filter((org) => org.type === typeFilter);
    const sorted = [...filtered].sort((a, b) => a.name.localeCompare(b.name));
    return alphaSort === "asc" ? sorted : sorted.reverse();
  }, [orgs, typeFilter, alphaSort]);

  const pagination = useMemo(
    () => paginateItems(visibleOrgs, page),
    [visibleOrgs, page],
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
        style={{ justifyContent: "space-between", marginBottom: 8 }}
      >
        <h2 style={{ margin: 0 }}>Organizations</h2>
        <button
          className="button"
          type="button"
          onClick={() => setCreateModalOpen(true)}
        >
          Create Organization
        </button>
      </div>
      {error && <div className="notice notice-error">{error}</div>}

      {createModalOpen && (
        <div
          className="modal-backdrop"
          onClick={() => setCreateModalOpen(false)}
        >
          <div className="modal" onClick={(event) => event.stopPropagation()}>
            <div className="modal-header">
              <h3 style={{ margin: 0 }}>Create Organization</h3>
              <button
                className="button ghost"
                type="button"
                onClick={() => setCreateModalOpen(false)}
              >
                Close
              </button>
            </div>
            <form className="form-grid" onSubmit={handleSubmit}>
              <label>
                Organization Name
                <input
                  required
                  value={form.name}
                  onChange={(e) => handleChange("name", e.target.value)}
                />
              </label>
              <label>
                Type
                <select
                  value={form.type}
                  onChange={(e) => handleChange("type", e.target.value)}
                >
                  <option value="broker">Broker</option>
                  <option value="sponsor">Plan Sponsor</option>
                </select>
              </label>
              <label>
                Domain
                <input
                  required
                  placeholder="legacybrokerskc.com"
                  value={form.domain}
                  onChange={(e) => handleChange("domain", e.target.value)}
                />
              </label>
              <div className="inline-actions">
                <button className="button" type="submit">
                  Create Organization
                </button>
                <button
                  className="button ghost"
                  type="button"
                  onClick={() => setCreateModalOpen(false)}
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      <div className="inline-actions" style={{ marginTop: 12 }}>
        <label>
          Filter Type
          <select
            value={typeFilter}
            onChange={(e) =>
              setTypeFilter(e.target.value as "all" | "broker" | "sponsor")
            }
          >
            <option value="all">All</option>
            <option value="broker">Broker</option>
            <option value="sponsor">Plan Sponsor</option>
          </select>
        </label>
        <label>
          Sort
          <select
            value={alphaSort}
            onChange={(e) => setAlphaSort(e.target.value as "asc" | "desc")}
          >
            <option value="asc">Alphabetical (A-Z)</option>
            <option value="desc">Alphabetical (Z-A)</option>
          </select>
        </label>
      </div>

      <div className="table-scroll">
        <table className="table" style={{ marginTop: 20 }}>
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Domain</th>
              <th>Created</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {pagination.pageItems.map((org) => {
              const isEditing = editingId === org.id;
              return (
                <tr key={org.id}>
                  <td>
                    {isEditing ? (
                      <input
                        value={editDraft.name}
                        onChange={(e) =>
                          handleEditChange("name", e.target.value)
                        }
                      />
                    ) : (
                      <Link
                        className="table-link"
                        to={`/admin/organizations/${org.id}`}
                      >
                        {org.name}
                      </Link>
                    )}
                  </td>
                  <td>
                    {isEditing ? (
                      <select
                        value={editDraft.type}
                        onChange={(e) =>
                          handleEditChange("type", e.target.value)
                        }
                      >
                        <option value="broker">Broker</option>
                        <option value="sponsor">Plan Sponsor</option>
                      </select>
                    ) : (
                      <span className="badge primary">
                        {org.type === "broker" ? "Broker" : "Plan Sponsor"}
                      </span>
                    )}
                  </td>
                  <td>
                    {isEditing ? (
                      <input
                        value={editDraft.domain}
                        onChange={(e) =>
                          handleEditChange("domain", e.target.value)
                        }
                      />
                    ) : (
                      org.domain
                    )}
                  </td>
                  <td>{new Date(org.created_at).toLocaleDateString()}</td>
                  <td>
                    {isEditing ? (
                      <div className="inline-actions">
                        <button
                          className="button secondary"
                          type="button"
                          onClick={() => handleEditSave(org.id)}
                        >
                          Save
                        </button>
                        <button
                          className="button ghost"
                          type="button"
                          onClick={() => setEditingId(null)}
                        >
                          Cancel
                        </button>
                      </div>
                    ) : (
                      <div className="inline-actions">
                        <button
                          className="button ghost"
                          type="button"
                          onClick={() => startEdit(org)}
                        >
                          Edit
                        </button>
                        <button
                          className="button secondary"
                          type="button"
                          onClick={() => toggleAssign(org)}
                        >
                          {assignOrgId === org.id ? "Close" : "Assign Quotes"}
                        </button>
                        <button
                          className="button"
                          type="button"
                          onClick={() => handleDelete(org)}
                        >
                          Delete
                        </button>
                      </div>
                    )}
                  </td>
                </tr>
              );
            })}
            {visibleOrgs.length === 0 && (
              <tr>
                <td colSpan={5} className="helper">
                  No organizations match current filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <TablePagination
        page={pagination.currentPage}
        totalItems={visibleOrgs.length}
        onPageChange={setPage}
      />

      {assignOrgId && (
        <div className="section" style={{ marginTop: 20 }}>
          <h3>Assign Quotes</h3>
          <div className="helper" style={{ marginBottom: 12 }}>
            Select quotes to attach to this organization. This updates broker or
            sponsor access immediately.
          </div>
          <div className="form-grid">
            {quotes.map((quote) => (
              <label key={quote.id} style={{ flexDirection: "row", gap: 10 }}>
                <input
                  type="checkbox"
                  checked={selectedQuoteIds.includes(quote.id)}
                  onChange={(e) => {
                    const checked = e.target.checked;
                    setSelectedQuoteIds((prev) => {
                      if (checked) return [...prev, quote.id];
                      return prev.filter((id) => id !== quote.id);
                    });
                  }}
                />
                <span>{quote.company}</span>
              </label>
            ))}
          </div>
          <div className="inline-actions" style={{ marginTop: 12 }}>
            <button
              className="button"
              type="button"
              onClick={() => {
                const org = orgs.find((o) => o.id === assignOrgId);
                if (org) handleAssignSave(org);
              }}
            >
              Save Assignments
            </button>
            <button
              className="button ghost"
              type="button"
              onClick={() => {
                setAssignOrgId(null);
                setSelectedQuoteIds([]);
              }}
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </section>
  );
}
