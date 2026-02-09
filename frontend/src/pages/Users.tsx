import { useEffect, useState } from "react";
import {
  assignUserQuotes,
  assignUserTasks,
  createUser,
  deleteUser,
  getQuotes,
  getTasks,
  getUsers,
  updateUser,
  User,
} from "../api";
import { useAccess } from "../access";
import { useAutoDismissMessage } from "../hooks/useAutoDismissMessage";

export default function Users() {
  const { email } = useAccess();
  const [users, setUsers] = useState<User[]>([]);
  const [quotes, setQuotes] = useState<{ id: string; company: string; assigned_user_id?: string | null }[]>([]);
  const [tasks, setTasks] = useState<
    { id: string; title: string; installation_company?: string | null; assigned_user_id?: string | null }[]
  >([]);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [form, setForm] = useState({
    first_name: "",
    last_name: "",
    email: "",
    password: "",
    confirm_password: "",
    phone: "",
    job_title: "",
    organization: "",
    role: "broker",
  });
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editDraft, setEditDraft] = useState({
    first_name: "",
    last_name: "",
    email: "",
    phone: "",
    job_title: "",
    organization: "",
    role: "broker",
  });
  const [assignUserId, setAssignUserId] = useState<string | null>(null);
  const [selectedQuoteIds, setSelectedQuoteIds] = useState<string[]>([]);
  const [selectedTaskIds, setSelectedTaskIds] = useState<string[]>([]);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [passwordUser, setPasswordUser] = useState<User | null>(null);
  const [passwordDraft, setPasswordDraft] = useState("");
  const [passwordConfirm, setPasswordConfirm] = useState("");
  const statusMessageFading = useAutoDismissMessage(statusMessage, setStatusMessage, 5000, 500);

  const load = () => {
    getUsers()
      .then(setUsers)
      .catch((err) => setError(err.message));
    getQuotes({ role: "admin", email })
      .then((items) =>
        setQuotes(
          items.map((q) => ({
            id: q.id,
            company: q.company,
            assigned_user_id: q.assigned_user_id,
          }))
        )
      )
      .catch(() => setQuotes([]));
    getTasks({ role: "admin", email })
      .then((items) =>
        setTasks(
          items.map((task) => ({
            id: task.id,
            title: task.title,
            installation_company: task.installation_company || null,
            assigned_user_id: task.assigned_user_id,
          }))
        )
      )
      .catch(() => setTasks([]));
  };

  useEffect(() => {
    load();
  }, [email]);

  const handleChange = (field: string, value: string) => {
    setForm((prev) => ({ ...prev, [field]: value }));
  };

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError(null);
    setStatusMessage(null);
    const password = form.password.trim();
    if (!password) {
      setError("Password is required.");
      return;
    }
    if (password.length < 8) {
      setError("Password must be at least 8 characters.");
      return;
    }
    if (password !== form.confirm_password) {
      setError("Password confirmation does not match.");
      return;
    }
    try {
      await createUser({
        first_name: form.first_name.trim(),
        last_name: form.last_name.trim(),
        email: form.email.trim().toLowerCase(),
        password,
        phone: form.phone.trim(),
        job_title: form.job_title.trim(),
        organization: form.organization.trim(),
        role: form.role,
      });
      setForm({
        first_name: "",
        last_name: "",
        email: "",
        password: "",
        confirm_password: "",
        phone: "",
        job_title: "",
        organization: "",
        role: "broker",
      });
      setCreateModalOpen(false);
      setStatusMessage("User created.");
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const openPasswordModal = (user: User) => {
    setPasswordUser(user);
    setPasswordDraft("");
    setPasswordConfirm("");
    setError(null);
    setStatusMessage(null);
  };

  const closePasswordModal = () => {
    setPasswordUser(null);
    setPasswordDraft("");
    setPasswordConfirm("");
  };

  const handlePasswordSave = async () => {
    if (!passwordUser) return;
    setError(null);
    setStatusMessage(null);
    const password = passwordDraft.trim();
    if (!password) {
      setError("Password is required.");
      return;
    }
    if (password.length < 8) {
      setError("Password must be at least 8 characters.");
      return;
    }
    if (password !== passwordConfirm) {
      setError("Password confirmation does not match.");
      return;
    }
    try {
      await updateUser(passwordUser.id, { password });
      closePasswordModal();
      setStatusMessage(`Password updated for ${passwordUser.first_name} ${passwordUser.last_name}.`);
    } catch (err: any) {
      setError(err.message);
    }
  };

  const startEdit = (user: User) => {
    setEditingId(user.id);
    setEditDraft({
      first_name: user.first_name,
      last_name: user.last_name,
      email: user.email,
      phone: user.phone || "",
      job_title: user.job_title,
      organization: user.organization,
      role: user.role,
    });
  };

  const handleEditChange = (field: string, value: string) => {
    setEditDraft((prev) => ({ ...prev, [field]: value }));
  };

  const handleEditSave = async (userId: string) => {
    setError(null);
    setStatusMessage(null);
    try {
      await updateUser(userId, {
        first_name: editDraft.first_name.trim(),
        last_name: editDraft.last_name.trim(),
        email: editDraft.email.trim().toLowerCase(),
        phone: editDraft.phone.trim(),
        job_title: editDraft.job_title.trim(),
        organization: editDraft.organization.trim(),
        role: editDraft.role,
      });
      setEditingId(null);
      setStatusMessage("User updated.");
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const handleDelete = async (user: User) => {
    const confirmed = window.confirm(`Delete ${user.first_name} ${user.last_name}?`);
    if (!confirmed) return;
    setError(null);
    setStatusMessage(null);
    try {
      await deleteUser(user.id);
      setStatusMessage("User deleted.");
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const toggleAssign = (user: User) => {
    if (assignUserId === user.id) {
      setAssignUserId(null);
      setSelectedQuoteIds([]);
      setSelectedTaskIds([]);
      return;
    }
    setAssignUserId(user.id);
    setSelectedQuoteIds(
      quotes.filter((quote) => quote.assigned_user_id === user.id).map((quote) => quote.id)
    );
    setSelectedTaskIds(
      tasks.filter((task) => task.assigned_user_id === user.id).map((task) => task.id)
    );
  };

  const handleAssignSave = async (user: User) => {
    setError(null);
    setStatusMessage(null);
    try {
      await assignUserQuotes(user.id, selectedQuoteIds);
      await assignUserTasks(user.id, selectedTaskIds);
      setAssignUserId(null);
      setSelectedQuoteIds([]);
      setSelectedTaskIds([]);
      setStatusMessage("Assignments updated.");
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  return (
    <section className="section">
      <div className="inline-actions" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <h2 style={{ margin: 0 }}>Users</h2>
        <button className="button" type="button" onClick={() => setCreateModalOpen(true)}>
          Create User
        </button>
      </div>
      {error && <div className="notice">{error}</div>}
      {statusMessage && (
        <div className={`notice notice-success ${statusMessageFading ? "fade-out" : ""}`}>
          {statusMessage}
        </div>
      )}

      {createModalOpen && (
        <div className="modal-backdrop" onClick={() => setCreateModalOpen(false)}>
          <div className="modal" onClick={(event) => event.stopPropagation()}>
            <div className="modal-header">
              <h3 style={{ margin: 0 }}>Create User</h3>
              <button className="button ghost" type="button" onClick={() => setCreateModalOpen(false)}>
                Close
              </button>
            </div>
            <form className="form-grid" onSubmit={handleSubmit}>
              <label>
                First Name
                <input required value={form.first_name} onChange={(e) => handleChange("first_name", e.target.value)} />
              </label>
              <label>
                Last Name
                <input required value={form.last_name} onChange={(e) => handleChange("last_name", e.target.value)} />
              </label>
              <label>
                Email
                <input
                  required
                  type="email"
                  value={form.email}
                  onChange={(e) => handleChange("email", e.target.value)}
                />
              </label>
              <label>
                Password
                <input
                  required
                  type="password"
                  value={form.password}
                  onChange={(e) => handleChange("password", e.target.value)}
                />
              </label>
              <label>
                Confirm Password
                <input
                  required
                  type="password"
                  value={form.confirm_password}
                  onChange={(e) => handleChange("confirm_password", e.target.value)}
                />
              </label>
              <label>
                Phone
                <input value={form.phone} onChange={(e) => handleChange("phone", e.target.value)} />
              </label>
              <label>
                Job Title
                <input required value={form.job_title} onChange={(e) => handleChange("job_title", e.target.value)} />
              </label>
              <label>
                Organization
                <input required value={form.organization} onChange={(e) => handleChange("organization", e.target.value)} />
              </label>
              <label>
                Role
                <select value={form.role} onChange={(e) => handleChange("role", e.target.value)}>
                  <option value="broker">Broker</option>
                  <option value="sponsor">Sponsor</option>
                  <option value="admin">Admin</option>
                </select>
              </label>
              <div className="inline-actions">
                <button className="button" type="submit">
                  Create User
                </button>
                <button className="button ghost" type="button" onClick={() => setCreateModalOpen(false)}>
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {passwordUser && (
        <div className="modal-backdrop" onClick={closePasswordModal}>
          <div className="modal" onClick={(event) => event.stopPropagation()}>
            <div className="modal-header">
              <h3 style={{ margin: 0 }}>
                Set Password: {passwordUser.first_name} {passwordUser.last_name}
              </h3>
              <button className="button ghost" type="button" onClick={closePasswordModal}>
                Close
              </button>
            </div>
            <form
              className="form-grid"
              onSubmit={(event) => {
                event.preventDefault();
                handlePasswordSave();
              }}
            >
              <label>
                New Password
                <input
                  required
                  type="password"
                  value={passwordDraft}
                  onChange={(e) => setPasswordDraft(e.target.value)}
                />
              </label>
              <label>
                Confirm Password
                <input
                  required
                  type="password"
                  value={passwordConfirm}
                  onChange={(e) => setPasswordConfirm(e.target.value)}
                />
              </label>
              <div className="inline-actions">
                <button className="button" type="submit">
                  Save Password
                </button>
                <button className="button ghost" type="button" onClick={closePasswordModal}>
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      <table className="table" style={{ marginTop: 20 }}>
        <thead>
          <tr>
            <th>Name</th>
            <th>Email</th>
            <th>Phone</th>
            <th>Job Title</th>
            <th>Organization</th>
            <th>Role</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {users.map((user) => {
            const isEditing = editingId === user.id;
            return (
              <tr key={user.id}>
                <td>
                  {isEditing ? (
                    <div className="inline-actions">
                      <input value={editDraft.first_name} onChange={(e) => handleEditChange("first_name", e.target.value)} />
                      <input value={editDraft.last_name} onChange={(e) => handleEditChange("last_name", e.target.value)} />
                    </div>
                  ) : (
                    `${user.first_name} ${user.last_name}`
                  )}
                </td>
                <td>
                  {isEditing ? (
                    <input value={editDraft.email} onChange={(e) => handleEditChange("email", e.target.value)} />
                  ) : (
                    user.email
                  )}
                </td>
                <td>
                  {isEditing ? (
                    <input value={editDraft.phone} onChange={(e) => handleEditChange("phone", e.target.value)} />
                  ) : (
                    user.phone || "—"
                  )}
                </td>
                <td>
                  {isEditing ? (
                    <input value={editDraft.job_title} onChange={(e) => handleEditChange("job_title", e.target.value)} />
                  ) : (
                    user.job_title
                  )}
                </td>
                <td>
                  {isEditing ? (
                    <input
                      value={editDraft.organization}
                      onChange={(e) => handleEditChange("organization", e.target.value)}
                    />
                  ) : (
                    user.organization
                  )}
                </td>
                <td>
                  {isEditing ? (
                    <select value={editDraft.role} onChange={(e) => handleEditChange("role", e.target.value)}>
                      <option value="broker">Broker</option>
                      <option value="sponsor">Sponsor</option>
                      <option value="admin">Admin</option>
                    </select>
                  ) : (
                    <span className="badge primary">{user.role}</span>
                  )}
                </td>
                <td>
                  {isEditing ? (
                    <div className="inline-actions">
                      <button className="button secondary" type="button" onClick={() => handleEditSave(user.id)}>
                        Save
                      </button>
                      <button className="button ghost" type="button" onClick={() => setEditingId(null)}>
                        Cancel
                      </button>
                    </div>
                  ) : (
                    <div className="inline-actions">
                      <button className="button ghost" type="button" onClick={() => startEdit(user)}>
                        Edit
                      </button>
                      <button className="button ghost" type="button" onClick={() => openPasswordModal(user)}>
                        Password
                      </button>
                      <button className="button secondary" type="button" onClick={() => toggleAssign(user)}>
                        {assignUserId === user.id ? "Close" : "Assign"}
                      </button>
                      <button className="button" type="button" onClick={() => handleDelete(user)}>
                        Delete
                      </button>
                    </div>
                  )}
                </td>
              </tr>
            );
          })}
          {users.length === 0 && (
            <tr>
              <td colSpan={7} className="helper">
                No users yet.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      {assignUserId && (
        <div className="section" style={{ marginTop: 20 }}>
          <h3>Assign Quotes and Tasks</h3>
          <div className="grid grid-3">
            <div>
              <strong>Quotes</strong>
              <div className="helper" style={{ marginBottom: 8 }}>
                Assign quote ownership.
              </div>
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
            <div style={{ gridColumn: "span 2" }}>
              <strong>Tasks</strong>
              <div className="helper" style={{ marginBottom: 8 }}>
                Assign implementation tasks.
              </div>
              {tasks.map((task) => (
                <label key={task.id} style={{ flexDirection: "row", gap: 10 }}>
                  <input
                    type="checkbox"
                    checked={selectedTaskIds.includes(task.id)}
                    onChange={(e) => {
                      const checked = e.target.checked;
                      setSelectedTaskIds((prev) => {
                        if (checked) return [...prev, task.id];
                        return prev.filter((id) => id !== task.id);
                      });
                    }}
                  />
                  <span>
                    {task.title}
                    {task.installation_company ? ` · ${task.installation_company}` : ""}
                  </span>
                </label>
              ))}
            </div>
          </div>
          <div className="inline-actions" style={{ marginTop: 12 }}>
            <button
              className="button"
              type="button"
              onClick={() => {
                const user = users.find((u) => u.id === assignUserId);
                if (user) handleAssignSave(user);
              }}
            >
              Save Assignments
            </button>
            <button
              className="button ghost"
              type="button"
              onClick={() => {
                setAssignUserId(null);
                setSelectedQuoteIds([]);
                setSelectedTaskIds([]);
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
