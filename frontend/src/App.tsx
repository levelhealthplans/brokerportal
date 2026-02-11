import { useEffect, useMemo, useState } from "react";
import { Link, NavLink, Navigate, Route, Routes } from "react-router-dom";
import {
  AuthProfile,
  Notification,
  getAuthProfile,
  getNotificationUnreadCount,
  getNotifications,
  markAllNotificationsRead,
  markNotificationRead,
  updateAuthProfile,
} from "./api";
import Dashboard from "./pages/Dashboard";
import QuotesList from "./pages/QuotesList";
import Tasks from "./pages/Tasks";
import NewQuote from "./pages/NewQuote";
import QuoteDetail from "./pages/QuoteDetail";
import ImplementationsList from "./pages/ImplementationsList";
import ImplementationDetail from "./pages/ImplementationDetail";
import Organizations from "./pages/Organizations";
import OrganizationDetail from "./pages/OrganizationDetail";
import Configuration from "./pages/Configuration";
import Users from "./pages/Users";
import Login from "./pages/Login";
import AuthVerify from "./pages/AuthVerify";
import { AccessProvider, useAccess } from "./access";

function resolveNotificationHref(notification: Notification): string | null {
  const entityId = (notification.entity_id || "").trim();
  if (!entityId) return null;
  if (notification.entity_type === "quote") return `/quotes/${entityId}`;
  if (notification.entity_type === "installation") return `/implementations/${entityId}`;
  return null;
}

function formatNotificationTime(value: string): string {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

type ProfileDraft = {
  first_name: string;
  last_name: string;
  phone: string;
  job_title: string;
  email: string;
  organization: string;
  password: string;
  confirm_password: string;
};

const emptyProfileDraft = (): ProfileDraft => ({
  first_name: "",
  last_name: "",
  phone: "",
  job_title: "",
  email: "",
  organization: "",
  password: "",
  confirm_password: "",
});

function AppShell() {
  const { role, user, logout, setUser } = useAccess();
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [notificationsLoading, setNotificationsLoading] = useState(false);
  const [notificationError, setNotificationError] = useState("");
  const [accountOpen, setAccountOpen] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const [profileLoading, setProfileLoading] = useState(false);
  const [profileSaving, setProfileSaving] = useState(false);
  const [profileError, setProfileError] = useState("");
  const [profileMessage, setProfileMessage] = useState("");
  const [profileDraft, setProfileDraft] = useState<ProfileDraft>(emptyProfileDraft());

  const loadUnreadCount = async () => {
    const result = await getNotificationUnreadCount();
    setUnreadCount(result.unread_count);
  };

  const loadNotifications = async () => {
    setNotificationsLoading(true);
    try {
      const [countData, listData] = await Promise.all([
        getNotificationUnreadCount(),
        getNotifications(60),
      ]);
      setUnreadCount(countData.unread_count);
      setNotifications(listData);
      setNotificationError("");
    } catch (err: any) {
      setNotificationError(err?.message || "Failed to load notifications.");
    } finally {
      setNotificationsLoading(false);
    }
  };

  useEffect(() => {
    let isActive = true;
    const poll = async () => {
      try {
        if (notificationsOpen) {
          const listData = await getNotifications(60);
          const countData = await getNotificationUnreadCount();
          if (!isActive) return;
          setNotifications(listData);
          setUnreadCount(countData.unread_count);
          setNotificationError("");
          return;
        }
        const countData = await getNotificationUnreadCount();
        if (!isActive) return;
        setUnreadCount(countData.unread_count);
      } catch {
        if (!isActive) return;
      }
    };
    void poll();
    const intervalId = window.setInterval(() => {
      void poll();
    }, 30000);
    return () => {
      isActive = false;
      window.clearInterval(intervalId);
    };
  }, [notificationsOpen]);

  useEffect(() => {
    setNotificationsOpen(false);
    setNotifications([]);
    setUnreadCount(0);
    setNotificationError("");
    setAccountOpen(false);
    setProfileOpen(false);
    setProfileDraft(emptyProfileDraft());
    setProfileError("");
    setProfileMessage("");
  }, [user?.email]);

  const unreadBadgeText = useMemo(() => (unreadCount > 99 ? "99+" : String(unreadCount)), [unreadCount]);
  const userInitials = useMemo(() => {
    const first = (user?.first_name || "").trim().charAt(0).toUpperCase();
    const last = (user?.last_name || "").trim().charAt(0).toUpperCase();
    return `${first}${last}` || "U";
  }, [user?.first_name, user?.last_name]);

  const handleToggleNotifications = async () => {
    const nextOpen = !notificationsOpen;
    setAccountOpen(false);
    setNotificationsOpen(nextOpen);
    try {
      if (nextOpen) {
        await loadNotifications();
      } else {
        setNotificationError("");
        await loadUnreadCount();
      }
    } catch (err: any) {
      setNotificationError(err?.message || "Failed to load notifications.");
    }
  };

  const handleMarkNotificationRead = async (notificationId: string) => {
    try {
      await markNotificationRead(notificationId);
      setNotifications((prev) =>
        prev.map((item) =>
          item.id === notificationId ? { ...item, is_read: true, read_at: new Date().toISOString() } : item
        )
      );
      setUnreadCount((prev) => Math.max(0, prev - 1));
      setNotificationError("");
    } catch (err: any) {
      setNotificationError(err?.message || "Failed to update notification.");
      throw err;
    }
  };

  const handleNotificationOpen = async (notification: Notification) => {
    if (!notification.is_read) {
      try {
        await handleMarkNotificationRead(notification.id);
      } catch {
        return;
      }
    }
    setNotificationsOpen(false);
  };

  const handleMarkAllRead = async () => {
    try {
      const result = await markAllNotificationsRead();
      if (result.updated_count <= 0) return;
      setNotifications((prev) =>
        prev.map((item) => ({ ...item, is_read: true, read_at: item.read_at || new Date().toISOString() }))
      );
      setUnreadCount(0);
      setNotificationError("");
    } catch (err: any) {
      setNotificationError(err?.message || "Failed to update notifications.");
    }
  };

  const loadProfileDraft = (profile: AuthProfile) => {
    setProfileDraft({
      first_name: profile.first_name || "",
      last_name: profile.last_name || "",
      phone: profile.phone || "",
      job_title: profile.job_title || "",
      email: profile.email || "",
      organization: profile.organization || "",
      password: "",
      confirm_password: "",
    });
  };

  const handleOpenProfile = async () => {
    setProfileLoading(true);
    setProfileError("");
    setProfileMessage("");
    try {
      const profile = await getAuthProfile();
      loadProfileDraft(profile);
      setProfileOpen(true);
      setAccountOpen(false);
    } catch (err: any) {
      setProfileError(err?.message || "Failed to load profile.");
      setProfileOpen(true);
    } finally {
      setProfileLoading(false);
    }
  };

  const handleSaveProfile = async () => {
    const firstName = profileDraft.first_name.trim();
    const lastName = profileDraft.last_name.trim();
    const jobTitle = profileDraft.job_title.trim();
    const phone = profileDraft.phone.trim();
    const password = profileDraft.password;
    const confirmPassword = profileDraft.confirm_password;

    if (!firstName || !lastName || !jobTitle) {
      setProfileError("First name, last name, and job title are required.");
      return;
    }
    if (password && password !== confirmPassword) {
      setProfileError("Password confirmation does not match.");
      return;
    }

    setProfileSaving(true);
    setProfileError("");
    setProfileMessage("");
    try {
      const updated = await updateAuthProfile({
        first_name: firstName,
        last_name: lastName,
        phone,
        job_title: jobTitle,
        password: password || undefined,
      });
      setUser({
        email: updated.email,
        role: updated.role,
        first_name: updated.first_name,
        last_name: updated.last_name,
        organization: updated.organization,
      });
      loadProfileDraft(updated);
      setProfileMessage("Profile updated.");
    } catch (err: any) {
      setProfileError(err?.message || "Failed to save profile.");
    } finally {
      setProfileSaving(false);
    }
  };

  return (
    <div className="app-shell">
      <aside className="side-nav">
        <div className="side-brand">
          <img className="side-brand-logo" src="/logo.png" alt="Level Health" />
        </div>
        <div className="side-links">
          <NavLink to="/" end className={({ isActive }) => `side-link${isActive ? " active" : ""}`}>
            Home
          </NavLink>
          <NavLink to="/tasks" end className={({ isActive }) => `side-link${isActive ? " active" : ""}`}>
            Tasks
          </NavLink>
          <NavLink to="/quotes" end className={({ isActive }) => `side-link${isActive ? " active" : ""}`}>
            Quotes
          </NavLink>
          <NavLink
            to="/implementations"
            className={({ isActive }) => `side-link${isActive ? " active" : ""}`}
          >
            Implementation
          </NavLink>
          {role === "admin" && (
            <NavLink
              to="/admin/configuration"
              className={({ isActive }) => `side-link${isActive ? " active" : ""}`}
            >
              Configuration
            </NavLink>
          )}
          {role === "admin" && (
            <NavLink
              to="/admin/organizations"
              className={({ isActive }) => `side-link${isActive ? " active" : ""}`}
            >
              Organizations
            </NavLink>
          )}
          {role === "admin" && (
            <NavLink
              to="/admin/users"
              className={({ isActive }) => `side-link${isActive ? " active" : ""}`}
            >
              Users
            </NavLink>
          )}
        </div>
      </aside>
      <div className="app-content">
        <div className="app-toolbar">
          <div className="app-toolbar-controls">
            <div className="notifications-shell">
              <button
                className="button subtle notification-toggle"
                type="button"
                onClick={() => {
                  void handleToggleNotifications();
                }}
              >
                Notifications
                {unreadCount > 0 && <span className="notification-count">{unreadBadgeText}</span>}
              </button>
              {notificationsOpen && (
                <div className="notification-panel">
                  <div className="notification-panel-header">
                    <strong>Notifications</strong>
                    <button
                      className="button subtle"
                      type="button"
                      disabled={unreadCount === 0}
                      onClick={() => {
                        void handleMarkAllRead();
                      }}
                    >
                      Mark all read
                    </button>
                  </div>
                  {notificationsLoading && <div className="helper">Loading...</div>}
                  {!notificationsLoading && notificationError && (
                    <div className="helper">{notificationError}</div>
                  )}
                  {!notificationsLoading && !notificationError && notifications.length === 0 && (
                    <div className="helper">No notifications yet.</div>
                  )}
                  {!notificationsLoading &&
                    !notificationError &&
                    notifications.map((notification) => {
                      const href = resolveNotificationHref(notification);
                      return (
                        <div
                          key={notification.id}
                          className={`notification-item${notification.is_read ? "" : " unread"}`}
                        >
                          <div className="notification-item-head">
                            <strong>{notification.title}</strong>
                            <span className="helper">{formatNotificationTime(notification.created_at)}</span>
                          </div>
                          <div className="helper">{notification.body}</div>
                          <div className="notification-item-actions">
                            {href ? (
                              <Link
                                className="button subtle"
                                to={href}
                                onClick={() => {
                                  void handleNotificationOpen(notification);
                                }}
                              >
                                Open
                              </Link>
                            ) : (
                              <span />
                            )}
                            {!notification.is_read && (
                              <button
                                className="button subtle"
                                type="button"
                                onClick={() => {
                                  void handleMarkNotificationRead(notification.id);
                                }}
                              >
                                Mark read
                              </button>
                            )}
                          </div>
                        </div>
                      );
                    })}
                </div>
              )}
            </div>
            <div className="account-shell">
              <button
                className="button subtle account-toggle"
                type="button"
                onClick={() => {
                  setNotificationsOpen(false);
                  setAccountOpen((prev) => !prev);
                }}
              >
                <span className="account-avatar" aria-hidden="true">
                  {userInitials}
                </span>
                <span className="account-label">Account</span>
              </button>
              {accountOpen && (
                <div className="account-panel">
                  <div className="helper">
                    <strong>
                      {user?.first_name} {user?.last_name}
                    </strong>
                    <br />
                    {user?.email}
                  </div>
                  <div className="inline-actions">
                    <button
                      className="button subtle"
                      type="button"
                      onClick={() => {
                        void handleOpenProfile();
                      }}
                    >
                      Profile settings
                    </button>
                    <button className="button subtle" type="button" onClick={logout}>
                      Log Out
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
        <main className="app-main">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/quotes" element={<QuotesList />} />
            <Route path="/tasks" element={<Tasks />} />
            <Route path="/quotes/new" element={<NewQuote />} />
            <Route path="/quotes/:id" element={<QuoteDetail />} />
            <Route path="/implementations" element={<ImplementationsList />} />
            <Route path="/implementations/:id" element={<ImplementationDetail />} />
            <Route path="/network-assignment" element={<Navigate to="/quotes" replace />} />
            <Route path="/admin/configuration" element={<Configuration />} />
            <Route path="/admin/organizations" element={<Organizations />} />
            <Route path="/admin/organizations/:id" element={<OrganizationDetail />} />
            <Route path="/admin/users" element={<Users />} />
          </Routes>
        </main>
        {profileOpen && (
          <div
            className="modal-backdrop"
            onClick={() => {
              if (profileSaving) return;
              setProfileOpen(false);
            }}
          >
            <div className="modal profile-modal" onClick={(event) => event.stopPropagation()}>
              <div className="modal-header">
                <h3>Profile Settings</h3>
                <button
                  className="button subtle"
                  type="button"
                  disabled={profileSaving}
                  onClick={() => setProfileOpen(false)}
                >
                  Close
                </button>
              </div>
              {profileLoading ? (
                <div className="helper">Loading profile...</div>
              ) : (
                <>
                  {profileError && <div className="helper">{profileError}</div>}
                  {profileMessage && <div className="helper">{profileMessage}</div>}
                  <div className="form-grid">
                    <label>
                      First Name
                      <input
                        value={profileDraft.first_name}
                        onChange={(event) =>
                          setProfileDraft((prev) => ({ ...prev, first_name: event.target.value }))
                        }
                      />
                    </label>
                    <label>
                      Last Name
                      <input
                        value={profileDraft.last_name}
                        onChange={(event) =>
                          setProfileDraft((prev) => ({ ...prev, last_name: event.target.value }))
                        }
                      />
                    </label>
                    <label>
                      Job Title
                      <input
                        value={profileDraft.job_title}
                        onChange={(event) =>
                          setProfileDraft((prev) => ({ ...prev, job_title: event.target.value }))
                        }
                      />
                    </label>
                    <label>
                      Phone
                      <input
                        value={profileDraft.phone}
                        onChange={(event) =>
                          setProfileDraft((prev) => ({ ...prev, phone: event.target.value }))
                        }
                      />
                    </label>
                    <label>
                      Email (Read only)
                      <input value={profileDraft.email} disabled />
                    </label>
                    <label>
                      Organization (Read only)
                      <input value={profileDraft.organization} disabled />
                    </label>
                    <label>
                      New Password
                      <input
                        type="password"
                        value={profileDraft.password}
                        onChange={(event) =>
                          setProfileDraft((prev) => ({ ...prev, password: event.target.value }))
                        }
                        placeholder="Leave blank to keep current password"
                      />
                    </label>
                    <label>
                      Confirm Password
                      <input
                        type="password"
                        value={profileDraft.confirm_password}
                        onChange={(event) =>
                          setProfileDraft((prev) => ({ ...prev, confirm_password: event.target.value }))
                        }
                        placeholder="Re-enter new password"
                      />
                    </label>
                  </div>
                  <div className="inline-actions" style={{ marginTop: 16 }}>
                    <button
                      className="button"
                      type="button"
                      disabled={profileSaving || profileLoading}
                      onClick={() => {
                        void handleSaveProfile();
                      }}
                    >
                      {profileSaving ? "Saving..." : "Save Profile"}
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        )}
        <footer className="tagline">Big care for small business.</footer>
      </div>
    </div>
  );
}

function AppContent() {
  const { loading, isAuthenticated } = useAccess();

  if (loading) {
    return <div className="section">Loading session...</div>;
  }

  if (!isAuthenticated) {
    return (
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="/auth/verify" element={<AuthVerify />} />
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    );
  }

  return (
    <Routes>
      <Route path="/auth/verify" element={<Navigate to="/" replace />} />
      <Route path="/*" element={<AppShell />} />
    </Routes>
  );
}

export default function App() {
  return (
    <AccessProvider>
      <AppContent />
    </AccessProvider>
  );
}
