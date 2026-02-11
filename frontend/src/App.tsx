import { useEffect, useMemo, useState } from "react";
import { Link, NavLink, Navigate, Route, Routes } from "react-router-dom";
import {
  Notification,
  getNotificationUnreadCount,
  getNotifications,
  markAllNotificationsRead,
  markNotificationRead,
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

function AppShell() {
  const { role, user, logout } = useAccess();
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [notificationsLoading, setNotificationsLoading] = useState(false);
  const [notificationError, setNotificationError] = useState("");

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
  }, [user?.email]);

  const unreadBadgeText = useMemo(() => (unreadCount > 99 ? "99+" : String(unreadCount)), [unreadCount]);

  const handleToggleNotifications = async () => {
    const nextOpen = !notificationsOpen;
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
        <div className="access-switcher">
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
          <div className="helper">
            Signed in as
            <br />
            <strong>
              {user?.first_name} {user?.last_name}
            </strong>
            <br />
            {user?.email}
          </div>
          <button className="button subtle" type="button" onClick={logout}>
            Log Out
          </button>
        </div>
      </aside>
      <div className="app-content">
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
