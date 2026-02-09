import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import QuotesList from "./pages/QuotesList";
import Tasks from "./pages/Tasks";
import NewQuote from "./pages/NewQuote";
import QuoteDetail from "./pages/QuoteDetail";
import ImplementationsList from "./pages/ImplementationsList";
import ImplementationDetail from "./pages/ImplementationDetail";
import NetworkAssignments from "./pages/NetworkAssignments";
import Organizations from "./pages/Organizations";
import OrganizationDetail from "./pages/OrganizationDetail";
import Configuration from "./pages/Configuration";
import Users from "./pages/Users";
import Login from "./pages/Login";
import AuthVerify from "./pages/AuthVerify";
import { AccessProvider, useAccess } from "./access";

function AppShell() {
  const { role, user, logout } = useAccess();

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
          <NavLink to="/quotes" end className={({ isActive }) => `side-link${isActive ? " active" : ""}`}>
            Quotes
          </NavLink>
          <NavLink to="/tasks" end className={({ isActive }) => `side-link${isActive ? " active" : ""}`}>
            Tasks
          </NavLink>
          <NavLink to="/quotes/new" className={({ isActive }) => `side-link${isActive ? " active" : ""}`}>
            New Quote
          </NavLink>
          <NavLink
            to="/implementations"
            className={({ isActive }) => `side-link${isActive ? " active" : ""}`}
          >
            Implementation
          </NavLink>
          <NavLink
            to="/network-assignment"
            className={({ isActive }) => `side-link${isActive ? " active" : ""}`}
          >
            Network Assignment
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
            <Route path="/network-assignment" element={<NetworkAssignments />} />
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
