import { useState } from "react";
import { useAccess } from "../access";
import { useNavigate } from "react-router-dom";
import { requestAccess, requestMagicLink } from "../api";

export default function Login() {
  const { login } = useAccess();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [magicError, setMagicError] = useState<string | null>(null);
  const [magicMessage, setMagicMessage] = useState<string | null>(null);
  const [devMagicLink, setDevMagicLink] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [magicBusy, setMagicBusy] = useState(false);
  const [accessOpen, setAccessOpen] = useState(false);
  const [accessBusy, setAccessBusy] = useState(false);
  const [accessError, setAccessError] = useState<string | null>(null);
  const [accessMessage, setAccessMessage] = useState<string | null>(null);
  const [accessForm, setAccessForm] = useState({
    first_name: "",
    last_name: "",
    email: "",
    requested_role: "broker" as "broker" | "sponsor",
  });

  const onSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await login(email, password);
      navigate("/", { replace: true });
    } catch (err: any) {
      setError(err.message || "Invalid email or password.");
    } finally {
      setBusy(false);
    }
  };

  const onRequestMagicLink = async () => {
    setMagicBusy(true);
    setMagicError(null);
    setMagicMessage(null);
    setDevMagicLink(null);
    try {
      const normalizedEmail = email.trim().toLowerCase();
      if (!normalizedEmail) {
        throw new Error("Enter your work email first.");
      }
      const result = await requestMagicLink(normalizedEmail);
      if (result.status === "dev_link" && result.link) {
        setMagicMessage("Local dev link generated.");
        setDevMagicLink(result.link);
        return;
      }
      setMagicMessage("Magic link sent. Check your inbox.");
    } catch (err: any) {
      setMagicError(err.message || "Unable to send magic link.");
    } finally {
      setMagicBusy(false);
    }
  };

  const onAccessFieldChange = (field: keyof typeof accessForm, value: string) => {
    setAccessForm((prev) => ({ ...prev, [field]: value }));
  };

  const onToggleAccess = () => {
    setAccessOpen((prev) => !prev);
    setAccessError(null);
    setAccessMessage(null);
    if (!accessForm.email && email.trim()) {
      onAccessFieldChange("email", email.trim().toLowerCase());
    }
  };

  const onSubmitAccessRequest = async (event: React.FormEvent) => {
    event.preventDefault();
    setAccessBusy(true);
    setAccessError(null);
    setAccessMessage(null);
    try {
      const result = await requestAccess({
        first_name: accessForm.first_name.trim(),
        last_name: accessForm.last_name.trim(),
        email: accessForm.email.trim().toLowerCase(),
        requested_role: accessForm.requested_role,
      });
      setAccessMessage(result.message);
      if (result.status === "approved" || result.status === "existing_user") {
        setEmail(accessForm.email.trim().toLowerCase());
      }
    } catch (err: any) {
      setAccessError(err.message || "Unable to submit access request.");
    } finally {
      setAccessBusy(false);
    }
  };

  return (
    <div className="login-screen">
      <div className="login-layout">
        <section className="login-brand-panel">
          <div className="login-brand-card">
            <img className="login-brand-logo" src="/logo.png" alt="Level Health" />
            <p className="login-brand-eyebrow">Broker Portal</p>
            <h1>Big care for small business.</h1>
            <p className="login-brand-copy">
              Secure access for quoting, implementations, and broker operations.
            </p>
          </div>
        </section>
        <section className="login-form-panel">
          <div className="login-form-card">
            <h2>Log In</h2>
            <form onSubmit={onSubmit} className="login-form-grid">
              <label>
                Work Email
                <input
                  type="email"
                  required
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@company.com"
                />
              </label>
              <label>
                Password
                <input
                  type="password"
                  required
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Enter password"
                />
              </label>
              <div className="inline-actions">
                <button className="button" type="submit" disabled={busy}>
                  {busy ? "Signing In..." : "Sign In"}
                </button>
                <button className="button subtle" type="button" disabled={magicBusy} onClick={onRequestMagicLink}>
                  {magicBusy ? "Sending..." : magicMessage ? "Resend Magic Link" : "Email Magic Link"}
                </button>
              </div>
              <div className="helper">
                Use password sign-in or receive a one-time magic link by email.
              </div>
            </form>
            {error && <div className="notice login-notice">{error}</div>}
            {magicError && <div className="notice login-notice">{magicError}</div>}
            {magicMessage && <div className="notice notice-success login-notice">{magicMessage}</div>}
            {devMagicLink && (
              <div className="helper" style={{ marginTop: 8 }}>
                Local link:{" "}
                <a href={devMagicLink} target="_blank" rel="noreferrer">
                  Open sign-in link
                </a>
              </div>
            )}
            <div className="inline-actions" style={{ marginTop: 12 }}>
              <button className="button ghost" type="button" onClick={onToggleAccess}>
                {accessOpen ? "Hide Access Request" : "Need an account? Request Access"}
              </button>
            </div>
            {accessOpen && (
              <form className="login-form-grid" style={{ marginTop: 12 }} onSubmit={onSubmitAccessRequest}>
                <label>
                  First Name
                  <input
                    required
                    value={accessForm.first_name}
                    onChange={(e) => onAccessFieldChange("first_name", e.target.value)}
                    placeholder="First name"
                  />
                </label>
                <label>
                  Last Name
                  <input
                    required
                    value={accessForm.last_name}
                    onChange={(e) => onAccessFieldChange("last_name", e.target.value)}
                    placeholder="Last name"
                  />
                </label>
                <label>
                  Work Email
                  <input
                    type="email"
                    required
                    value={accessForm.email}
                    onChange={(e) => onAccessFieldChange("email", e.target.value)}
                    placeholder="you@company.com"
                  />
                </label>
                <label>
                  Requested Role
                  <select
                    value={accessForm.requested_role}
                    onChange={(e) =>
                      onAccessFieldChange("requested_role", e.target.value as "broker" | "sponsor")
                    }
                  >
                    <option value="broker">Broker</option>
                    <option value="sponsor">Plan Sponsor</option>
                  </select>
                </label>
                <div className="inline-actions">
                  <button className="button secondary" type="submit" disabled={accessBusy}>
                    {accessBusy ? "Submitting..." : "Submit Access Request"}
                  </button>
                </div>
                <div className="helper">
                  Your work email domain is used to determine organization routing. Broker requests from known domains
                  are approved automatically. Sponsor requests go to admin review.
                </div>
              </form>
            )}
            {accessError && <div className="notice login-notice">{accessError}</div>}
            {accessMessage && <div className="notice notice-success login-notice">{accessMessage}</div>}
          </div>
        </section>
      </div>
      <span className="login-box login-box-a" aria-hidden="true" />
      <span className="login-box login-box-b" aria-hidden="true" />
      <span className="login-box login-box-c" aria-hidden="true" />
      <span className="login-box login-box-d" aria-hidden="true" />
    </div>
  );
}
