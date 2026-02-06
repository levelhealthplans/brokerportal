import { useState } from "react";
import { useAccess } from "../access";
import { Link, useNavigate } from "react-router-dom";

export default function Login() {
  const { requestMagicLink, setUser } = useAccess();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [devLink, setDevLink] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const onSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    setMessage(null);
    setDevLink(null);
    try {
      const result = await requestMagicLink(email);
      if (result.status === "dev_link" && result.link) {
        setMessage("Dev magic link generated.");
        setDevLink(result.link);
      } else {
        setMessage("Magic link sent. Check your email.");
      }
    } catch (err: any) {
      setError(err.message || "Failed to send magic link.");
    } finally {
      setBusy(false);
    }
  };

  const handleBypassLogin = () => {
    const bypassEmail = (email.trim() || "jake@levelhealthplans.com").toLowerCase();
    setUser({
      email: bypassEmail,
      role: "admin",
      first_name: "Jake",
      last_name: "Page",
      organization: "Level Health",
    });
    navigate("/", { replace: true });
  };

  return (
    <section className="section" style={{ maxWidth: 460, margin: "48px auto" }}>
      <h2>Log In</h2>
      <form onSubmit={onSubmit} className="form-grid">
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
        <div className="inline-actions">
          <button className="button" type="submit" disabled={busy}>
            Send Magic Link
          </button>
          <button className="button ghost" type="button" onClick={handleBypassLogin}>
            Continue as Admin (Bypass)
          </button>
        </div>
      </form>
      {message && <div className="notice notice-success" style={{ marginTop: 12 }}>{message}</div>}
      {devLink && (
        <div className="notice notice-success" style={{ marginTop: 12 }}>
          <div style={{ marginBottom: 6 }}>Click to sign in now:</div>
          <a className="table-link" href={devLink}>
            {devLink}
          </a>
        </div>
      )}
      <div className="helper" style={{ marginTop: 10 }}>
        If you already have a link, open it directly or go to <Link className="table-link" to="/auth/verify">verify page</Link>.
      </div>
      {error && <div className="notice" style={{ marginTop: 12 }}>{error}</div>}
    </section>
  );
}
