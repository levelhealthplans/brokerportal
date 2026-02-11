import { useState } from "react";
import { useAccess } from "../access";
import { useNavigate } from "react-router-dom";

export default function Login() {
  const { login } = useAccess();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

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
              </div>
            </form>
            {error && <div className="notice login-notice">{error}</div>}
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
