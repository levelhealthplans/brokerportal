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
            Sign In
          </button>
        </div>
      </form>
      {error && <div className="notice" style={{ marginTop: 12 }}>{error}</div>}
    </section>
  );
}
