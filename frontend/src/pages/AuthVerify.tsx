import { useEffect, useState } from "react";
import { Navigate, useSearchParams } from "react-router-dom";
import { verifyMagicLink } from "../api";
import { useAccess } from "../access";

export default function AuthVerify() {
  const [params] = useSearchParams();
  const token = params.get("token") || "";
  const { setUser } = useAccess();
  const [status, setStatus] = useState<"loading" | "ok" | "error">("loading");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!token) {
      setStatus("error");
      setError("Missing token.");
      return;
    }
    verifyMagicLink(token)
      .then((user) => {
        setUser(user);
        setStatus("ok");
      })
      .catch((err) => {
        setError(err.message || "Magic link is invalid or expired.");
        setStatus("error");
      });
  }, [token, setUser]);

  if (status === "ok") {
    return <Navigate to="/" replace />;
  }

  return (
    <section className="section" style={{ maxWidth: 460, margin: "48px auto" }}>
      <h2>Magic Link Verification</h2>
      {status === "loading" && <div className="helper">Signing you in...</div>}
      {status === "error" && <div className="notice">{error}</div>}
    </section>
  );
}
