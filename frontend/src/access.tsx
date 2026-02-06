import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { AuthUser, getAuthMe, loginWithPassword, logoutAuth } from "./api";

export type AccessRole = "broker" | "sponsor" | "admin";

type AccessContextValue = {
  role: AccessRole;
  email: string;
  domain: string;
  user: AuthUser | null;
  loading: boolean;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<void>;
  refreshSession: () => Promise<void>;
  logout: () => Promise<void>;
  setUser: (user: AuthUser | null) => void;
};

const AccessContext = createContext<AccessContextValue | null>(null);
const extractDomain = (email: string) => {
  const normalized = email.trim().toLowerCase();
  if (!normalized.includes("@")) return "";
  return normalized.split("@")[1] || "";
};

export function AccessProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [loading, setLoading] = useState(true);

  const refreshSession = async () => {
    try {
      const me = await getAuthMe();
      setUser(me);
    } catch {
      setUser(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refreshSession();
  }, []);

  const login = async (email: string, password: string) => {
    const authed = await loginWithPassword(email.trim().toLowerCase(), password);
    setUser(authed);
  };

  const logout = async () => {
    try {
      await logoutAuth();
    } catch {}
    setUser(null);
  };

  const value = useMemo(
    () => ({
      role: (user?.role || "broker") as AccessRole,
      email: user?.email || "",
      domain: extractDomain(user?.email || ""),
      user,
      loading,
      isAuthenticated: Boolean(user),
      login,
      refreshSession,
      logout,
      setUser,
    }),
    [user, loading]
  );

  return <AccessContext.Provider value={value}>{children}</AccessContext.Provider>;
}

export function useAccess() {
  const ctx = useContext(AccessContext);
  if (!ctx) throw new Error("useAccess must be used within AccessProvider");
  return ctx;
}
