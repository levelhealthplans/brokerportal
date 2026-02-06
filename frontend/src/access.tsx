import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { AuthUser, getAuthMe, logoutAuth, requestMagicLink as requestMagicLinkApi } from "./api";

export type AccessRole = "broker" | "sponsor" | "admin";

type AccessContextValue = {
  role: AccessRole;
  email: string;
  domain: string;
  user: AuthUser | null;
  loading: boolean;
  isAuthenticated: boolean;
  requestMagicLink: (email: string) => Promise<{ status: string; link?: string }>;
  refreshSession: () => Promise<void>;
  logout: () => Promise<void>;
  setUser: (user: AuthUser | null) => void;
};

const AccessContext = createContext<AccessContextValue | null>(null);
const LOCAL_AUTH_USER_KEY = "lh_local_auth_user";

const extractDomain = (email: string) => {
  const normalized = email.trim().toLowerCase();
  if (!normalized.includes("@")) return "";
  return normalized.split("@")[1] || "";
};

const loadLocalAuthUser = (): AuthUser | null => {
  try {
    const raw = window.localStorage.getItem(LOCAL_AUTH_USER_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as AuthUser;
    if (!parsed?.email || !parsed?.role) return null;
    return parsed;
  } catch {
    return null;
  }
};

export function AccessProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(() => loadLocalAuthUser());
  const [loading, setLoading] = useState(true);

  const persistUser = (nextUser: AuthUser | null) => {
    setUser(nextUser);
    try {
      if (nextUser) {
        window.localStorage.setItem(LOCAL_AUTH_USER_KEY, JSON.stringify(nextUser));
      } else {
        window.localStorage.removeItem(LOCAL_AUTH_USER_KEY);
      }
    } catch {
      // best-effort local persistence only
    }
  };

  const refreshSession = async () => {
    try {
      const me = await getAuthMe();
      persistUser(me);
    } catch {
      persistUser(loadLocalAuthUser());
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refreshSession();
  }, []);

  const requestMagicLink = async (email: string) => {
    return requestMagicLinkApi(email.trim().toLowerCase());
  };

  const logout = async () => {
    try {
      await logoutAuth();
    } catch {
      // local bypass sessions should still log out cleanly
    }
    persistUser(null);
  };

  const value = useMemo(
    () => ({
      role: (user?.role || "broker") as AccessRole,
      email: user?.email || "",
      domain: extractDomain(user?.email || ""),
      user,
      loading,
      isAuthenticated: Boolean(user),
      requestMagicLink,
      refreshSession,
      logout,
      setUser: persistUser,
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
