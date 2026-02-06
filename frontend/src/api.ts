export type Quote = {
  id: string;
  company: string;
  employer_street?: string | null;
  employer_city?: string | null;
  state: string;
  employer_zip?: string | null;
  employer_domain?: string | null;
  quote_deadline?: string | null;
  employer_sic?: string | null;
  effective_date: string;
  current_enrolled: number;
  current_eligible: number;
  current_insurance_type: string;
  employees_eligible: number;
  expected_enrollees: number;
  broker_fee_pepm: number;
  include_specialty: boolean;
  notes: string;
  high_cost_info: string;
  broker_first_name?: string | null;
  broker_last_name?: string | null;
  broker_email: string;
  broker_phone: string;
  agent_of_record?: boolean | null;
  broker_org?: string | null;
  sponsor_domain?: string | null;
  assigned_user_id?: string | null;
  manual_network?: string | null;
  proposal_url?: string | null;
  status: string;
  version: number;
  needs_action: boolean;
  created_at: string;
  updated_at: string;
  latest_assignment?: {
    recommendation: string;
    confidence: number;
  } | null;
};

export type Upload = {
  id: string;
  quote_id: string;
  type: string;
  filename: string;
  path: string;
  created_at: string;
};

export type StandardizationRun = {
  id: string;
  quote_id: string;
  issues_json: {
    row: number;
    field: string;
    issue: string;
    value?: string;
    mapped_value?: string;
  }[];
  issue_count: number;
  status: string;
  detected_headers: string[];
  sample_data: Record<string, string[]>;
  sample_rows: Record<string, any>[];
  total_rows: number;
  issue_rows: number;
  standardized_filename?: string | null;
  standardized_path?: string | null;
  created_at: string;
};

export type AssignmentRun = {
  id: string;
  quote_id: string;
  result_json: {
    group_summary?: {
      primary_network: string;
      coverage_percentage: number;
      fallback_used: boolean;
      review_required: boolean;
      total_members: number;
      invalid_rows: { row: number; zip: string }[];
    };
    coverage_by_network?: Record<string, number>;
    member_assignments?: {
      row: number;
      zip: string;
      assigned_network: string;
      matched: boolean;
    }[];
    ranked_contracts?: { name: string; score: number; fit?: string }[];
    member_fit?: {
      in_network?: number;
      out_of_network?: number;
      no_match?: number;
    };
  };
  recommendation: string;
  confidence: number;
  rationale: string;
  created_at: string;
};

export type Proposal = {
  id: string;
  quote_id: string;
  filename: string;
  path: string;
  status: string;
  created_at: string;
};

export type QuoteDetail = {
  quote: Quote;
  uploads: Upload[];
  standardizations: StandardizationRun[];
  assignments: AssignmentRun[];
  proposals: Proposal[];
};

export type Installation = {
  id: string;
  quote_id: string;
  company: string;
  broker_org?: string | null;
  sponsor_domain?: string | null;
  effective_date: string;
  status: string;
  created_at: string;
  updated_at: string;
};

export type Task = {
  id: string;
  installation_id: string;
  title: string;
  owner: string;
  assigned_user_id?: string | null;
  installation_company?: string | null;
  due_date: string | null;
  state: string;
  task_url?: string | null;
};

export type InstallationDocument = {
  id: string;
  installation_id: string;
  filename: string;
  path: string;
  created_at: string;
};

export type InstallationDetail = {
  installation: Installation;
  tasks: Task[];
  documents: InstallationDocument[];
};

export type Organization = {
  id: string;
  name: string;
  type: string;
  domain: string;
  created_at: string;
};

export type OrganizationCreate = {
  name: string;
  type: "broker" | "sponsor";
  domain: string;
};

export type User = {
  id: string;
  first_name: string;
  last_name: string;
  email: string;
  phone: string;
  job_title: string;
  organization: string;
  role: string;
  created_at: string;
  updated_at: string;
};

export type UserCreate = {
  first_name: string;
  last_name: string;
  email: string;
  phone: string;
  job_title: string;
  organization: string;
  role: string;
};

export type AccessParams = {
  role?: "broker" | "sponsor" | "admin";
  email?: string;
};

export type AuthUser = {
  email: string;
  role: "broker" | "sponsor" | "admin";
  first_name: string;
  last_name: string;
  organization: string;
};

export type NetworkMapping = {
  zip: string;
  network: string;
};

export type NetworkSettings = {
  default_network: string;
  coverage_threshold: number;
};

const API_BASE = (import.meta.env.VITE_API_BASE || "/api").replace(/\/+$/, "");
const REQUEST_TIMEOUT_MS = 15000;

function parseErrorMessage(raw: string): string {
  const text = (raw || "").trim();
  if (!text) return "";
  try {
    const parsed = JSON.parse(text) as { detail?: unknown; message?: unknown };
    if (typeof parsed.detail === "string" && parsed.detail.trim()) {
      return parsed.detail.trim();
    }
    if (typeof parsed.message === "string" && parsed.message.trim()) {
      return parsed.message.trim();
    }
  } catch {
    return text;
  }
  return text;
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(`${API_BASE}${path}`, {
      credentials: "include",
      ...options,
      signal: controller.signal,
    });
  } catch (err: any) {
    if (err?.name === "AbortError") {
      throw new Error("Request timed out. Check that backend is running on port 8000.");
    }
    throw new Error(err?.message || "Network error");
  } finally {
    window.clearTimeout(timeout);
  }
  if (!res.ok) {
    const message = parseErrorMessage(await res.text());
    throw new Error(message || "Request failed");
  }
  try {
    return (await res.json()) as T;
  } catch {
    throw new Error("Unexpected server response");
  }
}

function withAccessParams(path: string, access?: AccessParams) {
  if (!access?.role && !access?.email) return path;
  const params = new URLSearchParams();
  if (access?.role) params.set("role", access.role);
  if (access?.email) params.set("email", access.email);
  const qs = params.toString();
  return qs ? `${path}?${qs}` : path;
}

export function getQuotes(access?: AccessParams) {
  return request<Quote[]>(withAccessParams("/quotes", access));
}

export function getQuote(id: string) {
  return request<QuoteDetail>(`/quotes/${id}`);
}

export function createQuote(
  payload: Omit<
    Quote,
    "id" | "created_at" | "updated_at" | "version" | "needs_action"
  > & { status?: string }
) {
  return request<Quote>("/quotes", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function updateQuote(id: string, payload: Partial<Quote>) {
  return request<Quote>(`/quotes/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function uploadFile(id: string, file: File, uploadType: string) {
  const form = new FormData();
  form.append("type", uploadType);
  form.append("file", file);
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(`${API_BASE}/quotes/${id}/uploads`, {
      method: "POST",
      body: form,
      credentials: "include",
      signal: controller.signal,
    });
  } catch (err: any) {
    if (err?.name === "AbortError") {
      throw new Error("Upload timed out. Check backend connectivity and file size.");
    }
    throw new Error(err?.message || "Upload failed");
  } finally {
    window.clearTimeout(timeout);
  }
  if (!res.ok) {
    const message = await res.text();
    throw new Error(message || "Upload failed");
  }
  return res.json() as Promise<Upload>;
}

export function listUploads(id: string) {
  return request<Upload[]>(`/quotes/${id}/uploads`);
}

export function deleteUpload(quoteId: string, uploadId: string) {
  return request<{ status: string }>(`/quotes/${quoteId}/uploads/${uploadId}`, {
    method: "DELETE",
  });
}

export function standardizeQuote(
  id: string,
  mappings?: {
    gender_map?: Record<string, string>;
    relationship_map?: Record<string, string>;
    tier_map?: Record<string, string>;
    header_map?: Record<string, string>;
  }
) {
  return request<StandardizationRun>(`/quotes/${id}/standardize`, {
    method: "POST",
    headers: mappings ? { "Content-Type": "application/json" } : undefined,
    body: mappings ? JSON.stringify(mappings) : undefined,
  });
}

export function resolveStandardization(id: string, issues_json: StandardizationRun["issues_json"]) {
  return request<StandardizationRun>(`/quotes/${id}/standardize/resolve`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ issues_json }),
  });
}

export function assignNetwork(id: string) {
  return request<AssignmentRun>(`/quotes/${id}/assign-network`, {
    method: "POST",
  });
}

export function getNetworkOptions() {
  return request<string[]>("/network-options");
}

export function requestMagicLink(email: string) {
  return request<{ status: string; link?: string }>("/auth/request-link", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email }),
  });
}

export function loginWithPassword(email: string, password: string) {
  return request<AuthUser>("/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
}

export function verifyMagicLink(token: string) {
  return request<AuthUser>(`/auth/verify?token=${encodeURIComponent(token)}`);
}

export function getAuthMe() {
  return request<AuthUser>("/auth/me");
}

export function logoutAuth() {
  return request<{ status: string }>("/auth/logout", { method: "POST" });
}

export function createNetworkOption(name: string) {
  return request<string[]>("/network-options", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
}

export function updateNetworkOption(currentName: string, name: string) {
  return request<string[]>(`/network-options/${encodeURIComponent(currentName)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
}

export function deleteNetworkOption(name: string) {
  return request<string[]>(`/network-options/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
}

export function getNetworkMappings() {
  return request<NetworkMapping[]>("/network-mappings");
}

export function createNetworkMapping(payload: NetworkMapping) {
  return request<NetworkMapping[]>("/network-mappings", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function updateNetworkMapping(zipCode: string, payload: NetworkMapping) {
  return request<NetworkMapping[]>(`/network-mappings/${encodeURIComponent(zipCode)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function deleteNetworkMapping(zipCode: string) {
  return request<NetworkMapping[]>(`/network-mappings/${encodeURIComponent(zipCode)}`, {
    method: "DELETE",
  });
}

export function getNetworkSettings() {
  return request<NetworkSettings>("/network-settings");
}

export function updateNetworkSettings(payload: NetworkSettings) {
  return request<NetworkSettings>("/network-settings", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function createProposal(id: string) {
  return request<Proposal>(`/quotes/${id}/proposal`, { method: "POST" });
}

export function markProposalSigned(id: string) {
  return request<{ status: string }>(`/quotes/${id}/mark-signed`, {
    method: "POST",
  });
}

export function convertToInstallation(id: string, access?: AccessParams) {
  return request<Installation>(withAccessParams(`/quotes/${id}/convert-to-installation`, access), {
    method: "POST",
  });
}

export function getInstallations(access?: AccessParams) {
  return request<Installation[]>(withAccessParams("/installations", access));
}

export function getOrganizations() {
  return request<Organization[]>("/organizations");
}

export function getOrganization(id: string) {
  return request<Organization>(`/organizations/${id}`);
}

export function getOrganizationUsers(id: string) {
  return request<User[]>(`/organizations/${id}/users`);
}

export function createOrganization(payload: OrganizationCreate) {
  return request<Organization>("/organizations", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function updateOrganization(id: string, payload: Partial<OrganizationCreate>) {
  return request<Organization>(`/organizations/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function deleteOrganization(id: string) {
  return request<{ status: string }>(`/organizations/${id}`, {
    method: "DELETE",
  });
}

export function assignOrganizationQuotes(id: string, quoteIds: string[]) {
  return request<{ status: string }>(`/organizations/${id}/assign-quotes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ quote_ids: quoteIds }),
  });
}

export function getInstallation(id: string, access?: AccessParams) {
  return request<InstallationDetail>(withAccessParams(`/installations/${id}`, access));
}

export function getTasks(access?: AccessParams) {
  return request<Task[]>(withAccessParams("/tasks", access));
}

export function getUsers() {
  return request<User[]>("/users");
}

export function createUser(payload: UserCreate) {
  return request<User>("/users", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function updateUser(id: string, payload: Partial<UserCreate>) {
  return request<User>(`/users/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function deleteUser(id: string) {
  return request<{ status: string }>(`/users/${id}`, { method: "DELETE" });
}

export function assignUserQuotes(id: string, quoteIds: string[]) {
  return request<{ status: string }>(`/users/${id}/assign-quotes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ quote_ids: quoteIds }),
  });
}

export function assignUserTasks(id: string, taskIds: string[]) {
  return request<{ status: string }>(`/users/${id}/assign-tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ task_ids: taskIds }),
  });
}

export function advanceTask(installationId: string, taskId: string) {
  return request<Task>(
    `/installations/${installationId}/tasks/${taskId}/advance`,
    { method: "POST" }
  );
}

export function updateTask(
  installationId: string,
  taskId: string,
  payload: Partial<Pick<Task, "state" | "task_url">>,
  access?: AccessParams
) {
  return request<Task>(
    withAccessParams(`/installations/${installationId}/tasks/${taskId}`, access),
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }
  );
}

export async function uploadInstallationDocument(installationId: string, file: File) {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(`${API_BASE}/installations/${installationId}/documents`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) {
    const message = await res.text();
    throw new Error(message || "Upload failed");
  }
  return res.json() as Promise<InstallationDocument>;
}

export function deleteInstallationDocument(installationId: string, documentId: string) {
  return request<{ status: string }>(
    `/installations/${installationId}/documents/${documentId}`,
    { method: "DELETE" }
  );
}
