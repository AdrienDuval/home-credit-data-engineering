const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export type TokenResponse = { access_token: string; token_type: string };

export async function login(username: string, password: string): Promise<TokenResponse> {
  const res = await fetch(`${API_URL}/token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error((err as { detail?: string }).detail ?? "Login failed");
  }
  return res.json();
}

export type ClientsRiskResponse = {
  data: Record<string, unknown>[];
  page: number;
  page_size: number;
  total_count: number;
};

export type ClientsRiskFilters = {
  risk_segment?: string;
  client_id?: number;
  min_income?: number;
  max_income?: number;
  min_credit_exposure?: number;
  max_credit_exposure?: number;
};

function buildQuery(params: Record<string, string | number | undefined>): string {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== "" && v !== null) search.set(k, String(v));
  });
  const q = search.toString();
  return q ? `&${q}` : "";
}

export async function getClientsRisk(
  token: string,
  page: number = 1,
  pageSize: number = 50,
  filters?: ClientsRiskFilters
): Promise<ClientsRiskResponse> {
  const extra = filters
    ? buildQuery({
        risk_segment: filters.risk_segment,
        client_id: filters.client_id,
        min_income: filters.min_income,
        max_income: filters.max_income,
        min_credit_exposure: filters.min_credit_exposure,
        max_credit_exposure: filters.max_credit_exposure,
      })
    : "";
  const res = await fetch(
    `${API_URL}/clients/risk?page=${page}&page_size=${pageSize}${extra}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  if (!res.ok) throw new Error("Failed to fetch clients risk");
  return res.json();
}

export type ClientRiskDetail = Record<string, unknown>;

export async function getClientById(
  token: string,
  skIdCurr: number
): Promise<ClientRiskDetail | null> {
  const res = await fetch(`${API_URL}/clients/risk/${skIdCurr}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error("Failed to fetch client");
  return res.json();
}

export async function getClientBureau(
  token: string,
  skIdCurr: number
): Promise<Record<string, unknown>[]> {
  const res = await fetch(`${API_URL}/clients/risk/${skIdCurr}/bureau`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) return [];
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

export async function getClientPreviousApplications(
  token: string,
  skIdCurr: number
): Promise<Record<string, unknown>[]> {
  const res = await fetch(`${API_URL}/clients/risk/${skIdCurr}/previous-applications`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) return [];
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

export type PortfolioSummaryRow = {
  risk_segment: string;
  client_count: number;
  total_exposure: number;
  avg_default_rate: number;
  avg_income?: number;
};

export async function getPortfolioSummary(token: string): Promise<PortfolioSummaryRow[]> {
  const res = await fetch(`${API_URL}/portfolio/summary`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error("Failed to fetch portfolio summary");
  return res.json();
}
