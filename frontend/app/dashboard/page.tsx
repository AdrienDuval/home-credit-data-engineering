"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import {
  getClientsRisk,
  getPortfolioSummary,
  type PortfolioSummaryRow,
  type ClientsRiskFilters,
} from "@/lib/api";

const SEGMENT_COLORS: Record<string, string> = {
  HIGH: "#f85149",
  MEDIUM: "#d29922",
  LOW: "#3fb950",
};

const RISK_SEGMENTS = ["", "HIGH", "MEDIUM", "LOW"];

/** Column headers to show when client table is empty so the table structure is visible */
const CLIENT_TABLE_HEADERS = [
  "sk_id_curr",
  "income",
  "credit_exposure",
  "risk_segment",
  "default_flag",
  "bureau_debt_ratio",
  "payment_delay_score",
  "previous_rejection_rate",
];

export default function DashboardPage() {
  const router = useRouter();
  const [token, setToken] = useState<string | null>(null);
  const [portfolio, setPortfolio] = useState<PortfolioSummaryRow[]>([]);
  const [clients, setClients] = useState<Record<string, unknown>[]>([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [appliedFilters, setAppliedFilters] = useState<ClientsRiskFilters>({});
  const [filterForm, setFilterForm] = useState<{
    risk_segment: string;
    min_income: string;
    max_income: string;
    min_credit_exposure: string;
    max_credit_exposure: string;
  }>({ risk_segment: "", min_income: "", max_income: "", min_credit_exposure: "", max_credit_exposure: "" });
  const [clientIdSearch, setClientIdSearch] = useState("");

  useEffect(() => {
    const t = typeof window !== "undefined" ? sessionStorage.getItem("token") : null;
    if (!t) {
      router.replace("/");
      return;
    }
    setToken(t);
  }, [router]);

  useEffect(() => {
    if (!token) return;
    let cancelled = false;
    setLoading(true);
    setError("");
    Promise.all([
      getPortfolioSummary(token),
      getClientsRisk(token, page, pageSize, appliedFilters),
    ])
      .then(([portfolioData, clientsData]) => {
        if (cancelled) return;
        setPortfolio(Array.isArray(portfolioData) ? portfolioData : []);
        setClients(clientsData.data ?? []);
        setTotalCount(clientsData.total_count ?? 0);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [token, page, pageSize, JSON.stringify(appliedFilters)]);

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  const applyFilters = () => {
    const next: ClientsRiskFilters = {};
    if (filterForm.risk_segment) next.risk_segment = filterForm.risk_segment;
    const minI = filterForm.min_income.trim();
    if (minI) next.min_income = Number(minI);
    const maxI = filterForm.max_income.trim();
    if (maxI) next.max_income = Number(maxI);
    const minC = filterForm.min_credit_exposure.trim();
    if (minC) next.min_credit_exposure = Number(minC);
    const maxC = filterForm.max_credit_exposure.trim();
    if (maxC) next.max_credit_exposure = Number(maxC);
    setAppliedFilters(next);
    setPage(1);
  };

  const goToClientId = () => {
    const id = clientIdSearch.trim();
    if (!id) return;
    const num = parseInt(id, 10);
    if (!Number.isNaN(num)) router.push(`/dashboard/client/${num}`);
  };

  const getRowId = (row: Record<string, unknown>) => {
    const id = row.sk_id_curr ?? row.SK_ID_CURR;
    return id != null ? String(id) : null;
  };

  const getRiskSegment = (row: Record<string, unknown>) => {
    const s = row.risk_segment ?? row.RISK_SEGMENT;
    return (s != null ? String(s) : "") as keyof typeof SEGMENT_COLORS;
  };

  const ROW_BG: Record<string, string> = {
    HIGH: "rgba(248, 81, 73, 0.12)",
    MEDIUM: "rgba(210, 153, 34, 0.12)",
    LOW: "rgba(63, 185, 80, 0.12)",
  };

  if (!token) return null;

  return (
    <main style={{ padding: 24, maxWidth: 1200, margin: "0 auto" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.5rem" }}>Dashboard</h1>
        <button
          type="button"
          onClick={() => {
            sessionStorage.removeItem("token");
            router.replace("/");
          }}
          style={{
            padding: "8px 16px",
            background: "var(--surface)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            color: "var(--text)",
          }}
        >
          Sign out
        </button>
      </div>

      <p style={{ color: "var(--muted)", marginBottom: 24, fontSize: "0.9rem" }}>
        Portfolio risk overview and client-level risk profiles from the datamart.
      </p>

      {error && (
        <p style={{ color: "#f85149", marginBottom: 16 }}>{error}</p>
      )}

      {loading ? (
        <p style={{ color: "var(--muted)" }}>Loading…</p>
      ) : (
        <>
          <section style={{ marginBottom: 48 }}>
            <h2 style={{ marginBottom: 8, fontSize: "1.25rem" }}>Portfolio summary</h2>
            <p style={{ color: "var(--muted)", marginBottom: 16, fontSize: "0.875rem" }}>
              Aggregated metrics by risk segment (HIGH / MEDIUM / LOW). Use these charts to compare exposure and default rates across segments.
            </p>
            {portfolio.length === 0 && (
              <div style={{ background: "var(--surface)", padding: 20, borderRadius: 12, border: "1px solid var(--border)", marginBottom: 24 }}>
                <p style={{ color: "var(--muted)", marginBottom: 8 }}>No portfolio data yet. To load charts:</p>
                <ol style={{ color: "var(--muted)", fontSize: "0.875rem", marginLeft: 20 }}>
                  <li>Run Silver, then Gold (see <code style={{ background: "var(--bg)", padding: "2px 6px", borderRadius: 4 }}>run.md</code>).</li>
                  <li>Run Gold with <code style={{ background: "var(--bg)", padding: "2px 6px", borderRadius: 4 }}>--write-datamart</code> to fill <code>datamart.datamart_portfolio_summary</code> and <code>datamart.datamart_client_risk</code>.</li>
                  <li>Refresh this page.</li>
                </ol>
              </div>
            )}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 24 }}>
              <div style={{ background: "var(--surface)", padding: 20, borderRadius: 12, border: "1px solid var(--border)" }}>
                <h3 style={{ marginBottom: 4, fontSize: "1rem", color: "var(--muted)" }}>1. Risk distribution (client count)</h3>
                <p style={{ fontSize: "0.75rem", color: "var(--muted)", marginBottom: 12 }}>Number of clients in each risk segment.</p>
                {portfolio.length > 0 ? (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={portfolio} margin={{ top: 8, right: 8, left: 8, bottom: 8 }}>
                      <XAxis dataKey="risk_segment" tick={{ fill: "var(--muted)", fontSize: 12 }} />
                      <YAxis tick={{ fill: "var(--muted)", fontSize: 12 }} />
                      <Tooltip contentStyle={{ background: "var(--surface)", border: "1px solid var(--border)" }} />
                      <Bar dataKey="client_count" name="Clients" radius={[4, 4, 0, 0]}>
                        {portfolio.map((row, i) => (
                          <Cell key={i} fill={SEGMENT_COLORS[row.risk_segment] ?? "var(--accent)"} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div style={{ height: 220, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--muted)", fontSize: "0.875rem" }}>No data</div>
                )}
              </div>
              <div style={{ background: "var(--surface)", padding: 20, borderRadius: 12, border: "1px solid var(--border)" }}>
                <h3 style={{ marginBottom: 4, fontSize: "1rem", color: "var(--muted)" }}>2. Default rate per segment</h3>
                <p style={{ fontSize: "0.75rem", color: "var(--muted)", marginBottom: 12 }}>Average default rate (TARGET) for clients in each segment.</p>
                {portfolio.length > 0 ? (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={portfolio} margin={{ top: 8, right: 8, left: 8, bottom: 8 }}>
                      <XAxis dataKey="risk_segment" tick={{ fill: "var(--muted)", fontSize: 12 }} />
                      <YAxis tick={{ fill: "var(--muted)", fontSize: 12 }} tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
                      <Tooltip contentStyle={{ background: "var(--surface)", border: "1px solid var(--border)" }} formatter={(v: number) => [(v * 100).toFixed(2) + "%", "Default rate"]} />
                      <Bar dataKey="avg_default_rate" name="Default rate" radius={[4, 4, 0, 0]}>
                        {portfolio.map((row, i) => (
                          <Cell key={i} fill={SEGMENT_COLORS[row.risk_segment] ?? "var(--accent)"} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div style={{ height: 220, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--muted)", fontSize: "0.875rem" }}>No data</div>
                )}
              </div>
              <div style={{ background: "var(--surface)", padding: 20, borderRadius: 12, border: "1px solid var(--border)" }}>
                <h3 style={{ marginBottom: 4, fontSize: "1rem", color: "var(--muted)" }}>3. Credit exposure per segment</h3>
                <p style={{ fontSize: "0.75rem", color: "var(--muted)", marginBottom: 12 }}>Total credit amount (AMT_CREDIT) per risk segment.</p>
                {portfolio.length > 0 ? (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={portfolio} margin={{ top: 8, right: 8, left: 8, bottom: 8 }}>
                      <XAxis dataKey="risk_segment" tick={{ fill: "var(--muted)", fontSize: 12 }} />
                      <YAxis tick={{ fill: "var(--muted)", fontSize: 12 }} tickFormatter={(v) => `${(v / 1e6).toFixed(0)}M`} />
                      <Tooltip contentStyle={{ background: "var(--surface)", border: "1px solid var(--border)" }} formatter={(v: number) => [v?.toLocaleString?.() ?? v, "Exposure"]} />
                      <Bar dataKey="total_exposure" name="Exposure" radius={[4, 4, 0, 0]}>
                        {portfolio.map((row, i) => (
                          <Cell key={i} fill={SEGMENT_COLORS[row.risk_segment] ?? "var(--accent)"} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <div style={{ height: 220, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--muted)", fontSize: "0.875rem" }}>No data</div>
                )}
              </div>
            </div>
          </section>

          <section style={{ marginTop: 40 }}>
            <h2 style={{ marginBottom: 8, fontSize: "1.25rem" }}>Client risk (paginated)</h2>
            <p style={{ color: "var(--muted)", marginBottom: 16, fontSize: "0.875rem" }}>
              One row per client. Click a row to open the client’s detailed risk profile (history, bureau credits, previous applications). Use the lookup and filters below to find clients.
            </p>

            {/* Lookup + filters panel — above the table only */}
            <div style={{ marginBottom: 20, background: "var(--surface)", padding: 20, borderRadius: 12, border: "1px solid var(--border)" }}>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 24 }}>
                <div>
                  <h3 style={{ marginBottom: 10, fontSize: "0.95rem", color: "var(--muted)" }}>Look up client by ID</h3>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                    <input
                      type="number"
                      placeholder="e.g. 100001"
                      value={clientIdSearch}
                      onChange={(e) => setClientIdSearch(e.target.value)}
                      onKeyDown={(e) => e.key === "Enter" && goToClientId()}
                      style={{
                        padding: "8px 12px",
                        background: "var(--bg)",
                        border: "1px solid var(--border)",
                        borderRadius: 8,
                        color: "var(--text)",
                        width: 160,
                      }}
                    />
                    <button
                      type="button"
                      onClick={goToClientId}
                      style={{
                        padding: "8px 16px",
                        background: "var(--accent)",
                        color: "#fff",
                        border: "none",
                        borderRadius: 8,
                        fontWeight: 600,
                      }}
                    >
                      View details
                    </button>
                  </div>
                </div>
                <div>
                  <h3 style={{ marginBottom: 10, fontSize: "0.95rem", color: "var(--muted)" }}>Filter table</h3>
                  <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "flex-end" }}>
                    <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                      <span style={{ fontSize: "0.75rem", color: "var(--muted)" }}>Risk</span>
                      <select
                        value={filterForm.risk_segment}
                        onChange={(e) => setFilterForm((f) => ({ ...f, risk_segment: e.target.value }))}
                        style={{ padding: "8px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 8, color: "var(--text)", minWidth: 100 }}
                      >
                        {RISK_SEGMENTS.map((s) => (
                          <option key={s} value={s}>{s || "All"}</option>
                        ))}
                      </select>
                    </label>
                    <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                      <span style={{ fontSize: "0.75rem", color: "var(--muted)" }}>Min income</span>
                      <input type="number" placeholder="—" value={filterForm.min_income} onChange={(e) => setFilterForm((f) => ({ ...f, min_income: e.target.value }))} style={{ padding: "8px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 8, color: "var(--text)", width: 100 }} />
                    </label>
                    <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                      <span style={{ fontSize: "0.75rem", color: "var(--muted)" }}>Max income</span>
                      <input type="number" placeholder="—" value={filterForm.max_income} onChange={(e) => setFilterForm((f) => ({ ...f, max_income: e.target.value }))} style={{ padding: "8px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 8, color: "var(--text)", width: 100 }} />
                    </label>
                    <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                      <span style={{ fontSize: "0.75rem", color: "var(--muted)" }}>Min credit</span>
                      <input type="number" placeholder="—" value={filterForm.min_credit_exposure} onChange={(e) => setFilterForm((f) => ({ ...f, min_credit_exposure: e.target.value }))} style={{ padding: "8px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 8, color: "var(--text)", width: 100 }} />
                    </label>
                    <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                      <span style={{ fontSize: "0.75rem", color: "var(--muted)" }}>Max credit</span>
                      <input type="number" placeholder="—" value={filterForm.max_credit_exposure} onChange={(e) => setFilterForm((f) => ({ ...f, max_credit_exposure: e.target.value }))} style={{ padding: "8px 10px", background: "var(--bg)", border: "1px solid var(--border)", borderRadius: 8, color: "var(--text)", width: 100 }} />
                    </label>
                    <button type="button" onClick={applyFilters} style={{ padding: "8px 14px", background: "var(--accent)", color: "#fff", border: "none", borderRadius: 8, fontWeight: 600 }}>Apply</button>
                  </div>
                </div>
              </div>
            </div>

            {totalCount === 0 && !loading && (
              <p style={{ color: "var(--muted)", marginBottom: 12, fontSize: "0.875rem" }}>
                No clients in the datamart. Run the pipeline (Silver → Gold) then load the datamart with <code style={{ background: "var(--bg)", padding: "2px 6px", borderRadius: 4 }}>--write-datamart</code>. See run.md section 5.
              </p>
            )}

            {/* Loading state for table */}
            {loading ? (
              <div style={{ background: "var(--surface)", borderRadius: 12, border: "1px solid var(--border)", padding: 24, textAlign: "center" }}>
                <div style={{ display: "inline-block", width: 32, height: 32, border: "3px solid var(--border)", borderTopColor: "var(--accent)", borderRadius: "50%", animation: "spin 0.8s linear infinite" }} />
                <p style={{ marginTop: 12, color: "var(--muted)", fontSize: "0.875rem" }}>Loading client risk table…</p>
              </div>
            ) : (
              <>
                <div style={{ overflowX: "auto", background: "var(--surface)", borderRadius: 12, border: "1px solid var(--border)", boxShadow: "0 1px 3px rgba(0,0,0,0.2)" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.875rem", minWidth: 800 }}>
                    <thead>
                      <tr style={{ borderBottom: "2px solid var(--border)", background: "var(--bg)" }}>
                        {(clients[0] ? Object.keys(clients[0]) : CLIENT_TABLE_HEADERS).map((k) => (
                          <th key={k} style={{ textAlign: "left", padding: "14px 16px", color: "var(--muted)", fontWeight: 600, whiteSpace: "nowrap" }}>
                            {k.replace(/_/g, " ")}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {clients.length === 0 ? (
                        <tr>
                          <td colSpan={CLIENT_TABLE_HEADERS.length} style={{ padding: "32px 16px", color: "var(--muted)", fontSize: "0.875rem", textAlign: "center" }}>
                            No rows — load datamart to see clients. Click a row once data is loaded to open the client detail (history, bureau, previous applications).
                          </td>
                        </tr>
                      ) : (
                        clients.map((row, i) => {
                          const id = getRowId(row);
                          const keys = Object.keys(row);
                          const riskSeg = getRiskSegment(row);
                          const rowBg = ROW_BG[riskSeg] || "transparent";
                          return (
                            <tr
                              key={i}
                              style={{
                                borderBottom: "1px solid var(--border)",
                                cursor: id ? "pointer" : undefined,
                                background: rowBg,
                              }}
                              onClick={() => id && router.push(`/dashboard/client/${id}`)}
                              onMouseEnter={(e) => { if (id) e.currentTarget.style.filter = "brightness(1.08)"; }}
                              onMouseLeave={(e) => { e.currentTarget.style.filter = "none"; }}
                              role={id ? "link" : undefined}
                            >
                              {keys.map((k) => (
                                <td key={k} style={{ padding: "12px 16px", whiteSpace: "nowrap" }}>
                                  {row[k] != null ? String(row[k]) : "—"}
                                </td>
                              ))}
                            </tr>
                          );
                        })
                      )}
                    </tbody>
                  </table>
                </div>
                <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 12, marginTop: 16 }}>
                  <span style={{ color: "var(--muted)", fontSize: "0.875rem" }}>
                    Page {page} of {totalPages} ({totalCount.toLocaleString()} total)
                  </span>
                  <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: "0.875rem", color: "var(--muted)" }}>
                    Rows per page
                    <select
                      value={pageSize}
                      onChange={(e) => { setPageSize(Number(e.target.value)); setPage(1); }}
                      style={{ padding: "4px 8px", background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)" }}
                    >
                      {[10, 20, 50, 100].map((n) => (
                        <option key={n} value={n}>{n}</option>
                      ))}
                    </select>
                  </label>
                  <div style={{ display: "flex", gap: 6 }}>
                    <button type="button" disabled={page <= 1} onClick={() => setPage(1)} style={{ padding: "6px 10px", background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", fontWeight: 600 }}>First</button>
                    <button type="button" disabled={page <= 1} onClick={() => setPage((p) => p - 1)} style={{ padding: "6px 12px", background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)" }}>Previous</button>
                    <button type="button" disabled={page >= totalPages} onClick={() => setPage((p) => p + 1)} style={{ padding: "6px 12px", background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)" }}>Next</button>
                    <button type="button" disabled={page >= totalPages} onClick={() => setPage(totalPages)} style={{ padding: "6px 10px", background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 6, color: "var(--text)", fontWeight: 600 }}>Last</button>
                  </div>
                </div>
              </>
            )}
          </section>
        </>
      )}
    </main>
  );
}
