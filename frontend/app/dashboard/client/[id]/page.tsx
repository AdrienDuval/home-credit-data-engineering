"use client";

import { useEffect, useState, useMemo } from "react";
import { useRouter, useParams } from "next/navigation";
import Link from "next/link";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
  Legend,
} from "recharts";
import {
  getClientById,
  getClientBureau,
  getClientPreviousApplications,
} from "@/lib/api";

const SEGMENT_COLORS: Record<string, string> = {
  HIGH: "#f85149",
  MEDIUM: "#d29922",
  LOW: "#3fb950",
};

const FIELD_META: Record<string, { label: string; description: string }> = {
  sk_id_curr: { label: "Client ID", description: "Unique identifier (SK_ID_CURR)." },
  SK_ID_CURR: { label: "Client ID", description: "Unique identifier (SK_ID_CURR)." },
  income: { label: "Income", description: "Total declared income (AMT_INCOME_TOTAL)." },
  credit_exposure: { label: "Credit exposure", description: "Total credit for this application (AMT_CREDIT)." },
  default_flag: { label: "Default flag", description: "1 = payment difficulties; 0 or null otherwise." },
  bureau_total_debt: { label: "Bureau total debt", description: "Sum of debt from credit bureau." },
  bureau_debt_ratio: { label: "Bureau debt ratio", description: "Bureau debt / credit exposure." },
  payment_avg_delay_days: { label: "Avg payment delay (days)", description: "Positive = late, negative = early." },
  payment_late_count: { label: "Late payment count", description: "Installments paid late in previous loans." },
  payment_delay_score: { label: "Payment delay score", description: "Higher = riskier." },
  previous_rejection_rate: { label: "Previous rejection rate", description: "Share of previous applications rejected (0–1)." },
  risk_segment: { label: "Risk segment", description: "Rule-based: HIGH / MEDIUM / LOW." },
};

function getLabel(key: string): string {
  return FIELD_META[key]?.label ?? key.replace(/_/g, " ");
}

function formatValue(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "number") return Number.isInteger(v) ? String(v) : v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return String(v);
}

function formatCurrency(v: unknown): string {
  if (v == null) return "—";
  const n = typeof v === "number" ? v : Number(v);
  if (Number.isNaN(n)) return String(v);
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

// Friendly labels and preferred column order for previous_application
const PREV_APP_LABELS: Record<string, string> = {
  sk_id_prev: "App ID",
  sk_id_curr: "Client ID",
  name_contract_type: "Contract type",
  name_contract_status: "Status",
  amt_credit: "Credit amount",
  amt_application: "Application amount",
  amt_annuity: "Annuity",
  amt_goods_price: "Goods price",
  days_decision: "Days to decision",
  name_payment_type: "Payment type",
  code_reject_reason: "Reject reason",
  weekdays_appr_process_start: "Weekday",
  hour_appr_process_start: "Hour",
};

const PREV_APP_COL_ORDER = [
  "sk_id_prev",
  "name_contract_type",
  "name_contract_status",
  "amt_credit",
  "amt_application",
  "days_decision",
  "amt_annuity",
  "code_reject_reason",
];

// Friendly labels for bureau columns
const BUREAU_LABELS: Record<string, string> = {
  sk_id_bureau: "Bureau ID",
  sk_id_curr: "Client ID",
  credit_active: "Credit active",
  credit_currency: "Currency",
  days_credit: "Days credit",
  credit_day_overdue: "Days overdue",
  amt_credit_max_overdue: "Max overdue amount",
  cnt_credit_prolong: "Prolongations",
  amt_credit_sum: "Credit sum",
  amt_credit_sum_debt: "Debt",
  amt_credit_sum_overdue: "Overdue",
  amt_credit_sum_limit: "Limit",
};

function getPrevAppLabel(key: string): string {
  return PREV_APP_LABELS[key] ?? key.replace(/_/g, " ");
}

function getBureauLabel(key: string): string {
  return BUREAU_LABELS[key] ?? key.replace(/_/g, " ");
}

/** Build a short "how it went" summary for one previous application row */
function previousAppSummary(row: Record<string, unknown>): string {
  const status = (row.name_contract_status ?? row.NAME_CONTRACT_STATUS ?? "") as string;
  const amt = row.amt_credit ?? row.AMT_CREDIT;
  const days = row.days_decision ?? row.DAYS_DECISION;
  const parts: string[] = [];
  if (status) parts.push(String(status));
  if (amt != null) parts.push(`Amount: ${formatCurrency(amt)}`);
  if (days != null) parts.push(`Decision in ${days} days`);
  return parts.length ? parts.join(" · ") : "—";
}

export default function ClientDetailPage() {
  const router = useRouter();
  const params = useParams();
  const id = params?.id ? String(params.id) : null;
  const [client, setClient] = useState<Record<string, unknown> | null>(null);
  const [bureau, setBureau] = useState<Record<string, unknown>[]>([]);
  const [previousApps, setPreviousApps] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const t = typeof window !== "undefined" ? sessionStorage.getItem("token") : null;
    if (!t) {
      router.replace("/");
      return;
    }
    if (!id) return;
    const numId = parseInt(id, 10);
    if (Number.isNaN(numId)) {
      setError("Invalid client ID");
      setLoading(false);
      return;
    }
    let cancelled = false;
    Promise.all([
      getClientById(t, numId),
      getClientBureau(t, numId),
      getClientPreviousApplications(t, numId),
    ])
      .then(([clientData, bureauData, prevData]) => {
        if (cancelled) return;
        setClient(clientData ?? null);
        setBureau(bureauData ?? []);
        setPreviousApps(prevData ?? []);
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
  }, [router, id]);

  const riskSegment = client ? String((client.risk_segment ?? client.RISK_SEGMENT ?? "")).toUpperCase() : "";
  const segmentColor = SEGMENT_COLORS[riskSegment] ?? "var(--muted)";

  const chartRiskFactors = useMemo(() => {
    if (!client) return [];
    const debtRatio = Number(client.bureau_debt_ratio ?? 0) || 0;
    const delayScore = Number(client.payment_delay_score ?? 0) ?? 0;
    const rejRate = Number(client.previous_rejection_rate ?? 0) ?? 0;
    return [
      { name: "Bureau debt ratio", value: Math.min(1, Math.max(0, debtRatio)), fill: "#d29922" },
      { name: "Payment delay score", value: Math.min(1, Math.max(0, (delayScore + 30) / 60)), fill: "#f85149" },
      { name: "Rejection rate", value: Math.min(1, Math.max(0, rejRate)), fill: "#8b949e" },
    ];
  }, [client]);

  const chartIncomeCredit = useMemo(() => {
    if (!client) return [];
    const income = Number(client.income ?? 0) ?? 0;
    const credit = Number(client.credit_exposure ?? 0) ?? 0;
    const bureauDebt = Number(client.bureau_total_debt ?? 0) ?? 0;
    return [
      { name: "Income", value: income, fill: "#3fb950" },
      { name: "Credit exposure", value: credit, fill: "var(--accent)" },
      ...(bureauDebt > 0 ? [{ name: "Bureau debt", value: bureauDebt, fill: "#d29922" }] : []),
    ].filter((d) => d.value > 0);
  }, [client]);

  const riskSummaryLine = useMemo(() => {
    if (!client) return "";
    const seg = riskSegment || "Unknown";
    const defaultFlag = client.default_flag ?? client.TARGET;
    const parts: string[] = [`Risk segment: ${seg}.`];
    if (defaultFlag === 1) parts.push("Has payment difficulties (default).");
    const ratio = Number(client.bureau_debt_ratio) || 0;
    if (ratio > 0.5) parts.push("High debt ratio vs credit.");
    const rej = Number(client.previous_rejection_rate) || 0;
    if (rej > 0) parts.push(`${(rej * 100).toFixed(0)}% of previous applications rejected.`);
    return parts.join(" ");
  }, [client, riskSegment]);

  if (!id) return null;

  return (
    <main className="client-detail">
      <div className="client-detail__nav">
        <Link href="/dashboard" className="client-detail__back">
          ← Back to dashboard
        </Link>
      </div>

      <header className="client-detail__header">
        <h1 className="client-detail__title">Client risk profile</h1>
        <p className="client-detail__subtitle">
          Risk metrics, history, and assessment for client ID <strong>{id}</strong>.
        </p>
      </header>

      {error && <p className="client-detail__error">{error}</p>}

      {loading ? (
        <p className="client-detail__muted">Loading…</p>
      ) : !client ? (
        <p className="client-detail__muted">Client not found. The ID may not exist in the datamart.</p>
      ) : (
        <>
          {/* Risk summary card */}
          <section className="client-detail__summary-card">
            <div className="client-detail__summary-row">
              <span
                className="client-detail__segment-badge"
                style={{ backgroundColor: segmentColor, color: "#0f1419" }}
              >
                {riskSegment || "—"}
              </span>
              <span className="client-detail__default-badge">
                Default: {formatValue(client.default_flag ?? client.TARGET)}
              </span>
            </div>
            <p className="client-detail__summary-text">{riskSummaryLine}</p>
          </section>

          {/* Charts row */}
          <div className="client-detail__charts">
            <section className="client-detail__chart-card">
              <h2 className="client-detail__chart-title">Risk factors (normalized)</h2>
              {chartRiskFactors.length > 0 ? (
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={chartRiskFactors} layout="vertical" margin={{ left: 8, right: 8 }}>
                    <XAxis type="number" domain={[0, 1]} tickFormatter={(v) => `${Math.round(v * 100)}%`} stroke="var(--muted)" />
                    <YAxis type="category" dataKey="name" width={120} tick={{ fontSize: 11 }} stroke="var(--muted)" />
                    <Tooltip formatter={(v: number) => [`${Math.round(v * 100)}%`, ""]} contentStyle={{ background: "var(--surface)", border: "1px solid var(--border)" }} />
                    <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                      {chartRiskFactors.map((entry, i) => (
                        <Cell key={i} fill={entry.fill} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              ) : (
                <p className="client-detail__muted">No risk factor data to display.</p>
              )}
            </section>
            <section className="client-detail__chart-card">
              <h2 className="client-detail__chart-title">Income vs credit & debt</h2>
              {chartIncomeCredit.length > 0 ? (
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={chartIncomeCredit} margin={{ left: 8, right: 8 }}>
                    <XAxis dataKey="name" stroke="var(--muted)" tick={{ fontSize: 11 }} />
                    <YAxis tickFormatter={(v) => (v >= 1e6 ? `${(v / 1e6).toFixed(1)}M` : `${(v / 1e3).toFixed(0)}k`)} stroke="var(--muted)" />
                    <Tooltip formatter={(v: number) => [v.toLocaleString(), ""]} contentStyle={{ background: "var(--surface)", border: "1px solid var(--border)" }} />
                    <Legend />
                    <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                      {chartIncomeCredit.map((entry, i) => (
                        <Cell key={i} fill={entry.fill} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              ) : (
                <p className="client-detail__muted">No income/credit data.</p>
              )}
            </section>
          </div>

          {/* Key metrics grid */}
          <section className="client-detail__metrics">
            <h2 className="client-detail__section-title">Key metrics</h2>
            <div className="client-detail__metrics-grid">
              {["income", "credit_exposure", "bureau_total_debt", "bureau_debt_ratio", "payment_avg_delay_days", "payment_late_count", "previous_rejection_rate", "payment_delay_score"].map((key) => {
                const val = client[key];
                const label = getLabel(key);
                const isRatio = key.includes("ratio") || key === "previous_rejection_rate";
                const display = isRatio && typeof val === "number" ? `${(val * 100).toFixed(1)}%` : formatValue(val);
                return (
                  <div key={key} className="client-detail__metric-cell">
                    <span className="client-detail__metric-label">{label}</span>
                    <span className="client-detail__metric-value">{display}</span>
                  </div>
                );
              })}
            </div>
          </section>

          {/* Previous applications — history */}
          <section className="client-detail__section">
            <h2 className="client-detail__section-title">Previous applications (history)</h2>
            <p className="client-detail__section-desc">
              Past loan applications and how each one went (status, amount, decision time).
            </p>
            {previousApps.length === 0 ? (
              <p className="client-detail__empty">
                No previous applications in the datamart. Run the extended datamart loader (spark/gold/datamart_extended.py) with the same ingest date as Bronze to populate this table.
              </p>
            ) : (() => {
              const prevCols = previousApps[0]
                ? [...PREV_APP_COL_ORDER.filter((k) => k in previousApps[0]), ...Object.keys(previousApps[0]).filter((k) => !PREV_APP_COL_ORDER.includes(k))].slice(0, 10)
                : [];
              return (
                <div className="client-detail__table-wrap">
                  <table className="client-detail__table">
                    <thead>
                      <tr>
                        <th>Summary (how it went)</th>
                        {prevCols.map((k) => (
                          <th key={k}>{getPrevAppLabel(k)}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {previousApps.map((row, i) => (
                        <tr key={i}>
                          <td className="client-detail__summary-cell">{previousAppSummary(row)}</td>
                          {prevCols.map((k) => (
                            <td key={k}>{/^amt_/i.test(k) ? formatCurrency(row[k]) : formatValue(row[k])}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
          </section>

          {/* Bureau credits */}
          <section className="client-detail__section">
            <h2 className="client-detail__section-title">Bureau credits</h2>
            <p className="client-detail__section-desc">
              External credit bureau records (one row per credit).
            </p>
            {bureau.length === 0 ? (
              <p className="client-detail__empty">
                No bureau data. Run the extended datamart loader to populate this table.
              </p>
            ) : (
              <div className="client-detail__table-wrap">
                <table className="client-detail__table">
                  <thead>
                    <tr>
                      {Object.keys(bureau[0]).map((k) => (
                        <th key={k}>{getBureauLabel(k)}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {bureau.map((row, i) => (
                      <tr key={i}>
                        {Object.keys(bureau[0]).map((k) => (
                          <td key={k}>{formatValue(row[k])}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </>
      )}
    </main>
  );
}
