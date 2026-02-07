"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { login } from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [username, setUsername] = useState("api_user");
  const [password, setPassword] = useState("demo");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const { access_token } = await login(username, password);
      if (typeof window !== "undefined") {
        sessionStorage.setItem("token", access_token);
      }
      router.push("/dashboard");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main
      style={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "var(--bg)",
      }}
    >
      <div
        style={{
          width: "100%",
          maxWidth: 360,
          padding: 24,
          background: "var(--surface)",
          borderRadius: 12,
          border: "1px solid var(--border)",
        }}
      >
        <h1 style={{ marginBottom: 8, fontSize: "1.5rem" }}>Home Credit Datamart</h1>
        <p style={{ color: "var(--muted)", marginBottom: 24, fontSize: "0.9rem" }}>
          Sign in to view the dashboard
        </p>
        <form onSubmit={handleSubmit}>
          <label style={{ display: "block", marginBottom: 8, fontSize: "0.875rem" }}>
            Username
          </label>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
            style={{
              width: "100%",
              padding: "10px 12px",
              marginBottom: 16,
              background: "var(--bg)",
              border: "1px solid var(--border)",
              borderRadius: 8,
              color: "var(--text)",
            }}
          />
          <label style={{ display: "block", marginBottom: 8, fontSize: "0.875rem" }}>
            Password
          </label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            style={{
              width: "100%",
              padding: "10px 12px",
              marginBottom: 24,
              background: "var(--bg)",
              border: "1px solid var(--border)",
              borderRadius: 8,
              color: "var(--text)",
            }}
          />
          {error && (
            <p style={{ color: "#f85149", marginBottom: 16, fontSize: "0.875rem" }}>
              {error}
            </p>
          )}
          <button
            type="submit"
            disabled={loading}
            style={{
              width: "100%",
              padding: "10px 16px",
              background: "var(--accent)",
              color: "#fff",
              border: "none",
              borderRadius: 8,
              fontWeight: 600,
            }}
          >
            {loading ? "Signing in…" : "Sign in"}
          </button>
        </form>
      </div>
    </main>
  );
}
