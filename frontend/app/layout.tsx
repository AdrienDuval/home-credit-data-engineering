import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Home Credit Datamart",
  description: "Credit risk dashboard",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
