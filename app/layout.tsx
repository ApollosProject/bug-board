import type { Metadata } from "next";
import Link from "next/link";
import Image from "next/image";
import { session } from "@/lib/auth";
import "./globals.css";
export function generateMetadata(): Metadata {
  return {
    metadataBase: new URL(
      process.env.APP_URL || `http://localhost:${process.env.PORT || 3000}`,
    ),
    title: {
      default: "Apollos Engineering · Bug Board",
      template: "%s · Bug Board",
    },
    description:
      "Linear issues, GitHub PR stats, app releases, and Airflow fleet health.",
    icons: { icon: "/static/brand-mark.svg" },
    openGraph: {
      title: "Apollos Engineering · Bug Board",
      images: [{ url: "/static/og-image.png", width: 1200, height: 630 }],
    },
  };
}
export const dynamic = "force-dynamic";
export default async function Layout({
  children,
}: {
  children: React.ReactNode;
}) {
  const user = await session();
  return (
    <html lang="en">
      <body>
        <header className="site-header">
          <nav className="container">
            <Link className="brand" href="/" prefetch={false}>
              <Image
                src="/static/brand-mark.svg"
                alt=""
                width={32}
                height={32}
                unoptimized
              />
              <span>
                Apollos <small>Engineering</small>
              </span>
            </Link>
            <div className="nav-links">
              {["Team", "Reviews", "Apps", "Projects", "DAGs"].map((name) => (
                <Link
                  key={name}
                  href={`/${name.toLowerCase()}`}
                  prefetch={false}
                >
                  {name}
                </Link>
              ))}
            </div>
            {user && (
              <form action="/logout" method="post">
                <button className="outline" type="submit">
                  Log out @{String(user.login)}
                </button>
              </form>
            )}
          </nav>
        </header>
        <main className="container">{children}</main>
        <footer className="container">
          <span>Apollos Engineering · Bug Board</span>
          <div>
            <a href="https://differential-ka.sentry.io/issues">Sentry</a>
            <a href="https://telemetry.betterstack.com/team/278287/tail?s=1201580">
              Logs
            </a>
            <a href="https://apollos-metabase-app-8bc87d621513.herokuapp.com/">
              Metabase
            </a>
            <a href="/static/resources/engineering-expectations.pdf">
              Engineering Expectations
            </a>
          </div>
        </footer>
      </body>
    </html>
  );
}
