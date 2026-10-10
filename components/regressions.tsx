import { regressionDashboard } from "@/lib/regressions";
import { Unavailable } from "./dashboard";
export async function Regressions({ slug }: { slug?: string }) {
  const summary = await regressionDashboard();
  if (!summary?.configured)
    return (
      <Unavailable
        name="Regression metrics"
        detail="Regression metrics require Linear, GitHub, and the scheduled attribution refresh."
      />
    );
  const author = slug
      ? summary.author_metrics.find((m) => m.slug === slug)
      : null,
    reviewer = slug
      ? summary.reviewer_metrics.find((m) => m.slug === slug)
      : null;
  return (
    <section>
      <h2>
        Regression Signals <small>30d</small>
      </h2>
      <div className="cards">
        {[
          {
            label: "Authored Regressions",
            value: slug
              ? author?.regression_count
              : summary.authored_regression_count,
            rate: slug ? author?.rate : summary.author_regression_rate,
            row: author,
          },
          {
            label: "Approved Regressions",
            value: slug
              ? reviewer?.regression_count
              : summary.approved_regression_count,
            rate: slug ? reviewer?.rate : summary.reviewer_escape_rate,
            row: reviewer,
          },
        ].map((card) => (
          <article key={card.label}>
            <small>{card.label}</small>
            <strong className="metric-value">{card.value ?? "n/a"}</strong>
            <small>
              {card.rate === null || card.rate === undefined
                ? "No comparison cohort"
                : `${card.rate}% of PRs`}
            </small>
            {!!card.row?.attributions.length && (
              <details>
                <summary>Attribution evidence</summary>
                {card.row.attributions.map((a) => (
                  <p key={a.identifier}>
                    <a href={a.issue_url}>{a.identifier}</a> ·{" "}
                    <a href={a.attribution!.url}>Inducing PR</a> ·{" "}
                    {a.attribution!.line_count} matching deleted lines ·{" "}
                    {a.manual_override
                      ? "Manual correction"
                      : `Ranked first of ${a.candidates.length}`}
                    {!a.complete && " · Incomplete blame data"}
                    <br />
                    Fixes:{" "}
                    {a.fixing_urls.map((url) => (
                      <a key={url} href={url}>
                        {url.split("/").slice(-3).join("/")}{" "}
                      </a>
                    ))}
                  </p>
                ))}
              </details>
            )}
          </article>
        ))}
      </div>
      <p className="muted">
        Automated blame is directional, not proof of causality.
        {!summary.complete && " Attribution data is incomplete."}
      </p>
    </section>
  );
}
