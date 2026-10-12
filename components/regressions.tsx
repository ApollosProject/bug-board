import { regressionDashboard } from "@/lib/regressions";
import { Unavailable } from "./dashboard";
// https://github.com/owner/repo/pull/123 → repo#123
const pullRequest = (url: string) => {
  const [, , , , repo, , number] = url.split("/");
  return number ? `${repo}#${number}` : url;
};
export async function Regressions({ slug }: { slug?: string }) {
  const summary = await regressionDashboard();
  if (!summary?.configured)
    return (
      <Unavailable
        name="Introduced-bug metrics"
        detail="Introduced-bug metrics require Linear, GitHub, and the scheduled attribution refresh."
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
        Introduced Bugs <small>30d</small>
      </h2>
      <div className="cards">
        {[
          {
            label: "Bugs in Authored PRs",
            value: slug
              ? author?.regression_count
              : summary.authored_regression_count,
            rate: slug ? author?.rate : summary.author_regression_rate,
            row: author,
          },
          {
            label: "Bugs in Approved PRs",
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
                <ul className="evidence">
                  {card.row.attributions.map((a) => (
                    <li key={a.identifier}>
                      <a href={a.issue_url}>{a.identifier}</a> ←{" "}
                      <a href={a.attribution!.url}>
                        {pullRequest(a.attribution!.url)}
                      </a>
                      <small>
                        {a.attribution!.line_count} matching deleted lines ·{" "}
                        {a.manual_override
                          ? "manual correction"
                          : `ranked first of ${a.candidates.length}`}
                        {!a.complete && " · incomplete blame data"}
                      </small>
                      <small>
                        fixed by{" "}
                        {a.fixing_urls.map((url, i) => (
                          <span key={url}>
                            {i > 0 && ", "}
                            <a href={url}>{pullRequest(url)}</a>
                          </span>
                        ))}
                      </small>
                    </li>
                  ))}
                </ul>
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
