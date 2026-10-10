export const refreshJobs = [
  "fleet",
  "metrics",
  "apps",
  "regressions",
  "notifications",
] as const;
export type Job = (typeof refreshJobs)[number];
export type Connection<T> = {
  nodes: T[];
  pageInfo: { hasNextPage: boolean; endCursor: string | null };
};
export type Identity = { name?: string; displayName?: string; login?: string };
export type Issue = {
  id: string;
  identifier: string;
  number: number;
  title: string;
  url: string;
  priority: number;
  createdAt: string;
  updatedAt: string;
  completedAt?: string;
  startedAt?: string;
  dueDate?: string;
  slaBreachesAt?: string;
  slaHighRiskAt?: string;
  slaMediumRiskAt?: string;
  slaType?: string;
  slaStartedAt?: string;
  assignee: Identity | null;
  project: { id: string; name: string } | null;
  state: { name: string; type: string };
  labels: { nodes: { name: string }[] };
  attachments?: {
    nodes: { metadata: { url?: string; status?: string; linkKind?: string } }[];
  };
  history?: {
    edges: { node: { toAssignee: Identity | null; updatedAt: string } }[];
  };
};
export type Project = {
  id: string;
  name: string;
  url: string;
  health?: string;
  priorityLabel?: string;
  status: { name: string; type: string };
  completedAt?: string;
  startDate?: string;
  targetDate?: string;
  lastUpdate?: { createdAt: string };
  inverseRelations?: {
    nodes: {
      type: string;
      project: Pick<Project, "status" | "completedAt"> | null;
      projectMilestone: { status: string } | null;
    }[];
  };
  lead: Identity | null;
  members: { nodes: Identity[] };
  initiatives: { nodes: { id: string; name: string }[] };
};
export type Review = {
  author: Identity | null;
  state: string;
  submittedAt?: string;
};
export type PullRequest = {
  id: string;
  number: number;
  title: string;
  url: string;
  createdAt: string;
  mergedAt?: string;
  updatedAt?: string;
  timelineCount?: { totalCount: number };
  author: Identity | null;
  reviews: Connection<Review>;
  baseRefName: string;
  headRefName: string;
  additions: number;
  deletions: number;
  mergeable: string;
  reviewDecision: string | null;
  statusCheckRollup: { state: string } | null;
  repository: {
    nameWithOwner: string;
    defaultBranchRef: { name: string } | null;
  };
  reviewRequests: { nodes: { requestedReviewer: Identity | null }[] };
  timelineItems?: {
    nodes: {
      __typename: string;
      createdAt: string;
      requestedReviewer?: Identity;
    }[];
  };
  commits?: {
    nodes: {
      commit: {
        author: { user: Identity | null };
        authors: { nodes: { user: Identity | null }[] };
      };
    }[];
  };
};
export type Search = Record<string, string | string[] | undefined>;
export type Window = {
  start: string;
  end: string;
  days: number;
  preset_days: number | null;
  label: string;
  after: string;
  before: string;
  query: Record<string, string>;
};
export type Dag = { dag_id: string; state: string; dag_run_id: string };
export type Fleet = {
  status: "healthy" | "degraded" | "unknown";
  checked_at: string;
  active_dags_total: number;
  evaluated_dags: number;
  failed_fetches: number;
  dags_without_runs: number;
  non_terminal_dags: number;
  failed_runs: number;
  failure_ratio: number;
  threshold_ratio: number;
  dags: Dag[];
  failed_dags: Dag[];
  top_failed_dags: Dag[];
};
export type AppRow = {
  church: string;
  build_church?: string | null;
  deploy_target_count?: number;
  apollos_platform: string;
  application_name: string;
  bundle_id: string;
  apollos_version: string | null;
  app_version?: string;
  native_build?: string;
  native_version?: string;
  source_revision?: string;
  source_version?: string;
  deployment_track?: string;
  latest_seen_at?: string;
  canonical_source_version?: string | null;
  live_runtime_display?: string;
  live_status_detail?: string;
  version_status_label?: string;
  comparison_display?: string;
  freshness_display?: string;
  is_outdated?: boolean;
};
