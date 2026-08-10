import type { MonitoredRepo, Project } from '../../api/client';

/**
 * Keep the client-side Delivery admission preview aligned with the server's
 * local-only V1 policy. The server remains authoritative; this helper only
 * prevents presenting choices that it will deterministically reject.
 */
export function isDeliveryCompatible(
  project: Project | null | undefined,
  repo: MonitoredRepo,
): boolean {
  return Boolean(
    project
    && project.worker_id == null
    && project.has_remote
    && project.local_path
    && repo.project_id === project.id
    && repo.worker_id == null
    && repo.enabled
    && repo.status === 'active'
    && repo.merge_queue_mode === 'manual'
    && repo.review_mode === 'panel'
    && repo.wait_for_ci
    && repo.required_checks.length > 0
    && repo.default_branch === project.default_branch,
  );
}

export function filterDeliveryRepos(
  project: Project | null | undefined,
  repos: MonitoredRepo[],
): MonitoredRepo[] {
  return repos.filter((repo) => isDeliveryCompatible(project, repo));
}
