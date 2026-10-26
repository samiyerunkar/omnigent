import { authenticatedFetch } from "./identity";

export interface SandboxProfile {
  id: string;
  name: string;
  description: string;
  provider: string;
  revision: string;
}

export interface SandboxProfilesResponse {
  profiles: SandboxProfile[];
  managed_providers: string[];
  next_cursor: string | null;
}

export const SANDBOX_PROFILES_QUERY_KEY = ["sandbox-profiles"];

async function profileRequest(path = "", init?: RequestInit): Promise<Response> {
  const response = await authenticatedFetch(`/v1/sandbox-profiles${path}`, init);
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(
      body?.error?.message ??
        (typeof body?.detail === "string" ? body.detail : null) ??
        `Sandbox profile request failed (${response.status}).`,
    );
  }
  return response;
}

export async function fetchSandboxProfiles(provider?: string): Promise<SandboxProfilesResponse> {
  const scope = provider ? `&provider=${encodeURIComponent(provider)}` : "";
  const result: SandboxProfilesResponse = await (await profileRequest(`?limit=100${scope}`)).json();
  while (result.next_cursor) {
    // eslint-disable-next-line no-await-in-loop -- Each page needs the preceding cursor.
    const page: SandboxProfilesResponse = await profileRequest(
      `?limit=100${scope}&cursor=${encodeURIComponent(result.next_cursor)}`,
    ).then((response) => response.json());
    result.profiles.push(...page.profiles);
    result.next_cursor = page.next_cursor;
  }
  return result;
}
