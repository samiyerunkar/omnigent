import { expect, it, vi } from "vitest";
import { authenticatedFetch } from "./identity";
import { fetchSandboxProfiles } from "./sandboxProfilesApi";

vi.mock("./identity", () => ({ authenticatedFetch: vi.fn() }));

it("keeps discovery scoped to the selected provider across pages", async () => {
  const page = (id: string, next_cursor: string | null) =>
    new Response(
      JSON.stringify({
        profiles: [{ id, provider: "company" }],
        managed_providers: ["company"],
        next_cursor,
      }),
    );
  vi.mocked(authenticatedFetch)
    .mockResolvedValueOnce(page("small", "first"))
    .mockResolvedValueOnce(page("large", null));
  const result = await fetchSandboxProfiles("company");
  expect(result.profiles.map((p) => p.id)).toEqual(["small", "large"]);
  expect(result.managed_providers).toEqual(["company"]);
  expect(result.next_cursor).toBeNull();
  expect(vi.mocked(authenticatedFetch).mock.calls).toEqual([
    ["/v1/sandbox-profiles?limit=100&provider=company", undefined],
    ["/v1/sandbox-profiles?limit=100&provider=company&cursor=first", undefined],
  ]);
});
