import { act, cleanup, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { ReactNode } from "react";
import { FALLBACK_SERVER_INFO, type ServerInfo } from "@/lib/capabilities";
import type * as SandboxProfilesModule from "@/lib/sandboxProfilesApi";
import {
  fetchSandboxProfiles,
  SANDBOX_PROFILES_QUERY_KEY,
  type SandboxProfile,
  type SandboxProfilesResponse,
} from "@/lib/sandboxProfilesApi";
import {
  SandboxProfilePicker,
  useSandboxProfileCatalog,
  type SandboxProfileSelection,
} from "./SandboxProfilePicker";

const capability = vi.hoisted(() => ({ info: "loading" as ServerInfo | "loading" }));
vi.mock("@/lib/CapabilitiesContext", () => ({ useServerInfo: () => capability.info }));

vi.mock("@/lib/sandboxProfilesApi", async (importOriginal) => ({
  ...(await importOriginal<typeof SandboxProfilesModule>()),
  fetchSandboxProfiles: vi.fn(),
}));

beforeEach(() => {
  capability.info = {
    ...FALLBACK_SERVER_INFO,
    managed_sandboxes_enabled: true,
    features: { sandbox_profiles: true },
  };
  vi.mocked(fetchSandboxProfiles).mockReset();
});
afterEach(cleanup);

const selected: SandboxProfile = {
  id: "large",
  name: "Large workspace",
  description: "For builds",
  provider: "kubernetes",
  revision: "revision-1",
};

function catalog(profile: SandboxProfile): SandboxProfilesResponse {
  return {
    profiles: [profile],
    managed_providers: ["kubernetes"],
    next_cursor: null,
  };
}

function mount(data?: SandboxProfilesResponse, value: SandboxProfileSelection | null = null) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  if (data) client.setQueryData([...SANDBOX_PROFILES_QUERY_KEY, "kubernetes"], data);
  const onChange = vi.fn();
  render(
    <QueryClientProvider client={client}>
      <SandboxProfilePicker provider="kubernetes" value={value} onChange={onChange} />
    </QueryClientProvider>,
  );
  return { client, onChange };
}

it("allows long profile names and descriptions to wrap in the constrained menu", () => {
  const profile = {
    ...selected,
    name: "Small development sandbox",
    description: "A small development sandbox for building and testing applications.",
  };
  const { onChange } = mount(catalog(profile));
  fireEvent.pointerDown(screen.getByRole("button", { name: "Sandbox profile: Choose profile" }), {
    button: 0,
  });
  expect(screen.getByText(profile.name)).toHaveClass("whitespace-normal", "break-words");
  expect(screen.getByText(profile.description)).toHaveClass("whitespace-normal", "break-words");
  fireEvent.click(screen.getByRole("menuitem", { name: /^Small development sandbox/ }));
  expect(onChange).toHaveBeenCalledWith({
    id: profile.id,
    name: profile.name,
    provider: profile.provider,
    revision: profile.revision,
  });
});

it.each(["edited", "removed"])(
  "keeps the selected revision after a profile is %s",
  async (change) => {
    const data = catalog(selected);
    const { client, onChange } = mount(data, selected);
    expect(screen.queryByText("(changed)")).toBeNull();
    const current = {
      ...selected,
      revision: "revision-2",
    };
    await act(async () => {
      client.setQueryData([...SANDBOX_PROFILES_QUERY_KEY, "kubernetes"], {
        ...data,
        profiles: change === "removed" ? [] : [current],
      });
    });
    expect(await screen.findByText("(changed)")).toBeVisible();
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.pointerDown(
      screen.getByRole("button", { name: "Sandbox profile: Large workspace" }),
      {
        button: 0,
      },
    );
    expect(await screen.findByRole("status")).toHaveTextContent("Choose a profile again");
    if (change === "removed") {
      expect(screen.queryByRole("menuitem", { name: /^Large workspace/ })).toBeNull();
      expect(screen.queryByRole("menuitem", { name: /^Default/ })).toBeNull();
    } else {
      fireEvent.click(screen.getByRole("menuitem", { name: /^Large workspace/ }));
      expect(onChange).toHaveBeenCalledWith({
        id: current.id,
        name: current.name,
        provider: current.provider,
        revision: current.revision,
      });
    }
  },
);

it.each([null, { ...selected, provider: "other" }])(
  "requires an explicit managed choice and scopes selections to the provider (%j)",
  (value) => {
    const { onChange } = mount(
      {
        ...catalog(selected),
        profiles: [{ ...selected, provider: "other", name: "Other provider" }, selected],
        managed_providers: [selected.provider],
      },
      value,
    );
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.pointerDown(screen.getByRole("button", { name: "Sandbox profile: Choose profile" }), {
      button: 0,
    });
    expect(screen.getByRole("status")).toHaveTextContent("Choose a sandbox profile to continue");
    expect(screen.queryByRole("menuitem", { name: /^Default/ })).toBeNull();
    expect(screen.queryByText("Other provider")).toBeNull();
    fireEvent.click(screen.getByRole("menuitem", { name: /^Large workspace/ }));
    expect(onChange).toHaveBeenCalledWith({
      id: selected.id,
      name: selected.name,
      provider: selected.provider,
      revision: selected.revision,
    });
  },
);

it("shows catalog loading and a recoverable error without suggesting a default", async () => {
  let rejectCatalog!: (error: Error) => void;
  vi.mocked(fetchSandboxProfiles).mockImplementationOnce(
    () =>
      new Promise((_, reject) => {
        rejectCatalog = reject;
      }),
  );
  const { onChange } = mount();
  fireEvent.pointerDown(
    screen.getByRole("button", { name: "Sandbox profile: Loading profiles…" }),
    {
      button: 0,
    },
  );
  expect(screen.getByRole("status")).toHaveTextContent("Loading sandbox profiles");
  expect(screen.queryByRole("menuitem", { name: /^Default/ })).toBeNull();
  await act(async () => rejectCatalog(new Error("Unavailable")));
  expect(await screen.findByRole("alert")).toHaveTextContent("Could not load sandbox profiles");
  expect(screen.getByText("Profiles unavailable")).toBeVisible();
  expect(screen.queryByRole("menuitem", { name: /^Default/ })).toBeNull();
  vi.mocked(fetchSandboxProfiles).mockResolvedValue({
    ...catalog(selected),
    managed_providers: [selected.provider],
  });
  fireEvent.click(screen.getByRole("menuitem", { name: "Try again" }));
  expect(
    await screen.findByRole("button", { name: "Sandbox profile: Choose profile" }),
  ).toBeVisible();
  expect(onChange).not.toHaveBeenCalled();
});

it.each([false, true])("shows an empty catalog (managed=%s)", (managed) => {
  const { onChange } = mount({
    ...catalog(selected),
    profiles: [],
    managed_providers: managed ? [selected.provider] : [],
  });
  if (!managed) {
    expect(screen.queryByRole("button")).toBeNull();
    return;
  }
  fireEvent.pointerDown(
    screen.getByRole("button", { name: "Sandbox profile: No profiles available" }),
    {
      button: 0,
    },
  );
  expect(screen.getByRole("status")).toHaveTextContent(
    managed ? "No sandbox profiles available" : "No saved profiles",
  );
  expect(!!screen.queryByRole("menuitem", { name: /^Default/ })).toBe(!managed);
  expect(onChange).not.toHaveBeenCalled();
});

it.each(["loading", "missing", "disabled"])(
  "does not query or block legacy launches when feature is %s",
  async (state) => {
    capability.info =
      state === "loading"
        ? "loading"
        : {
            ...FALLBACK_SERVER_INFO,
            managed_sandboxes_enabled: true,
            features: state === "missing" ? {} : { sandbox_profiles: false },
          };
    vi.mocked(fetchSandboxProfiles).mockRejectedValue(new Error("Old server: 404"));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData([...SANDBOX_PROFILES_QUERY_KEY, "kubernetes"], {
      ...catalog(selected),
      managed_providers: ["kubernetes"],
    });
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useSandboxProfileCatalog("kubernetes", selected), {
      wrapper,
    });
    render(<SandboxProfilePicker provider="kubernetes" value={selected} onChange={vi.fn()} />, {
      wrapper,
    });
    await act(async () => {});
    expect(result.current.selection).toBeNull();
    expect(result.current.launchError).toBeNull();
    expect(result.current.required).toBe(false);
    expect(screen.queryByRole("button")).toBeNull();
    expect(fetchSandboxProfiles).not.toHaveBeenCalled();
  },
);
