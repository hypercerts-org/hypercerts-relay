import { afterEach, expect, test, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import DiscoverySettings from "../src/DiscoverySettings.svelte";
import { api } from "../src/api";

vi.mock("../src/api", () => ({ api: vi.fn() }));
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});

test("loads and saves the discovery maximum account limit", async () => {
  const call = vi.mocked(api);
  call
    .mockResolvedValueOnce({ maxAccountLimit: 100000 })
    .mockResolvedValueOnce({ maxAccountLimit: 25000 });
  render(DiscoverySettings, { csrf: "csrf-fixture" });
  const input = (await screen.findByLabelText("Maximum account limit")) as HTMLInputElement;
  expect(input.value).toBe("100000");
  expect(call).toHaveBeenCalledWith("/sources/discovery/settings");

  await fireEvent.input(input, { target: { value: "25000" } });
  await fireEvent.click(screen.getByRole("button", { name: "Save maximum" }));
  await waitFor(() => expect(screen.getByText("Discovery account limit maximum saved.")).toBeTruthy());
  expect(call).toHaveBeenLastCalledWith(
    "/sources/discovery/settings",
    { maxAccountLimit: 25000 },
    "csrf-fixture",
  );
  expect(screen.getByText(/Currently 25,000 accounts/)).toBeTruthy();
});

test("rejects a maximum below 1,000 without saving", async () => {
  const call = vi.mocked(api);
  call.mockResolvedValueOnce({ maxAccountLimit: 100000 });
  render(DiscoverySettings, { csrf: "csrf-fixture" });
  const input = await screen.findByLabelText("Maximum account limit");
  await fireEvent.input(input, { target: { value: "500" } });
  await fireEvent.submit(input.closest("form")!);
  expect(screen.getByRole("alert").textContent).toContain("at least 1,000");
  expect(call).toHaveBeenCalledTimes(1);
});
