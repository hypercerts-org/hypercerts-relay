import { afterEach, expect, test, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import ArchiveKeys from "../src/ArchiveKeys.svelte";
import { api } from "../src/api";

vi.mock("../src/api", () => ({ api: vi.fn() }));
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});

test("issues a key with an existing or free-form owner and reveals its token once", async () => {
  const call = vi.mocked(api);
  const existing = {
    id: "AbCdEfGhIjKl",
    name: "indexer",
    owner: "Data team",
    requestsPerMinute: 60,
    archiveMegabytesPerMinute: 100,
    createdAt: "2026-09-28T12:00:00Z",
  };
  call.mockResolvedValueOnce({ keys: [existing] }).mockResolvedValueOnce({
    key: { ...existing, id: "MnOpQrStUvWx", name: "mirror" },
    token: "hck_secret_for_one_time_display",
  });
  const writeText = vi.fn().mockResolvedValue(undefined);
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText },
  });

  render(ArchiveKeys, { csrf: "csrf-fixture" });
  await waitFor(() => expect(screen.getByText("Data team")).toBeTruthy());
  expect(document.querySelector('#archive-key-owners option[value="Data team"]')).toBeTruthy();

  await fireEvent.input(screen.getByLabelText("Key name"), { target: { value: "mirror" } });
  await fireEvent.input(screen.getByLabelText("Owner"), { target: { value: "Data team" } });
  await fireEvent.input(screen.getByLabelText("Requests/minute"), { target: { value: "25" } });
  await fireEvent.input(screen.getByLabelText("Archive MB/minute"), { target: { value: "5" } });
  await fireEvent.click(screen.getByRole("button", { name: "Create key" }));
  await waitFor(() => expect(screen.getByRole("heading", { name: "Copy this key now" })).toBeTruthy());
  expect(call).toHaveBeenLastCalledWith(
    "/archive-keys",
    { name: "mirror", owner: "Data team", requestsPerMinute: 25, archiveMegabytesPerMinute: 5 },
    "csrf-fixture",
  );
  expect((screen.getByLabelText("New consumer API key") as HTMLInputElement).value).toBe("hck_secret_for_one_time_display");
  expect(screen.getByRole("button", { name: "Create key" }).hasAttribute("disabled")).toBe(true);

  await fireEvent.click(screen.getByRole("button", { name: "Copy key" }));
  expect(writeText).toHaveBeenCalledWith("hck_secret_for_one_time_display");
  await fireEvent.click(screen.getByRole("button", { name: "Dismiss" }));
  expect(screen.queryByLabelText("New consumer API key")).toBeNull();
  expect(screen.getByRole("button", { name: "Create key" }).hasAttribute("disabled")).toBe(false);
}, 15000);

test("requires inline confirmation before revoking an active key", async () => {
  const call = vi.mocked(api);
  const key = {
    id: "AbCdEfGhIjKl",
    name: "indexer",
    owner: "Data team",
    requestsPerMinute: 60,
    archiveMegabytesPerMinute: 100,
    createdAt: "2026-09-28T12:00:00Z",
  };
  call
    .mockResolvedValueOnce({ keys: [key] })
    .mockResolvedValueOnce(undefined)
    .mockResolvedValueOnce({ keys: [{ ...key, revokedAt: "2026-09-28T12:05:00Z" }] });

  render(ArchiveKeys, { csrf: "csrf-fixture" });
  await waitFor(() => expect(screen.getByRole("button", { name: "Revoke indexer" })).toBeTruthy());
  await fireEvent.click(screen.getByRole("button", { name: "Revoke indexer" }));
  expect(call).toHaveBeenCalledTimes(1);
  await fireEvent.click(screen.getByRole("button", { name: "Confirm revoke" }));
  await waitFor(() => expect(screen.getByText("Revoked")).toBeTruthy());
  expect(call).toHaveBeenNthCalledWith(2, "/archive-keys/AbCdEfGhIjKl", undefined, "csrf-fixture", undefined, "DELETE");
  expect(screen.queryByRole("button", { name: "Revoke indexer" })).toBeNull();
});
