import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { smokeContainerProperties, validateWorkloadIdentityConfig } from "../src/workload-identity-smoke.mjs";

const tenantId = "11111111-1111-1111-1111-111111111111";
const clientId = "22222222-2222-2222-2222-222222222222";

function withTokenFile(run) {
  const directory = mkdtempSync(join(tmpdir(), "polyedge-workload-identity-"));
  const tokenPath = join(directory, "token");
  writeFileSync(tokenPath, "test-token");
  chmodSync(tokenPath, 0o600);
  try {
    return run(tokenPath, directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

function validEnv(tokenPath) {
  return {
    AZURE_TOKEN_CREDENTIALS: "WorkloadIdentityCredential",
    AZURE_TENANT_ID: tenantId,
    AZURE_CLIENT_ID: clientId,
    AZURE_FEDERATED_TOKEN_FILE: tokenPath,
    AZURE_STORAGE_ACCOUNT_NAME: "polyedgestorage"
  };
}

test("workload identity smoke requires the workload credential selector", () => withTokenFile((tokenPath) => {
  assert.throws(() => validateWorkloadIdentityConfig({ ...validEnv(tokenPath), AZURE_TOKEN_CREDENTIALS: "" }), /AZURE_TOKEN_CREDENTIALS/);
  assert.throws(() => validateWorkloadIdentityConfig({ ...validEnv(tokenPath), AZURE_TOKEN_CREDENTIALS: "ManagedIdentityCredential" }), /AZURE_TOKEN_CREDENTIALS/);
}));

test("workload identity smoke rejects unsafe token file paths", () => withTokenFile((tokenPath, directory) => {
  assert.throws(() => validateWorkloadIdentityConfig({ ...validEnv(tokenPath), AZURE_FEDERATED_TOKEN_FILE: "token" }), /absolute path/);
  chmodSync(tokenPath, 0o644);
  assert.throws(() => validateWorkloadIdentityConfig(validEnv(tokenPath)), /owner-only/);
  chmodSync(tokenPath, 0o600);
  const linkPath = join(directory, "token-link");
  symlinkSync(tokenPath, linkPath);
  assert.throws(() => validateWorkloadIdentityConfig(validEnv(linkPath)), /non-symlink/);
}));

test("workload identity smoke validates a local configuration without network access", () => withTokenFile((tokenPath) => {
  assert.deepEqual(validateWorkloadIdentityConfig(validEnv(tokenPath)), {
    accountName: "polyedgestorage",
    credentialOptions: {
      requiredEnvVars: [
        "AZURE_TOKEN_CREDENTIALS",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_FEDERATED_TOKEN_FILE"
      ],
      managedIdentityClientId: clientId,
      workloadIdentityClientId: clientId
    }
  });
}));

test("workload identity smoke reads only bot-events container properties", async () => withTokenFile(async (tokenPath) => {
  const calls = [];
  class DefaultAzureCredential {
    constructor(options) {
      calls.push(["credential", options]);
    }
  }
  class ContainerClient {
    constructor(url, credential) {
      calls.push(["container", url, credential]);
    }

    async getProperties() {
      calls.push(["getProperties"]);
    }
  }

  const config = validateWorkloadIdentityConfig(validEnv(tokenPath));
  await smokeContainerProperties(config, { DefaultAzureCredential, ContainerClient });
  assert.equal(calls[1][1], "https://polyedgestorage.blob.core.windows.net/bot-events");
  assert.deepEqual(calls.map(([operation]) => operation), ["credential", "container", "getProperties"]);
}));
