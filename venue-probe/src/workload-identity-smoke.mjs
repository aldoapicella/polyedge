import { lstatSync } from "node:fs";
import { isAbsolute } from "node:path";

const REQUIRED_SELECTOR = "WorkloadIdentityCredential";
const REQUIRED_ENV = [
  "AZURE_TOKEN_CREDENTIALS",
  "AZURE_TENANT_ID",
  "AZURE_CLIENT_ID",
  "AZURE_FEDERATED_TOKEN_FILE",
  "AZURE_STORAGE_ACCOUNT_NAME"
];
const GUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const STORAGE_ACCOUNT = /^[a-z0-9]{3,24}$/;

function required(env, name) {
  const value = env[name];
  if (typeof value !== "string" || !value || value !== value.trim()) {
    throw new Error(`missing or unsafe ${name}`);
  }
  return value;
}

export function validateWorkloadIdentityConfig(env = process.env, stat = lstatSync) {
  if (env.AZURE_TOKEN_CREDENTIALS !== REQUIRED_SELECTOR) {
    throw new Error(`AZURE_TOKEN_CREDENTIALS must be ${REQUIRED_SELECTOR}`);
  }

  const values = Object.fromEntries(REQUIRED_ENV.map((name) => [name, required(env, name)]));
  if (!GUID.test(values.AZURE_TENANT_ID) || !GUID.test(values.AZURE_CLIENT_ID)) {
    throw new Error("AZURE_TENANT_ID and AZURE_CLIENT_ID must be GUIDs");
  }
  if (!STORAGE_ACCOUNT.test(values.AZURE_STORAGE_ACCOUNT_NAME)) {
    throw new Error("AZURE_STORAGE_ACCOUNT_NAME must be an Azure storage account name");
  }
  if (!isAbsolute(values.AZURE_FEDERATED_TOKEN_FILE)) {
    throw new Error("AZURE_FEDERATED_TOKEN_FILE must be an absolute path");
  }

  let tokenFile;
  try {
    tokenFile = stat(values.AZURE_FEDERATED_TOKEN_FILE);
  } catch {
    throw new Error("AZURE_FEDERATED_TOKEN_FILE must be an accessible regular file");
  }
  if (!tokenFile.isFile() || tokenFile.isSymbolicLink() || (tokenFile.mode & 0o077) !== 0) {
    throw new Error("AZURE_FEDERATED_TOKEN_FILE must be a non-symlink owner-only regular file");
  }

  return {
    accountName: values.AZURE_STORAGE_ACCOUNT_NAME,
    credentialOptions: {
      requiredEnvVars: REQUIRED_ENV.slice(0, 4),
      managedIdentityClientId: values.AZURE_CLIENT_ID,
      workloadIdentityClientId: values.AZURE_CLIENT_ID
    }
  };
}

export async function smokeContainerProperties(config, { DefaultAzureCredential, ContainerClient }) {
  const credential = new DefaultAzureCredential(config.credentialOptions);
  const container = new ContainerClient(
    `https://${config.accountName}.blob.core.windows.net/bot-events`,
    credential
  );
  await container.getProperties();
}

async function main() {
  const config = validateWorkloadIdentityConfig();
  const [{ DefaultAzureCredential }, { ContainerClient }] = await Promise.all([
    import("@azure/identity"),
    import("@azure/storage-blob")
  ]);
  await smokeContainerProperties(config, { DefaultAzureCredential, ContainerClient });
  process.stdout.write('{"ok":true,"credential":"WorkloadIdentityCredential","container":"bot-events","operation":"getProperties"}\n');
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(() => {
    process.stderr.write("workload identity smoke failed\n");
    process.exitCode = 1;
  });
}
