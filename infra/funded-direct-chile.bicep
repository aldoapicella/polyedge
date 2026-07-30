targetScope = 'resourceGroup'

@description('Eligible Azure region dedicated to funded execution.')
param location string = 'chilecentral'
param expectedCountry string = 'CL'
param venueProbeImage string
param registryName string = 'crpolyedge6urdjr5nmwx7w'
param storageAccountName string = 'stpolyedge6urdjr5nmwx7w'
param shadowEventsContainerName string = 'polyedge-shadow-events'
param researchContainerName string = 'polyedge-research'
param fundedEvidenceContainerName string = 'polyedge-funded-evidence'
param modelContainerName string = 'polyedge-models'
param keyVaultName string = 'kvpolyedge6urdjr5nmwx7w'
param logAnalyticsWorkspaceName string = 'log-polyedge-dev-6urdjr5nmwx7w'
param serviceBusNamespaceName string = 'sb-polyedge-funded-cl-6urdjr5nmwx7w'
param serviceBusQueueName string = 'funded-dynamic-quote-intents'
param producerIdentityName string = 'polyedge-shadow-neu-id'
param producerPublicIpName string = 'pip-polyedge-venue-neu-egress-2'
param alertActionGroupName string = 'polyedge-dev-research-alerts'
param funderAddress string = '0x3d701b05d7c36aFaB01a06Fd26eBe789c0B7baD8'
param fundedDirectEnabled bool = false
@secure()
param fundedDirectSessionManifestJson string
param fundedDirectSessionManifestBlobName string
param fundedDirectSessionManifestSha256 string
param fundedDirectCampaignId string
param fundedDirectStartingCollateral string = '11.09862'
param fundedDirectMaxAccountLoss string = '11.09862'
param fundedDirectTargetOrderNotional string = '10.5'
param fundedDirectMaxOrderNotional string = '10.5'

var environmentName = 'polyedge-execution-cl-env'
var identityName = 'polyedge-execution-cl-id'
var publicIpName = 'pip-polyedge-execution-cl-egress'
var natName = 'nat-polyedge-execution-cl'
var vnetName = 'vnet-polyedge-execution-cl'
var originCheckJobName = 'polyedge-origin-check-cl-job'
var fundedJobName = 'polyedge-funded-direct-cl-job'
var fundedServiceName = 'polyedge-funded-direct-cl'
var fundedWarmupJobName = 'polyedge-funded-warmup-cl'
var tags = {
  app: 'polyedge'
  environment: 'dev'
  managedBy: 'bicep'
  workload: 'funded-dynamic-quote-execution'
  executionRegion: location
  walletCredentials: 'key-vault-only'
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: registryName
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' existing = {
  parent: storage
  name: 'default'
}

resource shadowEventsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing = {
  parent: blobService
  name: shadowEventsContainerName
}

resource researchContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing = {
  parent: blobService
  name: researchContainerName
}

resource fundedEvidenceContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing = {
  parent: blobService
  name: fundedEvidenceContainerName
}

resource modelContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing = {
  parent: blobService
  name: modelContainerName
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

resource polymarketPrivateKeySecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-private-key'
}

resource polymarketApiKeySecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-api-key'
}

resource polymarketApiSecretSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-api-secret'
}

resource polymarketApiPassphraseSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-api-passphrase'
}

resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = {
  name: logAnalyticsWorkspaceName
}

resource alertActionGroup 'Microsoft.Insights/actionGroups@2023-01-01' existing = {
  name: alertActionGroupName
}

resource producerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: producerIdentityName
}

resource producerPublicIp 'Microsoft.Network/publicIPAddresses@2023-09-01' existing = {
  name: producerPublicIpName
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
  tags: tags
}

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2024-01-01' = {
  name: serviceBusNamespaceName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
  properties: {
    disableLocalAuth: true
    minimumTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
  }
}

resource serviceBusNetworkRules 'Microsoft.ServiceBus/namespaces/networkRuleSets@2024-01-01' = {
  parent: serviceBusNamespace
  name: 'default'
  properties: {
    publicNetworkAccess: 'Enabled'
    defaultAction: 'Deny'
    trustedServiceAccessEnabled: false
    virtualNetworkRules: []
    ipRules: [
      {
        ipMask: producerPublicIp.properties.ipAddress
        action: 'Allow'
      }
      {
        ipMask: publicIp.properties.ipAddress
        action: 'Allow'
      }
    ]
  }
}

resource serviceBusQueue 'Microsoft.ServiceBus/namespaces/queues@2024-01-01' = {
  parent: serviceBusNamespace
  name: serviceBusQueueName
  properties: {
    status: 'Active'
    lockDuration: 'PT30S'
    defaultMessageTimeToLive: 'PT10S'
    deadLetteringOnMessageExpiration: true
    maxDeliveryCount: 3
    requiresDuplicateDetection: true
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    enableExpress: false
    enablePartitioning: false
    maxSizeInMegabytes: 1024
  }
}

resource producerServiceBusSender 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusQueue.id, producerIdentity.id, 'service-bus-data-sender')
  scope: serviceBusQueue
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '69a216fc-b8fb-44d8-bc22-1f3c2cd27a39')
    principalId: producerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource executorServiceBusReceiver 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusQueue.id, identity.id, 'service-bus-data-receiver')
  scope: serviceBusQueue
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4f6d3b9b-027b-4f4c-9142-0e5a2a2247e0')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource publicIp 'Microsoft.Network/publicIPAddresses@2023-09-01' = {
  name: publicIpName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Regional'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
    publicIPAddressVersion: 'IPv4'
    idleTimeoutInMinutes: 15
  }
}

resource natGateway 'Microsoft.Network/natGateways@2023-09-01' = {
  name: natName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
  }
  properties: {
    idleTimeoutInMinutes: 10
    publicIpAddresses: [
      {
        id: publicIp.id
      }
    ]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2023-09-01' = {
  name: vnetName
  location: location
  tags: tags
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.43.0.0/16'
      ]
    }
    subnets: [
      {
        name: 'container-apps-infrastructure'
        properties: {
          addressPrefix: '10.43.0.0/23'
          natGateway: {
            id: natGateway.id
          }
          delegations: [
            {
              name: 'Microsoft.App.environments'
              properties: {
                serviceName: 'Microsoft.App/environments'
              }
            }
          ]
        }
      }
    ]
  }
}

resource managedEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: environmentName
  location: location
  tags: tags
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalyticsWorkspace.properties.customerId
        sharedKey: logAnalyticsWorkspace.listKeys().primarySharedKey
      }
    }
    vnetConfiguration: {
      infrastructureSubnetId: resourceId('Microsoft.Network/virtualNetworks/subnets', vnet.name, 'container-apps-infrastructure')
      internal: false
    }
    workloadProfiles: [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
    ]
  }
}

resource privateKeySecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(polymarketPrivateKeySecret.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketPrivateKeySecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource apiKeySecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(polymarketApiKeySecret.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketApiKeySecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource apiSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(polymarketApiSecretSecret.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketApiSecretSecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource apiPassphraseSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(polymarketApiPassphraseSecret.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketApiPassphraseSecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource fundedEvidenceContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(fundedEvidenceContainer.id, identity.id, 'blob-data-contributor')
  scope: fundedEvidenceContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowEventsReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowEventsContainer.id, identity.id, 'blob-data-reader')
  scope: shadowEventsContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource researchReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, identity.id, 'blob-data-reader')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource modelReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(modelContainer.id, identity.id, 'blob-data-reader')
  scope: modelContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource acrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, identity.id, 'acr-pull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource originCheckJob 'Microsoft.App/jobs@2024-03-01' = {
  name: originCheckJobName
  location: location
  tags: union(tags, {
    operation: 'geoblock-preflight'
    fundedExecution: 'disabled'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  dependsOn: [
    acrPull
  ]
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 60
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
    }
    template: {
      containers: [
        {
          name: 'origin-check'
          image: venueProbeImage
          command: [
            'node'
            '-e'
            'fetch("https://polymarket.com/api/geoblock").then(r=>r.json()).then(v=>console.log(JSON.stringify(v)))'
          ]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
        }
      ]
    }
  }
}

resource fundedJob 'Microsoft.App/jobs@2024-03-01' = {
  name: fundedJobName
  location: location
  tags: union(tags, {
    trigger: 'manual-only'
    operation: 'funded-dynamic-quote-operator-direct'
    fundedExecution: 'disabled'
    dryRun: 'true'
    retiredReason: 'replaced-by-continuous-service'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  dependsOn: [
    privateKeySecretReader
    apiKeySecretReader
    apiSecretReader
    apiPassphraseSecretReader
    fundedEvidenceContributor
    shadowEventsReader
    researchReader
    modelReader
    acrPull
    serviceBusNetworkRules
    executorServiceBusReceiver
  ]
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 290
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
      secrets: [
        {
          name: 'polymarket-private-key'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-private-key'
          identity: identity.id
        }
        {
          name: 'polymarket-api-key'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-key'
          identity: identity.id
        }
        {
          name: 'polymarket-api-secret'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-secret'
          identity: identity.id
        }
        {
          name: 'polymarket-api-passphrase'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-passphrase'
          identity: identity.id
        }
      ]
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
    }
    template: {
      containers: [
        {
          name: 'funded-direct'
          image: venueProbeImage
          command: [
            'node'
            'src/funded-direct-worker.mjs'
          ]
          env: [
            { name: 'FUNDED_DIRECT_WORKER_ENABLED', value: 'false' }
            { name: 'ALLOW_FUNDED_DIRECT', value: 'false' }
            { name: 'FUNDED_DIRECT_DRY_RUN', value: 'true' }
            { name: 'FUNDED_DIRECT_MAX_ITERATIONS', value: '200' }
            { name: 'FUNDED_DIRECT_POLL_INTERVAL_MS', value: '1000' }
            { name: 'FUNDED_DIRECT_MAX_IDLE_MS', value: '240000' }
            { name: 'FUNDED_DIRECT_CONTROL_PREFIX', value: 'reports/funded/dynamic-quote' }
            { name: 'FUNDED_DIRECT_SESSION_MANIFEST_JSON', value: fundedDirectSessionManifestJson }
            { name: 'FUNDED_DIRECT_SESSION_MANIFEST_BLOB_NAME', value: fundedDirectSessionManifestBlobName }
            { name: 'FUNDED_DIRECT_SESSION_MANIFEST_SHA256', value: fundedDirectSessionManifestSha256 }
            { name: 'FUNDED_DIRECT_MIN_REMAINING_TTL_MS', value: '7000' }
            { name: 'FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS', value: '2000' }
            { name: 'FUNDED_EVIDENCE_TRUST_BOUNDARY_READY', value: 'false' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'ALLOW_STRATEGY_CANARY', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'STRATEGY_CANARY_INTENT_PREFIX', value: 'reports/research/venue-probe/control/strategy-canary/intents' }
            { name: 'STRATEGY_CANARY_INTENT_CONTAINER_NAME', value: shadowEventsContainer.name }
            { name: 'STRATEGY_CANARY_MANIFEST_CONTAINER_NAME', value: fundedEvidenceContainer.name }
            { name: 'STRATEGY_CANARY_CANDIDATE_NAME', value: 'dynamic_quote_style' }
            { name: 'STRATEGY_CANARY_CANDIDATE_VERSION', value: 'dynamic_quote_style@2026-06-14' }
            { name: 'STRATEGY_CANARY_CANDIDATE_CONFIG_HASH', value: 'sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c' }
            { name: 'STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION', value: 'conservative-execution-prior-v1' }
            { name: 'STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE', value: 'chainlink_reference' }
            { name: 'STRATEGY_INTENT_TARGET_ORDER_NOTIONAL', value: fundedDirectTargetOrderNotional }
            { name: 'STRATEGY_CANARY_MAX_ORDER_NOTIONAL', value: fundedDirectMaxOrderNotional }
            { name: 'STRATEGY_INTENT_MIN_SECONDS_TO_EXPIRY', value: '360' }
            { name: 'STRATEGY_INTENT_MAX_SECONDS_TO_EXPIRY', value: '900' }
            { name: 'STRATEGY_CANARY_MAX_REFERENCE_AGE_MS', value: '2000' }
            { name: 'STRATEGY_CANARY_MAX_BOOK_AGE_MS', value: '1000' }
            { name: 'STRATEGY_CANARY_REST_SECONDS', value: '30' }
            { name: 'MAX_OPEN_ORDERS', value: '1' }
            { name: 'VENUE_PROBE_FUNDED_CAMPAIGN_ID', value: fundedDirectCampaignId }
            { name: 'VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY', value: fundedDirectStartingCollateral }
            { name: 'VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR', value: '0' }
            { name: 'VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN', value: fundedDirectMaxAccountLoss }
            { name: 'VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY', value: '0.01' }
            { name: 'VENUE_PROBE_CAMPAIGN_CASH_FLOWS', value: '[]' }
            { name: 'VENUE_PROBE_MAX_CLOCK_DRIFT_MS', value: '5000' }
            { name: 'VENUE_PROBE_MAX_CLOCK_UNCERTAINTY_MS', value: '750' }
            { name: 'VENUE_PROBE_EXECUTION_ORIGIN', value: 'azure_chile_central_static_egress' }
            { name: 'VENUE_PROBE_EXPECTED_COUNTRY', value: expectedCountry }
            { name: 'VENUE_PROBE_EXPECTED_EGRESS_IP', value: publicIp.properties.ipAddress }
            { name: 'POLYMARKET_FUNDER_ADDRESS', value: funderAddress }
            { name: 'POLYMARKET_SIGNATURE_TYPE', value: '3' }
            { name: 'POLYMARKET_PRIVATE_KEY', secretRef: 'polymarket-private-key' }
            { name: 'POLYMARKET_API_KEY', secretRef: 'polymarket-api-key' }
            { name: 'POLYMARKET_API_SECRET', secretRef: 'polymarket-api-secret' }
            { name: 'POLYMARKET_API_PASSPHRASE', secretRef: 'polymarket-api-passphrase' }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: storage.name }
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: fundedEvidenceContainer.name }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
    }
  }
}

resource fundedService 'Microsoft.App/containerApps@2024-03-01' = {
  name: fundedServiceName
  location: location
  tags: union(tags, {
    trigger: 'continuous-min-replicas-one'
    operation: 'funded-dynamic-quote-operator-direct'
    fundedExecution: fundedDirectEnabled ? 'enabled' : 'disabled'
    dryRun: fundedDirectEnabled ? 'false' : 'true'
    publicIngress: 'disabled'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  dependsOn: [
    fundedJob
    privateKeySecretReader
    apiKeySecretReader
    apiSecretReader
    apiPassphraseSecretReader
    fundedEvidenceContributor
    shadowEventsReader
    researchReader
    modelReader
    acrPull
  ]
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
      secrets: [
        {
          name: 'polymarket-private-key'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-private-key'
          identity: identity.id
        }
        {
          name: 'polymarket-api-key'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-key'
          identity: identity.id
        }
        {
          name: 'polymarket-api-secret'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-secret'
          identity: identity.id
        }
        {
          name: 'polymarket-api-passphrase'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-passphrase'
          identity: identity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'funded-direct'
          image: venueProbeImage
          command: [
            'node'
            'src/funded-direct-service.mjs'
          ]
          env: [
            { name: 'FUNDED_DIRECT_SERVICE_ENABLED', value: fundedDirectEnabled ? 'true' : 'false' }
            { name: 'FUNDED_DIRECT_ENGINE', value: 'persistent_v1' }
            { name: 'FUNDED_DIRECT_SERVICE_BUS_ENABLED', value: 'true' }
            { name: 'FUNDED_DIRECT_SERVICE_BUS_NAMESPACE', value: serviceBusNamespace.name }
            { name: 'FUNDED_DIRECT_SERVICE_BUS_QUEUE', value: serviceBusQueue.name }
            { name: 'FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS', value: '2000' }
            { name: 'FUNDED_DIRECT_SERVICE_RESTART_DELAY_MS', value: '1000' }
            { name: 'FUNDED_DIRECT_SERVICE_RISK_PAUSE_MS', value: '60000' }
            { name: 'FUNDED_DIRECT_SERVICE_HEARTBEAT_MS', value: '60000' }
            { name: 'FUNDED_DIRECT_SERVICE_MAX_CYCLES', value: '0' }
            { name: 'FUNDED_DIRECT_WORKER_ENABLED', value: fundedDirectEnabled ? 'true' : 'false' }
            { name: 'ALLOW_FUNDED_DIRECT', value: fundedDirectEnabled ? 'true' : 'false' }
            { name: 'FUNDED_DIRECT_DRY_RUN', value: fundedDirectEnabled ? 'false' : 'true' }
            { name: 'FUNDED_DIRECT_MAX_ITERATIONS', value: '2000' }
            { name: 'FUNDED_DIRECT_POLL_INTERVAL_MS', value: '1000' }
            { name: 'FUNDED_DIRECT_MAX_IDLE_MS', value: '3600000' }
            { name: 'FUNDED_DIRECT_CONTROL_PREFIX', value: 'reports/funded/dynamic-quote' }
            { name: 'FUNDED_DIRECT_SESSION_MANIFEST_JSON', value: fundedDirectSessionManifestJson }
            { name: 'FUNDED_DIRECT_SESSION_MANIFEST_BLOB_NAME', value: fundedDirectSessionManifestBlobName }
            { name: 'FUNDED_DIRECT_SESSION_MANIFEST_SHA256', value: fundedDirectSessionManifestSha256 }
            { name: 'FUNDED_DIRECT_MIN_REMAINING_TTL_MS', value: '7000' }
            { name: 'FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS', value: '2000' }
            { name: 'FUNDED_EVIDENCE_TRUST_BOUNDARY_READY', value: 'false' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'ALLOW_STRATEGY_CANARY', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'STRATEGY_CANARY_INTENT_PREFIX', value: 'reports/research/venue-probe/control/strategy-canary/intents' }
            { name: 'STRATEGY_CANARY_INTENT_CONTAINER_NAME', value: shadowEventsContainer.name }
            { name: 'STRATEGY_CANARY_MANIFEST_CONTAINER_NAME', value: fundedEvidenceContainer.name }
            { name: 'STRATEGY_CANARY_CANDIDATE_NAME', value: 'dynamic_quote_style' }
            { name: 'STRATEGY_CANARY_CANDIDATE_VERSION', value: 'dynamic_quote_style@2026-06-14' }
            { name: 'STRATEGY_CANARY_CANDIDATE_CONFIG_HASH', value: 'sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c' }
            { name: 'STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION', value: 'conservative-execution-prior-v1' }
            { name: 'STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE', value: 'chainlink_reference' }
            { name: 'STRATEGY_INTENT_TARGET_ORDER_NOTIONAL', value: fundedDirectTargetOrderNotional }
            { name: 'STRATEGY_CANARY_MAX_ORDER_NOTIONAL', value: fundedDirectMaxOrderNotional }
            { name: 'STRATEGY_INTENT_MIN_SECONDS_TO_EXPIRY', value: '360' }
            { name: 'STRATEGY_INTENT_MAX_SECONDS_TO_EXPIRY', value: '900' }
            { name: 'STRATEGY_CANARY_MAX_REFERENCE_AGE_MS', value: '2000' }
            { name: 'STRATEGY_CANARY_MAX_BOOK_AGE_MS', value: '1000' }
            { name: 'STRATEGY_CANARY_REST_SECONDS', value: '30' }
            { name: 'MAX_OPEN_ORDERS', value: '1' }
            { name: 'VENUE_PROBE_FUNDED_CAMPAIGN_ID', value: fundedDirectCampaignId }
            { name: 'VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY', value: fundedDirectStartingCollateral }
            { name: 'VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR', value: '0' }
            { name: 'VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN', value: fundedDirectMaxAccountLoss }
            { name: 'VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY', value: '0.01' }
            { name: 'VENUE_PROBE_CAMPAIGN_CASH_FLOWS', value: '[]' }
            { name: 'VENUE_PROBE_MAX_CLOCK_DRIFT_MS', value: '5000' }
            { name: 'VENUE_PROBE_MAX_CLOCK_UNCERTAINTY_MS', value: '750' }
            { name: 'VENUE_PROBE_EXECUTION_ORIGIN', value: 'azure_chile_central_static_egress' }
            { name: 'VENUE_PROBE_EXPECTED_COUNTRY', value: expectedCountry }
            { name: 'VENUE_PROBE_EXPECTED_EGRESS_IP', value: publicIp.properties.ipAddress }
            { name: 'POLYMARKET_FUNDER_ADDRESS', value: funderAddress }
            { name: 'POLYMARKET_SIGNATURE_TYPE', value: '3' }
            { name: 'POLYMARKET_PRIVATE_KEY', secretRef: 'polymarket-private-key' }
            { name: 'POLYMARKET_API_KEY', secretRef: 'polymarket-api-key' }
            { name: 'POLYMARKET_API_SECRET', secretRef: 'polymarket-api-secret' }
            { name: 'POLYMARKET_API_PASSPHRASE', secretRef: 'polymarket-api-passphrase' }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: storage.name }
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: fundedEvidenceContainer.name }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: fundedDirectEnabled ? 1 : 0
        maxReplicas: 1
      }
    }
  }
}

resource fundedWarmupJob 'Microsoft.App/jobs@2024-03-01' = {
  name: fundedWarmupJobName
  location: location
  tags: union(tags, {
    trigger: 'manual-only'
    operation: 'funded-cloud-no-sign-rehearsal'
    fundedExecution: 'disabled'
    noSign: 'true'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${producerIdentity.id}': {}
    }
  }
  dependsOn: [
    producerServiceBusSender
    serviceBusNetworkRules
  ]
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 1
      replicaTimeout: 60
      registries: [
        {
          server: registry.properties.loginServer
          identity: producerIdentity.id
        }
      ]
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
    }
    template: {
      containers: [
        {
          name: 'funded-warmup'
          image: venueProbeImage
          command: [
            'node'
            'src/funded-direct-warmup.mjs'
          ]
          env: [
            { name: 'FUNDED_DIRECT_SERVICE_BUS_NAMESPACE', value: serviceBusNamespace.name }
            { name: 'FUNDED_DIRECT_SERVICE_BUS_QUEUE', value: serviceBusQueue.name }
            { name: 'AZURE_CLIENT_ID', value: producerIdentity.properties.clientId }
          ]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
        }
      ]
    }
  }
}

resource fundedServiceLogAlert 'Microsoft.Insights/scheduledQueryRules@2022-06-15' = {
  name: 'polyedge-funded-direct-safety-or-latency'
  location: location
  kind: 'LogAlert'
  tags: tags
  properties: {
    displayName: 'PolyEdge funded executor safety or latency alert'
    description: 'Funded execution paused, breached latency, lost WebSocket continuity, or failed a durable safety invariant.'
    severity: 0
    enabled: true
    scopes: [
      logAnalyticsWorkspace.id
    ]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    autoMitigate: false
    criteria: {
      allOf: [
        {
          query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s == "${fundedServiceName}" | where Log_s has_any ("rolling_p95_slo_breached\\":true", "engine_paused_by_consecutive_latency_breaches", "websocket", "reconnect reconciliation", "dead letter", "authorization", "reservation", "duplicate", "paused_by_account_risk_state", "equity", "cash flow", "position", "open order") | where Log_s !has "\\"status\\":\\"persistent_service_heartbeat\\""'
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 0
          failingPeriods: {
            numberOfEvaluationPeriods: 1
            minFailingPeriodsToAlert: 1
          }
        }
      ]
    }
    actions: {
      actionGroups: [
        alertActionGroup.id
      ]
      customProperties: {
        funded_app: fundedServiceName
        service_bus_queue: serviceBusQueue.name
        recommended_action: 'Keep funded execution paused; reconcile orders, trades, authorization, reservation, account equity, and WebSocket continuity.'
      }
    }
  }
}

resource serviceBusDeadLetterAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'polyedge-funded-direct-dead-letters'
  location: 'global'
  tags: tags
  properties: {
    description: 'Funded intent queue has one or more dead-lettered messages.'
    severity: 0
    enabled: true
    scopes: [
      serviceBusNamespace.id
    ]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    autoMitigate: false
    targetResourceType: 'Microsoft.ServiceBus/namespaces'
    targetResourceRegion: location
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: 'DeadletteredMessages'
          metricName: 'DeadletteredMessages'
          metricNamespace: 'Microsoft.ServiceBus/namespaces'
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Maximum'
          criterionType: 'StaticThresholdCriterion'
          dimensions: [
            {
              name: 'EntityName'
              operator: 'Include'
              values: [
                serviceBusQueue.name
              ]
            }
          ]
        }
      ]
    }
    actions: [
      {
        actionGroupId: alertActionGroup.id
      }
    ]
  }
}

output environmentName string = managedEnvironment.name
output originCheckJobName string = originCheckJob.name
output fundedJobName string = fundedJob.name
output fundedServiceName string = fundedService.name
output fundedWarmupJobName string = fundedWarmupJob.name
output staticEgressIp string = publicIp.properties.ipAddress
output fundedIdentityClientId string = identity.properties.clientId
output serviceBusNamespaceName string = serviceBusNamespace.name
output serviceBusQueueName string = serviceBusQueue.name
