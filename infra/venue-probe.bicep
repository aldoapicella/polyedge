targetScope = 'resourceGroup'

@description('Azure region for the isolated authenticated execution worker.')
param location string = 'northeurope'

param venueProbeImage string
@description('Rust backend image for the credential-free continuous shadow strategy runtime.')
param backendImage string
param registryName string = 'crpolyedge6urdjr5nmwx7w'
param storageAccountName string = 'stpolyedge6urdjr5nmwx7w'
param shadowEventsContainerName string = 'polyedge-shadow-events'
param researchContainerName string = 'polyedge-research'
param fundedEvidenceContainerName string = 'polyedge-funded-evidence'
param modelContainerName string = 'polyedge-models'
param keyVaultName string = 'kvpolyedge6urdjr5nmwx7w'
param logAnalyticsWorkspaceName string = 'log-polyedge-dev-6urdjr5nmwx7w'
param funderAddress string = '0x3d701b05d7c36aFaB01a06Fd26eBe789c0B7baD8'
param relayerApiKeyAddress string = '0xc9f6f0D01e5eEf2446819Ce21C4f1F9b688A9921'
@description('Set true only after polymarket-relayer-api-key exists in Key Vault.')
param relayerApiKeySecretConfigured bool = false

var environmentName = 'polyedge-venue-neu-env'
var identityName = 'polyedge-venue-neu-id'
var shadowIdentityName = 'polyedge-shadow-neu-id'
var shadowResearchIdentityName = 'polyedge-shadow-research-neu-id'
var promotionTransitionIdentityName = 'polyedge-promotion-transition-neu-id'
var jobName = 'polyedge-venue-probe-neu-job'
var strategyCanaryJobName = 'polyedge-strategy-canary-neu-job'
var fundedLadderJobName = 'polyedge-funded-ladder-neu-job'
var promotionTransitionJobName = 'polyedge-promotion-neu-job'
var redemptionJobName = 'polyedge-redeem-neu-job'
var shadowAppName = 'polyedge-shadow-neu'
var shadowDailyJobName = 'polyedge-shadow-daily-neu-job'
var vnetName = 'vnet-polyedge-venue-neu'
var natName = 'nat-polyedge-venue-neu'
var publicIpName = 'pip-polyedge-venue-neu-egress'
var tags = {
  app: 'polyedge'
  environment: 'dev'
  managedBy: 'bicep'
  workload: 'authenticated-venue-evidence'
  executionRegion: 'northeurope'
  paperStrategyRuntime: 'true'
}

var conservativePriorVersion = 'conservative-execution-prior-v1'
var conservativePriorSha256 = 'sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4'
var conservativePriorBlobName = 'reports/research/venue-probe/models/${conservativePriorVersion}-${substring(conservativePriorSha256, 7, 64)}.json'
var conservativePriorBlobUri = 'azure://${storageAccountName}/${researchContainerName}/${conservativePriorBlobName}'

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

resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' existing = {
  parent: storage
  name: 'default'
}

resource shadowEventIndexTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: 'ShadowBotEventIndex'
}

resource shadowChartSeriesTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: 'ShadowBotChartSeries'
}

resource shadowMarketCatalogTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: 'ShadowBotMarketCatalog'
}

resource shadowEventsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: shadowEventsContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource researchContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: researchContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource fundedEvidenceContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: fundedEvidenceContainerName
  properties: {
    publicAccess: 'None'
  }
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

resource polymarketRelayerApiKeySecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = if (relayerApiKeySecretConfigured) {
  parent: keyVault
  name: 'polymarket-relayer-api-key'
}

resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = {
  name: logAnalyticsWorkspaceName
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
  tags: tags
}

// Deliberately isolated from the funded worker identity. This principal writes
// only shadow events, reads only canonical/model control needed for post-100
// intent binding, and pulls images. It never receives Key Vault access or a
// funded-control write role and therefore cannot resolve or spend credentials.
resource shadowIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: shadowIdentityName
  location: location
  tags: union(tags, {
    workload: 'profitability-shadow'
    walletCredentials: 'absent'
  })
}

// Research publishing is isolated from both the event writer and the funded
// worker. It can read immutable shadow events and write derived research, but
// has neither venue credentials nor write access to funded control/evidence.
resource shadowResearchIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: shadowResearchIdentityName
  location: location
  tags: union(tags, {
    workload: 'profitability-shadow-research'
    walletCredentials: 'absent'
  })
}

// Canonical state transitions never need venue credentials. This identity can
// read the evidence/model inputs and atomically update funded control state,
// but has no Key Vault role and cannot submit an order.
resource promotionTransitionIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: promotionTransitionIdentityName
  location: location
  tags: union(tags, {
    workload: 'promotion-state-transition'
    walletCredentials: 'absent'
  })
}

resource publicIp 'Microsoft.Network/publicIPAddresses@2023-09-01' = {
  name: publicIpName
  location: location
  zones: [
    '1'
    '2'
    '3'
  ]
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
        '10.42.0.0/16'
      ]
    }
    subnets: [
      {
        name: 'container-apps-infrastructure'
        properties: {
          addressPrefix: '10.42.0.0/23'
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

resource fundedPrivateKeySecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(polymarketPrivateKeySecret.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketPrivateKeySecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource fundedApiKeySecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(polymarketApiKeySecret.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketApiKeySecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource fundedApiSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(polymarketApiSecretSecret.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketApiSecretSecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource fundedApiPassphraseSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(polymarketApiPassphraseSecret.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketApiPassphraseSecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource fundedRelayerSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (relayerApiKeySecretConfigured) {
  name: guid(polymarketRelayerApiKeySecret!.id, identity.id, 'key-vault-secrets-user')
  scope: polymarketRelayerApiKeySecret
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource blobDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(fundedEvidenceContainer.id, identity.id, 'blob-data-contributor')
  scope: fundedEvidenceContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource fundedResearchBlobReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, identity.id, 'blob-data-reader')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource fundedShadowEventsBlobReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowEventsContainer.id, identity.id, 'blob-data-reader')
  scope: shadowEventsContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource fundedModelBlobReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
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

resource shadowEventsBlobDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowEventsContainer.id, shadowIdentity.id, 'blob-data-contributor')
  scope: shadowEventsContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// The credential-free paper runtime reads canonical post-checkpoint-100
// control and its exact immutable model. Both scopes are read-only; it still
// has no funded write role, Key Vault role, or live execution capability.
resource shadowFundedControlBlobReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(fundedEvidenceContainer.id, shadowIdentity.id, 'blob-data-reader')
  scope: fundedEvidenceContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowModelBlobReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(modelContainer.id, shadowIdentity.id, 'blob-data-reader')
  scope: modelContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}


resource shadowResearchEventsReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowEventsContainer.id, shadowResearchIdentity.id, 'blob-data-reader')
  scope: shadowEventsContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: shadowResearchIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowResearchBlobDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, shadowResearchIdentity.id, 'blob-data-contributor')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: shadowResearchIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowEventIndexTableContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowEventIndexTable.id, shadowIdentity.id, 'table-data-contributor')
  scope: shadowEventIndexTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowChartSeriesTableContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowChartSeriesTable.id, shadowIdentity.id, 'table-data-contributor')
  scope: shadowChartSeriesTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowMarketCatalogTableContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowMarketCatalogTable.id, shadowIdentity.id, 'table-data-contributor')
  scope: shadowMarketCatalogTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, shadowIdentity.id, 'acr-pull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}


resource shadowResearchAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, shadowResearchIdentity.id, 'acr-pull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: shadowResearchIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource promotionTransitionResearchReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, promotionTransitionIdentity.id, 'blob-data-reader')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: promotionTransitionIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource promotionTransitionFundedContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(fundedEvidenceContainer.id, promotionTransitionIdentity.id, 'blob-data-contributor')
  scope: fundedEvidenceContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: promotionTransitionIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource promotionTransitionModelReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(modelContainer.id, promotionTransitionIdentity.id, 'blob-data-reader')
  scope: modelContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: promotionTransitionIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource promotionTransitionAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, promotionTransitionIdentity.id, 'acr-pull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: promotionTransitionIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: shadowAppName
  location: location
  tags: union(tags, {
    workload: 'profitability-shadow'
    publicIngress: 'disabled'
    walletCredentials: 'absent'
    fundedExecution: 'disabled'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${shadowIdentity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: [
        {
          server: registry.properties.loginServer
          identity: shadowIdentity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'shadow-runtime'
          image: backendImage
          env: [
            { name: 'APP_NAME', value: 'polyedge-shadow-neu' }
            { name: 'RUNTIME_ROLE', value: 'profitability_shadow' }
            { name: 'EXECUTION_MODE', value: 'paper' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'RUN_BOT_ON_STARTUP', value: 'true' }
            { name: 'REQUIRE_API_AUTH', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'ALLOW_EMERGENCY_ACCOUNT_CANCEL', value: 'false' }
            { name: 'PAPER_MAKER_FILL_POLICY', value: 'none' }
            { name: 'PAPER_ORDER_LIVE_AFTER_MS', value: '250' }
            { name: 'ADAPTIVE_REGIME_ENABLED', value: 'true' }
            { name: 'ADAPTIVE_REGIME_MODE', value: 'dynamic_quote_style' }
            { name: 'BASE_ORDER_SIZE', value: '1' }
            { name: 'MAX_ORDER_SIZE', value: '1' }
            { name: 'MAX_POSITION_PER_MARKET', value: '1' }
            { name: 'MAX_TOTAL_POSITION', value: '1' }
            { name: 'MAX_DAILY_LOSS', value: '1' }
            { name: 'MAX_OPEN_ORDERS', value: '1' }
            { name: 'TARGET_ASSET', value: 'BTC' }
            { name: 'TARGET_ASSET_NAME', value: 'Bitcoin' }
            { name: 'TARGET_HORIZON', value: '15m' }
            { name: 'TARGET_CHAINLINK_SYMBOL', value: 'btc/usd' }
            { name: 'TARGET_BINANCE_SYMBOL', value: 'btcusdt' }
            { name: 'TARGET_COINBASE_PRODUCT_ID', value: 'BTC-USD' }
            { name: 'ENABLE_DIRECT_BINANCE_BOOK_TICKER', value: 'false' }
            { name: 'AZURE_CLIENT_ID', value: shadowIdentity.properties.clientId }
            { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: storage.name }
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: shadowEventsContainer.name }
            { name: 'AZURE_STORAGE_TABLE_NAME', value: shadowEventIndexTable.name }
            { name: 'AZURE_CHART_TABLE_NAME', value: shadowChartSeriesTable.name }
            { name: 'AZURE_MARKET_TABLE_NAME', value: shadowMarketCatalogTable.name }
            { name: 'AZURE_FUNDED_STORAGE_CONTAINER_NAME', value: fundedEvidenceContainer.name }
            { name: 'AZURE_MODEL_STORAGE_CONTAINER_NAME', value: modelContainer.name }
            { name: 'AZURE_EVENT_BLOB_PREFIX', value: 'shadow-events/campaign-2026-07-12' }
            { name: 'COMPACT_SHADOW_RECORDING', value: 'true' }
            { name: 'SHADOW_BOOK_SAMPLE_MS', value: '1000' }
            { name: 'PUBLISH_STRATEGY_CANARY_INTENTS', value: 'true' }
            { name: 'STRATEGY_CANARY_INTENT_PREFIX', value: 'reports/research/venue-probe/control/strategy-canary/intents' }
            { name: 'STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION', value: conservativePriorVersion }
            { name: 'STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI', value: conservativePriorBlobUri }
            { name: 'STRATEGY_CANARY_EXECUTION_MODEL_SHA256', value: conservativePriorSha256 }
            { name: 'AZURE_EVENT_INDEX_TYPES', value: 'runtime_provenance,market,market_start_price,paper_settlement,fair_value,decision,execution_report,feed_error,reference' }
          ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

resource shadowDailyJob 'Microsoft.App/jobs@2024-03-01' = {
  name: shadowDailyJobName
  location: location
  tags: union(tags, {
    workload: 'profitability-shadow-daily'
    fundedExecution: 'disabled'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${shadowResearchIdentity.id}': {}
    }
  }
  dependsOn: [
    shadowResearchAcrPull
  ]
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Schedule'
      replicaRetryLimit: 1
      replicaTimeout: 43200
      scheduleTriggerConfig: {
        cronExpression: '15 2 * * *'
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: shadowResearchIdentity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'shadow-daily'
          image: backendImage
          command: [
            '/bin/sh'
            '/app/research/run_shadow_daily.sh'
          ]
          args: []
          env: [
            { name: 'APP_NAME', value: 'polyedge-shadow-neu' }
            { name: 'EXECUTION_MODE', value: 'paper' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'RUN_BOT_ON_STARTUP', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'AZURE_CLIENT_ID', value: shadowResearchIdentity.properties.clientId }
            { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: storage.name }
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: researchContainer.name }
            { name: 'SHADOW_SOURCE_CONTAINER_NAME', value: shadowEventsContainer.name }
            { name: 'SHADOW_EXECUTION_MODEL_BLOB_URI', value: conservativePriorBlobUri }
            { name: 'SHADOW_EXECUTION_MODEL_BLOB_NAME', value: conservativePriorBlobName }
            { name: 'SHADOW_EXECUTION_MODEL_SHA256', value: conservativePriorSha256 }
          ]
          resources: {
            cpu: json('4')
            memory: '8Gi'
          }
        }
      ]
    }
  }
}

resource venueProbeJob 'Microsoft.App/jobs@2024-03-01' = {
  name: jobName
  location: location
  tags: union(tags, {
    trigger: 'manual-only'
    takerOrders: 'disabled'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 3600
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
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
          name: 'venue-probe'
          image: venueProbeImage
          env: [
            { name: 'EXECUTION_MODE', value: 'venue_probe' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'ALLOW_VENUE_PROBE', value: 'true' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'MAX_OPEN_ORDERS', value: '1' }
            { name: 'MAX_DAILY_LOSS', value: '1' }
            { name: 'VENUE_PROBE_CAMPAIGN_ENABLED', value: 'true' }
            { name: 'VENUE_PROBE_MAXIMUM_ORDERS', value: '25' }
            { name: 'VENUE_PROBE_MAX_ORDER_NOTIONAL', value: '1' }
            { name: 'VENUE_PROBE_MIN_ORDER_NOTIONAL', value: '1' }
            { name: 'VENUE_PROBE_STARTING_CAPITAL', value: '9.23' }
            { name: 'VENUE_PROBE_FUNDED_CAMPAIGN_ID', value: 'funded-campaign-2026-07-12' }
            { name: 'VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY', value: '5.030521' }
            { name: 'VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR', value: '4.03' }
            { name: 'VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN', value: '1' }
            { name: 'VENUE_PROBE_MIN_ORDER_PRICE', value: '0.05' }
            { name: 'VENUE_PROBE_REST_HORIZONS_SECONDS', value: '1,5,30,60' }
            { name: 'VENUE_PROBE_INTER_ORDER_DELAY_MS', value: '1000' }
            { name: 'VENUE_PROBE_MAX_CLOCK_DRIFT_MS', value: '5000' }
            { name: 'VENUE_PROBE_MAX_CLOCK_UNCERTAINTY_MS', value: '750' }
            { name: 'VENUE_PROBE_KILL_SWITCH', value: 'false' }
            { name: 'VENUE_PROBE_DRY_RUN', value: 'true' }
            { name: 'FUNDED_EVIDENCE_TRUST_BOUNDARY_READY', value: 'false' }
            { name: 'VENUE_PROBE_EXPECTED_COUNTRY', value: 'IE' }
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

resource strategyCanaryJob 'Microsoft.App/jobs@2024-03-01' = {
  name: strategyCanaryJobName
  location: location
  tags: union(tags, {
    trigger: 'manual-only'
    workload: 'strategy-qualified-canary'
    fundedExecution: 'disabled'
    takerOrders: 'disabled'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 3600
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
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
          name: 'strategy-canary'
          image: venueProbeImage
          command: [
            'node'
            'src/canary-controller.mjs'
          ]
          env: [
            { name: 'EXECUTION_MODE', value: 'strategy_canary' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'ALLOW_STRATEGY_CANARY', value: 'false' }
            { name: 'STRATEGY_CANARY_CONTROLLER_ENABLED', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'STRATEGY_CANARY_DRY_RUN', value: 'true' }
            { name: 'FUNDED_EVIDENCE_TRUST_BOUNDARY_READY', value: 'false' }
            { name: 'STRATEGY_CANARY_HUMAN_GRANT_BLOB_NAME', value: '' }
            { name: 'STRATEGY_CANARY_HUMAN_GRANT_SHA256', value: '' }
            { name: 'STRATEGY_CANARY_INTENT_PREFIX', value: 'reports/research/venue-probe/control/strategy-canary/intents' }
            { name: 'STRATEGY_CANARY_INTENT_CONTAINER_NAME', value: shadowEventsContainer.name }
            { name: 'STRATEGY_CANARY_MANIFEST_CONTAINER_NAME', value: researchContainer.name }
            { name: 'STRATEGY_CANARY_CONTROLLER_MAX_WAIT_SECONDS', value: '300' }
            { name: 'STRATEGY_CANARY_CONTROLLER_POLL_INTERVAL_MS', value: '5000' }
            { name: 'STRATEGY_CANARY_INTENT_BLOB_NAME', value: '' }
            { name: 'STRATEGY_CANARY_INTENT_SHA256', value: '' }
            { name: 'STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME', value: '' }
            { name: 'STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256', value: '' }
            { name: 'STRATEGY_CANARY_AUTHORIZATION_BLOB_NAME', value: '' }
            { name: 'STRATEGY_CANARY_AUTHORIZATION_SHA256', value: '' }
            { name: 'STRATEGY_CANARY_CANDIDATE_NAME', value: 'dynamic_quote_style' }
            { name: 'STRATEGY_CANARY_CANDIDATE_VERSION', value: 'dynamic_quote_style@2026-06-14' }
            { name: 'STRATEGY_CANARY_CANDIDATE_CONFIG_HASH', value: 'sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c' }
            { name: 'STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION', value: 'conservative-execution-prior-v1' }
            { name: 'STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE', value: 'chainlink_reference' }
            { name: 'STRATEGY_CANARY_MAX_ORDER_NOTIONAL', value: '1' }
            { name: 'STRATEGY_CANARY_MAX_REFERENCE_AGE_MS', value: '2000' }
            { name: 'STRATEGY_CANARY_MAX_BOOK_AGE_MS', value: '1000' }
            // The effective rest remains bounded by the exact strategy intent's
            // valid_until safety boundary and frozen 30-second intent TTL.
            { name: 'STRATEGY_CANARY_REST_SECONDS', value: '30' }
            { name: 'MAX_OPEN_ORDERS', value: '1' }
            { name: 'VENUE_PROBE_STARTING_CAPITAL', value: '9.23' }
            { name: 'VENUE_PROBE_FUNDED_CAMPAIGN_ID', value: 'funded-campaign-2026-07-12' }
            { name: 'VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY', value: '5.030521' }
            { name: 'VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR', value: '4.03' }
            { name: 'VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN', value: '1' }
            { name: 'VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY', value: '0.01' }
            { name: 'VENUE_PROBE_MAX_CLOCK_DRIFT_MS', value: '5000' }
            { name: 'VENUE_PROBE_MAX_CLOCK_UNCERTAINTY_MS', value: '750' }
            { name: 'VENUE_PROBE_EXPECTED_COUNTRY', value: 'IE' }
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

resource fundedLadderJob 'Microsoft.App/jobs@2024-03-01' = {
  name: fundedLadderJobName
  location: location
  tags: union(tags, {
    trigger: 'manual-only'
    operation: 'funded-ladder-5-25-100-200'
    fundedExecution: 'disabled'
    dryRun: 'true'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${identity.id}': {} }
  }
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 3600
      manualTriggerConfig: { parallelism: 1, replicaCompletionCount: 1 }
      registries: [{ server: registry.properties.loginServer, identity: identity.id }]
      secrets: [
        { name: 'polymarket-private-key', keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-private-key', identity: identity.id }
        { name: 'polymarket-api-key', keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-key', identity: identity.id }
        { name: 'polymarket-api-secret', keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-secret', identity: identity.id }
        { name: 'polymarket-api-passphrase', keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-api-passphrase', identity: identity.id }
      ]
    }
    template: {
      containers: [{
        name: 'funded-ladder'
        image: venueProbeImage
        command: ['node', 'src/funded-ladder-controller.mjs']
        env: [
          { name: 'FUNDED_LADDER_CONTROLLER_ENABLED', value: 'false' }
          { name: 'ALLOW_FUNDED_LADDER', value: 'false' }
          { name: 'FUNDED_LADDER_DRY_RUN', value: 'true' }
          { name: 'FUNDED_EVIDENCE_TRUST_BOUNDARY_READY', value: 'false' }
          { name: 'ALLOW_LIVE', value: 'false' }
          { name: 'ALLOW_STRATEGY_CANARY', value: 'false' }
          { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
          { name: 'FUNDED_LADDER_MANIFEST_BLOB_NAME', value: '' }
          { name: 'FUNDED_LADDER_MANIFEST_SHA256', value: '' }
          { name: 'FUNDED_LADDER_GRANT_BLOB_NAME', value: '' }
          { name: 'FUNDED_LADDER_GRANT_SHA256', value: '' }
          { name: 'FUNDED_LADDER_CONSUMPTION_BLOB_NAME', value: '' }
          { name: 'FUNDED_LADDER_CONSUMPTION_SHA256', value: '' }
          { name: 'FUNDED_LADDER_CONTROL_PREFIX', value: 'reports/research/venue-probe/control/funded-ladder' }
          { name: 'FUNDED_LADDER_RESEARCH_CONTAINER_NAME', value: researchContainer.name }
          { name: 'FUNDED_LADDER_INTENT_CONTAINER_NAME', value: shadowEventsContainer.name }
          { name: 'STRATEGY_CANARY_INTENT_PREFIX', value: 'reports/research/venue-probe/control/strategy-canary/intents' }
          { name: 'STRATEGY_CANARY_INTENT_CONTAINER_NAME', value: shadowEventsContainer.name }
          { name: 'STRATEGY_CANARY_MANIFEST_CONTAINER_NAME', value: fundedEvidenceContainer.name }
          { name: 'STRATEGY_CANARY_CANDIDATE_NAME', value: 'dynamic_quote_style' }
          { name: 'STRATEGY_CANARY_CANDIDATE_VERSION', value: 'dynamic_quote_style@2026-06-14' }
          { name: 'STRATEGY_CANARY_CANDIDATE_CONFIG_HASH', value: 'sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c' }
          { name: 'STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION', value: 'conservative-execution-prior-v1' }
          { name: 'STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE', value: 'chainlink_reference' }
          { name: 'STRATEGY_CANARY_MAX_ORDER_NOTIONAL', value: '1' }
          { name: 'STRATEGY_CANARY_MAX_REFERENCE_AGE_MS', value: '2000' }
          { name: 'STRATEGY_CANARY_MAX_BOOK_AGE_MS', value: '1000' }
          { name: 'STRATEGY_CANARY_REST_SECONDS', value: '30' }
          { name: 'MAX_OPEN_ORDERS', value: '1' }
          { name: 'VENUE_PROBE_FUNDED_CAMPAIGN_ID', value: 'funded-campaign-2026-07-12' }
          { name: 'VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY', value: '5.030521' }
          { name: 'VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR', value: '4.03' }
          { name: 'VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN', value: '1' }
          { name: 'VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY', value: '0.01' }
          { name: 'VENUE_PROBE_MAX_CLOCK_DRIFT_MS', value: '5000' }
          { name: 'VENUE_PROBE_MAX_CLOCK_UNCERTAINTY_MS', value: '750' }
          { name: 'VENUE_PROBE_EXPECTED_COUNTRY', value: 'IE' }
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
        resources: { cpu: json('0.5'), memory: '1Gi' }
      }]
    }
  }
}

resource promotionTransitionJob 'Microsoft.App/jobs@2024-03-01' = {
  name: promotionTransitionJobName
  location: location
  tags: union(tags, {
    trigger: 'manual-only'
    operation: 'validated-profitability-state-transition'
    fundedExecution: 'disabled'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${promotionTransitionIdentity.id}': {}
    }
  }
  dependsOn: [
    promotionTransitionAcrPull
  ]
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 3600
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: promotionTransitionIdentity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'promotion-transition'
          image: backendImage
          command: [
            '/bin/sh'
            '/app/research/run_promotion_transition.sh'
          ]
          env: [
            { name: 'PROMOTION_TRANSITION_ENABLED', value: 'false' }
            { name: 'PROMOTION_TRANSITION_MODE', value: '' }
            { name: 'PROMOTION_TRANSITION_EXPECTED_CANONICAL_SHA256', value: '' }
            { name: 'PROMOTION_OUTPUT_BLOB_NAME', value: 'reports/research/profitability/latest.json' }
            { name: 'PROMOTION_SHADOW_MANIFEST_URI', value: '' }
            { name: 'PROMOTION_SHADOW_MANIFEST_SHA256', value: '' }
            { name: 'PROMOTION_CANARY_EVIDENCE_URI', value: '' }
            { name: 'PROMOTION_CANARY_EVIDENCE_BLOB_NAME', value: '' }
            { name: 'PROMOTION_CANARY_EVIDENCE_SHA256', value: '' }
            { name: 'PROMOTION_CANARY_CONSUMPTION_URI', value: '' }
            { name: 'PROMOTION_CANARY_CONSUMPTION_SHA256', value: '' }
            { name: 'PROMOTION_TERMINAL_EVIDENCE_URI', value: '' }
            { name: 'PROMOTION_TERMINAL_EVIDENCE_BLOB_NAME', value: '' }
            { name: 'PROMOTION_TERMINAL_EVIDENCE_SHA256', value: '' }
            { name: 'PROMOTION_PRIOR_MANIFEST_URI', value: '' }
            { name: 'PROMOTION_PRIOR_MANIFEST_SHA256', value: '' }
            { name: 'PROMOTION_CHECKPOINT_URI', value: '' }
            { name: 'PROMOTION_CHECKPOINT_SHA256', value: '' }
            { name: 'PROMOTION_STAGE_BLOCK_URI', value: '' }
            { name: 'PROMOTION_STAGE_BLOCK_SHA256', value: '' }
            { name: 'PROMOTION_NEXT_MODEL_URI', value: '' }
            { name: 'PROMOTION_NEXT_MODEL_SHA256', value: '' }
            { name: 'AZURE_CLIENT_ID', value: promotionTransitionIdentity.properties.clientId }
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

resource venueRedemptionJob 'Microsoft.App/jobs@2024-03-01' = {
  name: redemptionJobName
  location: location
  tags: union(tags, {
    trigger: 'manual-only'
    operation: 'resolved-position-redemption'
    takerOrders: 'disabled'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 600
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
      secrets: concat([
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
      ], relayerApiKeySecretConfigured ? [
        {
          name: 'polymarket-relayer-api-key'
          keyVaultUrl: '${keyVault.properties.vaultUri}secrets/polymarket-relayer-api-key'
          identity: identity.id
        }
      ] : [])
    }
    template: {
      containers: [
        {
          name: 'venue-redemption'
          image: venueProbeImage
          command: [
            'node'
            'src/redeem.mjs'
          ]
          env: concat([
            { name: 'EXECUTION_MODE', value: 'venue_redemption' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'VENUE_REDEMPTION_ENABLED', value: 'false' }
            { name: 'VENUE_REDEMPTION_DRY_RUN', value: 'true' }
            { name: 'FUNDED_EVIDENCE_TRUST_BOUNDARY_READY', value: 'false' }
            { name: 'VENUE_REDEMPTION_MAX_PAYOUT', value: '25' }
            { name: 'VENUE_REDEMPTION_MAX_CONDITIONS', value: '5' }
            { name: 'VENUE_PROBE_STARTING_CAPITAL', value: '9.23' }
            { name: 'VENUE_PROBE_EXPECTED_COUNTRY', value: 'IE' }
            { name: 'VENUE_PROBE_EXPECTED_EGRESS_IP', value: publicIp.properties.ipAddress }
            { name: 'POLYMARKET_FUNDER_ADDRESS', value: funderAddress }
            { name: 'POLYMARKET_SIGNATURE_TYPE', value: '3' }
            { name: 'POLYMARKET_RELAYER_API_KEY_ADDRESS', value: relayerApiKeyAddress }
            { name: 'POLYMARKET_PRIVATE_KEY', secretRef: 'polymarket-private-key' }
            { name: 'POLYMARKET_API_KEY', secretRef: 'polymarket-api-key' }
            { name: 'POLYMARKET_API_SECRET', secretRef: 'polymarket-api-secret' }
            { name: 'POLYMARKET_API_PASSPHRASE', secretRef: 'polymarket-api-passphrase' }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: storage.name }
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: fundedEvidenceContainer.name }
          ], relayerApiKeySecretConfigured ? [
            { name: 'POLYMARKET_RELAYER_API_KEY', secretRef: 'polymarket-relayer-api-key' }
          ] : [])
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
        }
      ]
    }
  }
}

output venueProbeJobName string = venueProbeJob.name
output strategyCanaryJobName string = strategyCanaryJob.name
output fundedLadderJobName string = fundedLadderJob.name
output promotionTransitionJobName string = promotionTransitionJob.name
output venueRedemptionJobName string = venueRedemptionJob.name
output shadowAppName string = shadowApp.name
output shadowDailyJobName string = shadowDailyJob.name
output managedEnvironmentName string = managedEnvironment.name
output managedIdentityName string = identity.name
output shadowManagedIdentityName string = shadowIdentity.name
output shadowResearchManagedIdentityName string = shadowResearchIdentity.name
output promotionTransitionManagedIdentityName string = promotionTransitionIdentity.name
output fundedManagedIdentityPrincipalId string = identity.properties.principalId
output shadowManagedIdentityPrincipalId string = shadowIdentity.properties.principalId
output shadowResearchManagedIdentityPrincipalId string = shadowResearchIdentity.properties.principalId
output promotionTransitionManagedIdentityPrincipalId string = promotionTransitionIdentity.properties.principalId
output shadowEventsContainerName string = shadowEventsContainer.name
output researchContainerName string = researchContainer.name
output fundedEvidenceContainerName string = fundedEvidenceContainer.name
output modelContainerName string = modelContainer.name
output conservativeExecutionPriorBlobName string = conservativePriorBlobName
output conservativeExecutionPriorSha256 string = conservativePriorSha256
output staticEgressIp string = publicIp.properties.ipAddress
output executionRegion string = location
