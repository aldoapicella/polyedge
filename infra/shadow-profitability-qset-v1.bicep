targetScope = 'resourceGroup'

@description('North Europe location of the existing isolated shadow environment.')
param location string = 'northeurope'

@description('Immutable backend image tag or digest for the qset-v1 campaign.')
param backendImage string

@description('Keep false through the first manually verified D+1 settlement-carry seal.')
param enableDailySchedule bool = false

@description('Create the qset-v1 freeze-control retention policy only during initial deployment.')
param initializeFreezeBoundary bool = true

@description('SHA-256 of the immutable source freeze manifest uploaded by the deployment workflow.')
@minLength(71)
@maxLength(71)
param codeFreezeSha256 string

@description('Relative blob path of the immutable source freeze manifest.')
param codeFreezeManifestPath string

param registryName string = 'crpolyedge6urdjr5nmwx7w'
param storageAccountName string = 'stpolyedge6urdjr5nmwx7w'
param environmentName string = 'polyedge-venue-neu-env'
param appIdentityName string = 'polyedge-dev-id'
param githubDeployIdentityName string = 'id-github-polyedge-dev'
param fundedEvidenceContainerName string = 'polyedge-funded-evidence'
param modelContainerName string = 'polyedge-models'

var campaignId = 'campaign-2026-07-28-qset-v1'
var campaignStart = '2026-07-28'
var campaignEventPrefix = 'shadow-events/${campaignId}'
var preflightEventPrefix = 'shadow-events/preflight/${campaignId}'
var campaignReportRoot = 'reports/research/shadow/campaigns/${campaignId}'
var campaignLeaseBlob = 'data/research/shadow/${campaignId}/control/replay.lock'
var campaignContract = 'research/configs/profitability_gate_v3_2026-07-28_qset_v1.yaml'
var shadowAppName = 'polyedge-shadow-qset-neu'
var shadowDailyJobName = 'polyedge-shadow-qset-neu-job'
var shadowIdentityName = 'polyedge-shadow-qset-neu-id'
var shadowResearchIdentityName = 'polyedge-qset-research-neu-id'
var shadowEventsContainerName = 'polyedge-shadow-qset-events'
var researchContainerName = 'polyedge-research-qset'
var freezeControlContainerName = 'polyedge-qset-control'
var eventIndexTableName = 'ShadowQsetEventIndex'
var chartSeriesTableName = 'ShadowQsetChartSeries'
var marketCatalogTableName = 'ShadowQsetMarketCatalog'
var conservativePriorVersion = 'conservative-execution-prior-v1'
var conservativePriorSha256 = 'sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4'
var conservativePriorBlobName = 'reports/research/venue-probe/models/${conservativePriorVersion}-${substring(conservativePriorSha256, 7, 64)}.json'
var conservativePriorBlobUri = 'azure://${storageAccountName}/${researchContainerName}/${conservativePriorBlobName}'
var tags = {
  app: 'polyedge'
  environment: 'dev'
  managedBy: 'bicep'
  workload: 'profitability-shadow-qset-v1'
  executionRegion: 'northeurope'
  paperStrategyRuntime: 'true'
  fundedExecution: 'disabled'
  campaignId: campaignId
  evidenceFrozen: 'true'
}
var dailyTriggerConfiguration = enableDailySchedule ? {
  triggerType: 'Schedule'
  scheduleTriggerConfig: {
    cronExpression: '15 2 * * *'
    parallelism: 1
    replicaCompletionCount: 1
  }
} : {
  triggerType: 'Manual'
  manualTriggerConfig: {
    parallelism: 1
    replicaCompletionCount: 1
  }
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

resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' existing = {
  parent: storage
  name: 'default'
}

resource managedEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' existing = {
  name: environmentName
}

resource appIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: appIdentityName
}

resource githubDeployIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: githubDeployIdentityName
}

resource shadowIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: shadowIdentityName
  location: location
  tags: union(tags, {
    workload: 'profitability-shadow-qset-writer'
    walletCredentials: 'absent'
  })
}

resource shadowResearchIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: shadowResearchIdentityName
  location: location
  tags: union(tags, {
    workload: 'profitability-shadow-qset-research'
    walletCredentials: 'absent'
  })
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

resource freezeControlContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: freezeControlContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource freezeControlImmutabilityPolicy 'Microsoft.Storage/storageAccounts/blobServices/containers/immutabilityPolicies@2023-05-01' = if (initializeFreezeBoundary) {
  parent: freezeControlContainer
  name: 'default'
  properties: {
    immutabilityPeriodSinceCreationInDays: 90
    allowProtectedAppendWrites: false
  }
}

resource eventIndexTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: eventIndexTableName
}

resource chartSeriesTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: chartSeriesTableName
}

resource marketCatalogTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: marketCatalogTableName
}

resource shadowEventsContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowEventsContainer.id, shadowIdentity.id, 'qset-blob-data-contributor')
  scope: shadowEventsContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowResearchEventsReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowEventsContainer.id, shadowResearchIdentity.id, 'qset-blob-data-reader')
  scope: shadowEventsContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: shadowResearchIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowResearchContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, shadowResearchIdentity.id, 'qset-blob-data-contributor')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: shadowResearchIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource appResearchReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, appIdentity.id, 'qset-blob-data-reader')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: appIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource githubResearchContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, githubDeployIdentity.id, 'qset-blob-data-contributor')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: githubDeployIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource githubEventsReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(shadowEventsContainer.id, githubDeployIdentity.id, 'qset-blob-data-reader')
  scope: shadowEventsContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: githubDeployIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource githubFreezeControlContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(freezeControlContainer.id, githubDeployIdentity.id, 'qset-freeze-control-blob-data-contributor')
  scope: freezeControlContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: githubDeployIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource eventIndexContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventIndexTable.id, shadowIdentity.id, 'qset-table-data-contributor')
  scope: eventIndexTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource chartSeriesContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(chartSeriesTable.id, shadowIdentity.id, 'qset-table-data-contributor')
  scope: chartSeriesTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource marketCatalogContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(marketCatalogTable.id, shadowIdentity.id, 'qset-table-data-contributor')
  scope: marketCatalogTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, shadowIdentity.id, 'qset-acr-pull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: shadowIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowResearchAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, shadowResearchIdentity.id, 'qset-acr-pull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: shadowResearchIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource shadowApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: shadowAppName
  location: location
  tags: tags
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
            { name: 'APP_NAME', value: shadowAppName }
            { name: 'RUNTIME_ROLE', value: 'profitability_shadow' }
            { name: 'EXECUTION_MODE', value: 'paper' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'RUN_BOT_ON_STARTUP', value: 'true' }
            { name: 'RUST_LOG', value: 'warn,polyedge_api::runtime=info' }
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
            { name: 'AZURE_STORAGE_TABLE_NAME', value: eventIndexTableName }
            { name: 'AZURE_CHART_TABLE_NAME', value: chartSeriesTableName }
            { name: 'AZURE_MARKET_TABLE_NAME', value: marketCatalogTableName }
            { name: 'AZURE_FUNDED_STORAGE_CONTAINER_NAME', value: fundedEvidenceContainerName }
            { name: 'AZURE_MODEL_STORAGE_CONTAINER_NAME', value: modelContainerName }
            { name: 'AZURE_EVENT_BLOB_PREFIX', value: preflightEventPrefix }
            { name: 'AZURE_EVENT_BLOB_PREFIX_AFTER_CUTOVER', value: campaignEventPrefix }
            { name: 'AZURE_EVENT_BLOB_PREFIX_CUTOVER_UTC', value: '${campaignStart}T00:00:00Z' }
            { name: 'COMPACT_SHADOW_RECORDING', value: 'true' }
            { name: 'SHADOW_BOOK_SAMPLE_MS', value: '1000' }
            { name: 'PUBLISH_STRATEGY_CANARY_INTENTS', value: 'true' }
            { name: 'STRATEGY_CANARY_INTENT_PREFIX', value: 'control/strategy-canary/intents/${campaignId}' }
            { name: 'STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION', value: conservativePriorVersion }
            { name: 'STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI', value: conservativePriorBlobUri }
            { name: 'STRATEGY_CANARY_EXECUTION_MODEL_SHA256', value: conservativePriorSha256 }
            { name: 'SHADOW_CAMPAIGN_ID', value: campaignId }
            { name: 'SHADOW_EVIDENCE_VERSION', value: 'protocol-v3-qset-v1' }
            { name: 'SHADOW_CODE_FREEZE_SHA256', value: codeFreezeSha256 }
            { name: 'SHADOW_CODE_FREEZE_MANIFEST', value: 'azure://${storage.name}/${freezeControlContainer.name}/${codeFreezeManifestPath}' }
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
  dependsOn: [
    shadowEventsContributor
    eventIndexContributor
    chartSeriesContributor
    marketCatalogContributor
    shadowAcrPull
  ]
}

resource shadowDailyJob 'Microsoft.App/jobs@2024-03-01' = {
  name: shadowDailyJobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${shadowResearchIdentity.id}': {}
    }
  }
  properties: {
    environmentId: managedEnvironment.id
    configuration: union(dailyTriggerConfiguration, {
      replicaRetryLimit: 1
      replicaTimeout: 16800
      registries: [
        {
          server: registry.properties.loginServer
          identity: shadowResearchIdentity.id
        }
      ]
    })
    template: {
      containers: [
        {
          name: 'shadow-daily'
          image: backendImage
          command: [
            'polyedge-rs'
          ]
          args: [
            'research'
            'with-azure-lease'
            '--account'
            storage.name
            '--container'
            researchContainer.name
            '--blob'
            campaignLeaseBlob
            '--'
            '/bin/sh'
            '/app/research/run_shadow_daily.sh'
          ]
          env: [
            { name: 'APP_NAME', value: shadowAppName }
            { name: 'EXECUTION_MODE', value: 'paper' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'RUN_BOT_ON_STARTUP', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'PAPER_MAKER_FILL_POLICY', value: 'none' }
            { name: 'AZURE_CLIENT_ID', value: shadowResearchIdentity.properties.clientId }
            { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: storage.name }
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: researchContainer.name }
            { name: 'SHADOW_SOURCE_CONTAINER_NAME', value: shadowEventsContainer.name }
            { name: 'SHADOW_EXECUTION_MODEL_BLOB_URI', value: conservativePriorBlobUri }
            { name: 'SHADOW_EXECUTION_MODEL_BLOB_NAME', value: conservativePriorBlobName }
            { name: 'SHADOW_EXECUTION_MODEL_SHA256', value: conservativePriorSha256 }
            { name: 'SHADOW_CAMPAIGN_ID', value: campaignId }
            { name: 'SHADOW_CAMPAIGN_START', value: campaignStart }
            { name: 'SHADOW_CAMPAIGN_PREFIX', value: campaignEventPrefix }
            { name: 'SHADOW_CAMPAIGN_REPORT_ROOT', value: campaignReportRoot }
            { name: 'SHADOW_CAMPAIGN_CONTRACT', value: campaignContract }
            { name: 'SHADOW_PROJECTED_CACHE_ROOT', value: 'azure://${storage.name}/${researchContainer.name}/data/research/shadow/${campaignId}/projected-cache' }
            { name: 'SHADOW_CORRECTION_ROOT', value: '${campaignReportRoot}/corrections' }
            { name: 'SHADOW_EVIDENCE_VERSION', value: 'protocol-v3-qset-v1' }
            { name: 'SHADOW_CODE_FREEZE_SHA256', value: codeFreezeSha256 }
            { name: 'SHADOW_CODE_FREEZE_MANIFEST', value: 'azure://${storage.name}/${freezeControlContainer.name}/${codeFreezeManifestPath}' }
          ]
          resources: {
            cpu: json('4')
            memory: '8Gi'
          }
        }
      ]
    }
  }
  dependsOn: [
    shadowResearchEventsReader
    shadowResearchContributor
    shadowResearchAcrPull
  ]
}

output campaignId string = campaignId
output campaignStart string = campaignStart
output campaignEventPrefix string = campaignEventPrefix
output campaignReportRoot string = campaignReportRoot
output campaignLeaseBlob string = campaignLeaseBlob
output campaignContract string = campaignContract
output shadowAppName string = shadowApp.name
output shadowDailyJobName string = shadowDailyJob.name
output shadowEventsContainerName string = shadowEventsContainer.name
output researchContainerName string = researchContainer.name
output freezeControlContainerName string = freezeControlContainer.name
output shadowIdentityName string = shadowIdentity.name
output shadowResearchIdentityName string = shadowResearchIdentity.name
output dailyScheduleEnabled bool = enableDailySchedule
output freezeBoundaryInitialized bool = initializeFreezeBoundary
