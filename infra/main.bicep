targetScope = 'resourceGroup'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Short app name used for resource names.')
param appName string = 'polyedge'

@description('Backend container image to run. The workflow deploys the current image first, then updates to the built image.')
param image string = 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'

@description('Frontend container image. Leave empty for backend-only bootstrap deployments.')
param frontendImage string = ''

@description('Authenticated venue probe image. Leave empty to omit the isolated manual probe and model jobs.')
param venueProbeImage string = ''

@description('Bearer token required to access the public API.')
@secure()
param apiBearerToken string

@description('Owner password required by the frontend dashboard login.')
@secure()
param dashboardAuthPassword string

@description('Secret used to sign frontend dashboard session cookies.')
@secure()
param dashboardSessionSecret string

@description('Dashboard session TTL in seconds.')
param dashboardSessionTtlSeconds int = 43200

@description('Minimum replicas. Use 1 for continuous market observation.')
param minReplicas int = 1

@description('Maximum replicas. Keep 1 to avoid duplicate bot collectors.')
param maxReplicas int = 1

@description('Whether the backend starts the market data writer on startup. Keep true for the active paper-mode stack.')
param runBotOnStartup bool = true

@description('Container CPU allocation.')
param cpu string = '0.5'

@description('Container memory allocation.')
param memory string = '1Gi'

@description('Frontend container CPU allocation.')
param frontendCpu string = '0.5'

@description('Frontend container memory allocation.')
param frontendMemory string = '1Gi'

@description('Backend API base URL used by the frontend server proxy.')
param frontendBackendApiBaseUrl string = 'http://127.0.0.1:8081/api/v1'

@description('Backend WebSocket URL used by the frontend realtime proxy when BACKEND_SSE_URL is not set.')
param frontendBackendWsUrl string = 'ws://127.0.0.1:8081/api/v1/ws/live'

@description('Optional upstream Server-Sent Events URL for frontend proxy overrides. Leave empty for the active Rust sidecar.')
param frontendBackendSseUrl string = ''

@description('Deployment environment tag.')
param environmentName string = 'dev'

@description('Optional email address for PolyEdge Azure Monitor alerts. Leave empty to deploy the action group without email receivers.')
param alertEmailAddress string = ''

@description('Optional webhook URI for Teams, Slack, or automation alert routing. Leave empty to skip webhook routing.')
@secure()
param alertWebhookUri string = ''

var suffix = uniqueString(subscription().id, resourceGroup().id, appName)
var safeAppName = toLower(replace(appName, '-', ''))
var storageName = take('st${safeAppName}${suffix}', 24)
var acrName = take('cr${safeAppName}${suffix}', 50)
var keyVaultName = take('kv${safeAppName}${suffix}', 24)
var logAnalyticsWorkspaceName = take('log-${appName}-${environmentName}-${suffix}', 63)
var managedEnvironmentName = '${appName}-${environmentName}-env'
var containerAppName = '${appName}-${environmentName}'
var shadowContainerAppName = 'polyedge-shadow-neu'
var storageContainerName = 'bot-events'
var researchStorageContainerName = 'polyedge-research'
var fundedEvidenceContainerName = 'polyedge-funded-evidence'
var modelStorageContainerName = 'polyedge-models'
var storageTableName = 'BotEventIndex'
var chartTableName = 'BotChartSeries'
var marketTableName = 'BotMarketCatalog'
var labTableNames = [
  'PolyEdgeDataFreshness'
  'PolyEdgeResearchRuns'
  'PolyEdgeProspectiveResults'
  'PolyEdgeExclusionWindows'
  'PolyEdgeResearchArtifacts'
  'PolyEdgeJobStatus'
]
var frontendEnabled = !empty(frontendImage)
var containerAppIdentityName = '${containerAppName}-id'
var venueModelIdentityName = '${containerAppName}-venue-model-id'
var githubDeployIdentityName = 'id-github-polyedge-dev'
var venueProbeEnabled = !empty(venueProbeImage)
var tags = {
  app: appName
  environment: environmentName
  managedBy: 'bicep'
}
var jobCommonEnv = [
  {
    name: 'APP_NAME'
    value: 'polyedge'
  }
  {
    name: 'EXECUTION_MODE'
    value: 'paper'
  }
  {
    name: 'ALLOW_LIVE'
    value: 'false'
  }
  {
    name: 'RUN_BOT_ON_STARTUP'
    value: 'false'
  }
  {
    name: 'REQUIRE_API_AUTH'
    value: 'true'
  }
  {
    name: 'API_BEARER_TOKEN'
    secretRef: 'api-bearer-token'
  }
  {
    name: 'AZURE_CLIENT_ID'
    value: containerAppIdentity.properties.clientId
  }
  {
    name: 'AZURE_SUBSCRIPTION_ID'
    value: subscription().subscriptionId
  }
  {
    name: 'AZURE_RESOURCE_GROUP'
    value: resourceGroup().name
  }
  {
    name: 'AZURE_STORAGE_ACCOUNT_NAME'
    value: storage.name
  }
  {
    name: 'AZURE_STORAGE_CONTAINER_NAME'
    value: storageContainerName
  }
  {
    name: 'AZURE_STORAGE_TABLE_NAME'
    value: storageTableName
  }
  {
    name: 'AZURE_CHART_TABLE_NAME'
    value: chartTableName
  }
  {
    name: 'AZURE_MARKET_TABLE_NAME'
    value: marketTableName
  }
  {
    name: 'ENABLE_TAKER_ORDERS'
    value: 'false'
  }
  {
    name: 'ALLOW_EMERGENCY_ACCOUNT_CANCEL'
    value: 'false'
  }
]
var researchJobDefinitions = [
  {
    id: 'freshness-check'
    name: 'polyedge-data-freshness-job'
    triggerType: 'Schedule'
    cron: '*/5 * * * *'
    replicaTimeout: 300
    cpu: cpu
    memory: memory
    command: 'polyedge-rs research azure-freshness --account "$AZURE_STORAGE_ACCOUNT_NAME" --container "$AZURE_STORAGE_CONTAINER_NAME" --prefix "events/" --out "data_quality/freshness/latest.json"'
  }
  {
    id: 'hourly-quality-audit'
    name: 'polyedge-hourly-quality-job'
    triggerType: 'Schedule'
    cron: '10 * * * *'
    replicaTimeout: 1800
    cpu: cpu
    memory: memory
    command: 'TARGET=$(date -u -d "1 hour ago" +%Y/%m/%d/%H); DAY=\${TARGET%/*}; HOUR=\${TARGET##*/}; polyedge-rs research audit --input "azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/events/$DAY/$HOUR/?prefetch_blobs=8" --out "reports/research/hourly/$DAY/$HOUR/audit.json" --markdown "reports/research/hourly/$DAY/$HOUR/audit.md" --exclude-file "data_quality/exclusion_windows.yaml"'
  }
  {
    id: 'daily-research-report'
    name: 'polyedge-daily-research-job'
    triggerType: 'Schedule'
    cron: '30 0 * * *'
    replicaTimeout: 46800
    cpu: '2'
    memory: '4Gi'
    command: 'set -eu; DATE=$(date -u -d "yesterday" +%Y-%m-%d); DAY=$(date -u -d "$DATE" +%Y/%m/%d); RUN_ID="daily-$DATE-$(date -u +%Y%m%dT%H%M%SZ)"; INPUT="azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/events/$DAY/?prefetch_blobs=16"; NORMALIZED="data/research/daily/$DATE/normalized"; STAGING="reports/research/staging/$RUN_ID"; MARKETS="$STAGING/markets_summary.json"; mkdir -p "$STAGING" "data/research/daily/$DATE"; polyedge-rs research audit --input "$INPUT" --exclude-file "data_quality/exclusion_windows.yaml" --out "$STAGING/raw_data_audit.json" --markdown "$STAGING/raw_data_audit.md"; polyedge-rs research normalize --input "$INPUT" --out "$NORMALIZED" --format jsonl-indexed-gzip-sharded --overwrite true; polyedge-rs research audit --input "$NORMALIZED" --exclude-file "data_quality/exclusion_windows.yaml" --out "$STAGING/data_audit.json" --markdown "$STAGING/data_audit.md"; polyedge-rs research execution-quality --input "$NORMALIZED" --exclude-file "data_quality/exclusion_windows.yaml" --out "$STAGING/execution_quality.json" --markdown "$STAGING/execution_quality.md"; polyedge-rs research build-markets --input "$NORMALIZED" --exclude-file "data_quality/exclusion_windows.yaml" --out "$MARKETS" --markdown "$STAGING/markets_summary.md"; polyedge-rs research baseline --input "$NORMALIZED" --markets "$MARKETS" --exclude-file "data_quality/exclusion_windows.yaml" --out "$STAGING/baseline.json" --markdown "$STAGING/baseline.md"; polyedge-rs research regimes --input "$NORMALIZED" --markets "$MARKETS" --fill-model queue_proxy_conservative --profile-config "research/configs/frozen_candidates.yaml" --exclude-file "data_quality/exclusion_windows.yaml" --out "$STAGING/regimes.json" --markdown "$STAGING/regimes.md"; polyedge-rs research calibration --input "$NORMALIZED" --markets "$MARKETS" --exclude-file "data_quality/exclusion_windows.yaml" --out "$STAGING/calibration.json" --markdown "$STAGING/calibration.md"; polyedge-rs research sample-size --results "$STAGING/baseline.json" --out "$STAGING/sample_size.json" --markdown "$STAGING/sample_size.md"; polyedge-rs research report --reports-dir "$STAGING" --out "$STAGING/final_report.json" --markdown "$STAGING/final_report.md"; INPUT_SHA="sha256:$(sha256sum "$NORMALIZED/events_manifest.json" | cut -d" " -f1)"; polyedge-rs research publish-daily-bundle --date "$DATE" --run-id "$RUN_ID" --input-sha256 "$INPUT_SHA" --expected-runtime-role primary --source-dir "$STAGING" --output-root "reports/research/daily" --data-audit "$STAGING/data_audit.json"; polyedge-rs research report --reports-dir "$STAGING" --out "reports/research/latest_daily_report.json" --markdown "reports/research/latest_daily_report.md"; polyedge-rs research validate-prospective --since "2026-07-13T00:00:00Z" --candidates "research/configs/frozen_candidates.yaml" --reports-dir "reports/research/daily" --expected-daily-date "$DATE" --out "reports/research/prospective/prospective_validation.json" --markdown "reports/research/prospective/prospective_validation.md"'
  }
  {
    id: 'prospective-validation'
    name: 'polyedge-prospective-job'
    triggerType: 'Manual'
    cron: ''
    replicaTimeout: 1800
    cpu: cpu
    memory: memory
    command: 'polyedge-rs research validate-prospective --since "2026-07-13T00:00:00Z" --candidates "research/configs/frozen_candidates.yaml" --reports-dir "reports/research/daily" --out "reports/research/prospective/prospective_validation.json" --markdown "reports/research/prospective/prospective_validation.md"'
  }
  {
    id: 'compact-replay-index'
    name: 'polyedge-replay-index-job'
    triggerType: 'Schedule'
    cron: '0 3 * * *'
    replicaTimeout: 43200
    cpu: '2'
    memory: '4Gi'
    command: 'set -eu; DATE=$(date -u -d "yesterday" +%Y-%m-%d); DAY=$(date -u -d "$DATE" +%Y/%m/%d); INPUT="azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/events/$DAY/?prefetch_blobs=16"; NORMALIZED="data/research/replay-index/$DATE/normalized"; mkdir -p "data/research/replay-index/$DATE"; polyedge-rs research normalize --input "$INPUT" --out "$NORMALIZED" --format jsonl-indexed-gzip-sharded --overwrite true; polyedge-rs research build-replay-index --input "$NORMALIZED" --exclude-file "data_quality/exclusion_windows.yaml" --out "data/research/replay-index/$DATE"'
  }
  {
    id: 'chart-backfill'
    name: 'polyedge-chart-backfill-job'
    triggerType: 'Manual'
    cron: ''
    replicaTimeout: 7200
    cpu: cpu
    memory: memory
    command: 'INPUT=$CHART_BACKFILL_INPUT; if [ -z "$INPUT" ]; then DATE=$(date -u -d "yesterday" +%Y-%m-%d); INPUT="data/research/daily/$DATE/normalized"; fi; polyedge-rs research chart-backfill --input "$INPUT" --exclude-file "data_quality/exclusion_windows.yaml" --out "reports/jobs/latest/chart-backfill.json" --markdown "reports/jobs/latest/chart-backfill.md"'
  }
  {
    id: 'adx-ingestion'
    name: 'polyedge-adx-ingestion-job'
    triggerType: 'Schedule'
    cron: '15 * * * *'
    replicaTimeout: 1800
    cpu: cpu
    memory: memory
    command: 'mkdir -p reports/jobs/latest; printf \'%s\' \'{"job_id":"adx-ingestion","job_type":"adx-ingestion","status":"defined_pending_pipeline","started_ts":null,"finished_ts":null,"artifacts":[],"warnings":["ADX ingestion is defined for Azure visibility but no ingestion endpoint is configured"],"errors":[],"data_quality":"unknown","research_only":true,"live_trading_enabled":false}\' > reports/jobs/latest/adx-ingestion.json'
  }
  {
    id: 'manual-backfill'
    name: 'polyedge-backfill-job'
    triggerType: 'Manual'
    cron: ''
    replicaTimeout: 10800
    cpu: cpu
    memory: memory
    command: 'START=$BACKFILL_START; if [ -z "$START" ]; then START=2026-06-14; fi; END=$BACKFILL_END; if [ -z "$END" ]; then END=$START; fi; TASK=$BACKFILL_TASK; if [ -z "$TASK" ]; then TASK=all; fi; polyedge-rs research backfill --start "$START" --end "$END" --task "$TASK" --exclude-file "data_quality/exclusion_windows.yaml" --out "reports/research/backfill/$START-$END-$TASK.json" --markdown "reports/research/backfill/$START-$END-$TASK.md"'
  }
]
var storageMetricAlerts = [
  {
    name: 'blob-ingress-zero-10m'
    displayName: 'PolyEdge blob ingress zero for 10 minutes'
    metricName: 'Ingress'
    threshold: 0
    operator: 'LessThanOrEqual'
  }
  {
    name: 'blob-transactions-zero-10m'
    displayName: 'PolyEdge blob transactions zero for 10 minutes'
    metricName: 'Transactions'
    threshold: 0
    operator: 'LessThanOrEqual'
  }
]
var runtimeMetricAlerts = [
  {
    name: 'working-set-over-750mb'
    displayName: 'PolyEdge container working set over 750 MiB'
    metricName: 'WorkingSetBytes'
    threshold: 786432000
    operator: 'GreaterThan'
  }
]
var logAlerts = [
  {
    name: 'no-new-blob-for-3-minutes'
    displayName: 'PolyEdge no new blob for 3 minutes'
    severity: 0
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s == "polyedge-data-freshness-job" | where (Log_s has "status" and Log_s has "critical") or Log_s has "no new blob"'
  }
  {
    name: 'tiny-blob-anomaly'
    displayName: 'PolyEdge tiny blob anomaly'
    severity: 1
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s == "polyedge-data-freshness-job" | where Log_s has "tiny blob ratio" or (Log_s has "tiny_blob_ratio" and Log_s has "warning")'
  }
  {
    name: 'hour-missing-minute-blobs'
    displayName: 'PolyEdge hour missing minute blobs'
    severity: 1
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s == "polyedge-hourly-quality-job" | where Log_s has "missing minute" or Log_s has "hour_missing_minute_blobs"'
  }
  {
    name: 'recorder-failed-total-gt-0'
    displayName: 'PolyEdge recorder has unrecovered durable evidence'
    severity: 0
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s in ("${containerAppName}", "${shadowContainerAppName}") | extend durable = tolong(extract(@\'"recorder_unrecovered_durable_events":([0-9]+)\', 1, Log_s)), flush = extract(@\'"recorder_flush_unrecovered":(true|false)\', 1, Log_s) | where durable > 0 or flush == "true" or Log_s has "worker_alive=false"'
  }
  {
    name: 'recorder-dropped-count-gt-0'
    displayName: 'PolyEdge recorder dropped count > 0'
    severity: 0
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s in ("${containerAppName}", "${shadowContainerAppName}") | extend value = tolong(extract(@\'"recorder_dropped_count":([0-9]+)\', 1, Log_s)) | where value > 0'
  }
  {
    name: 'recorder-queue-over-1000'
    displayName: 'PolyEdge recorder queue over 1000 events'
    severity: 1
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s in ("${containerAppName}", "${shadowContainerAppName}") | extend value = tolong(extract(@\'"recorder_queued":([0-9]+)\', 1, Log_s)) | where value > 1000'
  }
  {
    name: 'runtime-container-restarted'
    displayName: 'PolyEdge runtime container restarted or backed off'
    severity: 0
    query: 'ContainerAppSystemLogs_CL | where ContainerAppName_s in ("${containerAppName}", "${shadowContainerAppName}") | where Reason_s in ("ContainerBackOff", "ContainerCrashing", "Unhealthy", "OOMKilled") or Log_s has_any ("Back-off restarting failed container", "OOMKilled")'
  }
  {
    name: 'shadow-runtime-health-missing'
    displayName: 'PolyEdge shadow runtime health heartbeat missing'
    severity: 0
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s == "${shadowContainerAppName}" | where Log_s has "runtime_health" | summarize heartbeat_count = count() | where heartbeat_count == 0'
  }
  {
    name: 'job-failed'
    displayName: 'PolyEdge job failed'
    severity: 0
    query: 'union isfuzzy=true ContainerAppConsoleLogs_CL, ContainerAppSystemLogs_CL | where (ContainerAppName_s has "polyedge-" and ContainerAppName_s has "-job") or (JobName_s has "polyedge-" and JobName_s has "-job") | where Reason_s in ("Error", "BackoffLimitExceeded", "DeadlineExceeded", "Failed", "FailedMount", "ErrImagePull", "ImagePullBackOff", "ContainerCrashing") or Log_s has_any ("panicked", "exited with status Failed", "exit code: 1", "exit code 1", "has failed", "failed to", "runtime recorder flush failed")'
  }
  {
    name: 'job-duration-too-long'
    displayName: 'PolyEdge job duration too long'
    severity: 1
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s has "polyedge-" and ContainerAppName_s has "-job" | where Log_s has "duration_seconds" and Log_s has "too_long"'
  }
  {
    name: 'adx-ingestion-failed'
    displayName: 'PolyEdge ADX ingestion failed'
    severity: 1
    query: 'ContainerAppConsoleLogs_CL | where ContainerAppName_s == "polyedge-adx-ingestion-job" | where Log_s has "error" or Log_s has "failed"'
  }
]

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
  properties: {
    changeFeed: {
      enabled: true
      retentionInDays: 30
    }
    deleteRetentionPolicy: {
      enabled: true
      days: 14
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 14
    }
  }
}

resource eventContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: storageContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource researchContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: researchStorageContainerName
  properties: { publicAccess: 'None' }
}

resource fundedEvidenceContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: fundedEvidenceContainerName
  properties: { publicAccess: 'None' }
}

resource modelContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: modelStorageContainerName
  properties: { publicAccess: 'None' }
}

resource githubDeployIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: githubDeployIdentityName
}

resource githubDeployEventReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventContainer.id, githubDeployIdentity.id, 'github-deploy-blob-reader')
  scope: eventContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: githubDeployIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource githubDeployResearchContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, githubDeployIdentity.id, 'github-deploy-blob-contributor')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: githubDeployIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource githubDeployFundedContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(fundedEvidenceContainer.id, githubDeployIdentity.id, 'github-deploy-blob-contributor')
  scope: fundedEvidenceContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: githubDeployIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource tableService 'Microsoft.Storage/storageAccounts/tableServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource eventIndexTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: storageTableName
}

resource chartSeriesTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: chartTableName
}

resource marketCatalogTable 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = {
  parent: tableService
  name: marketTableName
}

resource labTables 'Microsoft.Storage/storageAccounts/tableServices/tables@2023-05-01' = [for tableName in labTableNames: {
  parent: tableService
  name: tableName
}]

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  tags: tags
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
  }
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: union(tags, {
    purpose: 'venue-probe-credentials'
  })
  properties: {
    tenantId: tenant().tenantId
    enableRbacAuthorization: true
    enablePurgeProtection: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    publicNetworkAccess: 'Enabled'
    sku: {
      family: 'A'
      name: 'standard'
    }
  }
}

resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsWorkspaceName
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

resource managedEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: managedEnvironmentName
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
  }
}

resource containerAppIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: containerAppIdentityName
  location: location
  tags: tags
}

// Research-only identity for checkpoint-100 queue calibration. It is never
// granted Key Vault access and therefore cannot obtain venue credentials.
resource venueModelIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = if (venueProbeEnabled) {
  name: venueModelIdentityName
  location: location
  tags: tags
}

resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: containerAppName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${containerAppIdentity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: frontendEnabled ? 3000 : 8081
        transport: 'http'
        allowInsecure: false
      }
      secrets: [
        {
          name: 'api-bearer-token'
          value: apiBearerToken
        }
        {
          name: 'dashboard-auth-password'
          value: dashboardAuthPassword
        }
        {
          name: 'dashboard-session-secret'
          value: dashboardSessionSecret
        }
      ]
      registries: [
        {
          server: acr.properties.loginServer
          identity: containerAppIdentity.id
        }
      ]
    }
    template: {
      containers: concat([
        {
          name: 'bot'
          image: image
          env: [
            {
              name: 'APP_NAME'
              value: 'polyedge'
            }
            {
              name: 'EXECUTION_MODE'
              value: 'paper'
            }
            {
              name: 'ALLOW_LIVE'
              value: 'false'
            }
            {
              name: 'RUN_BOT_ON_STARTUP'
              value: runBotOnStartup ? 'true' : 'false'
            }
            {
              name: 'REQUIRE_API_AUTH'
              value: 'true'
            }
            {
              name: 'API_BEARER_TOKEN'
              secretRef: 'api-bearer-token'
            }
            {
              name: 'AZURE_CLIENT_ID'
              value: containerAppIdentity.properties.clientId
            }
            {
              name: 'AZURE_SUBSCRIPTION_ID'
              value: subscription().subscriptionId
            }
            {
              name: 'AZURE_RESOURCE_GROUP'
              value: resourceGroup().name
            }
            {
              name: 'AZURE_LOG_ANALYTICS_WORKSPACE_ID'
              value: logAnalyticsWorkspace.properties.customerId
            }
            {
              name: 'TARGET_ASSET'
              value: 'BTC'
            }
            {
              name: 'TARGET_ASSET_NAME'
              value: 'Bitcoin'
            }
            {
              name: 'TARGET_HORIZON'
              value: '15m'
            }
            {
              name: 'TARGET_CHAINLINK_SYMBOL'
              value: 'btc/usd'
            }
            {
              name: 'TARGET_BINANCE_SYMBOL'
              value: 'btcusdt'
            }
            {
              name: 'ENABLE_DIRECT_BINANCE_BOOK_TICKER'
              value: 'false'
            }
            {
              name: 'TARGET_COINBASE_PRODUCT_ID'
              value: 'BTC-USD'
            }
            {
              name: 'AZURE_STORAGE_ACCOUNT_NAME'
              value: storage.name
            }
            {
              name: 'AZURE_STORAGE_CONTAINER_NAME'
              value: storageContainerName
            }
            {
              name: 'AZURE_RESEARCH_STORAGE_CONTAINER_NAME'
              value: researchStorageContainerName
            }
            {
              name: 'AZURE_FUNDED_STORAGE_CONTAINER_NAME'
              value: fundedEvidenceContainerName
            }
            {
              name: 'AZURE_MODEL_STORAGE_CONTAINER_NAME'
              value: modelStorageContainerName
            }
            {
              name: 'SHADOW_CAMPAIGN_ID'
              value: 'campaign-2026-07-23'
            }
            {
              name: 'SHADOW_CAMPAIGN_START'
              value: '2026-07-23'
            }
            {
              name: 'SHADOW_CAMPAIGN_REPORT_ROOT'
              value: 'reports/research/shadow/campaigns/campaign-2026-07-23'
            }
            {
              name: 'SHADOW_CORRECTION_ROOT'
              value: 'reports/research/shadow/campaigns/campaign-2026-07-23/corrections'
            }
            {
              name: 'AZURE_STORAGE_TABLE_NAME'
              value: storageTableName
            }
            {
              name: 'AZURE_CHART_TABLE_NAME'
              value: chartTableName
            }
            {
              name: 'AZURE_MARKET_TABLE_NAME'
              value: marketTableName
            }
            {
              name: 'AZURE_EVENT_INDEX_TYPES'
              value: 'runtime_provenance,market,market_start_price,paper_settlement,fair_value,decision,execution_report,feed_error,reference,live_heartbeat'
            }
            {
              name: 'ENABLE_TAKER_ORDERS'
              value: 'false'
            }
            {
              name: 'PAPER_MAKER_FILL_POLICY'
              value: 'touch_after_quote_was_live'
            }
            {
              name: 'PAPER_ORDER_LIVE_AFTER_MS'
              value: '250'
            }
            {
              name: 'ALLOW_EMERGENCY_ACCOUNT_CANCEL'
              value: 'false'
            }
            {
              name: 'ENABLE_LIVE_HEARTBEAT'
              value: 'true'
            }
          ]
          resources: {
            cpu: json(cpu)
            memory: memory
          }
        }
      ], frontendEnabled ? [
        {
          name: 'frontend'
          image: frontendImage
          env: concat([
            {
              name: 'NODE_ENV'
              value: 'production'
            }
            {
              name: 'BACKEND_API_BASE_URL'
              value: frontendBackendApiBaseUrl
            }
            {
              name: 'BACKEND_WS_URL'
              value: frontendBackendWsUrl
            }
            {
              name: 'BACKEND_API_BEARER_TOKEN'
              secretRef: 'api-bearer-token'
            }
            {
              name: 'DASHBOARD_AUTH_PASSWORD'
              secretRef: 'dashboard-auth-password'
            }
            {
              name: 'DASHBOARD_SESSION_SECRET'
              secretRef: 'dashboard-session-secret'
            }
            {
              name: 'DASHBOARD_SESSION_TTL_SECONDS'
              value: string(dashboardSessionTtlSeconds)
            }
          ], !empty(frontendBackendSseUrl) ? [
            {
              name: 'BACKEND_SSE_URL'
              value: frontendBackendSseUrl
            }
          ] : [])
          resources: {
            cpu: json(frontendCpu)
            memory: frontendMemory
          }
        }
      ] : [])
      scale: {
        minReplicas: minReplicas
        maxReplicas: maxReplicas
      }
    }
  }
}

resource researchJobs 'Microsoft.App/jobs@2024-03-01' = [for job in researchJobDefinitions: {
  name: job.name
  location: location
  tags: union(tags, {
    researchJob: job.id
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${containerAppIdentity.id}': {}
    }
  }
  properties: {
    environmentId: managedEnvironment.id
    configuration: union({
      triggerType: job.triggerType
      replicaRetryLimit: 1
      replicaTimeout: job.replicaTimeout
      registries: [
        {
          server: acr.properties.loginServer
          identity: containerAppIdentity.id
        }
      ]
      secrets: [
        {
          name: 'api-bearer-token'
          value: apiBearerToken
        }
      ]
    }, job.triggerType == 'Schedule' ? {
      scheduleTriggerConfig: {
        cronExpression: job.cron
        parallelism: 1
        replicaCompletionCount: 1
      }
    } : {
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
    })
    template: {
      containers: [
        {
          name: 'research-job'
          image: image
          command: [
            '/bin/sh'
            '-lc'
          ]
          args: [
            job.command
          ]
          env: jobCommonEnv
          resources: {
            cpu: json(job.cpu)
            memory: job.memory
          }
        }
      ]
    }
  }
}]

resource venueModelJob 'Microsoft.App/jobs@2024-03-01' = if (venueProbeEnabled) {
  name: 'polyedge-venue-model-job'
  location: location
  tags: union(tags, {
    researchJob: 'venue-fill-model'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${venueModelIdentity!.id}': {}
    }
  }
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 300
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: acr.properties.loginServer
          identity: venueModelIdentity!.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'venue-model'
          image: venueProbeImage
          command: [
            'node'
          ]
          args: [
            'src/train.mjs'
          ]
          env: [
            {
              name: 'AZURE_CLIENT_ID'
              value: venueModelIdentity!.properties.clientId
            }
            {
              name: 'AZURE_STORAGE_ACCOUNT_NAME'
              value: storage.name
            }
            {
              name: 'AZURE_STORAGE_CONTAINER_NAME'
              value: fundedEvidenceContainerName
            }
            {
              name: 'QUEUE_MODEL_SOURCE_CONTAINER_NAME'
              value: fundedEvidenceContainerName
            }
            {
              name: 'QUEUE_MODEL_OUTPUT_CONTAINER_NAME'
              value: modelStorageContainerName
            }
            {
              name: 'QUEUE_MODEL_TRAINING_ENABLED'
              value: 'false'
            }
            {
              name: 'QUEUE_MODEL_CHECKPOINT_BLOB_NAME'
              value: ''
            }
            {
              name: 'QUEUE_MODEL_CHECKPOINT_SHA256'
              value: ''
            }
            {
              name: 'ALLOW_LIVE'
              value: 'false'
            }
            {
              name: 'ENABLE_TAKER_ORDERS'
              value: 'false'
            }
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

resource venueModelFundedEvidenceReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (venueProbeEnabled) {
  name: guid(fundedEvidenceContainer.id, venueModelIdentity!.id, 'venue-model-funded-reader')
  scope: fundedEvidenceContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: venueModelIdentity!.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource venueModelOutputContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (venueProbeEnabled) {
  name: guid(modelContainer.id, venueModelIdentity!.id, 'venue-model-output-contributor')
  scope: modelContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: venueModelIdentity!.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource venueModelAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (venueProbeEnabled) {
  name: guid(acr.id, venueModelIdentity!.id, 'venue-model-acr-pull')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: venueModelIdentity!.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource blobDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventContainer.id, containerAppIdentity.id, 'blob-data-contributor')
  scope: eventContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource appFundedEvidenceReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(fundedEvidenceContainer.id, containerAppIdentity.id, 'app-funded-evidence-reader')
  scope: fundedEvidenceContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource appModelReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(modelContainer.id, containerAppIdentity.id, 'app-model-reader')
  scope: modelContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource appResearchReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, containerAppIdentity.id, 'app-research-reader')
  scope: researchContainer
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource eventIndexTableContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventIndexTable.id, containerAppIdentity.id, 'table-data-contributor')
  scope: eventIndexTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource chartSeriesTableContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(chartSeriesTable.id, containerAppIdentity.id, 'table-data-contributor')
  scope: chartSeriesTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource marketCatalogTableContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(marketCatalogTable.id, containerAppIdentity.id, 'table-data-contributor')
  scope: marketCatalogTable
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource labTableContributors 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for (tableName, index) in labTableNames: {
  name: guid(labTables[index].id, containerAppIdentity.id, 'table-data-contributor')
  scope: labTables[index]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}]

resource acrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, containerAppIdentity.id, 'acr-pull')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource logAnalyticsReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(logAnalyticsWorkspace.id, containerAppIdentity.id, 'log-analytics-reader')
  scope: logAnalyticsWorkspace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '73c42c96-874c-492b-b04d-ab87d138a893')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource researchJobOperator 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for (job, index) in researchJobDefinitions: {
  name: guid(researchJobs[index].id, containerAppIdentity.id, 'research-job-operator')
  scope: researchJobs[index]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b24988ac-6180-42a0-ab88-20f7382dd24c')
    principalId: containerAppIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}]

resource actionGroup 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: '${containerAppName}-research-alerts'
  location: 'global'
  tags: tags
  properties: {
    groupShortName: 'polyedge'
    enabled: true
    emailReceivers: !empty(alertEmailAddress) ? [
      {
        name: 'polyedge-operator-email'
        emailAddress: alertEmailAddress
        useCommonAlertSchema: true
      }
    ] : []
    webhookReceivers: !empty(alertWebhookUri) ? [
      {
        name: 'polyedge-automation-webhook'
        serviceUri: alertWebhookUri
        useCommonAlertSchema: true
      }
    ] : []
  }
}

resource storageMetricAlertRules 'Microsoft.Insights/metricAlerts@2018-03-01' = [for alert in storageMetricAlerts: {
  name: '${containerAppName}-${alert.name}'
  location: 'global'
  tags: tags
  properties: {
    description: alert.displayName
    severity: 1
    enabled: true
    scopes: [
      storage.id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT10M'
    autoMitigate: false
    targetResourceType: 'Microsoft.Storage/storageAccounts'
    targetResourceRegion: location
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: alert.metricName
          metricName: alert.metricName
          metricNamespace: 'Microsoft.Storage/storageAccounts'
          operator: alert.operator
          threshold: alert.threshold
          timeAggregation: 'Total'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
    }
    actions: [
      {
        actionGroupId: actionGroup.id
        webHookProperties: {
          environment: environmentName
          storage_account: storage.name
          container: storageContainerName
          recommended_action: 'Check PolyEdge data freshness and recorder status.'
        }
      }
    ]
  }
}]

resource runtimeMetricAlertRules 'Microsoft.Insights/metricAlerts@2018-03-01' = [for alert in runtimeMetricAlerts: {
  name: '${containerAppName}-${alert.name}'
  location: 'global'
  tags: tags
  properties: {
    description: alert.displayName
    severity: 1
    enabled: true
    scopes: [
      containerApp.id
    ]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    autoMitigate: true
    targetResourceType: 'Microsoft.App/containerApps'
    targetResourceRegion: location
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: alert.metricName
          metricName: alert.metricName
          metricNamespace: 'Microsoft.App/containerApps'
          operator: alert.operator
          threshold: alert.threshold
          timeAggregation: 'Maximum'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
    }
    actions: [
      {
        actionGroupId: actionGroup.id
        webHookProperties: {
          environment: environmentName
          container_app: containerApp.name
          recommended_action: 'Inspect replica memory, recorder queue, and recent book event volume.'
        }
      }
    ]
  }
}]

resource logAlertRules 'Microsoft.Insights/scheduledQueryRules@2022-06-15' = [for alert in logAlerts: {
  name: '${containerAppName}-${alert.name}'
  location: location
  kind: 'LogAlert'
  tags: tags
  properties: {
    displayName: alert.displayName
    description: alert.displayName
    severity: alert.severity
    enabled: true
    scopes: [
      logAnalyticsWorkspace.id
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT10M'
    autoMitigate: false
    criteria: {
      allOf: [
        {
          query: alert.query
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
        actionGroup.id
      ]
      customProperties: {
        environment: environmentName
        storage_account: storage.name
        container: storageContainerName
        latest_blob: 'see freshness job output'
        latest_blob_last_modified: 'see freshness job output'
        latest_blob_size: 'see freshness job output'
        job_execution_id: 'see Container Apps job execution'
        recommended_action: 'Open the PolyEdge Data Quality and Job Monitor pages before trusting research output.'
      }
    }
  }
}]

output acrName string = acr.name
output acrLoginServer string = acr.properties.loginServer
output keyVaultName string = keyVault.name
output containerAppName string = containerApp.name
output containerAppFqdn string = containerApp.properties.configuration.ingress.fqdn
output containerAppIdentityName string = containerAppIdentity.name
output logAnalyticsWorkspaceName string = logAnalyticsWorkspace.name
output storageAccountName string = storage.name
output storageContainerName string = storageContainerName
output storageTableName string = storageTableName
output chartTableName string = chartTableName
output marketTableName string = marketTableName
output labTableNames array = labTableNames
output researchJobNames array = [for job in researchJobDefinitions: job.name]
output venueModelJobName string = venueProbeEnabled ? venueModelJob.name : ''
output venueModelIdentityName string = venueProbeEnabled ? venueModelIdentity!.name : ''
