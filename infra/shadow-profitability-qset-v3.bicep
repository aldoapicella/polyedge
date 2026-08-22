targetScope = 'resourceGroup'

@description('Public GHCR image pinned by immutable digest for qset-v3 processing.')
param processorImage string = ''

@description('Exact 40-character source revision compiled into processorImage.')
param expectedSourceRevision string = ''

@description('Exact SHA-256 inventory for the read-only 2026-08-23 sealed raw day.')
param sealedDayOneInventorySha256 string = ''

@description('Exact SHA-256 inventory for the read-only 2026-08-24 sealed raw day.')
param sealedDayTwoInventorySha256 string = ''

@description('Exact SHA-256 of the final execution-freeze artifact.')
param executionFreezeSha256 string = ''

@description('Bounded relative path of the final immutable source-freeze artifact.')
param executionFreezeArtifactPath string = ''
param deployProcessorJob bool = false

param location string = 'northeurope'
param storageAccountName string = 'stpolyedge6urdjr5nmwx7w'
param environmentName string = 'polyedge-venue-neu-env'

var campaignId = 'campaign-2026-08-23-qset-v3'
var campaignStart = '2026-08-23'
var sealedDayOne = '2026-08-23'
var sealedDayTwo = '2026-08-24'
var rawContainerName = 'polyedge-shadow-qset-v3-events'
var researchContainerName = 'polyedge-research-qset-v3'
var controlContainerName = 'polyedge-qset-v3-control'
var eventIndexTableName = 'ShadowQsetV3EventIndex'
var chartSeriesTableName = 'ShadowQsetV3ChartSeries'
var marketCatalogTableName = 'ShadowQsetV3MarketCatalog'
var writerIdentityName = 'id-polyedge-conduit-shadow-qset-v3-writer'
var processorIdentityName = 'id-polyedge-conduit-shadow-qset-v3-processor'
var processorJobName = 'polyedge-shadow-qset-v3-neu-job'
var preflightRawPrefix = 'shadow-events/preflight/${campaignId}/'
var campaignRawPrefix = 'shadow-events/${campaignId}/'
var strategyCanaryIntentPrefix = 'control/strategy-canary/intents/${campaignId}/'
var campaignResearchPrefix = 'data/research/shadow/${campaignId}/'
var campaignReportPrefix = 'reports/research/shadow/campaigns/${campaignId}/'
var processorLeaseBlob = '${campaignResearchPrefix}control/replay.lock'
var blobDataReaderRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
var ociTableWriterRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'c8133837-ba14-4b6b-8f58-52ce675a33e4')
var ociBlobWriterRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '44fd8b56-84f7-403c-a44a-7aabab1d28b1')
var processorWriteCondition = '((!(ActionMatches{\'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write\'}) AND !(ActionMatches{\'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action\'})) OR ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals \'${researchContainerName}\') AND ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith \'${campaignResearchPrefix}\') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith \'${campaignReportPrefix}\'))))'
var writerWriteCondition = replace(replace(replace(replace('''
((!(ActionMatches{'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write'}) AND !(ActionMatches{'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action'})) OR ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals 'RAW_CONTAINER') AND ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'PREFLIGHT_PREFIX') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'CAMPAIGN_PREFIX') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'INTENT_PREFIX'))))
''', 'RAW_CONTAINER', rawContainerName), 'PREFLIGHT_PREFIX', preflightRawPrefix), 'CAMPAIGN_PREFIX', campaignRawPrefix), 'INTENT_PREFIX', strategyCanaryIntentPrefix)
var tags = {
  app: 'polyedge'
  managedBy: 'bicep'
  workload: 'profitability-shadow-qset-v3'
  campaignId: campaignId
  fundedExecution: 'disabled'
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

resource apiIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: 'id-polyedge-conduit-api'
}

resource writerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: writerIdentityName
  location: location
  tags: union(tags, {
    role: 'qset-v3-raw-writer'
    walletCredentials: 'absent'
  })
}

resource processorIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: processorIdentityName
  location: location
  tags: union(tags, {
    role: 'qset-v3-research-processor'
    walletCredentials: 'absent'
  })
}

resource rawContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: rawContainerName
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

resource controlContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: controlContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource controlContainerImmutabilityPolicy 'Microsoft.Storage/storageAccounts/blobServices/containers/immutabilityPolicies@2023-05-01' = {
  parent: controlContainer
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

resource writerRawContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(rawContainer.id, writerIdentity.id, ociBlobWriterRoleId, campaignId)
  scope: rawContainer
  properties: {
    principalId: writerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: ociBlobWriterRoleId
    conditionVersion: '2.0'
    condition: writerWriteCondition
    description: 'Only write or append qset-v3 preflight and campaign raw prefixes; the custom role has no delete, move, or container actions.'
  }
}

resource writerControlReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(controlContainer.id, writerIdentity.id, blobDataReaderRoleId)
  scope: controlContainer
  properties: {
    principalId: writerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobDataReaderRoleId
  }
}

resource writerEventIndexContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(eventIndexTable.id, writerIdentity.id, ociTableWriterRoleId)
  scope: eventIndexTable
  properties: {
    principalId: writerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: ociTableWriterRoleId
  }
}

resource writerChartSeriesContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(chartSeriesTable.id, writerIdentity.id, ociTableWriterRoleId)
  scope: chartSeriesTable
  properties: {
    principalId: writerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: ociTableWriterRoleId
  }
}

resource writerMarketCatalogContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(marketCatalogTable.id, writerIdentity.id, ociTableWriterRoleId)
  scope: marketCatalogTable
  properties: {
    principalId: writerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: ociTableWriterRoleId
  }
}

resource apiResearchReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, apiIdentity.id, blobDataReaderRoleId)
  scope: researchContainer
  properties: {
    principalId: apiIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobDataReaderRoleId
  }
}

resource processorRawReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(rawContainer.id, processorIdentity.id, blobDataReaderRoleId)
  scope: rawContainer
  properties: {
    principalId: processorIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobDataReaderRoleId
  }
}

resource processorControlReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(controlContainer.id, processorIdentity.id, blobDataReaderRoleId)
  scope: controlContainer
  properties: {
    principalId: processorIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobDataReaderRoleId
  }
}

resource processorCampaignWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(researchContainer.id, processorIdentity.id, ociBlobWriterRoleId, campaignId)
  scope: researchContainer
  properties: {
    principalId: processorIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: ociBlobWriterRoleId
    conditionVersion: '2.0'
    condition: processorWriteCondition
    description: 'Only write or append qset-v3 campaign research and report prefixes; the custom role has no delete, move, or container actions.'
  }
}

resource processorJob 'Microsoft.App/jobs@2024-03-01' = if (deployProcessorJob) {
  name: processorJobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${processorIdentity.id}': {}
    }
  }
  dependsOn: [
    processorRawReader
    processorControlReader
    processorCampaignWriter
    apiResearchReader
  ]
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 16800
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
    }
    template: {
      containers: [
        {
          name: 'shadow-qset-v3-processor'
          image: processorImage
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
            processorLeaseBlob
            '--'
            '/bin/sh'
            '-ec'
            '''
              validate_day() {
                date="$1"
                inventory="$2"
                validation="$(polyedge-rs seal-qset-v3-day --account "$AZURE_STORAGE_ACCOUNT_NAME" --client-id "$AZURE_CLIENT_ID" --date "$date" --source-freeze-blob "$EXECUTION_FREEZE_ARTIFACT_PATH" --source-freeze-sha256 "$EXECUTION_FREEZE_SHA256" --validate-only)"
                printf '%s
' "$validation" | jq -e --arg date "$date" --arg inventory "$inventory" --arg freeze "$EXECUTION_FREEZE_SHA256" --arg freeze_path "$EXECUTION_FREEZE_ARTIFACT_PATH" '
                  .schema == "polyedge.qset_v3_closed_day_validation.v1"
                  and .campaign_id == "campaign-2026-08-23-qset-v3"
                  and .date == $date
                  and .container == "polyedge-shadow-qset-v3-events"
                  and .blob_count == 1440
                  and .all_sealed == true
                  and .source_inventory_sha256 == $inventory
                  and .source_freeze.verified == true
                  and .source_freeze.sha256 == $freeze
                  and .source_freeze.blob == $freeze_path
                ' >/dev/null
              }
              printf '%s
' "$EXPECTED_PROCESSOR_IMAGE" | grep -Eq '^ghcr.io/[a-z0-9][a-z0-9._-]*/polyedge-rust-backend@sha256:[0-9a-f]{64}$'
              printf '%s
' "$EXPECTED_SOURCE_REVISION" | grep -Eq '^[0-9a-f]{40}$'
              for hash in "$SEALED_DAY_ONE_INVENTORY_SHA256" "$SEALED_DAY_TWO_INVENTORY_SHA256" "$EXECUTION_FREEZE_SHA256"; do
                printf '%s
' "$hash" | grep -Eq '^sha256:[0-9a-f]{64}$' || exit 1
              done
              test "$(printf '%s' "$EXECUTION_FREEZE_ARTIFACT_PATH" | wc -c)" -le 256
              case "$EXECUTION_FREEZE_ARTIFACT_PATH" in
                reports/research/shadow/campaigns/campaign-2026-08-23-qset-v3/control/code-freeze/source-[A-Za-z0-9._-]*.json) ;;
                *) exit 1 ;;
              esac
              validate_day '2026-08-23' "$SEALED_DAY_ONE_INVENTORY_SHA256"
              validate_day '2026-08-24' "$SEALED_DAY_TWO_INVENTORY_SHA256"
              exec /app/research/run_shadow_daily_v3.sh
            '''
          ]
          env: [
            { name: 'APP_NAME', value: processorJobName }
            { name: 'EXECUTION_MODE', value: 'paper' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'RUN_BOT_ON_STARTUP', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'AZURE_CLIENT_ID', value: processorIdentity.properties.clientId }
            { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: storage.name }
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: researchContainer.name }
            { name: 'SHADOW_SOURCE_CONTAINER_NAME', value: rawContainer.name }
            { name: 'SHADOW_EXECUTION_MODEL_BLOB_NAME', value: 'reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json' }
            { name: 'SHADOW_EXECUTION_MODEL_SHA256', value: 'sha256:91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4' }
            { name: 'SHADOW_EXECUTION_MODEL_BLOB_URI', value: 'azure://${storage.name}/${researchContainer.name}/reports/research/venue-probe/models/conservative-execution-prior-v1-91f29155d09f1a51f3354132befcbbb25d3f96b88c9a8a819f2304f4a7a28ed4.json' }
            { name: 'QSET_V3_CONTROL_CONTAINER_NAME', value: controlContainer.name }
            { name: 'QSET_RESEARCH_CONTAINER', value: researchContainer.name }
            { name: 'SHADOW_CODE_FREEZE_FINALIZED', value: 'true' }
            { name: 'SHADOW_CAMPAIGN_ID', value: campaignId }
            { name: 'SHADOW_CAMPAIGN_START', value: campaignStart }
            { name: 'SHADOW_CAMPAIGN_PREFIX', value: 'shadow-events/${campaignId}' }
            { name: 'SHADOW_CAMPAIGN_REPORT_ROOT', value: 'reports/research/shadow/campaigns/${campaignId}' }
            { name: 'SHADOW_CAMPAIGN_CONTRACT', value: 'research/configs/profitability_gate_v3_2026-08-23_qset_v3.yaml' }
            { name: 'SHADOW_PROJECTED_CACHE_ROOT', value: 'azure://${storage.name}/${researchContainer.name}/${campaignResearchPrefix}projected-cache' }
            { name: 'SHADOW_CORRECTION_ROOT', value: '${campaignReportPrefix}corrections' }
            { name: 'SHADOW_EVIDENCE_VERSION', value: 'protocol-v3-qset-v3' }
            { name: 'EXPECTED_SOURCE_REVISION', value: expectedSourceRevision }
            { name: 'EXPECTED_PROCESSOR_IMAGE', value: processorImage }
            { name: 'SEALED_DAY_ONE', value: sealedDayOne }
            { name: 'SEALED_DAY_ONE_INVENTORY_SHA256', value: sealedDayOneInventorySha256 }
            { name: 'SEALED_DAY_TWO', value: sealedDayTwo }
            { name: 'SEALED_DAY_TWO_INVENTORY_SHA256', value: sealedDayTwoInventorySha256 }
            { name: 'SHADOW_CODE_FREEZE_SHA256', value: executionFreezeSha256 }
            { name: 'SHADOW_CODE_FREEZE_MANIFEST', value: 'azure://${storage.name}/${controlContainer.name}/${executionFreezeArtifactPath}' }
            { name: 'EXECUTION_FREEZE_SHA256', value: executionFreezeSha256 }
            { name: 'EXECUTION_FREEZE_ARTIFACT_PATH', value: executionFreezeArtifactPath }
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

output campaignId string = campaignId
output processorJobName string = processorJobName
output processorJobDeployed bool = deployProcessorJob
output writerIdentityName string = writerIdentity.name
output processorIdentityName string = processorIdentity.name
output rawContainerName string = rawContainer.name
output researchContainerName string = researchContainer.name
output controlContainerName string = controlContainer.name
output apiResearchReaderRoleAssignmentId string = apiResearchReader.id
