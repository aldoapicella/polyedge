targetScope = 'resourceGroup'

param storageAccountName string = 'stpolyedge6urdjr5nmwx7w'

var campaignId = 'campaign-2026-09-01-qset-v6'
var rawContainerName = 'polyedge-shadow-qset-v6-events'
var researchContainerName = 'polyedge-research-qset-v6'
var controlContainerName = 'polyedge-qset-v6-control'
var eventIndexTableName = 'ShadowQsetV6EventIndex'
var chartSeriesTableName = 'ShadowQsetV6ChartSeries'
var marketCatalogTableName = 'ShadowQsetV6MarketCatalog'
var writerIdentityName = 'id-polyedge-conduit-shadow-qset-v6-writer'
var processorIdentityName = 'id-polyedge-conduit-shadow-qset-v6-processor'
var preflightRawPrefix = 'shadow-events/preflight/${campaignId}/'
var campaignRawPrefix = 'shadow-events/${campaignId}/'
var strategyCanaryIntentPrefix = 'control/strategy-canary/intents/${campaignId}/'
var campaignResearchPrefix = 'data/research/shadow/${campaignId}/'
var campaignReportPrefix = 'reports/research/shadow/campaigns/${campaignId}/'
var blobDataReaderRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
var ociTableWriterRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'c8133837-ba14-4b6b-8f58-52ce675a33e4')
var ociBlobWriterRoleId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '44fd8b56-84f7-403c-a44a-7aabab1d28b1')
var processorWriteCondition = '((!(ActionMatches{\'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write\'}) AND !(ActionMatches{\'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action\'})) OR ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals \'${researchContainerName}\') AND ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith \'${campaignResearchPrefix}\') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith \'${campaignReportPrefix}\'))))'
var writerWriteCondition = replace(replace(replace(replace('''
((!(ActionMatches{'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write'}) AND !(ActionMatches{'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action'})) OR ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals 'RAW_CONTAINER') AND ((@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'PREFLIGHT_PREFIX') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'CAMPAIGN_PREFIX') OR (@Resource[Microsoft.Storage/storageAccounts/blobServices/containers/blobs:path] StringStartsWith 'INTENT_PREFIX'))))
''', 'RAW_CONTAINER', rawContainerName), 'PREFLIGHT_PREFIX', preflightRawPrefix), 'CAMPAIGN_PREFIX', campaignRawPrefix), 'INTENT_PREFIX', strategyCanaryIntentPrefix)

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

resource apiIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: 'id-polyedge-conduit-api'
}

resource writerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: writerIdentityName
}

resource processorIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: processorIdentityName
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
    description: 'Only write or append qset-v6 preflight and campaign raw prefixes; the custom role has no delete, move, or container actions.'
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
    description: 'Only write or append qset-v6 campaign research and report prefixes; the custom role has no delete, move, or container actions.'
  }
}


output campaignId string = campaignId
output writerIdentityName string = writerIdentity.name
output processorIdentityName string = processorIdentity.name
output rawContainerName string = rawContainer.name
output researchContainerName string = researchContainer.name
output controlContainerName string = controlContainer.name
output apiResearchReaderRoleAssignmentId string = apiResearchReader.id
