targetScope = 'resourceGroup'

@description('North Europe, matching the credential-free shadow recorder.')
param location string = 'northeurope'

@description('Immutable validation image by ACR digest.')
param validationImage string

@description('Exact 40-character Git commit built into the validation image.')
@minLength(40)
@maxLength(40)
param expectedGitSha string

@description('Exact immutable July 23 Azure raw-source inventory SHA-256.')
@minLength(71)
@maxLength(71)
param expectedRawSourceInventorySha256 string

@description('Exact immutable canonical July 23 projected fileset SHA-256 before the precision repair.')
@minLength(71)
@maxLength(71)
param sourceProjectedFilesetSha256 string

param registryName string = 'crpolyedge6urdjr5nmwx7w'
param storageAccountName string = 'stpolyedge6urdjr5nmwx7w'
param environmentName string = 'polyedge-venue-neu-env'
param githubDeployIdentityName string = 'id-github-polyedge-dev'
param sourceContainerName string = 'polyedge-shadow-events'
param validationContainerName string = 'polyedge-research-validation'
param validationIdentityName string = 'polyedge-shadow-validation-neu-id'
param validationJobName string = 'polyedge-shadow-val-neu-job'

var validationId = 'campaign-2026-07-23-lossdiag-v3'
var validationLeaseBlob = 'data/research/shadow/${validationId}/control/validation.lock'
var blobDataReaderRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'
)
var blobDataContributorRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
var acrPullRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '7f951dda-4ed3-4680-a7ca-43fe172d538d'
)
var tags = {
  app: 'polyedge'
  environment: 'dev'
  managedBy: 'bicep'
  workload: 'shadow-lossdiag-validation'
  validationId: validationId
  promotionEligible: 'false'
  fundedExecution: 'disabled'
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

resource sourceContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing = {
  parent: blobService
  name: sourceContainerName
}

resource validationContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: validationContainerName
  properties: {
    publicAccess: 'None'
  }
}

resource managedEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' existing = {
  name: environmentName
}

resource validationIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: validationIdentityName
  location: location
  tags: tags
}

resource githubDeployIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: githubDeployIdentityName
}

resource validationSourceReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(sourceContainer.id, validationIdentity.id, blobDataReaderRoleId)
  scope: sourceContainer
  properties: {
    principalId: validationIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobDataReaderRoleId
  }
}

resource validationOutputContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(validationContainer.id, validationIdentity.id, blobDataContributorRoleId)
  scope: validationContainer
  properties: {
    principalId: validationIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobDataContributorRoleId
  }
}

resource validationProofReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(validationContainer.id, githubDeployIdentity.id, blobDataReaderRoleId)
  scope: validationContainer
  properties: {
    principalId: githubDeployIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobDataReaderRoleId
  }
}

resource validationAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, validationIdentity.id, acrPullRoleId)
  scope: registry
  properties: {
    principalId: validationIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: acrPullRoleId
  }
}

resource validationJob 'Microsoft.App/jobs@2024-03-01' = {
  name: validationJobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${validationIdentity.id}': {}
    }
  }
  dependsOn: [
    validationAcrPull
    validationOutputContributor
    validationSourceReader
  ]
  properties: {
    environmentId: managedEnvironment.id
    configuration: {
      triggerType: 'Manual'
      replicaRetryLimit: 0
      replicaTimeout: 14400
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: validationIdentity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'lossdiag-validation'
          image: validationImage
          command: [
            'polyedge-rs'
          ]
          args: [
            'research'
            'with-azure-lease'
            '--account'
            storage.name
            '--container'
            validationContainer.name
            '--blob'
            validationLeaseBlob
            '--'
            '/bin/sh'
            '/app/research/run_shadow_lossdiag_validation.sh'
          ]
          env: [
            { name: 'APP_NAME', value: 'polyedge-shadow-lossdiag-validation' }
            { name: 'EXECUTION_MODE', value: 'paper' }
            { name: 'ALLOW_LIVE', value: 'false' }
            { name: 'RUN_BOT_ON_STARTUP', value: 'false' }
            { name: 'ENABLE_TAKER_ORDERS', value: 'false' }
            { name: 'AZURE_CLIENT_ID', value: validationIdentity.properties.clientId }
            { name: 'AZURE_STORAGE_ACCOUNT_NAME', value: storage.name }
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: validationContainer.name }
            { name: 'SHADOW_SOURCE_CONTAINER_NAME', value: sourceContainer.name }
            { name: 'EXPECTED_GIT_SHA', value: expectedGitSha }
            { name: 'EXPECTED_RAW_SOURCE_INVENTORY_SHA256', value: expectedRawSourceInventorySha256 }
            { name: 'SOURCE_PROJECTED_FILESET_SHA256', value: sourceProjectedFilesetSha256 }
            { name: 'LOSSDIAG_VALIDATION_CONFIG', value: '/app/research/configs/shadow_lossdiag_validation_2026-07-23_v3.json' }
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

output validationJobName string = validationJob.name
output validationContainerName string = validationContainer.name
output validationIdentityPrincipalId string = validationIdentity.properties.principalId
