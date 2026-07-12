targetScope = 'resourceGroup'

@description('Azure region for the isolated authenticated execution worker.')
param location string = 'northeurope'

param venueProbeImage string
param registryName string = 'crpolyedge6urdjr5nmwx7w'
param storageAccountName string = 'stpolyedge6urdjr5nmwx7w'
param storageContainerName string = 'bot-events'
param keyVaultName string = 'kvpolyedge6urdjr5nmwx7w'
param logAnalyticsWorkspaceName string = 'log-polyedge-dev-6urdjr5nmwx7w'
param funderAddress string = '0x3d701b05d7c36aFaB01a06Fd26eBe789c0B7baD8'

var environmentName = 'polyedge-venue-neu-env'
var identityName = 'polyedge-venue-neu-id'
var jobName = 'polyedge-venue-probe-neu-job'
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

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: registryName
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = {
  name: logAnalyticsWorkspaceName
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
  tags: tags
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

resource keyVaultSecretsUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, identity.id, 'key-vault-secrets-user')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource blobDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, identity.id, 'blob-data-contributor')
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
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
            { name: 'MAX_DAILY_LOSS', value: '5' }
            { name: 'VENUE_PROBE_CAMPAIGN_ENABLED', value: 'true' }
            { name: 'VENUE_PROBE_MAXIMUM_ORDERS', value: '25' }
            { name: 'VENUE_PROBE_MAX_ORDER_NOTIONAL', value: '1.25' }
            { name: 'VENUE_PROBE_MIN_ORDER_NOTIONAL', value: '1' }
            { name: 'VENUE_PROBE_STARTING_CAPITAL', value: '9.23' }
            { name: 'VENUE_PROBE_MIN_ORDER_PRICE', value: '0.05' }
            { name: 'VENUE_PROBE_REST_HORIZONS_SECONDS', value: '1,5,30,60' }
            { name: 'VENUE_PROBE_INTER_ORDER_DELAY_MS', value: '1000' }
            { name: 'VENUE_PROBE_MAX_CLOCK_DRIFT_MS', value: '5000' }
            { name: 'VENUE_PROBE_KILL_SWITCH', value: 'false' }
            { name: 'VENUE_PROBE_DRY_RUN', value: 'true' }
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
            { name: 'AZURE_STORAGE_CONTAINER_NAME', value: storageContainerName }
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

output venueProbeJobName string = venueProbeJob.name
output managedEnvironmentName string = managedEnvironment.name
output managedIdentityName string = identity.name
output staticEgressIp string = publicIp.properties.ipAddress
output executionRegion string = location
