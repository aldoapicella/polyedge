targetScope = 'resourceGroup'

@description('Existing conduit funded-signer user-assigned managed identity.')
param fundedSignerIdentityName string = 'id-polyedge-conduit-funded-signer'

@description('Existing funded Key Vault.')
param keyVaultName string = 'kvpolyedge6urdjr5nmwx7w'

@description('Existing funded Service Bus namespace.')
param serviceBusNamespaceName string = 'sb-polyedge-funded-cl-6urdjr5nmwx7w'

@description('Existing funded intent queue.')
param serviceBusQueueName string = 'funded-dynamic-quote-intents'

var keyVaultSecretsUserRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '4633458b-17de-408a-b874-0445c86b69e6'
)
var serviceBusReceiverRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '4f6d3b9b-027b-4f4c-9142-0e5a2a2247e0'
)

resource fundedSignerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: fundedSignerIdentityName
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

resource privateKeySecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-private-key'
}

resource apiKeySecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-api-key'
}

resource apiSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-api-secret'
}

resource apiPassphraseSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-api-passphrase'
}

resource relayerApiKeySecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' existing = {
  parent: keyVault
  name: 'polymarket-relayer-api-key'
}

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2024-01-01' existing = {
  name: serviceBusNamespaceName
}

resource serviceBusQueue 'Microsoft.ServiceBus/namespaces/queues@2024-01-01' existing = {
  parent: serviceBusNamespace
  name: serviceBusQueueName
}

resource privateKeyReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(privateKeySecret.id, fundedSignerIdentity.id, 'key-vault-secrets-user')
  scope: privateKeySecret
  properties: {
    roleDefinitionId: keyVaultSecretsUserRoleId
    principalId: fundedSignerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource apiKeyReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(apiKeySecret.id, fundedSignerIdentity.id, 'key-vault-secrets-user')
  scope: apiKeySecret
  properties: {
    roleDefinitionId: keyVaultSecretsUserRoleId
    principalId: fundedSignerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource apiSecretReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(apiSecret.id, fundedSignerIdentity.id, 'key-vault-secrets-user')
  scope: apiSecret
  properties: {
    roleDefinitionId: keyVaultSecretsUserRoleId
    principalId: fundedSignerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource apiPassphraseReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(apiPassphraseSecret.id, fundedSignerIdentity.id, 'key-vault-secrets-user')
  scope: apiPassphraseSecret
  properties: {
    roleDefinitionId: keyVaultSecretsUserRoleId
    principalId: fundedSignerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource relayerApiKeyReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(relayerApiKeySecret.id, fundedSignerIdentity.id, 'key-vault-secrets-user')
  scope: relayerApiKeySecret
  properties: {
    roleDefinitionId: keyVaultSecretsUserRoleId
    principalId: fundedSignerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource queueReceiver 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(serviceBusQueue.id, fundedSignerIdentity.id, 'service-bus-data-receiver')
  scope: serviceBusQueue
  properties: {
    roleDefinitionId: serviceBusReceiverRoleId
    principalId: fundedSignerIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

output principalId string = fundedSignerIdentity.properties.principalId
output roleAssignmentIds array = [
  privateKeyReader.id
  apiKeyReader.id
  apiSecretReader.id
  apiPassphraseReader.id
  relayerApiKeyReader.id
  queueReceiver.id
]
