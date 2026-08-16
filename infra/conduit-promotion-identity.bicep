targetScope = 'resourceGroup'

@description('Public HTTPS SPIRE OIDC issuer. It must exactly match the controller JWT-SVID iss claim.')
@minLength(9)
@maxLength(600)
param issuer string

var identityName = 'id-polyedge-conduit-promotion-controller'
var roleDefinitionId = 'e4d113d2-e955-4259-8214-2b919eaef2c0'

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: identityName
  location: resourceGroup().location
  tags: {
    owner: 'polyedge'
    purpose: 'conduit-oci-promotion-controller'
    identityLane: 'promotion-controller'
  }
}

resource federatedCredential 'Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials@2024-11-30' = {
  parent: identity
  name: 'fic-spire-conduit-promotion-controller'
  properties: {
    issuer: issuer
    subject: 'spiffe://polyedge.local/conduit/promotion-controller'
    audiences: [
      'api://AzureADTokenExchange'
    ]
  }
}

resource promotionRole 'Microsoft.Authorization/roleDefinitions@2022-04-01' = {
  name: roleDefinitionId
  properties: {
    roleName: 'PolyEdge OCI Promotion Controller'
    description: 'Reads and changes only the selected Container App and hourly Job, including a bounded execution proof. It has no data-plane, registry, delete, or RBAC permissions.'
    type: 'CustomRole'
    permissions: [
      {
        actions: [
          'Microsoft.App/containerApps/read'
          'Microsoft.App/containerApps/write'
          'Microsoft.App/jobs/read'
          'Microsoft.App/jobs/write'
          'Microsoft.App/jobs/executions/read'
          'Microsoft.App/jobs/start/action'
          'Microsoft.App/jobs/stop/execution/action'
        ]
        notActions: []
        dataActions: []
        notDataActions: []
      }
    ]
    assignableScopes: [
      resourceGroup().id
    ]
  }
}

resource primaryApp 'Microsoft.App/containerApps@2024-03-01' existing = {
  name: 'polyedge-dev'
}

resource hourlyJob 'Microsoft.App/jobs@2024-03-01' existing = {
  name: 'polyedge-hourly-quality-job'
}

resource primaryAppPromotionWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(primaryApp.id, identity.id, promotionRole.id)
  scope: primaryApp
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: promotionRole.id
  }
}

resource hourlyJobPromotionWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(hourlyJob.id, identity.id, promotionRole.id)
  scope: hourlyJob
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: promotionRole.id
  }
}

output identityResourceId string = identity.id
output identityClientId string = identity.properties.clientId
output identityPrincipalId string = identity.properties.principalId
output federatedCredentialResourceId string = federatedCredential.id
output primaryAppAssignmentId string = primaryAppPromotionWriter.id
output hourlyJobAssignmentId string = hourlyJobPromotionWriter.id
