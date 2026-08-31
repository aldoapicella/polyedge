targetScope = 'resourceGroup'

@description('Identity lane to create or update. Deploy lanes sequentially.')
@allowed([
  'api'
  'research'
  'shadow-qset'
  'shadow-qset-v4-writer'
  'shadow-qset-v4-processor'
  'shadow-qset-v5-writer'
  'shadow-qset-v5-processor'
  'shadow-qset-v6-writer'
  'shadow-qset-v6-processor'
  'shadow-qset-v7-writer'
  'shadow-qset-v7-processor'
  'funded-signer'
  'funded-intent-producer'
])
param lane string

@description('Public HTTPS SPIRE OIDC issuer. Must exactly match the JWT-SVID iss claim.')
@minLength(9)
@maxLength(600)
param issuer string

@description('Azure region for the user-assigned managed identity.')
param location string = resourceGroup().location

@description('Additional non-sensitive resource tags.')
param tags object = {}

var audience = 'api://AzureADTokenExchange'
var lanes = {
  api: {
    identityName: 'id-polyedge-conduit-api'
    ficName: 'fic-spire-conduit-api'
    subject: 'spiffe://polyedge.local/conduit/api'
  }
  research: {
    identityName: 'id-polyedge-conduit-research'
    ficName: 'fic-spire-conduit-research'
    subject: 'spiffe://polyedge.local/conduit/research'
  }
  'shadow-qset': {
    identityName: 'id-polyedge-conduit-shadow-qset'
    ficName: 'fic-spire-conduit-shadow-qset'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset'
  }
  'shadow-qset-v4-writer': {
    identityName: 'id-polyedge-conduit-shadow-qset-v4-writer'
    ficName: 'fic-spire-conduit-shadow-qset-v4-writer'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset-v4-writer'
  }
  'shadow-qset-v4-processor': {
    identityName: 'id-polyedge-conduit-shadow-qset-v4-processor'
    ficName: 'fic-spire-conduit-shadow-qset-v4-processor'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset-v4-processor'
  }
  'shadow-qset-v5-writer': {
    identityName: 'id-polyedge-conduit-shadow-qset-v5-writer'
    ficName: 'fic-spire-conduit-shadow-qset-v5-writer'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset-v5-writer'
  }
  'shadow-qset-v5-processor': {
    identityName: 'id-polyedge-conduit-shadow-qset-v5-processor'
    ficName: 'fic-spire-conduit-shadow-qset-v5-processor'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset-v5-processor'
  }
  'shadow-qset-v6-writer': {
    identityName: 'id-polyedge-conduit-shadow-qset-v6-writer'
    ficName: 'fic-spire-conduit-shadow-qset-v6-writer'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset-v6-writer'
  }
  'shadow-qset-v6-processor': {
    identityName: 'id-polyedge-conduit-shadow-qset-v6-processor'
    ficName: 'fic-spire-conduit-shadow-qset-v6-processor'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset-v6-processor'
  }
  'shadow-qset-v7-writer': {
    identityName: 'id-polyedge-conduit-shadow-qset-v7-writer'
    ficName: 'fic-spire-conduit-shadow-qset-v7-writer'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset-v7-writer'
  }
  'shadow-qset-v7-processor': {
    identityName: 'id-polyedge-conduit-shadow-qset-v7-processor'
    ficName: 'fic-spire-conduit-shadow-qset-v7-processor'
    subject: 'spiffe://polyedge.local/conduit/shadow-qset-v7-processor'
  }
  'funded-signer': {
    identityName: 'id-polyedge-conduit-funded-signer'
    ficName: 'fic-spire-conduit-funded-signer'
    subject: 'spiffe://polyedge.local/conduit/funded-signer'
  }
  'funded-intent-producer': {
    identityName: 'id-polyedge-conduit-funded-intent-producer'
    ficName: 'fic-spire-conduit-funded-intent-producer'
    subject: 'spiffe://polyedge.local/conduit/funded-intent-producer'
  }
}
var selected = lanes[lane]

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: selected.identityName
  location: location
  tags: union(tags, {
    owner: 'polyedge'
    purpose: 'conduit-oci-federation'
    identityLane: lane
  })
}

resource federatedCredential 'Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials@2024-11-30' = {
  parent: identity
  name: selected.ficName
  properties: {
    issuer: issuer
    subject: selected.subject
    audiences: [
      audience
    ]
  }
}

output identityResourceId string = identity.id
output clientId string = identity.properties.clientId
output principalId string = identity.properties.principalId
output federatedCredentialResourceId string = federatedCredential.id
output federatedTrust object = {
  issuer: issuer
  subject: selected.subject
  audience: audience
}
