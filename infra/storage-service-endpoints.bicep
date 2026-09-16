targetScope = 'resourceGroup'

@description('Existing North Europe VNet used by the continuous shadow runtimes.')
param northEuropeVnetName string = 'vnet-polyedge-venue-neu'

@description('Existing North Europe NAT gateway that must remain attached to the Container Apps subnet.')
param northEuropeNatGatewayName string = 'nat-polyedge-venue-neu'

@description('Existing Chile VNet used by funded execution.')
param chileVnetName string = 'vnet-polyedge-execution-cl'

@description('Existing Chile NAT gateway that must remain attached to the Container Apps subnet.')
param chileNatGatewayName string = 'nat-polyedge-execution-cl'

var infrastructureSubnetName = 'container-apps-infrastructure'

resource northEuropeVnet 'Microsoft.Network/virtualNetworks@2023-09-01' existing = {
  name: northEuropeVnetName
}

resource northEuropeNatGateway 'Microsoft.Network/natGateways@2023-09-01' existing = {
  name: northEuropeNatGatewayName
}

resource northEuropeSubnet 'Microsoft.Network/virtualNetworks/subnets@2023-09-01' = {
  parent: northEuropeVnet
  name: infrastructureSubnetName
  properties: {
    addressPrefix: '10.42.0.0/23'
    natGateway: {
      id: northEuropeNatGateway.id
    }
    serviceEndpoints: [
      {
        service: 'Microsoft.Storage.Global'
      }
    ]
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

resource chileVnet 'Microsoft.Network/virtualNetworks@2023-09-01' existing = {
  name: chileVnetName
}

resource chileNatGateway 'Microsoft.Network/natGateways@2023-09-01' existing = {
  name: chileNatGatewayName
}

resource chileSubnet 'Microsoft.Network/virtualNetworks/subnets@2023-09-01' = {
  parent: chileVnet
  name: infrastructureSubnetName
  properties: {
    addressPrefix: '10.43.0.0/23'
    natGateway: {
      id: chileNatGateway.id
    }
    serviceEndpoints: [
      {
        service: 'Microsoft.Storage.Global'
      }
    ]
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

output updatedSubnetIds array = [
  northEuropeSubnet.id
  chileSubnet.id
]
