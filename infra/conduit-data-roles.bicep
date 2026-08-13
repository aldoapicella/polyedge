targetScope = 'subscription'

resource blobWriter 'Microsoft.Authorization/roleDefinitions@2022-04-01' = {
  name: '44fd8b56-84f7-403c-a44a-7aabab1d28b1'
  properties: {
    roleName: 'PolyEdge OCI Blob Writer'
    description: 'Read, create, and append PolyEdge blobs without delete permission.'
    type: 'CustomRole'
    permissions: [
      {
        actions: []
        notActions: []
        dataActions: [
          'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read'
          'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write'
          'Microsoft.Storage/storageAccounts/blobServices/containers/blobs/add/action'
        ]
        notDataActions: []
      }
    ]
    assignableScopes: [
      subscription().id
    ]
  }
}

resource tableWriter 'Microsoft.Authorization/roleDefinitions@2022-04-01' = {
  name: 'c8133837-ba14-4b6b-8f58-52ce675a33e4'
  properties: {
    roleName: 'PolyEdge OCI Table Writer'
    description: 'Read and update PolyEdge table entities without delete permission.'
    type: 'CustomRole'
    permissions: [
      {
        actions: []
        notActions: []
        dataActions: [
          'Microsoft.Storage/storageAccounts/tableServices/tables/entities/read'
          'Microsoft.Storage/storageAccounts/tableServices/tables/entities/write'
          'Microsoft.Storage/storageAccounts/tableServices/tables/entities/add/action'
          'Microsoft.Storage/storageAccounts/tableServices/tables/entities/update/action'
        ]
        notDataActions: []
      }
    ]
    assignableScopes: [
      subscription().id
    ]
  }
}

resource acaJobExecutionReader 'Microsoft.Authorization/roleDefinitions@2022-04-01' = {
  name: 'a6f3874a-dc9c-4880-8039-ed1a4e57e4ea'
  properties: {
    roleName: 'PolyEdge ACA Job Execution Reader'
    description: 'Read only the exact Azure Container Apps job execution required for parity provenance.'
    type: 'CustomRole'
    permissions: [
      {
        actions: [
          'Microsoft.App/jobs/execution/read'
        ]
        notActions: []
        dataActions: []
        notDataActions: []
      }
    ]
    assignableScopes: [
      subscription().id
    ]
  }
}

output blobWriterRoleDefinitionId string = blobWriter.id
output tableWriterRoleDefinitionId string = tableWriter.id
output acaJobExecutionReaderRoleDefinitionId string = acaJobExecutionReader.id
