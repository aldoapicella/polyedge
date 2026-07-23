targetScope = 'resourceGroup'

@description('Azure region for the scheduled query alert resources.')
param location string = resourceGroup().location

@description('Short app name used for existing resource names.')
param appName string = 'polyedge'

@description('Deployment environment used for existing resource names.')
param environmentName string = 'dev'

@description('Frozen profitability-shadow Container App monitored alongside the primary app.')
param shadowContainerAppName string = 'polyedge-shadow-neu'

var suffix = uniqueString(subscription().id, resourceGroup().id, appName)
var containerAppName = '${appName}-${environmentName}'
var logAnalyticsWorkspaceName = take('log-${appName}-${environmentName}-${suffix}', 63)
var actionGroupName = '${containerAppName}-research-alerts'
var tags = {
  app: appName
  environment: environmentName
  managedBy: 'bicep'
}
var recorderAlerts = [
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
]

resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' existing = {
  name: logAnalyticsWorkspaceName
}

resource actionGroup 'Microsoft.Insights/actionGroups@2023-01-01' existing = {
  name: actionGroupName
}

resource recorderAlertRules 'Microsoft.Insights/scheduledQueryRules@2022-06-15' = [for alert in recorderAlerts: {
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
        monitored_apps: '${containerAppName},${shadowContainerAppName}'
        recommended_action: 'Inspect recorder durability, queue health, and Container App restart state before trusting campaign evidence.'
      }
    }
  }
}]

output monitoredContainerApps array = [
  containerAppName
  shadowContainerAppName
]

output alertRuleNames array = [for alert in recorderAlerts: '${containerAppName}-${alert.name}']
