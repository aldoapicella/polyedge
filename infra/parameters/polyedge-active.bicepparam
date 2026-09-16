using '../main.bicep'

param location = 'eastus'
param appName = 'polyedge'
param environmentName = 'dev'
param minReplicas = 1
param maxReplicas = 1
param runBotOnStartup = true
param fundedDirectServiceBusEnabled = true
param fundedDirectServiceBusNamespace = 'sb-polyedge-funded-cl-6urdjr5nmwx7w'
param fundedDirectServiceBusQueue = 'funded-dynamic-quote-intents'
param cpu = '0.5'
param memory = '1Gi'
param frontendCpu = '0.5'
param frontendMemory = '1Gi'
param frontendMinReplicas = 0
param frontendMaxReplicas = 1
param frontendBackendApiBaseUrl = ''
param frontendBackendWsUrl = ''
param frontendBackendSseUrl = ''
param venueProbeImage = ''
param apiBearerToken = readEnvironmentVariable('API_BEARER_TOKEN')
param dashboardAuthPassword = readEnvironmentVariable('DASHBOARD_AUTH_PASSWORD')
param dashboardSessionSecret = readEnvironmentVariable('DASHBOARD_SESSION_SECRET')
param dashboardSessionTtlSeconds = 43200
