targetScope = 'subscription'

@description('Azure region used to record the subscription-scope deployment.')
param deploymentLocation string = 'eastus'

@description('Resource group whose PolyEdge cost is tracked independently.')
param polyedgeResourceGroupName string = 'rg-polyedge-dev'

@description('Container Apps managed resource groups whose platform resources are part of PolyEdge cost.')
param polyedgeManagedResourceGroupNames array = [
  'ME_polyedge-venue-neu-env_rg-polyedge-dev_northeurope'
  'ME_polyedge-execution-cl-env_rg-polyedge-dev_chilecentral'
]

@description('Monthly PolyEdge budget in subscription currency, including its managed resource groups.')
@minValue(1)
param polyedgeMonthlyBudget int = 275

@description('Monthly subscription budget in subscription currency.')
@minValue(1)
param subscriptionMonthlyBudget int = 350

@description('First day of the budget month. Azure requires a first-of-month UTC value.')
param budgetStartDate string = utcNow('yyyy-MM-01T00:00:00Z')

@description('Operators notified when actual or forecast cost crosses a threshold.')
@minLength(1)
param contactEmails array = [
  'aapicella@copaair.com'
]

resource polyedgeBudget 'Microsoft.Consumption/budgets@2023-11-01' = {
  name: 'polyedge-monthly-budget'
  properties: {
    amount: polyedgeMonthlyBudget
    category: 'Cost'
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: budgetStartDate
    }
    filter: {
      dimensions: {
        name: 'ResourceGroupName'
        operator: 'In'
        values: concat([
          polyedgeResourceGroupName
        ], polyedgeManagedResourceGroupNames)
      }
    }
    notifications: {
      actual75: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 75
        thresholdType: 'Actual'
        contactEmails: contactEmails
        contactGroups: []
        contactRoles: []
        locale: 'en-us'
      }
      actual90: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 90
        thresholdType: 'Actual'
        contactEmails: contactEmails
        contactGroups: []
        contactRoles: []
        locale: 'en-us'
      }
      actual100: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Actual'
        contactEmails: contactEmails
        contactGroups: []
        contactRoles: []
        locale: 'en-us'
      }
      forecast100: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Forecasted'
        contactEmails: contactEmails
        contactGroups: []
        contactRoles: []
        locale: 'en-us'
      }
    }
  }
}

resource subscriptionBudget 'Microsoft.Consumption/budgets@2023-11-01' = {
  name: 'polyedge-subscription-monthly-budget'
  properties: {
    amount: subscriptionMonthlyBudget
    category: 'Cost'
    timeGrain: 'Monthly'
    timePeriod: {
      startDate: budgetStartDate
    }
    notifications: {
      actual75: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 75
        thresholdType: 'Actual'
        contactEmails: contactEmails
        contactGroups: []
        contactRoles: []
        locale: 'en-us'
      }
      actual90: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 90
        thresholdType: 'Actual'
        contactEmails: contactEmails
        contactGroups: []
        contactRoles: []
        locale: 'en-us'
      }
      actual100: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Actual'
        contactEmails: contactEmails
        contactGroups: []
        contactRoles: []
        locale: 'en-us'
      }
      forecast100: {
        enabled: true
        operator: 'GreaterThan'
        threshold: 100
        thresholdType: 'Forecasted'
        contactEmails: contactEmails
        contactGroups: []
        contactRoles: []
        locale: 'en-us'
      }
    }
  }
}

output deploymentRegion string = deploymentLocation
output polyedgeBudgetName string = polyedgeBudget.name
output subscriptionBudgetName string = subscriptionBudget.name
