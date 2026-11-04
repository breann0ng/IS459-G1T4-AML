# Cost TRACKING. No aws_budgets_budget_action: a deny action once blocked EMR on this team's account.
# With budget_emails = [] (default) the budget only tracks: actual vs forecast vs limit in the
# Billing > Budgets console, nothing is sent. Add an email to get warned before money runs out.
resource "aws_budgets_budget" "monthly" {
  name         = "aml-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = length(var.budget_emails) > 0 ? [50, 80, 100] : []
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.budget_emails
    }
  }
}

# The same limit inside CloudWatch, next to the pipeline's logs and alarms. No alarm_actions:
# it only turns "In alarm" on the CloudWatch console, nobody is emailed.
# One-time prerequisite (account setting, not Terraform): Billing console > Billing preferences >
# "Receive CloudWatch billing alerts" ON. Billing metrics exist only in us-east-1 (our region).
resource "aws_cloudwatch_metric_alarm" "estimated_charges" {
  alarm_name          = "aml-estimated-charges-over-budget"
  alarm_description   = "Month-to-date AWS charges passed the project budget (tracking only, no actions)"
  namespace           = "AWS/Billing"
  metric_name         = "EstimatedCharges"
  dimensions          = { Currency = "USD" }
  statistic           = "Maximum"
  period              = 21600 # billing data updates a few times a day
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.budget_limit_usd
  treat_missing_data  = "notBreaching"
}

# Cost Anomaly Detection: new accounts usually get a default services monitor already (Billing console).
# Not managed here because an account allows only one services monitor.
