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

# No CloudWatch billing alarm: this is a linked (member) account, and only the payer account can
# turn on CloudWatch billing metrics. Cost is tracked in AWS Budgets + Cost Explorer + per-run cost
# in the run log; CloudWatch is used for pipeline logs, metrics and alarms.

# Cost Anomaly Detection: new accounts usually get a default services monitor already (Billing console).
# Not managed here because an account allows only one services monitor.
