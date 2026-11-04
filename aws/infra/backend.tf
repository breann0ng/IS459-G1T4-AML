# Shared state, so the whole team plans/applies against the same view of AWS.
# Lives in the lake bucket under tfstate/; use_lockfile stops two people applying at once.
terraform {
  backend "s3" {
    bucket       = "is459-g1t4-aml"
    key          = "tfstate/foundation.tfstate"
    region       = "us-east-1"
    use_lockfile = true
    encrypt      = true
  }
}
