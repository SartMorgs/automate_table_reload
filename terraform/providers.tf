provider "aws" {
  version = "3.27.0"
  region = var.region
  profile = var.profile
  shared_credentials_file = "/Users/morgana.sartor/.aws/credentials"
}