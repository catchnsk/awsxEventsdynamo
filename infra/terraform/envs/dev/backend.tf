terraform {
  backend "s3" {
    bucket         = "CHANGE_ME-webhooks-terraform"
    key            = "dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "CHANGE_ME-terraform-locks"
    encrypt        = true
  }
}
