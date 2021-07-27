variable "region" {
  type = string
  description = "AWS local when terraform will run"
  default = "us-east-1"
}

variable "profile" {
  type = string
  description = "AWS profile"
  default = "default"
}

variable "tags" {
  type = map
  description = ""
  default = {
    MOBI_NAME = "REPROCESSING ARTIFACTS"
    MOBI_WORKLOAD = "REPROCESSING ARTIFACTS"
    MOBI_STACK = "BI"
    MOBI_OWNER = "BI_SDP"
    MOBI_COST_CENTER = "SDP"
    MOBI_CUSTOMER = ""
    MOBI_PROJECT = ""
    MOBI_COMPANY = "AKROSS"
    MOBI_CONFIDENTIALITY = "LOW"
    MOBI_DATETIME = ""
  }
}