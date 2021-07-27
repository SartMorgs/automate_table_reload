resource "aws_s3_bucket" "reprocessing-artifacts" {
  bucket = "reprocessing-artifacts"
  acl = "private"
  versioning {
    enabled = false
  }

  tags = var.tags
}