data "aws_iam_role" "step-funvtion-bi" {
  name = "StepFunctions-BI_DataLake_Massacration_RAW-role-cdcdc2e4"
}

resource "aws_sfn_state_machine" "bi_automate_reprocessing" {
  name      = var.step_function_name
  role_arn  = 

  definition = 

}

