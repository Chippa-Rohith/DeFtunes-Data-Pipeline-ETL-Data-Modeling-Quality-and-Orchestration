# Complete the `"aws_glue_connection" "rds_connection"` resource
resource "aws_glue_connection" "rds_connection" {
  name = "${var.project}-connection-rds"

  # At `connection_properties`, add `var.username` and `var.password` to the `USERNAME` and `PASSWORD` parameters respectively
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://${var.host}:${var.port}/${var.database}"
    USERNAME            = var.username
    PASSWORD            = var.password
  }

  # At the `physical_connection_requirements` configuration, set the `subnet_id` to `data.aws_subnet.private_a.id` and 
  # the `security_group_id_list` to a list containing the element `data.aws_security_group.db_sg.id`
  physical_connection_requirements {
    availability_zone      = data.aws_subnet.public_a.availability_zone
    security_group_id_list = [data.aws_security_group.db_sg.id]
    subnet_id              = data.aws_subnet.public_a.id
  }
}

resource "aws_glue_job" "rds_ingestion_etl_job" {
  name         = "${var.project}-rds-extract-job"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  connections  = [aws_glue_connection.rds_connection.name]
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/de-c4w4a2-extract-songs-job.py"
    python_version  = 3
  }

  default_arguments = {
    "--enable-job-insights" = "true"
    "--job-language"        = "python"
    "--rds_connection"      = aws_glue_connection.rds_connection.name
    "--data_lake_bucket"    = var.data_lake_bucket
    "--ingest_date"         = "2020-02-01"
  }

  timeout = 5
  number_of_workers = 2

  worker_type       = "G.1X"
}

resource "aws_glue_job" "api_users_ingestion_etl_job" {
  name         = "${var.project}-api-users-extract-job"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/de-c4w4a2-api-extract-job.py"
    python_version  = 3
  }

  default_arguments = {
    "--enable-job-insights" = "true"
    "--job-language"        = "python"
    "--api_start_date"      = "2020-01-01"
    "--api_end_date"        = "2020-01-31"
    "--api_url"             = "http://ec2-100-25-67-97.compute-1.amazonaws.com/users"
    "--target_path"         = "s3://${var.data_lake_bucket}/landing_zone/api/users"
    "--ingest_date"         = "2020-02-01"
  }

  timeout = 5
  number_of_workers = 2

  worker_type       = "G.1X"
}

resource "aws_glue_job" "api_sessions_ingestion_etl_job" {
  name         = "${var.project}-api-sessions-extract-job"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/de-c4w4a2-api-extract-job.py"
    python_version  = 3
  }


  default_arguments = {
    "--enable-job-insights" = "true"
    "--job-language"        = "python"
    "--api_start_date"      = "2020-01-01"
    "--api_end_date"        = "2020-01-31"
    "--api_url"             = "http://ec2-100-25-67-97.compute-1.amazonaws.com/sessions"
    "--target_path"         = "s3://${var.data_lake_bucket}/landing_zone/api/sessions"
    "--ingest_date"         = "2020-02-01"
  }

  timeout = 5
  number_of_workers = 2

  worker_type       = "G.1X"
}