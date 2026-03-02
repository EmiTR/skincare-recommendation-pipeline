output "similarity_function_arn"  { value = aws_lambda_function.similarity.arn }
output "similarity_function_name" { value = aws_lambda_function.similarity.function_name }
output "loader_function_arn"      { value = aws_lambda_function.loader.arn }
output "loader_function_name"     { value = aws_lambda_function.loader.function_name }
