output "public_ip" {
  description = "Public IPv4 of the Azure VM."
  value       = azurerm_public_ip.vm.ip_address
}

output "ssh_command" {
  description = "SSH to the VM. Airflow and MinIO stay on the VM; tunnel them."
  value       = "ssh -i <your_ssh_private_key> ubuntu@${azurerm_public_ip.vm.ip_address}"
}

output "resource_group_name" {
  description = "Resource group to deallocate or destroy when you stop using the credit."
  value       = azurerm_resource_group.main.name
}
