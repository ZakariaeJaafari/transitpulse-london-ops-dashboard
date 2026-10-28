variable "subscription_id" {
  type        = string
  description = "Azure subscription that holds the $100 credit. From `az account show`."
}

variable "location" {
  type        = string
  description = "Azure region for the resource group and VM, e.g. uksouth."
  default     = "uksouth"
}

variable "ssh_allowed_cidr" {
  type        = string
  description = "Your public IP as CIDR for SSH only, e.g. 203.0.113.10/32."
}

variable "ssh_public_key" {
  type        = string
  description = "OpenSSH public key line for the ubuntu user on the VM."
}

variable "vm_size" {
  type        = string
  description = "Burstable x86 size. Standard_B2s is 2 vCPU / 4 GiB and stretches the credit. Use Standard_B2ms (8 GiB) if Airflow is killed for memory."
  default     = "Standard_B2s"
}

variable "project_name" {
  type        = string
  description = "Prefix for Azure resource names."
  default     = "transitpulse"
}
