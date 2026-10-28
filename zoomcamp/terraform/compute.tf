resource "azurerm_linux_virtual_machine" "vm" {
  name                            = "${var.project_name}-zoomcamp"
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  size                            = var.vm_size
  zone                            = "1"
  admin_username                  = "ubuntu"
  disable_password_authentication = true
  network_interface_ids           = [azurerm_network_interface.vm.id]

  # Gen2 Ubuntu image. Trusted launch matches that image.
  secure_boot_enabled = true
  vtpm_enabled        = true

  admin_ssh_key {
    username   = "ubuntu"
    public_key = var.ssh_public_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = 30
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  tags = azurerm_resource_group.main.tags
}
