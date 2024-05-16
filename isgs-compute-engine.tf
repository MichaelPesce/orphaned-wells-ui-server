resource "google_compute_address" "isgs-static-ip-address" {
  name = "isgs-static-ip-address"
  project = "tidy-outlet-412020"
  region = "us-central1"
}


resource "google_compute_network" "vpc_network" {
  name                    = "my-custom-mode-network"
  auto_create_subnetworks = false
  mtu                     = 1460
  project = "tidy-outlet-412020"
}

resource "google_compute_subnetwork" "default" {
  name          = "my-custom-subnet"
  ip_cidr_range = "10.0.1.0/24"
  region        = "us-central1"
  network       = google_compute_network.vpc_network.id
  project = "tidy-outlet-412020"
}

resource "google_compute_instance" "isgs-uow-server" {
  name         = "isgs-uow-server"
  machine_type = "e2-medium"
  zone         = "us-central1-a"
  tags         = ["http-server", "https-server", "tcp8001", "ssh"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 10
      type  = "pd-balanced"
    }

    mode   = "READ_WRITE"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.default.id

    access_config {
      # Include this section to give the VM an external IP address
      nat_ip = "${google_compute_address.isgs-static-ip-address.address}"
    }
  }

  project = "tidy-outlet-412020"

  reservation_affinity {
    type = "ANY_RESERVATION"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    provisioning_model  = "STANDARD"
  }

  service_account {
    email  = "1095146523031-compute@developer.gserviceaccount.com"
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_vtpm                 = true
  }
  
}

# resource "google_compute_instance" "isgs-uow-server" {
#   boot_disk {
#     auto_delete = true
#     device_name = "isgs-uow-server"

#     initialize_params {
#       image = "https://www.googleapis.com/compute/beta/projects/debian-cloud/global/images/debian-11-bullseye-v20240110"
#       size  = 10
#       type  = "pd-balanced"
#     }

#     mode   = "READ_WRITE"
#     source = "https://www.googleapis.com/compute/v1/projects/tidy-outlet-412020/zones/us-central1-a/disks/isgs-uow-server"
#   }

#   confidential_instance_config {
#     enable_confidential_compute = false
#   }

#   machine_type = "e2-medium"

#   metadata = {
#     ssh-keys = "mpesce:ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBBkizoso/2oBKiSD7qjxpVfQSzjxHr33vJkyOw/nT2EzftKxW6fTRpqQsMmFbpUn6gpV/I/tIFoB31ss9HZ930A= google-ssh {\"userName\":\"mpesce@lbl.gov\",\"expireOn\":\"2024-04-10T20:02:11+0000\"}\nmpesce:ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAGR+Xb9+qUHkwVPIDR39OIMEOkKm4oWJbcUyYHoxNt+OQrfF2u6lDU4NXi6JbHqOz1/Qif8XgsIbMcZUZuOvphEbQRkpFo6MU5k0PtCaVVwiyxFXjFSKlILhdJAQzEOGCnXiffYfsD5uEjgLmCgFwOg9Y9B5FbnCwHNRyyfT32CsyOengaD+G00Ky+mnBEVX4pJLhtjpd79jyUMRYU6coTSnEPb4IsK0K6A/71mRYYUFgR2L5NhhPN5sAEqrLr9vMv/Kscl3O/eFvtRGNGONWD2spnn5nv9YIhvSyugfmkTECjJ+9CgInmLdpYeuNl1BREMAuUgJed2rJee8zBgSU38= google-ssh {\"userName\":\"mpesce@lbl.gov\",\"expireOn\":\"2024-04-10T20:02:26+0000\"}"
#   }

#   name = "isgs-uow-server"

#   network_interface {
#     access_config {
#     #   nat_ip       = "34.71.116.181"
#       network_tier = "PREMIUM"
#     }

#     network            = "https://www.googleapis.com/compute/v1/projects/tidy-outlet-412020/global/networks/default"
#     # network_ip         = "10.128.0.2"
#     stack_type         = "IPV4_ONLY"
#     subnetwork         = "https://www.googleapis.com/compute/v1/projects/tidy-outlet-412020/regions/us-central1/subnetworks/default"
#     subnetwork_project = "tidy-outlet-412020"
#   }

#   project = "tidy-outlet-412020"

#   reservation_affinity {
#     type = "ANY_RESERVATION"
#   }

#   scheduling {
#     automatic_restart   = true
#     on_host_maintenance = "MIGRATE"
#     provisioning_model  = "STANDARD"
#   }

#   service_account {
#     email  = "1095146523031-compute@developer.gserviceaccount.com"
#     scopes = ["https://www.googleapis.com/auth/cloud-platform"]
#   }

#   shielded_instance_config {
#     enable_integrity_monitoring = true
#     enable_vtpm                 = true
#   }

#   tags = ["http-server", "https-server", "tcp8001"]
#   zone = "us-central1-a"
# }
# terraform import google_compute_instance.oprhaned_wells_ui_server_v0 projects/tidy-outlet-412020/zones/us-central1-a/instances/oprhaned-wells-ui-server-v0
