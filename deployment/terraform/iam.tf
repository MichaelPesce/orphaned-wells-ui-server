resource "google_project_iam_member" "mpesce_oslogin" {
  project = var.project_id
  role    = "roles/compute.osAdminLogin"
  member  = "user:mpesce@lbl.gov"
}