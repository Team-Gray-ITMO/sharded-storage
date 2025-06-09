variable "REGISTRY" {
  default = ""
}

variable "TAG" {
  default = "latest"
}

target "base" {
  context = "."
  dockerfile = "Dockerfile"
}

target "discovery" {
  inherits = ["base"]
  tags = ["${REGISTRY}sharded_storage_discovery:${TAG}"]
  target = "discovery-runtime"
}

target "master" {
  inherits = ["base"]
  tags = ["${REGISTRY}sharded_storage_master:${TAG}"]
  target = "master-runtime"
}

target "node" {
  inherits = ["base"]
  tags = ["${REGISTRY}sharded_storage_node:${TAG}"]
  target = "node-runtime"
}

group "default" {
  targets = ["discovery", "master", "node"]
}
