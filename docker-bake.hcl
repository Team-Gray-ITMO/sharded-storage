variable "REGISTRY" {
  default = ""
}

variable "TAG" {
  default = "latest"
}

target "base" {
  context = "."
}

# ------------------------
# STANDARD BUILDS
# ------------------------

target "discovery" {
  inherits = ["base"]
  dockerfile = "Dockerfile"
  target     = "discovery-runtime"
  tags = ["${REGISTRY}sharded_storage_discovery:${TAG}"]
}

target "master" {
  inherits = ["base"]
  dockerfile = "Dockerfile"
  target     = "master-runtime"
  tags = ["${REGISTRY}sharded_storage_master:${TAG}"]
}

target "node" {
  inherits = ["base"]
  dockerfile = "Dockerfile"
  target     = "node-runtime"
  tags = ["${REGISTRY}sharded_storage_node:${TAG}"]
}

# ------------------------
# FAILPOINT BUILDS
# ------------------------

target "discovery-fp" {
  inherits = ["base"]
  dockerfile = "Dockerfile.failpoint"
  target     = "discovery-runtime"
  tags = ["${REGISTRY}sharded_storage_discovery-fp:${TAG}"]
}

target "master-fp" {
  inherits = ["base"]
  dockerfile = "Dockerfile.failpoint"
  target     = "master-runtime"
  tags = ["${REGISTRY}sharded_storage_master-fp:${TAG}"]
}

target "node-fp" {
  inherits = ["base"]
  dockerfile = "Dockerfile.failpoint"
  target     = "node-runtime"
  tags = ["${REGISTRY}sharded_storage_node-fp:${TAG}"]
}

# ------------------------
# GROUPS
# ------------------------

group "default" {
  targets = ["discovery", "master", "node"]
}

group "failpoint" {
  targets = ["discovery-fp", "master-fp", "node-fp"]
}
