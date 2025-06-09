# Team Gray's Sharded Storage

A sharded key-value storage, implementing ordered equal-sized sharding.

### Build

(Docker Desktop 4.38+ required)

```bash
# Build all services
docker buildx bake

# Build specific service (node/master/discovery)
docker buildx bake node
```

### Start Core Services (Master and Discovery) with Compose
```bash
# Start discovery and master
docker compose up -d
```

### Run Multiple Nodes
On *nix
```bash
# Run multiple nodes with subsequent IDs (1..3)
./scripts/run-many-nodes.sh 3
```

On Windows
```powershell
# Run multiple nodes with subsequent IDs (1..3)
.\scripts\run-many-nodes.bat 3
```

### Run Specific Nodes
On *nix
```bash
# Run node with ID 1
./scripts/run-node.sh 1

# Run additional nodes (2...)
./scripts/run-node.sh 2
```

On Windows
```powershell
# Run node with ID 1
.\scripts\run-node.bat 1

# Run additional nodes (2...)
.\scripts\run-node.bat 2
```

### Run CLI Client
On *nix
```bash
./scripts/run-cli.sh
```

On Windows
```powershell
.\scripts\run-cli.bat
```

### Stop Specific Nodes
On *nix
```bash
# Stop node with ID 1
./scripts/stop-node.sh 1
```

On Windows
```powershell
# Stop node with ID 1
.\scripts\stop-node.bat 1
```

### Stop Everything (Master and Discovery too)
On *nix
```bash
# Stop nodes
./scripts/stop-all-nodes.sh
```

On Windows
```powershell
# Stop nodes
.\scripts\stop-all-nodes.bat
```

## Requirements
- Docker Engine 20.10+
- Buildx installed (`docker buildx install` if missing)
