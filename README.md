# Valkey Streams with MongoDB PoC

This PoC demonstrates streaming a large data file using Valkey streams with a producer-consumer pattern and saving the data to MongoDB.

## Prerequisites
- Docker (for Valkey)
- Node.js (v18.x recommended)
- npm

## Setup
1. **Start Valkey**:
   ```bash
   docker run -d --name valkey-container -p 6379:6379 -v ~/valkey-data:/data valkey/valkey:latest valkey-server --requirepass your-secure-password --appendonly yes