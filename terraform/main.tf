terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

resource "docker_image" "postgres" {
  name         = "postgres:16"
  keep_locally = true
}

resource "docker_container" "postgres" {
  name  = "tf-postgres"
  image = docker_image.postgres.image_id

  ports {
    internal = 5432
    external = 5433
  }

  env = [
    "POSTGRES_USER=newsuser",
    "POSTGRES_PASSWORD=newspass",
    "POSTGRES_DB=newsdb",
  ]
}

# ---- Self-managed Docker network so containers can resolve each other by name ----
resource "docker_network" "news_net" {
  name = "tf-news-network"
}

# ---- Kafka ----
resource "docker_image" "kafka" {
  name         = "confluentinc/cp-kafka:7.6.1"
  keep_locally = true
}

resource "docker_container" "kafka" {
  name  = "tf-kafka"
  image = docker_image.kafka.image_id

  networks_advanced {
    name = docker_network.news_net.name
  }

  ports {
    internal = 9092
    external = 9095
  }

  env = [
    "KAFKA_NODE_ID=1",
    "KAFKA_PROCESS_ROLES=broker,controller",
    "KAFKA_CONTROLLER_QUORUM_VOTERS=1@tf-kafka:9093",
    "KAFKA_LISTENERS=PLAINTEXT_HOST://0.0.0.0:9092,PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://localhost:9095,PLAINTEXT://tf-kafka:29092",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
    "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
    "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
    "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
  ]
}

# ---- Kafdrop (Kafka UI) ----
resource "docker_image" "kafdrop" {
  name         = "obsidiandynamics/kafdrop:latest"
  keep_locally = true
}

resource "docker_container" "kafdrop" {
  name  = "tf-kafdrop"
  image = docker_image.kafdrop.image_id

  networks_advanced {
    name = docker_network.news_net.name
  }

  ports {
    internal = 9000
    external = 9001
  }

  env = [
    "KAFKA_BROKERCONNECT=tf-kafka:29092",
  ]

  # Explicit dependency: no attribute reference to kafka, so declare it manually
  depends_on = [docker_container.kafka]
}

# ---- Grafana ----
resource "docker_image" "grafana" {
  name         = "grafana/grafana:11.3.0"
  keep_locally = true
}

resource "docker_container" "grafana" {
  name  = "tf-grafana"
  image = docker_image.grafana.image_id

  networks_advanced {
    name = docker_network.news_net.name
  }

  ports {
    internal = 3000
    external = 3001
  }

  env = [
    "GF_SECURITY_ADMIN_USER=admin",
    "GF_SECURITY_ADMIN_PASSWORD=admin",
  ]
}

# ---- Outputs: print service URLs after apply ----
output "service_urls" {
  description = "Access points for all infrastructure services"
  value = {
    postgres = "localhost:5433 (user=newsuser db=newsdb)"
    kafka    = "localhost:9095"
    kafdrop  = "http://localhost:9001"
    grafana  = "http://localhost:3001 (admin/admin)"
  }
}