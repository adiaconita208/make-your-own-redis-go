# Redis Clone in Go

A lightweight, concurrent, in-memory key-value store built from scratch in Go. This project is a custom implementation of a Redis server that speaks the standard Redis Serialization Protocol (RESP), allowing you to interact with it using the standard `redis-cli`. 

Currently, the project has successfully completed the base setup and the authentication (ACL) steps.

## 🚀 Features

* **TCP Server:** Listens on the default Redis port `6379` and handles multiple concurrent client connections.
* **Thread-Safe Storage:** Uses Go's `sync.Map` for safe concurrent reads and writes to the in-memory database.
* **Basic Commands:**
  * `PING` - Returns `PONG` to test the connection.
  * `ECHO` - Returns the provided message.
* **Key-Value Operations:**
  * `SET key value` - Stores a string value.
  * `SET key value PX milliseconds` - Stores a string value with a time-to-live (TTL) expiry.
  * `GET key` - Retrieves a string value.
* **Authentication & ACL (Access Control List):**
  * SHA-256 password hashing.
  * Automatic `default` user session management (with `nopass` flag).
  * `AUTH username password` - Authenticates a user.
  * `ACL SETUSER username >password` - Creates a user or adds a hashed password.
  * `ACL GETUSER username` - Retrieves user flags and hashed passwords.
  * `ACL WHOAMI` - Returns the currently authenticated user.

## 🛠️ Prerequisites

* [Go](https://go.dev/dl/) (1.18 or higher recommended)
* `redis-cli` (optional, for testing the server)

## 💻 Getting Started

**1. Clone the repository**
```bash
git clone [https://github.com/yourusername/make-your-own-redis-go.git](https://github.com/yourusername/make-your-own-redis-go.git)
cd make-your-own-redis-go