package main

import (
  "log"
  "database/sql"
  "net/http"
  "math/rand"
  "strconv"
  "encoding/json"
  "github.com/gorilla/mux"
  "github.com/segmentio/kafka-go"
  "context"
)

type Statistics struct {
  TotalMessages int `json:"total_messages"`
}

type Message struct {
  ID      string `json:"id"`
  Msg     string `json:"msg"`
  Author  string `json:"author"`
}

var (
  db  *sql.DB
  kafkaWriter *kafka.Writer
)
func initDB() {
  var err error
  connStr := "postgresql://postgres:OdwfDjrpjxZlYxpGlVvxMjAvXyibOZHe@postgres.railway.internal:5432/railway"
  db, err = sql.Open("postgres", connStr)
  if err != nil {
    log.Fatal(err)
  }

  _, err = db.Exec(`CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    msg TEXT,
    author TEXT
    )`)
    if err != nil {
    log.Fatal(err)
  }
}

func initKafka() {
  kafkaWriter = &kafka.Writer {
    Addr:   kafka.TCP("localhost:9092"),
    Topics: "message_topic",
    Balancer: &kafka.LeastBytes{},
  }
}

func getMessages(w http.ResponseWriter, r *http.Request) {
  rows, err := db.Query("SELECT id, msg, author FROM messages")
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
  defer rows.Close()

  var messages []Message
  for rows.Next() {
    var message Message
    err := rows.Scan(&message.ID, &message.Msg, &message.Author)
    if err != nil {
      http.Error(w, err.Error(), http.StatusInternalServerError)
      return
    }
    messages = append(messages, message)
  }

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(messages)
}

func getMessage(w http.ResponseWriter, r *http.Request) {
  params := mux.Vars(r)
  var message Message
  err := db.QueryRow("SELECT id, msgm author FROM message WHERE id = $1", params["id"]).Scan(&message.ID, &message.Msg, &message.Author)
  if err != nil {
    if err == sql.ErrNoRows {
      w.WriteHeader(http.StatusNotFound)
    } else {
      http.Error(w, err.Error(), http.StatusInternalServerError)
    }
    return
  }

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(&Message{})
}

func sendMessage(w http.ResponseWriter, r *http.Request) {
  var message Message
  _ = json.NewDecoder(r.Body).Decode(&message)
  message.ID = strconv.Itoa(rand.Intn(1000000))

  _, err := db.Exec("INSERT INTO messages (id, msg, author) VALUES ($1, $2, $3)", message.ID, message.Msg, message.Author)
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  err = kafkaWriter.WriteMessages(context.Background(),
    kafka.Message{
      Key: []byte(message.ID),
      Value: []byte(message.Msg),
      },
    )
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(message)
}

func updateMessage(w http.ResponseWriter, r *http.Request) {
  params := mux.Vars(r)
  var message Message
  _ = json.NewDecoder(r.Body).Decode(&message)
  message.ID = params["id"]

  _, err := db.Exec("UPDATE message SET msg = $1, author = $2 WHERE id = $3", message.Msg, message.Author, message.ID)
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(messages)
}

func deleteMessage(w http.ResponseWriter, r *http.Request) {
  params := mux.Vars(r)

  _, err := db.Exec("DELETE FROM messages WHERE id = $1", params["id"])
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(messages)
}

func getStatistcs(w http.ResponseWriter, r *http.Request) {
  var stats Statistics
  err := db.QueryRow("SELECT COUNT(*) FROM messages").Scan(&stats.TotalMessages)
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(messages)
}

func main() {
  initDB()
  defer db.Close()
  initKafka()
  defer kafkaWriter.Close()

  r := mux.NewRouter()
  messages = append(messages, Message{ID: "1", Msg: "Hello world!", Author: "Petya"})
  messages = append(messages, Message{ID: "2", Msg: "Hello hru?", Author: "Grisha"})
  r.HandleFunc("/messages", getMessages).Methods("GET")
  r.HandleFunc("/messages/{id}", getMessage).Methods("GET")
  r.HandleFunc("/messages", sendMessage).Methods("POST")
  r.HandleFunc("/messages/{id}", updateMessage).Methods("PUT")
  r.HandleFunc("/messages/{id}", deleteMessage).Methods("DELETE")
  log.Fatal(http.ListenAndServe(":8000", r))
}
