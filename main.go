package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sync"

	_ "github.com/lib/pq"           // Импорт драйвера PostgreSQL
	"github.com/segmentio/kafka-go" // Библиотека для работы с Apache Kafka
)

// Константы для подключения к PostgreSQL
const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "1234"
	dbname   = "orders_db"

	kafkaBroker = "localhost:9092" // Адрес Kafka-брокера
	kafkaTopic  = "orders"         // Топик для чтения заказов
)

// Глобальные переменные
var (
	cache = make(map[string]*Order) // Кэш для хранения заказов в оперативной памяти
	mu    sync.RWMutex              // Мьютекс для синхронизации доступа к кэшу
	db    *sql.DB                   // Соединение с базой данных
)

// Структура заказа
type Order struct {
	OrderUID          string   `json:"order_uid"`          // Уникальный идентификатор заказа
	TrackNumber       string   `json:"track_number"`       // Номер отслеживания
	Entry             string   `json:"entry"`              // Тип входа
	Delivery          Delivery `json:"delivery"`           // Информация о доставке
	Payment           Payment  `json:"payment"`            // Информация об оплате
	Items             []Item   `json:"items"`              // Список товаров
	Locale            string   `json:"locale"`             // Локализация
	InternalSignature string   `json:"internal_signature"` // Внутренняя подпись
	CustomerID        string   `json:"customer_id"`        // Идентификатор клиента
	DeliveryService   string   `json:"delivery_service"`   // Служба доставки
	Shardkey          string   `json:"shardkey"`           // Ключ шардирования
	SmID              int      `json:"sm_id"`              // Идентификатор магазина
	DateCreated       string   `json:"date_created"`       // Дата создания
	OOFShard          string   `json:"oof_shard"`          // Шардирование OOF
}

// Дополнительные структуры: Delivery, Payment, Item
type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDT    int    `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Item struct {
	ChrtID      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	RID         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

// Подключение к базе данных PostgreSQL
func connectToDB() (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Создание таблицы orders, если она еще не существует
func createTables(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS orders (
		order_uid TEXT PRIMARY KEY,
		track_number TEXT,
		entry TEXT,
		delivery JSONB,
		payment JSONB,
		items JSONB,
		locale TEXT,
		internal_signature TEXT,
		customer_id TEXT,
		delivery_service TEXT,
		shardkey TEXT,
		sm_id INT,
		date_created TIMESTAMP,
		oof_shard TEXT
	);`
	_, err := db.Exec(query)
	return err
}

// Сохранение заказа в кэш
func saveOrderToCache(order *Order) {
	mu.Lock()
	defer mu.Unlock()
	cache[order.OrderUID] = order
}

// Восстановление кэша из базы данных
func restoreCacheFromDB() error {
	query := `SELECT order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard FROM orders;`
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			order        Order
			deliveryJSON []byte
			paymentJSON  []byte
			itemsJSON    []byte
		)
		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry,
			&deliveryJSON, &paymentJSON, &itemsJSON,
			&order.Locale, &order.InternalSignature, &order.CustomerID,
			&order.DeliveryService, &order.Shardkey, &order.SmID,
			&order.DateCreated, &order.OOFShard,
		)
		if err != nil {
			return err
		}

		// Декодируем JSON в структуры
		_ = json.Unmarshal(deliveryJSON, &order.Delivery)
		_ = json.Unmarshal(paymentJSON, &order.Payment)
		_ = json.Unmarshal(itemsJSON, &order.Items)

		// Сохраняем заказ в кэш
		saveOrderToCache(&order)
	}

	return nil
}

// Потребитель сообщений из Kafka
func kafkaConsumer() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaTopic,
		GroupID: "order_service",
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var order Order
		err = json.Unmarshal(m.Value, &order)
		if err != nil {
			log.Printf("Error unmarshalling order: %v", err)
			continue
		}

		// Сохраняем заказ в базу данных и кэш
		err = saveOrderToDB(db, &order)
		if err != nil {
			log.Printf("Error saving order to DB: %v", err)
			continue
		}

		saveOrderToCache(&order)
		log.Printf("Order %s processed and cached", order.OrderUID)
	}
}

// Сохранение заказа в базу данных
func saveOrderToDB(db *sql.DB, order *Order) error {
	query := `
	INSERT INTO orders (order_uid, track_number, entry, delivery, payment, items, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	ON CONFLICT (order_uid) DO NOTHING;`
	deliveryJSON, _ := json.Marshal(order.Delivery)
	paymentJSON, _ := json.Marshal(order.Payment)
	itemsJSON, _ := json.Marshal(order.Items)

	_, err := db.Exec(query, order.OrderUID, order.TrackNumber, order.Entry, deliveryJSON, paymentJSON, itemsJSON, order.Locale, order.InternalSignature, order.CustomerID, order.DeliveryService, order.Shardkey, order.SmID, order.DateCreated, order.OOFShard)
	return err
}

// Главная страница
func indexHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("templates/index.html")
	if err != nil {
		http.Error(w, "Error generating page", http.StatusInternalServerError)
		return
	}
	tmpl.Execute(w, nil)
}

// Страница с информацией о заказе
func orderHandler(w http.ResponseWriter, r *http.Request) {
	orderUID := r.URL.Query().Get("order_uid")
	if orderUID == "" {
		http.Error(w, "Order UID is required", http.StatusBadRequest)
		return
	}

	// Проверяем, есть ли заказ в кэше
	mu.RLock()
	order, exists := cache[orderUID]
	mu.RUnlock()
	if !exists {
		// Если заказ не найден, показываем страницу ошибки
		tmpl, err := template.ParseFiles("templates/order_not_found.html")
		if err != nil {
			http.Error(w, "Error loading error template", http.StatusInternalServerError)
			return
		}
		tmpl.Execute(w, "Заказ не найден")
		return
	}

	// Если заказ найден, показываем его
	tmpl, err := template.ParseFiles("templates/order.html")
	if err != nil {
		http.Error(w, "Error loading template", http.StatusInternalServerError)
		return
	}
	tmpl.Execute(w, order)
}

// Точка входа в приложение
func main() {
	var err error

	// Подключаемся к базе данных
	db, err = connectToDB()
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	// Создаем таблицы
	err = createTables(db)
	if err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	// Восстанавливаем кэш из базы данных
	err = restoreCacheFromDB()
	if err != nil {
		log.Printf("Failed to restore cache from DB: %v", err)
	}

	// Запускаем Kafka-потребитель
	go kafkaConsumer()

	// Настраиваем маршруты и запускаем HTTP-сервер
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/order", orderHandler)

	log.Println("Server running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
