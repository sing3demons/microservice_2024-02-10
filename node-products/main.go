package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func init() {
	godotenv.Load(".env")
}

func main() {
	db, err := ConnectMonoDB()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	collection := db.Database("products").Collection("products")

	r := gin.Default()

	r.GET("/products", func(c *gin.Context) {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		scheme := "http"
		if c.Request.TLS != nil {
			scheme = "https"
		}

		// baseURL := fmt.Sprintf("%s://%s%s", scheme, r.Host, r.URL.Path)
		baseURL := fmt.Sprintf("%s://%s%s", scheme, c.Request.Host, c.Request.URL.Path)
		opts := options.FindOptions{}
		field := c.Query("fields")
		var fields []string
		if field != "" {
			fields = strings.Split(field, ",")
		}
		projection := bson.M{"_id": 0}
		if len(fields) != 0 {
			for _, f := range fields {
				projection[f] = 1
			}
			opts.SetProjection(projection)
		}
		// opts.SetProjection(bson.M{
		// 	"_id":  0,
		// 	"id":   1,
		// 	"name": 1,
		// })
		products := []Product{}
		cursor, err := collection.Find(ctx, bson.M{
			"deleteDate": primitive.Null{},
		}, &opts)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			var product Product
			cursor.Decode(&product)
			product.Href = fmt.Sprintf("%s/%s", baseURL, product.ID)

			products = append(products, product)
		}

		durationInMs := time.Since(start).Milliseconds()
		response := map[string]any{
			"durations": fmt.Sprintf("%.2f ms", float64(durationInMs)/1000.0),
			"products":  products[:100],
			"total":     len(products),
		}

		c.JSON(http.StatusOK, response)

	})

	r.GET("/products/:id", func(c *gin.Context) {
		start := time.Now()
		id := c.Param("id")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		scheme := "http"
		if c.Request.TLS != nil {
			scheme = "https"
		}

		baseURL := fmt.Sprintf("%s://%s%s", scheme, c.Request.Host, c.Request.URL.Path)

		product := Product{}

		if err := collection.FindOne(ctx, bson.M{
			"id":         id,
			"deleteDate": primitive.Null{},
		}).Decode(&product); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		product.Href = baseURL

		durationInMs := time.Since(start).Milliseconds()
		response := map[string]any{
			"durations": fmt.Sprintf("%.2f ms", float64(durationInMs)/1000.0),
			"product":   product,
		}

		c.JSON(http.StatusOK, response)
	})

	r.Run(":8080")
}

type Products struct {
	ID   string `json:"id" bson:"id"`
	Name string `json:"name" bson:"name"`
	Href string `json:"href"`
}

type Price struct {
	ID                 string                `json:"id,omitempty"`
	Name               string                `json:"name,omitempty"`
	Tax                *Tax                  `json:"tax,omitempty"`
	PopRelationships   []*PopRelationship    `json:"popRelationship,omitempty"`
	UnitOfMeasure      *UnitOfMeasure        `json:"unitOfMeasure,omitempty"`
	SupportingLanguage []*SupportingLanguage `json:"SupportingLanguage,omitempty"`
	Status             string                `json:"status,omitempty"`
	CreatedAt          string                `json:"createdAt,omitempty"`
	UpdatedAt          string                `json:"updatedAt,omitempty"`
}

type Tax struct {
	Type  string  `json:"type,omitempty"`
	Value float64 `json:"value,omitempty"`
}

type PopRelationship struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type UnitOfMeasure struct {
	Unit     string  `json:"unit,omitempty"`
	Amount   float64 `json:"amount,omitempty"`
	Currency string  `json:"currency,omitempty"`
}

type Category struct {
	ID                 string                `json:"id,omitempty"`
	Name               string                `json:"name,omitempty"`
	Description        string                `json:"description,omitempty"`
	SupportingLanguage []*SupportingLanguage `json:"SupportingLanguage,omitempty"`
	Status             string                `json:"status,omitempty"`
	CreatedAt          string                `json:"createdAt,omitempty"`
	UpdatedAt          string                `json:"updatedAt,omitempty"`
}

type SupportingLanguage struct {
	ID            string         `json:"id,omitempty"`
	Name          string         `json:"name,omitempty"`
	Description   string         `json:"description,omitempty"`
	LanguageCode  string         `json:"languageCode,omitempty"`
	UnitOfMeasure *UnitOfMeasure `json:"unitOfMeasure,omitempty"`
	Attachment    []*Attachment  `json:"attachment,omitempty"`
	Status        string         `json:"status,omitempty"`
	CreatedAt     string         `json:"createdAt,omitempty"`
	UpdatedAt     string         `json:"updatedAt,omitempty"`
}

type Attachment struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name,omitempty"`
	URL         string   `json:"url,omitempty"`
	Type        string   `json:"type,omitempty"`
	Status      string   `json:"status,omitempty"`
	CreatedAt   string   `json:"createdAt,omitempty"`
	UpdatedAt   string   `json:"updatedAt,omitempty"`
	Display     *Display `json:"display,omitempty"`
	RedirectURL string   `json:"redirectUrl,omitempty"`
}

type Display struct {
	Type  string `json:"type,omitempty"`
	Value string `json:"value,omitempty"`
}

type Product struct {
	ID                 string                `json:"id,omitempty"`
	Name               string                `json:"name,omitempty"`
	Href               string                `json:"href"`
	Price              []*Price              `json:"price,omitempty"`
	Category           []*Category           `json:"category,omitempty"`
	Description        string                `json:"description,omitempty"`
	Stock              int                   `json:"stock,omitempty"`
	Status             string                `json:"status,omitempty"`
	CreatedAt          string                `json:"createdAt,omitempty"`
	UpdatedAt          string                `json:"updatedAt,omitempty"`
	SupportingLanguage []*SupportingLanguage `json:"SupportingLanguage,omitempty"`
}

func ConnectMonoDB() (*mongo.Client, error) {
	uri := os.Getenv("MONGO_URL")
	if uri == "" {
		return nil, fmt.Errorf("MONGO_URL is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}
	return client, nil
}
