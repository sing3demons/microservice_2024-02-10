package database

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type IMongo interface {
	Collection(name string) *mongo.Collection
	Disconnect()
}

type DB struct {
	*mongo.Database
}

func New(dbName string) IMongo {
	client, err := ConnectMonoDB()
	if err != nil {
		log.Fatal(err)
	}

	return &DB{client.Database(dbName)}
}

func (db *DB) Collection(name string) *mongo.Collection {
	return db.Database.Collection(name)
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

func (db *DB) Disconnect() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := db.Client().Disconnect(ctx); err != nil {
		log.Fatal(err)
	}
}