package storage

import (
	"context"
	"log"
	"time"

	"github.com/knightfall22/augusta/internal"
	"github.com/knightfall22/augusta/internal/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoStore struct {
	db    *mongo.Database
	tasks *mongo.Collection
}

func NewMongoStore(DBName, uri string) (internal.StorageEngine, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		log.Fatalf("An error has occured: %v\n", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("An error has occured: %v\n", err)
	}

	db := client.Database(DBName)

	indexTasksModel := []mongo.IndexModel{
		{
			Keys: bson.D{
				bson.E{Key: "next_run_at", Value: 1},
			},
		},
	}

	_, err = db.Collection("tasks").Indexes().CreateMany(ctx, indexTasksModel)
	if err != nil {
		log.Fatalf("An error has occured: %v\n", err)
	}

	return &MongoStore{
		db:    db,
		tasks: db.Collection("tasks"),
	}, nil

}

func (m *MongoStore) AddTask(ctx context.Context, task *domain.Task) error {
	taskDto := tasks{}
	taskDto.fromDomain(task)

	_, err := m.tasks.InsertOne(ctx, taskDto)
	return err
}

func (m *MongoStore) GetTask(ctx context.Context, taskID string) (*domain.Task, error) {
	return nil, nil
}

func (m *MongoStore) GetTaskByName(ctx context.Context, taskName string) (*domain.Task, error) {
	return nil, nil
}
