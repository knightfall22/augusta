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

var leaseID = "augusta-lease"

type MongoStore struct {
	db    *mongo.Database
	tasks *mongo.Collection
	lease *mongo.Collection
}

func NewMongoStore(DBName, uri string) (*MongoStore, error) {
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
		lease: db.Collection("leases"),
	}, nil

}

func (m *MongoStore) AddTask(ctx context.Context, task *domain.Task) error {
	taskDto := tasks{}
	taskDto.fromDomain(task)

	_, err := m.tasks.InsertOne(ctx, taskDto)
	return err
}

func (m *MongoStore) DeleteTask(ctx context.Context, taskID string) error {
	_, err := m.tasks.DeleteOne(ctx, bson.M{"_id": taskID})
	return err
}

func (m *MongoStore) GetTask(ctx context.Context, taskID string) (*domain.Task, error) {
	var task tasks

	err := m.tasks.FindOne(ctx, bson.M{"_id": taskID}).Decode(&task)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, internal.ErrNoTaskFound
		}
		return nil, err
	}
	return task.toDomain(), nil
}

func (m *MongoStore) GetTaskByName(ctx context.Context, taskName string) (*domain.Task, error) {
	return nil, nil
}

func (m *MongoStore) AquireLease(ctx context.Context, lease *domain.Lease) error {

	//CAS update of lease
	filter := bson.M{
		"_id": leaseID,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": time.Now().UTC()}},
			{"candidate_id": lease.CandidateID},
		},
	}

	update := bson.M{"$set": bson.M{
		"_id":          leaseID,
		"candidate_id": lease.CandidateID,
		"last_renewed": time.Now().UTC(),
		"last_aquired": lease.LastAquired,
		"expires_at":   lease.ExpiresAt,
	}}

	err := m.lease.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetUpsert(true)).Err()
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoStore) GetLease(ctx context.Context) (*domain.Lease, error) {
	var lease leaseModel
	err := m.lease.FindOne(ctx, bson.M{"_id": leaseID}).Decode(&lease)
	if err != nil && err != mongo.ErrNoDocuments {
		return nil, err
	}

	if err == mongo.ErrNoDocuments {
		return nil, nil
	}

	return lease.toDomain(), nil
}

func (m *MongoStore) DeleteLease(ctx context.Context, candidateID string) error {
	_, err := m.lease.DeleteOne(ctx, bson.M{"candidate_id": candidateID})
	return err
}

func (m *MongoStore) Flush() error {
	_, err := m.tasks.DeleteMany(context.Background(), bson.M{})
	if err != nil {
		return err
	}

	_, err = m.lease.DeleteMany(context.Background(), bson.M{})
	if err != nil {
		return err
	}
	return nil
}
