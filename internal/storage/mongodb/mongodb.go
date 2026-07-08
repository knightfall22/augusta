package storage

import (
	"context"
	"log"
	"time"

	"github.com/knightfall22/augusta/internal"
	pb "github.com/knightfall22/augusta/internal/api/v1"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/knightfall22/augusta/utils"
	"github.com/sosodev/duration"
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
		{
			Keys: bson.D{
				bson.E{Key: "visibility_timeout", Value: 1},
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
	taskDto.Status = domain.Pending

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
			return nil, utils.ErrNoTaskFound
		}
		return nil, err
	}
	return task.toDomain(), nil
}

func (m *MongoStore) GetTaskByName(ctx context.Context, taskName string) (*domain.Task, error) {
	return nil, nil
}

func (m *MongoStore) GetPendingTasks(ctx context.Context) ([]*domain.Task, error) {
	var result []*domain.Task

	filter := bson.M{
		"disabled": false,
		"status":   "pending",
		"$or": []bson.M{
			{"next_run_at": bson.M{"$lt": time.Now().UTC().Add(10 * time.Second)}},
			{"next_run_at": bson.M{"$gt": time.Now().UTC().Add(10 * time.Second)}},
		},
	}

	cursor, err := m.tasks.Find(ctx, filter, options.Find().SetLimit(100))
	if err != nil {
		return nil, err
	}

	var ids []string
	for cursor.Next(ctx) {
		var task tasks
		err := cursor.Decode(&task)
		if err != nil {
			return nil, err
		}

		result = append(result, task.toDomain())
		ids = append(ids, task.ID)
	}

	if len(ids) > 0 {
		filter = bson.M{"_id": bson.M{"$in": ids}}
		set := bson.M{
			"$set": bson.M{
				"status":             "queued",
				"visibility_timeout": time.Now().UTC().Add(internal.DefaultWorkerLeaseDuration),
			},
		}

		if _, err := m.tasks.UpdateMany(ctx, filter, set); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (m *MongoStore) GetLeaseExpiredTasks(ctx context.Context) error {
	filter := bson.M{
		"disabled": false,
		"$or": []bson.M{
			{"status": "running"},
			{"status": "queued"},
		},
		"visibility_timeout": bson.M{"$lt": time.Now().UTC()},
	}

	cursor, err := m.tasks.Find(ctx, filter, options.Find().SetLimit(100))
	if err != nil {
		return err
	}

	for cursor.Next(ctx) {
		var task tasks
		err := cursor.Decode(&task)
		if err != nil {
			return err
		}

		if task.CurrentRetries >= task.Retries {
			if _, err := m.tasks.UpdateByID(ctx, task.ID, bson.M{
				"$set": bson.M{
					"disabled":           true,
					"last_error":         time.Now().UTC(),
					"visibility_timeout": time.Time{},
					"status":             "failed",
				},
			}); err != nil {
				return err
			}
			return nil
		}

		next_run_at := internal.CalculateExponientialBackoff(task.Epsilon, task.CurrentRetries)

		if _, err := m.tasks.UpdateByID(ctx, task.ID, bson.M{
			"$set": bson.M{
				"status":             "pending",
				"visibility_timeout": time.Time{},
				"current_retries":    task.CurrentRetries + 1,
				"last_error":         time.Now().UTC(),
				"last_run_at":        time.Now().UTC(),
				"next_run_at":        next_run_at,
			},
		}); err != nil {
			return err
		}

	}

	return nil
}

func (m *MongoStore) ExtendTaskLease(ctx context.Context, taskID []string) error {
	filter := bson.M{"_id": bson.M{"$in": taskID}}
	update := bson.M{"$set": bson.M{
		"visibility_timeout": time.Now().UTC().Add(internal.DefaultWorkerLeaseDuration),
		"status":             "running",
	}}

	_, err := m.tasks.UpdateMany(ctx, filter, update)
	return err
}

func (m *MongoStore) ProcessTaskResult(ctx context.Context, result *pb.TaskResult) error {
	task, err := m.GetTask(ctx, result.TaskId)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return err
	}

	set := bson.D{}

	switch result.Status {
	case pb.Status_OK:
		//calculate next run time if reoccurrence is greater that zero
		if task.Reoccurrence == -1 {
			//run indefinitely
			nextRunDuration, _ := duration.Parse(task.Schedule)
			nextRun := time.Now().UTC().Add(nextRunDuration.ToTimeDuration())

			set = append(set,
				bson.E{Key: "status", Value: "pending"},
				bson.E{Key: "next_run_at", Value: nextRun},
				bson.E{Key: "visibility_timeout", Value: time.Time{}},
				bson.E{Key: "last_run_at", Value: time.Now().UTC()},
				bson.E{Key: "last_success", Value: time.Now().UTC()},
			)
		} else if task.Reoccurrence == 0 {
			set = append(set,
				bson.E{Key: "status", Value: "finished"},
				bson.E{Key: "next_run_at", Value: time.Time{}},
				bson.E{Key: "visibility_timeout", Value: time.Time{}},
				bson.E{Key: "last_run_at", Value: time.Now().UTC()},
				bson.E{Key: "last_success", Value: time.Now().UTC()},
			)
		} else {
			nextRunDuration, _ := duration.Parse(task.Schedule)
			nextRun := time.Now().UTC().Add(nextRunDuration.ToTimeDuration())

			set = append(set,
				bson.E{Key: "status", Value: "pending"},
				bson.E{Key: "next_run_at", Value: nextRun},
				bson.E{Key: "visibility_timeout", Value: time.Time{}},
				bson.E{Key: "last_run_at", Value: time.Now().UTC()},
				bson.E{Key: "last_success", Value: time.Now().UTC()},
				bson.E{Key: "reoccurrence", Value: task.Reoccurrence - 1},
			)

		}
	case pb.Status_ERROR:
		if task.CurrentRetries != task.Retries {
			next_run_at := internal.CalculateExponientialBackoff(task.Epsilon, task.CurrentRetries)

			set = append(set,
				bson.E{Key: "status", Value: "pending"},
				bson.E{Key: "next_run_at", Value: next_run_at},
				bson.E{Key: "visibility_timeout", Value: time.Time{}},
				bson.E{Key: "last_error", Value: time.Now().UTC()},
				bson.E{Key: "current_retries", Value: task.CurrentRetries + 1},
			)
		} else {

			set = append(set,
				bson.E{Key: "status", Value: "finished"},
				bson.E{Key: "next_run_at", Value: time.Time{}},
				bson.E{Key: "visibility_timeout", Value: time.Time{}},
				bson.E{Key: "last_error", Value: time.Now().UTC()},
			)

		}
	}

	update := bson.M{"$set": set}

	_, err = m.tasks.UpdateOne(ctx, bson.M{"_id": task.ID}, update)
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoStore) ProcessBatchTaskResult(ctx context.Context, results []*pb.TaskResult) error {
	for _, result := range results {
		err := m.ProcessTaskResult(ctx, result)
		if err != nil {
			return err
		}
	}

	return nil
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

func (m *MongoStore) DisableTask(ctx context.Context, taskID string) error {
	_, err := m.tasks.UpdateOne(ctx, bson.M{"_id": taskID}, bson.M{"$set": bson.M{"disabled": true}})
	return err
}

func (m *MongoStore) EnableTask(ctx context.Context, taskID string) error {
	_, err := m.tasks.UpdateOne(ctx, bson.M{"_id": taskID}, bson.M{"$set": bson.M{"disabled": false}})
	return err
}

func (m *MongoStore) CheckConnection(ctx context.Context) error {
	return m.db.Client().Ping(ctx, nil)
}

func (m *MongoStore) Flush(ctx context.Context) error {
	_, err := m.tasks.DeleteMany(ctx, bson.M{})
	if err != nil {
		return err
	}

	_, err = m.lease.DeleteMany(ctx, bson.M{})
	if err != nil {
		return err
	}

	if err = m.db.Client().Disconnect(ctx); err != nil {
		return err
	}
	return nil
}
