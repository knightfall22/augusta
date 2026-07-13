package storage

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
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
	db        *mongo.Database
	tasks     *mongo.Collection
	taskStats *mongo.Collection
	lease     *mongo.Collection
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

	indexTaskStatsModel := []mongo.IndexModel{
		{
			Keys: bson.D{
				bson.E{Key: "task_id", Value: 1},
			},
		},
	}

	_, err = db.Collection("task_stats").Indexes().CreateMany(ctx, indexTaskStatsModel)
	if err != nil {
		log.Fatalf("An error has occured: %v\n", err)
	}

	return &MongoStore{
		db:        db,
		tasks:     db.Collection("tasks"),
		taskStats: db.Collection("task_stats"),
		lease:     db.Collection("leases"),
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
			{"next_run_at": bson.M{"$lt": time.Now().UTC().Add(20 * time.Second)}},
			{"next_run_at": bson.M{"$gt": time.Now().UTC().Add(20 * time.Second)}},
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

	session, err := m.db.Client().StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessionContext context.Context) (any, error) {
		task, err := m.GetTask(sessionContext, result.TaskId)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, nil
			}
			return nil, err
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

		_, err = m.tasks.UpdateOne(sessionContext, bson.M{"_id": task.ID}, update)
		if err != nil {
			return nil, err
		}

		_, err = m.taskStats.InsertOne(sessionContext, &taskStats{
			ID:            uuid.New().String(),
			TaskID:        task.ID,
			Status:        domain.Status(result.Status.String()),
			WorkerID:      result.WorkerId,
			OutputMessage: result.ErrorMessage,
			LastRunAt:     time.Now().UTC(),
		})

		return nil, nil

	})

	return err
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

func (m *MongoStore) CountTaskStats(ctx context.Context, taskID string) (int64, error) {
	count, err := m.taskStats.CountDocuments(ctx, bson.M{"task_id": taskID})
	return count, err
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
	task, err := m.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	nextRunDuration, err := duration.Parse(task.Schedule)
	if err != nil {
		return err
	}

	nextRunAt := time.Now().UTC().Add(nextRunDuration.ToTimeDuration())

	_, err = m.tasks.UpdateOne(ctx, bson.M{"_id": taskID}, bson.M{"$set": bson.M{
		"disabled":    false,
		"next_run_at": nextRunAt,
	}})
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

func (m *MongoStore) GetAllTasks(ctx context.Context, status string, limit int, offset int) (*domain.PaginatedList[*domain.TaskListResponse], error) {
	filter := bson.M{}
	if status != "" {
		filter["status"] = status
	}

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$facet", Value: bson.M{
			"metadata": []bson.M{
				{"$count": "total"},
			},
			"data": []bson.M{
				{"$skip": offset},
				{"$limit": limit},
				{"$lookup": bson.M{
					"from":         "task_stats",
					"localField":   "_id",
					"foreignField": "task_id",
					"as":           "stats",
				}},
				{"$project": bson.M{
					"id":         "$_id",
					"name":       1,
					"task_type":  1,
					"disabled":   1,
					"status":     1,
					"runs_count": bson.M{"$size": "$stats"},
				}},
			},
		}}},
	}

	cursor, err := m.tasks.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	type facetResult struct {
		Metadata []struct {
			Total int64 `bson:"total"`
		} `bson:"metadata"`
		Data []*domain.TaskListResponse `bson:"data"`
	}

	var results []facetResult
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return &domain.PaginatedList[*domain.TaskListResponse]{Data: []*domain.TaskListResponse{}, TotalCount: 0, HasNextPage: false}, nil
	}

	res := results[0]
	var total int64
	if len(res.Metadata) > 0 {
		total = res.Metadata[0].Total
	}

	if res.Data == nil {
		res.Data = make([]*domain.TaskListResponse, 0)
	}

	return &domain.PaginatedList[*domain.TaskListResponse]{
		Data:        res.Data,
		TotalCount:  total,
		HasNextPage: int64(offset+limit) < total,
	}, nil
}

func (m *MongoStore) GetTaskStatsList(ctx context.Context, taskID string, status string, limit int, offset int) (*domain.PaginatedList[*domain.TaskStat], error) {
	filter := bson.M{"task_id": taskID}
	if status != "" {
		filter["status"] = status
	}

	total, err := m.taskStats.CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}

	findOptions := options.Find().SetSkip(int64(offset)).SetLimit(int64(limit)).SetSort(bson.D{{Key: "last_run_at", Value: -1}})
	cursor, err := m.taskStats.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var stats []taskStats
	if err := cursor.All(ctx, &stats); err != nil {
		return nil, err
	}

	result := make([]*domain.TaskStat, 0, len(stats))
	for _, s := range stats {
		result = append(result, s.toDomain())
	}

	return &domain.PaginatedList[*domain.TaskStat]{
		Data:        result,
		TotalCount:  total,
		HasNextPage: int64(offset+limit) < total,
	}, nil
}
