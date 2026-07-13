package domain

type PaginatedList[T any] struct {
	Data        []T   `json:"data"`
	HasNextPage bool  `json:"has_next_page"`
	TotalCount  int64 `json:"total_count"`
}
