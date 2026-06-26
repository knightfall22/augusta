package domain

type APIResponse struct {
	Status  string `json:"status"`
	Error   bool   `json:"error"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}
