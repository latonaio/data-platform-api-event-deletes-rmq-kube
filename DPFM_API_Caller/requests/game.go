package requests

type Game struct {
	Event				int     `json:"Event"`
	Game				int     `json:"Game"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
