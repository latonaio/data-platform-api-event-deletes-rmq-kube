package requests

type Header struct {
	Event				int     `json:"Event"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
