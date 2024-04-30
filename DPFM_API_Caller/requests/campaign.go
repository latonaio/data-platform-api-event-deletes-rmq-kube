package requests

type Campaign struct {
	Event				int     `json:"Event"`
	Campaign			int     `json:"Campaign"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
