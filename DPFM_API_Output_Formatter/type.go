package dpfm_api_output_formatter

type SDC struct {
	ConnectionKey       string      `json:"connection_key"`
	Result              bool        `json:"result"`
	RedisKey            string      `json:"redis_key"`
	Filepath            string      `json:"filepath"`
	APIStatusCode       int         `json:"api_status_code"`
	RuntimeSessionID    string      `json:"runtime_session_id"`
	BusinessPartnerID   *int        `json:"business_partner"`
	ServiceLabel        string      `json:"service_label"`
	APIType             string      `json:"api_type"`
	Message             interface{} `json:"message"`
	APISchema           string      `json:"api_schema"`
	Accepter            []string    `json:"accepter"`
	Deleted             bool        `json:"deleted"`
	SQLUpdateResult     *bool       `json:"sql_update_result"`
	SQLUpdateError      string      `json:"sql_update_error"`
	SubfuncResult       *bool       `json:"subfunc_result"`
	SubfuncError        string      `json:"subfunc_error"`
	ExconfResult        *bool       `json:"exconf_result"`
	ExconfError         string      `json:"exconf_error"`
	APIProcessingResult *bool       `json:"api_processing_result"`
	APIProcessingError  string      `json:"api_processing_error"`
}

type Message struct {
	Header       		*Header         	`json:"Header"`
	Campaign       		*[]Campaign         `json:"Campaign"`
	Game				*[]Game 			`json:"Game"`
}

type Header struct {
	Event				int     `json:"Event"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}

type Campaign struct {
	Event				int     `json:"Event"`
	Campaign			int     `json:"Campaign"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}

type Game struct {
	Event				int     `json:"Event"`
	Game				int     `json:"Game"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
