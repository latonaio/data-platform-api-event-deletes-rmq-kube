package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-event-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-event-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-event-deletes-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncDeletes(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	switch input.APIType {
	case "deletes":
		response = c.deleteSqlProcess(input, output, accepter, log)
	default:
		log.Error("unknown api type %s", input.APIType)
	}
	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var headerData *dpfm_api_output_formatter.Header
	campaignData := make([]dpfm_api_output_formatter.Campaign, 0)
	gameData := make([]dpfm_api_output_formatter.Game, 0)
	for _, a := range accepter {
		switch a {
		case "Header":
			h, i, s := c.headerDelete(input, output, log)
			headerData = h
			if h == nil || i == nil || s == nil {
				continue
			}
			campaignData = append(campaignData, *i...)
			gameData = append(gameData, *s...)
		case "Campaign":
			i := c.campaignDelete(input, output, log)
			if i == nil {
				continue
			}
			campaignData = append(campaignData, *i...)
		case "Game":
			s := c.gameDelete(input, output, log)
			if s == nil {
				continue
			}
			gameData = append(gameData, *s...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		Header:		headerData,
		Campaign:	&campaignData,
		Game:		&gameData,
	}
}

func (c *DPFMAPICaller) headerDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.Header, *[]dpfm_api_output_formatter.Campaign, *[]dpfm_api_output_formatter.Game) {
	sessionID := input.RuntimeSessionID

	header := c.HeaderRead(input, log)
	if header == nil {
		return nil, nil, nil
	}
	header.IsMarkedForDeletion = input.Header.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "EventHeader", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "Header Data cannot delete"
		return nil, nil, nil
	}
	// headerの削除フラグが取り消された時は子に影響を与えない
	if !*header.IsMarkedForDeletion {
		return header, nil, nil
	}

	campaigns := c.CampaignsRead(input, log)
	for i := range *campaigns {
		(*campaigns)[i].IsMarkedForDeletion = input.Header.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*campaigns)[i], "function": "EventCampaign", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event Campaign Data cannot delete"
			return nil, nil, nil
		}
	}

	games := c.GamesRead(input, log)
	for i := range *games {
		(*games)[i].IsMarkedForDeletion = input.Header.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*games)[i], "function": "EventGame", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event Game Data cannot delete"
			return nil, nil, nil
		}
	}
	
	return header, campaigns, games
}

func (c *DPFMAPICaller) campaignDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Campaign) {
	sessionID := input.RuntimeSessionID
	campaign := input.Header.Campaign[0]

	campaigns := make([]dpfm_api_output_formatter.Campaign, 0)
	for _, v := range input.Header.Campaign {
		data := dpfm_api_output_formatter.Campaign{
			Event:					input.Header.Event,
			Campaign:				v.Campaign,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "EventCampaign", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event Campaign Data cannot delete"
			return nil
		}
	}

	// campaignが削除フラグ取り消しされた場合、headerの削除フラグも取り消す
	if !*input.Header.Campaign[0].IsMarkedForDeletion {
		header := c.HeaderRead(input, log)
		header.IsMarkedForDeletion = input.Header.Campaign[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "EventHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot delete"
			return nil
		}
	}

	return &campaigns
}

func (c *DPFMAPICaller) gameDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Game) {
	sessionID := input.RuntimeSessionID
	game := input.Header.Game[0]

	games := make([]dpfm_api_output_formatter.Game, 0)
	for _, v := range input.Header.Game {
		data := dpfm_api_output_formatter.Game{
			Event:					input.Header.Event,
			Game:					v.Game,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "EventGame", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Event Game Data cannot delete"
			return nil
		}
	}

	// gameが削除フラグ取り消しされた場合、headerの削除フラグも取り消す
	if !*input.Header.Game[0].IsMarkedForDeletion {
		header := c.HeaderRead(input, log)
		header.IsMarkedForDeletion = input.Header.Game[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "EventHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot delete"
			return nil
		}
	}

	return &games
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
