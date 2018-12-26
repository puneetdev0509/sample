package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/puneetdev0509/event"
	"github.com/puneetdev0509/logger"
)

type EventData struct {
	Old *SampleLearner `json:"old"`
	New *SampleLearner `json:"data"`
}

type SampleLearner struct {
	UserId int64  `json:"user_id"`
	OrgId  string `json:"org_id"`
	State  string `json:"state"`
}

type ResponseLearner struct {
	UserId string `json:"user_id"`
	OrgId  string `json:"org_id"`
	State  string `json:"state"`
}

type batonSampleLearner string

func (s batonSampleLearner) Run(ctx context.Context, records []event.Event) (*event.EventResponse, error) {
	logger.Info("Starting Sample Processor...")

	resp := &event.EventResponse{
		LastOffset: 0,
		Records:    make([]*event.ResponseRecord, 0),
	}

	for _, e := range records {
		cdcData := e.GetEventData()
		parsed := &EventData{}
		err := json.Unmarshal([]byte(cdcData), parsed)
		if err != nil {
			logger.Info( "error parsing the event, error = %sn", err)
			return nil, err
		}

		response, err := json.Marshal(&ResponseLearner{
			UserId: "123123123",
			OrgId:  "oauth",
			State:  "active",
		})

		if err != nil {
			return nil, errors.New("hello")
		}

		resp.Records = append(resp.Records, &event.ResponseRecord{
			Id:       e.GetEventId(),
			Response: string(response),
			UserId:   fmt.Sprintf("%d", parsed.New.UserId),
			SeriesId: "",
			EntityId: "",
		})

		resp.LastOffset = e.GetEventOffset()
	}

	return resp, nil
}

var BatonSampleLearner batonSampleLearner
